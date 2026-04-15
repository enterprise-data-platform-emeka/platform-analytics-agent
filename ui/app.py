"""Analytics Agent Streamlit UI.

Calls the FastAPI backend at localhost:8080/ask. Both processes run in the same
ECS Fargate container, started by entrypoint.sh after FastAPI is healthy.

Stakeholders open the ALB DNS address on port 8501 in a browser. All AWS API
calls happen server-side in the container — the browser never touches AWS.
"""

import html as html_lib

import requests
import streamlit as st
import streamlit.components.v1 as components

BACKEND_URL = "http://localhost:8080"

EXAMPLE_QUESTIONS = [
    "Which country has the highest total revenue?",
    "What are the top 10 best-selling products by revenue?",
    "Which carrier has the fastest average delivery time?",
    "Show me monthly revenue trends for the last year.",
]

st.set_page_config(
    page_title="EDP Analytics Agent",
    page_icon="📊",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* Constrain content width for readability */
.block-container { max-width: 960px; padding-top: 1.5rem; }

/* Insight callout card */
.insight-card {
    background: #f0f7ff;
    border-left: 4px solid #2563EB;
    padding: 14px 18px;
    border-radius: 0 6px 6px 0;
    font-size: 15px;
    line-height: 1.7;
    margin: 4px 0 14px 0;
    color: #1e293b;
}

/* Slightly larger chat text */
.stChatMessage p { font-size: 14.5px; line-height: 1.6; }

/* Tighten expander padding */
.streamlit-expanderContent { padding: 8px 12px !important; }

/* Metric label smaller */
[data-testid="stMetricLabel"] { font-size: 12px !important; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "history" not in st.session_state:
    st.session_state.history = []
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None


# ── Helpers ───────────────────────────────────────────────────────────────────


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024**2:.2f} MB"


def _format_cost(usd: float) -> str:
    if usd == 0:
        return "—"
    if usd < 0.001:
        return f"${usd:.6f}"
    return f"${usd:.4f}"


def _latin1_safe(text: str) -> str:
    """Sanitise text for fpdf2 core fonts (Helvetica = Latin-1 only).

    Replaces the most common non-Latin-1 characters so PDF generation
    doesn't raise FPDFUnicodeEncodingException. Any remaining unmappable
    chars are replaced with '?' rather than crashing.
    """
    return (
        text.replace("€", "EUR")
        .replace("£", "GBP")
        .replace("•", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .encode("latin-1", errors="replace")
        .decode("latin-1")
    )


def _build_pdf(turn: dict) -> bytes:
    """Generate a PDF report from a turn dict (question, insight, assumptions, chart PNG).

    Returns raw PDF bytes. Uses fpdf2 — same library as the email endpoint.
    """
    from fpdf import FPDF

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 12, "EDP Analytics Report", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Question
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Question", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.multi_cell(0, 7, _latin1_safe(turn["question"]))
    pdf.ln(6)

    # Chart image
    if turn.get("png_b64"):
        import base64
        import io

        png_bytes = base64.b64decode(turn["png_b64"])
        pdf.image(io.BytesIO(png_bytes), x=10, w=190)
        pdf.ln(6)

    # Summary
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.multi_cell(0, 7, _latin1_safe(turn["insight"]))

    # Assumptions
    if turn.get("assumptions"):
        pdf.ln(6)
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, "Assumptions", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        for item in turn["assumptions"]:
            pdf.multi_cell(0, 6, f"- {_latin1_safe(item)}")

    return bytes(pdf.output())


def _render_turn(turn: dict, form_key: str) -> None:
    """Render one Q&A turn. Works for both history replay and live response."""
    is_analytical = bool(turn.get("sql"))

    # Insight card
    escaped = html_lib.escape(turn["insight"]).replace("\n", "<br>")
    st.markdown(f'<div class="insight-card">{escaped}</div>', unsafe_allow_html=True)

    # Validation flags — shown inline, not buried
    for flag in turn.get("validation_flags", []):
        st.warning(f"Data quality notice: {flag}")

    # Chart
    if turn.get("html_chart"):
        components.html(turn["html_chart"], height=460, scrolling=False)

    # Download PDF — available for any turn that has a chart or insight
    try:
        pdf_bytes = _build_pdf(turn)
        st.download_button(
            label="Download report (PDF)",
            data=pdf_bytes,
            file_name="edp_report.pdf",
            mime="application/pdf",
            key=f"pdf_{form_key}",
        )
    except Exception:  # noqa: BLE001
        pass  # silently skip if PDF generation fails

    # Assumptions
    if turn.get("assumptions"):
        with st.expander("Assumptions"):
            for item in turn["assumptions"]:
                st.caption(f"• {item}")

    # Email + Query details — only for analytical turns that hit Athena
    if is_analytical:
        with st.expander("Send as email"):
            with st.form(key=f"email_{form_key}"):
                to_email = st.text_input("Recipient email address")
                send = st.form_submit_button("Send PDF report")
            if send:
                if not to_email:
                    st.warning("Enter a recipient email address.")
                else:
                    with st.spinner("Generating PDF and sending..."):
                        try:
                            r = requests.post(
                                f"{BACKEND_URL}/send-report",
                                json={
                                    "to_email": to_email,
                                    "question": turn["question"],
                                    "insight": turn["insight"],
                                    "png_b64": turn.get("png_b64"),
                                },
                                timeout=30,
                            )
                            r.raise_for_status()
                            st.success(f"Report sent to {to_email}")
                        except requests.exceptions.HTTPError as exc:
                            detail = (
                                exc.response.json().get("detail", str(exc))
                                if exc.response
                                else str(exc)
                            )
                            st.error(f"Failed: {detail}")
                        except Exception as exc:  # noqa: BLE001
                            st.error(f"Failed: {exc}")

        with st.expander("Query details"):
            st.code(turn["sql"], language="sql")
            c1, c2, c3 = st.columns(3)
            c1.metric("Athena cost", _format_cost(turn["cost_usd"]))
            c2.metric("Data scanned", _format_bytes(turn["bytes_scanned"]))
            c3.metric("Chart type", turn.get("chart_type", "—").title())


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Session")
    if st.session_state.session_id:
        st.caption(f"ID: `{st.session_state.session_id[:8]}...`")
        n = len(st.session_state.history)
        st.caption(f"{n} question{'s' if n != 1 else ''} this session")
    else:
        st.caption("No active session.")

    if st.button("Start new session", use_container_width=True):
        st.session_state.session_id = None
        st.session_state.history = []
        st.rerun()

    if st.session_state.history:
        st.divider()
        st.caption("**History**")
        for i, t in enumerate(st.session_state.history, 1):
            q = t["question"]
            label = q if len(q) <= 42 else q[:39] + "..."
            st.caption(f"{i}. {label}")

# ── Page header ───────────────────────────────────────────────────────────────
st.title("📊 EDP Analytics Agent")
st.caption(
    "Ask questions about your Gold data in any language. "
    "Follow-up questions remember prior context."
)

# ── Empty state — example questions ──────────────────────────────────────────
if not st.session_state.history:
    st.markdown("#### Try asking:")
    col_a, col_b = st.columns(2)
    for i, eq in enumerate(EXAMPLE_QUESTIONS):
        target_col = col_a if i % 2 == 0 else col_b
        if target_col.button(eq, key=f"ex_{i}", use_container_width=True):
            st.session_state.pending_question = eq
            st.rerun()

# ── Conversation history ──────────────────────────────────────────────────────
for idx, turn in enumerate(st.session_state.history):
    with st.chat_message("user"):
        st.write(turn["question"])
    with st.chat_message("assistant"):
        _render_turn(turn, form_key=str(idx))

# ── Question input ────────────────────────────────────────────────────────────
chat_question = st.chat_input("Ask a question about your data...")
question = chat_question or st.session_state.pending_question
if st.session_state.pending_question:
    st.session_state.pending_question = None

if question:
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying your data..."):
            payload: dict = {"question": question}
            if st.session_state.session_id:
                payload["session_id"] = st.session_state.session_id

            try:
                resp = requests.post(
                    f"{BACKEND_URL}/ask",
                    json=payload,
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.Timeout:
                st.error("Request timed out. The query may be complex — try again.")
                st.stop()
            except requests.exceptions.HTTPError as exc:
                detail = exc.response.json().get("detail", str(exc)) if exc.response else str(exc)
                st.error(f"Agent error: {detail}")
                st.stop()
            except requests.exceptions.RequestException as exc:
                st.error(f"Could not reach backend: {exc}")
                st.stop()

        st.session_state.session_id = data["session_id"]

        turn = {
            "question": question,
            "insight": data["insight"],
            "assumptions": data.get("assumptions", []),
            "html_chart": data.get("html_chart"),
            "sql": data.get("sql", ""),
            "png_b64": data.get("png_b64"),
            "cost_usd": data["cost_usd"],
            "bytes_scanned": data["bytes_scanned"],
            "validation_flags": data.get("validation_flags", []),
            "chart_type": data.get("chart_type", ""),
        }
        _render_turn(turn, form_key="current")
        st.session_state.history.append(turn)
