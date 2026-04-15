"""Analytics Agent Streamlit UI.

Calls the FastAPI backend at localhost:8080. Both processes run in the same
ECS Fargate container, started by entrypoint.sh after FastAPI is healthy.

Stakeholders open the ALB DNS address on port 8501 in a browser. All AWS API
calls happen server-side in the container — the browser never touches AWS.
"""

import html as html_lib
import json
import re

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

BACKEND_URL = "http://localhost:8080"

# Fallback example questions used when /examples endpoint is unreachable.
_FALLBACK_EXAMPLES = [
    "Which country has the highest total revenue?",
    "What are the top 10 best-selling products by revenue?",
    "Which carrier has the fastest average delivery time?",
    "Show me monthly revenue trends for the last year.",
]

# Rephrasing tips shown when SQL generation fails.
_REPHRASE_TIPS = (
    "**Tips for rephrasing:**\n"
    "- Be specific about time periods: *'in 2024'*, *'last 6 months'*\n"
    "- Ask about one metric at a time: revenue, orders, delivery time\n"
    "- Use table-level language: *'by country'*, *'per carrier'*, *'by product'*\n"
    "- Avoid open-ended questions — the Gold tables are pre-aggregated summaries"
)

st.set_page_config(
    page_title="EDP Analytics Agent",
    page_icon="📊",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
.block-container { max-width: 960px; padding-top: 1.5rem; }

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

.stChatMessage p { font-size: 14.5px; line-height: 1.6; }
.streamlit-expanderContent { padding: 8px 12px !important; }
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


def _clean_insight_stream(text: str) -> str:
    """Strip XML tags from partial streaming text for live display."""
    # Remove complete <chart_title> blocks
    text = re.sub(r"<chart_title>.*?</chart_title>", "", text, flags=re.DOTALL)
    # Remove incomplete opening/closing tag at the trailing edge
    text = re.sub(r"<[^>]*$", "", text)
    # Remove any remaining complete tags
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def _build_pdf(turn: dict) -> bytes:
    """Generate a PDF report from a turn dict. Returns raw PDF bytes."""
    from fpdf import FPDF

    pdf = FPDF()
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    W = pdf.epw  # effective page width, accounts for both margins

    try:
        pdf.add_font("DejaVu", fname="DejaVuSans.ttf")
        pdf.add_font("DejaVu", style="B", fname="DejaVuSans-Bold.ttf")
        font_name = "DejaVu"
    except Exception:
        font_name = "Helvetica"

    # Title
    pdf.set_font(font_name, "B", 18)
    pdf.cell(W, 12, "EDP Analytics Report", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Question
    pdf.set_font(font_name, "B", 12)
    pdf.cell(W, 8, "Question", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font(font_name, "", 11)
    pdf.multi_cell(W, 7, turn["question"])
    pdf.ln(6)

    # Chart image
    if turn.get("png_b64"):
        import base64
        import io

        png_bytes = base64.b64decode(turn["png_b64"])
        pdf.image(io.BytesIO(png_bytes), x=15, w=W)
        pdf.ln(6)

    # Summary
    pdf.set_font(font_name, "B", 12)
    pdf.cell(W, 8, "Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font(font_name, "", 11)
    pdf.multi_cell(W, 7, turn["insight"])

    # Assumptions
    if turn.get("assumptions"):
        pdf.ln(6)
        pdf.set_font(font_name, "B", 12)
        pdf.cell(W, 8, "Assumptions", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(font_name, "", 10)
        for item in turn["assumptions"]:
            pdf.multi_cell(W, 6, f"- {item}")

    return bytes(pdf.output())


def _render_turn(turn: dict, form_key: str) -> None:
    """Render one Q&A turn. Works for both history replay and live response."""
    is_analytical = bool(turn.get("sql"))

    # Insight card
    escaped = html_lib.escape(turn["insight"]).replace("\n", "<br>")
    st.markdown(f'<div class="insight-card">{escaped}</div>', unsafe_allow_html=True)

    # Validation flags
    for flag in turn.get("validation_flags", []):
        st.warning(f"Data quality notice: {flag}")

    # Chart with chart/table toggle
    if turn.get("html_chart"):
        chart_h = turn.get("chart_height", 400)
        has_raw = bool(turn.get("columns") and turn.get("rows"))

        if has_raw:
            view_key = f"view_{form_key}"
            view = st.radio("View as", ["Chart", "Table"], horizontal=True, key=view_key)
            if view == "Table":
                df = pd.DataFrame(turn["rows"])
                st.dataframe(df, use_container_width=True)
            else:
                components.html(turn["html_chart"], height=chart_h, scrolling=False)
        else:
            components.html(turn["html_chart"], height=chart_h, scrolling=False)

    # Download PDF
    try:
        pdf_bytes = _build_pdf(turn)
        st.download_button(
            label="Download report (PDF)",
            data=pdf_bytes,
            file_name="edp_report.pdf",
            mime="application/pdf",
            key=f"pdf_{form_key}",
        )
    except ImportError:
        st.caption("PDF unavailable — run `pip install fpdf2` to enable downloads.")
    except Exception as exc:  # noqa: BLE001
        st.caption(f"PDF generation failed: {exc}")

    # Assumptions
    if turn.get("assumptions"):
        with st.expander("Assumptions"):
            for item in turn["assumptions"]:
                st.caption(f"• {item}")

    # Email + Query details — only for analytical turns
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

            # Lazy query intent check — only revealed on demand so the extra
            # Claude call doesn't block page render.
            if turn.get("inferred_question"):
                st.divider()
                intent_shown_key = f"intent_shown_{form_key}"
                if not st.session_state.get(intent_shown_key):
                    if st.button("Show query intent check", key=f"intent_btn_{form_key}"):
                        st.session_state[intent_shown_key] = True
                        st.rerun()
                if st.session_state.get(intent_shown_key):
                    st.caption("**Query intent check**")
                    st.caption(
                        "Claude was shown only the SQL (not your question) and asked "
                        "what it thinks the query is trying to answer:"
                    )
                    inferred = turn["inferred_question"]
                    escaped_inferred = html_lib.escape(inferred)
                    st.markdown(
                        f'<div style="background:#f8fafc;border-left:3px solid #94a3b8;'
                        f"padding:10px 14px;border-radius:0 4px 4px 0;font-size:14px;"
                        f'color:#334155;margin:4px 0;">'
                        f"<strong>Inferred:</strong> {escaped_inferred}</div>",
                        unsafe_allow_html=True,
                    )


@st.cache_data(ttl=300, show_spinner=False)
def _load_examples() -> list[str]:
    """Fetch example questions from the backend. Falls back to hardcoded list."""
    try:
        r = requests.get(f"{BACKEND_URL}/examples", timeout=5)
        r.raise_for_status()
        questions = r.json().get("questions", [])
        if questions:
            return questions
    except Exception:  # noqa: BLE001
        pass
    return _FALLBACK_EXAMPLES


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
        # Export conversation as JSON
        export_data = [
            {
                "question": t["question"],
                "insight": t["insight"],
                "sql": t.get("sql", ""),
                "assumptions": t.get("assumptions", []),
            }
            for t in st.session_state.history
        ]
        st.download_button(
            "Export conversation (JSON)",
            data=json.dumps(export_data, indent=2, ensure_ascii=False),
            file_name="edp_conversation.json",
            mime="application/json",
            use_container_width=True,
        )

        st.divider()
        st.caption("**History**")
        for i, t in enumerate(st.session_state.history, 1):
            q = t["question"]
            label = q if len(q) <= 42 else q[:39] + "..."
            st.caption(f"{i}. {label}")

    # Import conversation
    st.divider()
    uploaded = st.file_uploader("Import conversation (JSON)", type="json", key="import_file")
    if uploaded is not None:
        try:
            imported: list[dict] = json.loads(uploaded.read())
            st.session_state.history = [
                {
                    "question": t["question"],
                    "insight": t["insight"],
                    "sql": t.get("sql", ""),
                    "assumptions": t.get("assumptions", []),
                    "html_chart": None,
                    "png_b64": None,
                    "cost_usd": 0.0,
                    "bytes_scanned": 0,
                    "validation_flags": [],
                    "chart_type": "",
                    "inferred_question": "",
                    "columns": [],
                    "rows": [],
                    "chart_height": 0,
                }
                for t in imported
            ]
            st.session_state.session_id = None
            st.success(f"Restored {len(imported)} turns.")
            st.rerun()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Import failed: {exc}")

# ── Page header ───────────────────────────────────────────────────────────────
st.title("📊 EDP Analytics Agent")
st.caption(
    "Ask questions about your Gold data in any language. "
    "Follow-up questions remember prior context."
)

# ── Empty state — example questions ──────────────────────────────────────────
if not st.session_state.history:
    st.markdown("#### Try asking:")
    example_questions = _load_examples()
    col_a, col_b = st.columns(2)
    for i, eq in enumerate(example_questions):
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
        _status = st.empty()
        _insight_stream = st.empty()

        payload: dict = {"question": question}
        if st.session_state.session_id:
            payload["session_id"] = st.session_state.session_id

        turn_data: dict | None = None
        streamed_insight = ""

        try:
            with requests.post(
                f"{BACKEND_URL}/ask/stream",
                json=payload,
                stream=True,
                timeout=120,
            ) as resp:
                resp.raise_for_status()
                for raw_line in resp.iter_lines():
                    if not raw_line:
                        continue
                    try:
                        event = json.loads(raw_line)
                    except Exception:  # noqa: BLE001
                        continue

                    etype = event.get("type")
                    if etype == "status":
                        _status.caption(f"_{event['text']}_")
                    elif etype == "token":
                        streamed_insight += event["text"]
                        cleaned = _clean_insight_stream(streamed_insight)
                        if cleaned:
                            _insight_stream.markdown(
                                f'<div class="insight-card">'
                                f"{html_lib.escape(cleaned).replace(chr(10), '<br>')}"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                    elif etype == "error":
                        _status.empty()
                        _insight_stream.empty()
                        st.error(f"Could not answer: {event['text']}")
                        with st.expander("Tips for rephrasing your question"):
                            st.markdown(_REPHRASE_TIPS)
                        st.stop()
                    elif etype == "done":
                        turn_data = event["data"]
                        _status.empty()
                        _insight_stream.empty()

        except requests.exceptions.Timeout:
            _status.empty()
            _insight_stream.empty()
            st.error("Request timed out. The query may be complex — try again.")
            st.stop()
        except requests.exceptions.HTTPError as exc:
            _status.empty()
            _insight_stream.empty()
            detail = exc.response.json().get("detail", str(exc)) if exc.response else str(exc)
            st.error(f"Agent error: {detail}")
            st.stop()
        except requests.exceptions.RequestException as exc:
            _status.empty()
            _insight_stream.empty()
            st.error(f"Could not reach backend: {exc}")
            st.stop()

        if turn_data:
            st.session_state.session_id = turn_data["session_id"]

            turn = {
                "question": question,
                "insight": turn_data["insight"],
                "assumptions": turn_data.get("assumptions", []),
                "html_chart": turn_data.get("html_chart"),
                "sql": turn_data.get("sql", ""),
                "png_b64": turn_data.get("png_b64"),
                "cost_usd": turn_data.get("cost_usd", 0.0),
                "bytes_scanned": turn_data.get("bytes_scanned", 0),
                "validation_flags": turn_data.get("validation_flags", []),
                "chart_type": turn_data.get("chart_type", ""),
                "inferred_question": turn_data.get("inferred_question", ""),
                "columns": turn_data.get("columns", []),
                "rows": turn_data.get("rows", []),
                "chart_height": turn_data.get("chart_height", 400),
            }
            _render_turn(turn, form_key="current")
            st.session_state.history.append(turn)
