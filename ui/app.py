"""Analytics Agent Streamlit UI.

Calls the FastAPI backend at localhost:8080. Both processes run in the same
ECS Fargate container, started by entrypoint.sh after FastAPI is healthy.

Stakeholders open the ALB DNS address on port 8501 in a browser. All AWS API
calls happen server-side in the container — the browser never touches AWS.
"""

import base64 as _b64
import html as html_lib
import json
import re
from datetime import datetime

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

# ── Page config ───────────────────────────────────────────────────────────────
# #7: layout="centered" — no CSS max-width hack needed.
st.set_page_config(
    page_title="EDP Analytics Agent",
    page_icon="📊",
    layout="centered",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
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

/* #11: Visible status badge during query processing. */
.status-badge {
    background: #eff6ff;
    border: 1px solid #bfdbfe;
    color: #1d4ed8;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 13px;
    margin: 6px 0;
}

/* #1: Card header metadata row. */
.turn-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 6px;
}
.turn-label {
    font-size: 11px;
    font-weight: 700;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.turn-time {
    font-size: 11px;
    color: #94a3b8;
}
.turn-question {
    font-size: 16px;
    font-weight: 600;
    color: #0f172a;
    margin: 4px 0 0 0;
}

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
if "confirm_clear" not in st.session_state:
    st.session_state.confirm_clear = False
# #12: Track when this session started for display in sidebar.
if "session_start" not in st.session_state:
    st.session_state.session_start = datetime.now().strftime("%H:%M")


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
    text = re.sub(r"<chart_title>.*?</chart_title>", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]*$", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


# #3: PDF generation is cached. Individual hashable fields are the cache key so
# Streamlit avoids rebuilding PDFs on every rerun for historical turns.
@st.cache_data(show_spinner=False)
def _cached_build_pdf(
    question: str,
    insight: str,
    sql: str,
    assumptions_json: str,
    png_b64: str,
    cost_usd: float,
    bytes_scanned: int,
    chart_type: str,
    inferred_question: str,
) -> bytes:
    """Build a complete PDF report. Called via _build_pdf(turn)."""
    import io

    from fpdf import FPDF

    assumptions: list[str] = json.loads(assumptions_json) if assumptions_json else []

    pdf = FPDF()
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    W = pdf.epw

    _DEJAVU_DIR = "/usr/share/fonts/truetype/dejavu"
    try:
        pdf.add_font("DejaVu", fname=f"{_DEJAVU_DIR}/DejaVuSans.ttf")
        pdf.add_font("DejaVu", style="B", fname=f"{_DEJAVU_DIR}/DejaVuSans-Bold.ttf")
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
    pdf.multi_cell(W, 7, question)
    pdf.ln(6)

    # Chart image
    if png_b64:
        png_bytes = _b64.b64decode(png_b64)
        pdf.image(io.BytesIO(png_bytes), x=15, w=W)
        pdf.ln(6)

    # Summary
    pdf.set_font(font_name, "B", 12)
    pdf.cell(W, 8, "Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font(font_name, "", 11)
    pdf.multi_cell(W, 7, insight)

    # Assumptions
    if assumptions:
        pdf.ln(6)
        pdf.set_font(font_name, "B", 12)
        pdf.cell(W, 8, "Assumptions", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(font_name, "", 10)
        for item in assumptions:
            pdf.multi_cell(W, 6, f"- {item}")

    # SQL Query
    if sql:
        pdf.ln(6)
        pdf.set_font(font_name, "B", 12)
        pdf.cell(W, 8, "SQL Query", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Courier", "", 9)
        pdf.multi_cell(W, 5, sql)

    # Query metadata
    if sql:
        pdf.ln(6)
        pdf.set_font(font_name, "B", 12)
        pdf.cell(W, 8, "Query Metadata", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(font_name, "", 10)
        pdf.cell(W, 6, f"Athena cost: {_format_cost(cost_usd)}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(W, 6, f"Data scanned: {_format_bytes(bytes_scanned)}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(W, 6, f"Chart type: {(chart_type or 'none').title()}", new_x="LMARGIN", new_y="NEXT")

    # Query intent check
    if inferred_question:
        pdf.ln(6)
        pdf.set_font(font_name, "B", 12)
        pdf.cell(W, 8, "Query Intent Check", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font(font_name, "", 10)
        pdf.multi_cell(W, 6, inferred_question)

    return bytes(pdf.output())


def _build_pdf(turn: dict) -> bytes:
    """Wrapper that extracts hashable fields from a turn dict and calls the cached builder."""
    return _cached_build_pdf(
        question=turn["question"],
        insight=turn["insight"],
        sql=turn.get("sql", ""),
        assumptions_json=json.dumps(turn.get("assumptions", [])),
        png_b64=turn.get("png_b64") or "",
        cost_usd=turn.get("cost_usd", 0.0),
        bytes_scanned=turn.get("bytes_scanned", 0),
        chart_type=turn.get("chart_type", ""),
        inferred_question=turn.get("inferred_question", ""),
    )


def _render_turn(turn: dict, form_key: str) -> None:
    """Render one Q&A turn's answer content. Called inside a turn card."""
    is_analytical = bool(turn.get("sql"))

    # Insight card
    escaped = html_lib.escape(turn["insight"]).replace("\n", "<br>")
    st.markdown(f'<div class="insight-card">{escaped}</div>', unsafe_allow_html=True)

    # Validation flags
    for flag in turn.get("validation_flags", []):
        st.warning(f"Data quality notice: {flag}")

    # #8: st.tabs replaces st.radio for chart/table toggle.
    if turn.get("html_chart"):
        chart_h = turn.get("chart_height", 400)
        has_raw = bool(turn.get("columns") and turn.get("rows"))
        if has_raw:
            tab_chart, tab_table = st.tabs(["Chart", "Table"])
            with tab_chart:
                # #5: +20px buffer so Plotly content never clips silently.
                components.html(turn["html_chart"], height=chart_h + 20, scrolling=False)
            with tab_table:
                df = pd.DataFrame(turn["rows"])
                st.dataframe(df, use_container_width=True)
        else:
            components.html(turn["html_chart"], height=chart_h + 20, scrolling=False)

    # #2: One row of action buttons. Download is a direct button; email toggles
    # an inline form via session state so the user doesn't need to hunt for it.
    # #6: PDF filename includes the turn number so browser downloads don't overwrite.
    pdf_num = int(form_key) + 1 if form_key.isdigit() else len(st.session_state.history) + 1
    try:
        pdf_bytes = _build_pdf(turn)  # #3: served from cache on reruns
        col_dl, col_email, _ = st.columns([1, 1, 2])
        with col_dl:
            st.download_button(
                "Download PDF",
                data=pdf_bytes,
                file_name=f"edp_report_q{pdf_num}.pdf",
                mime="application/pdf",
                key=f"pdf_{form_key}",
            )
        with col_email:
            email_open_key = f"email_open_{form_key}"
            label = "Close email" if st.session_state.get(email_open_key) else "Send as email"
            if st.button(label, key=f"email_toggle_{form_key}"):
                st.session_state[email_open_key] = not st.session_state.get(email_open_key, False)
                st.rerun()
    except ImportError:
        st.caption("PDF unavailable — run `pip install fpdf2` to enable downloads.")
    except Exception as exc:  # noqa: BLE001
        st.caption(f"PDF generation failed: {exc}")

    # Email inline form (shown when toggle is active)
    if st.session_state.get(f"email_open_{form_key}"):
        with st.form(key=f"email_{form_key}"):
            to_email = st.text_input("Recipient email address")
            send = st.form_submit_button("Send PDF report")
        if send:
            if not to_email:
                st.warning("Enter a recipient email address.")
            else:
                with st.spinner("Sending..."):
                    try:
                        pdf_bytes = _build_pdf(turn)
                        r = requests.post(
                            f"{BACKEND_URL}/send-report",
                            json={
                                "to_email": to_email,
                                "question": turn["question"],
                                "pdf_b64": _b64.b64encode(pdf_bytes).decode(),
                            },
                            timeout=30,
                        )
                        r.raise_for_status()
                        st.success(f"Report sent to {to_email}")
                        st.session_state[f"email_open_{form_key}"] = False
                    except requests.exceptions.HTTPError as exc:
                        detail = (
                            exc.response.json().get("detail", str(exc))
                            if exc.response
                            else str(exc)
                        )
                        st.error(f"Failed: {detail}")
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Failed: {exc}")

    # #2 + #4: Single "Details" expander — SQL, cost metrics, assumptions,
    # and query intent all in one place. Intent is rendered directly (no
    # button/rerun) since inferred_question is already in the turn dict.
    if is_analytical:
        with st.expander("Details"):
            st.code(turn["sql"], language="sql")
            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric("Athena cost", _format_cost(turn["cost_usd"]))
            c2.metric("Data scanned", _format_bytes(turn["bytes_scanned"]))
            c3.metric("Chart type", (turn.get("chart_type") or "—").title())

            if turn.get("assumptions"):
                st.divider()
                st.caption("**Assumptions**")
                for item in turn["assumptions"]:
                    st.caption(f"• {item}")

            if turn.get("inferred_question"):
                st.divider()
                st.caption("**Query intent check**")
                st.caption(
                    "Claude was shown only the SQL (not your question) and asked "
                    "what it thinks the query is trying to answer:"
                )
                escaped_inferred = html_lib.escape(turn["inferred_question"])
                st.markdown(
                    f'<div style="background:#f8fafc;border-left:3px solid #94a3b8;'
                    f"padding:10px 14px;border-radius:0 4px 4px 0;font-size:14px;"
                    f'color:#334155;margin:4px 0;">'
                    f"<strong>Inferred:</strong> {escaped_inferred}</div>",
                    unsafe_allow_html=True,
                )
    elif turn.get("assumptions"):
        with st.expander("Details"):
            st.caption("**Assumptions**")
            for item in turn["assumptions"]:
                st.caption(f"• {item}")


def _render_card(turn: dict, turn_number: int, form_key: str) -> None:
    """Render a complete turn: numbered card with question header and answer content."""
    ts = turn.get("timestamp", "")
    # #1: st.container(border=True) replaces st.chat_message. Clean bordered card,
    # no chat bubble, no avatar — looks like a data product not a chatbot.
    with st.container(border=True):
        st.markdown(
            f'<div class="turn-meta">'
            f'<span class="turn-label">Question {turn_number}</span>'
            f'{"<span class=\\"turn-time\\">" + ts + "</span>" if ts else ""}'
            f"</div>"
            f'<div class="turn-question">{html_lib.escape(turn["question"])}</div>',
            unsafe_allow_html=True,
        )
        st.divider()
        _render_turn(turn, form_key=form_key)


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
    n = len(st.session_state.history)
    # #9: No session ID — it means nothing to a stakeholder.
    # #12: Show session start time and question count instead.
    st.caption(f"Started at {st.session_state.session_start}")
    st.caption(f"{n} question{'s' if n != 1 else ''} answered")

    # #10: Confirm before wiping the session. Accidental click on "Start new
    # session" would previously clear everything with no way to recover.
    st.divider()
    if not st.session_state.confirm_clear:
        if st.button("Start new session", use_container_width=True):
            st.session_state.confirm_clear = True
            st.rerun()
    else:
        st.warning("This will clear all questions. Are you sure?")
        col_yes, col_no = st.columns(2)
        if col_yes.button("Yes, clear", type="primary", use_container_width=True):
            st.session_state.session_id = None
            st.session_state.history = []
            st.session_state.confirm_clear = False
            st.session_state.session_start = datetime.now().strftime("%H:%M")
            st.rerun()
        if col_no.button("Cancel", use_container_width=True):
            st.session_state.confirm_clear = False
            st.rerun()

    if st.session_state.history:
        st.divider()
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
                    "timestamp": "",
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
# #1: Each turn is a numbered card, not a chat bubble.
for idx, turn in enumerate(st.session_state.history):
    _render_card(turn, turn_number=idx + 1, form_key=str(idx))

# ── Question input ────────────────────────────────────────────────────────────
chat_question = st.chat_input("Ask a question about your data...")
question = chat_question or st.session_state.pending_question
if st.session_state.pending_question:
    st.session_state.pending_question = None

if question:
    n = len(st.session_state.history) + 1
    now_str = datetime.now().strftime("%H:%M")

    # #1: Live turn also uses the card container so it matches history cards.
    with st.container(border=True):
        st.markdown(
            f'<div class="turn-meta">'
            f'<span class="turn-label">Question {n}</span>'
            f'<span class="turn-time">{now_str}</span>'
            f"</div>"
            f'<div class="turn-question">{html_lib.escape(question)}</div>',
            unsafe_allow_html=True,
        )
        st.divider()

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
                        # #11: Styled badge replaces invisible grey caption.
                        _status.markdown(
                            f'<div class="status-badge">⏳ {html_lib.escape(event["text"])}</div>',
                            unsafe_allow_html=True,
                        )
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
                "timestamp": now_str,  # #12: stored so card header shows it on replay
            }
            _render_turn(turn, form_key="current")
            st.session_state.history.append(turn)
