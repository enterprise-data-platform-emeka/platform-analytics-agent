"""Analytics Agent Streamlit UI.

Calls the FastAPI backend at localhost:8080/ask. Both processes run in the same
ECS Fargate container, started by entrypoint.sh after FastAPI is healthy.

Stakeholders open the ALB DNS address on port 8501 in a browser. All AWS API
calls happen server-side in the container — the browser never touches AWS.
"""

import streamlit as st
import streamlit.components.v1 as components
import requests

BACKEND_URL = "http://localhost:8080"

st.set_page_config(
    page_title="EDP Analytics Agent",
    layout="wide",
)

st.title("EDP Analytics Agent")
st.caption("Ask plain-English questions about Gold data. Follow-up questions remember prior context.")

# ── Session state initialisation ──────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "history" not in st.session_state:
    st.session_state.history = []

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Session")
    if st.session_state.session_id:
        st.caption(f"Active session: `{st.session_state.session_id[:8]}...`")
    else:
        st.caption("No active session.")
    if st.button("Start new session", use_container_width=True):
        st.session_state.session_id = None
        st.session_state.history = []
        st.rerun()

# ── Conversation history ──────────────────────────────────────────────────────
for turn in st.session_state.history:
    with st.chat_message("user"):
        st.write(turn["question"])
    with st.chat_message("assistant"):
        st.write(turn["insight"].replace("$", r"\$"))
        if turn.get("assumptions"):
            with st.expander("Assumptions"):
                for assumption in turn["assumptions"]:
                    st.write(f"- {assumption}")
        if turn.get("html_chart"):
            components.html(turn["html_chart"], height=450, scrolling=False)
        with st.expander("Query details"):
            if turn.get("sql"):
                st.code(turn["sql"], language="sql")
            col1, col2 = st.columns(2)
            col1.metric("Cost", f"${turn['cost_usd']:.4f}")
            col2.metric("Bytes scanned", f"{turn['bytes_scanned']:,}")
            if turn.get("validation_flags"):
                st.warning("Flags: " + ", ".join(turn["validation_flags"]))

# ── Question input ────────────────────────────────────────────────────────────
question = st.chat_input("Ask a question about your data...")

if question:
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Running query..."):
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

        # Reached only if the request succeeded.
        st.session_state.session_id = data["session_id"]

        st.write(data["insight"].replace("$", r"\$"))

        if data.get("assumptions"):
            with st.expander("Assumptions"):
                for assumption in data["assumptions"]:
                    st.write(f"- {assumption}")

        if data.get("html_chart"):
            components.html(data["html_chart"], height=450, scrolling=False)

        with st.expander("Query details"):
            if data.get("sql"):
                st.code(data["sql"], language="sql")
            col1, col2 = st.columns(2)
            col1.metric("Cost", f"${data['cost_usd']:.4f}")
            col2.metric("Bytes scanned", f"{data['bytes_scanned']:,}")
            if data.get("validation_flags"):
                st.warning("Flags: " + ", ".join(data["validation_flags"]))

        st.session_state.history.append({
            "question": question,
            "insight": data["insight"],
            "assumptions": data.get("assumptions", []),
            "html_chart": data.get("html_chart"),
            "sql": data.get("sql"),
            "cost_usd": data["cost_usd"],
            "bytes_scanned": data["bytes_scanned"],
            "validation_flags": data.get("validation_flags", []),
        })
