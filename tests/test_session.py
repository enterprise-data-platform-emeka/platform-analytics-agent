"""Tests for session.py — Turn, Conversation, SessionStore."""

import time

import pytest

from agent.session import Conversation, SessionStore, Turn


# ── Helpers ────────────────────────────────────────────────────────────────────


def _turn(
    question: str = "Which country has the highest revenue?",
    sql: str = "SELECT country, total_revenue FROM revenue_by_country LIMIT 1",
    insight: str = "Germany leads with £432k revenue.",
    assumptions: list[str] | None = None,
) -> Turn:
    return Turn(
        question=question,
        sql=sql,
        insight=insight,
        assumptions=assumptions or ["Table: revenue_by_country"],
    )


# ── Turn ───────────────────────────────────────────────────────────────────────


class TestTurn:
    def test_timestamp_is_set_automatically(self) -> None:
        turn = _turn()
        assert turn.timestamp
        assert "T" in turn.timestamp  # ISO-8601 format contains 'T'

    def test_fields_stored_correctly(self) -> None:
        turn = _turn(question="Q?", sql="SELECT 1", insight="Answer.", assumptions=["A1"])
        assert turn.question == "Q?"
        assert turn.sql == "SELECT 1"
        assert turn.insight == "Answer."
        assert turn.assumptions == ["A1"]


# ── Conversation ───────────────────────────────────────────────────────────────


class TestConversation:
    def test_created_at_is_set(self) -> None:
        conv = Conversation(session_id="sid-1")
        assert conv.created_at
        assert "T" in conv.created_at

    def test_turns_default_empty(self) -> None:
        conv = Conversation(session_id="sid-1")
        assert conv.turns == []

    def test_context_summary_empty_when_no_turns(self) -> None:
        conv = Conversation(session_id="sid-1")
        assert conv.context_summary() == ""

    def test_context_summary_contains_question(self) -> None:
        conv = Conversation(session_id="sid-1", turns=[_turn(question="Revenue by country?")])
        summary = conv.context_summary()
        assert "Revenue by country?" in summary

    def test_context_summary_contains_insight(self) -> None:
        conv = Conversation(session_id="sid-1", turns=[_turn(insight="Germany leads.")])
        assert "Germany leads." in conv.context_summary()

    def test_context_summary_contains_sql(self) -> None:
        conv = Conversation(session_id="sid-1", turns=[_turn(sql="SELECT country LIMIT 1")])
        assert "SELECT country LIMIT 1" in conv.context_summary()

    def test_context_summary_starts_with_prior_conversation(self) -> None:
        conv = Conversation(session_id="sid-1", turns=[_turn()])
        assert conv.context_summary().startswith("Prior conversation:")

    def test_context_summary_caps_at_max_turns(self) -> None:
        # Add 7 turns — only the last 5 should appear in summary.
        turns = [_turn(question=f"Q{i}?", insight=f"A{i}.") for i in range(7)]
        conv = Conversation(session_id="sid-1", turns=turns)
        summary = conv.context_summary()
        assert "Q0?" not in summary
        assert "Q1?" not in summary
        assert "Q2?" in summary
        assert "Q6?" in summary

    def test_context_summary_multiple_turns_all_included_up_to_cap(self) -> None:
        turns = [_turn(question=f"Q{i}?") for i in range(3)]
        conv = Conversation(session_id="sid-1", turns=turns)
        summary = conv.context_summary()
        assert "Q0?" in summary
        assert "Q1?" in summary
        assert "Q2?" in summary

    def test_context_summary_is_string(self) -> None:
        conv = Conversation(session_id="sid-1", turns=[_turn()])
        assert isinstance(conv.context_summary(), str)


# ── SessionStore — create and get ──────────────────────────────────────────────


class TestSessionStoreCreateAndGet:
    def test_create_returns_string(self) -> None:
        store = SessionStore()
        session_id = store.create()
        assert isinstance(session_id, str)
        assert len(session_id) > 0

    def test_create_returns_unique_ids(self) -> None:
        store = SessionStore()
        ids = {store.create() for _ in range(10)}
        assert len(ids) == 10

    def test_get_returns_conversation_after_create(self) -> None:
        store = SessionStore()
        session_id = store.create()
        conv = store.get(session_id)
        assert conv is not None
        assert isinstance(conv, Conversation)

    def test_get_returns_none_for_unknown_id(self) -> None:
        store = SessionStore()
        assert store.get("does-not-exist") is None

    def test_get_session_id_matches(self) -> None:
        store = SessionStore()
        session_id = store.create()
        conv = store.get(session_id)
        assert conv is not None
        assert conv.session_id == session_id

    def test_fresh_session_has_no_turns(self) -> None:
        store = SessionStore()
        session_id = store.create()
        conv = store.get(session_id)
        assert conv is not None
        assert conv.turns == []


# ── SessionStore — append_turn ─────────────────────────────────────────────────


class TestSessionStoreAppendTurn:
    def test_append_adds_turn(self) -> None:
        store = SessionStore()
        session_id = store.create()
        store.append_turn(session_id, _turn(question="First question?"))
        conv = store.get(session_id)
        assert conv is not None
        assert len(conv.turns) == 1

    def test_append_preserves_question(self) -> None:
        store = SessionStore()
        session_id = store.create()
        store.append_turn(session_id, _turn(question="Revenue by country?"))
        conv = store.get(session_id)
        assert conv is not None
        assert conv.turns[0].question == "Revenue by country?"

    def test_append_multiple_turns_in_order(self) -> None:
        store = SessionStore()
        session_id = store.create()
        store.append_turn(session_id, _turn(question="First?"))
        store.append_turn(session_id, _turn(question="Second?"))
        store.append_turn(session_id, _turn(question="Third?"))
        conv = store.get(session_id)
        assert conv is not None
        assert [t.question for t in conv.turns] == ["First?", "Second?", "Third?"]

    def test_append_to_unknown_session_does_not_raise(self) -> None:
        store = SessionStore()
        # Must not raise even for an unknown session_id.
        store.append_turn("nonexistent-id", _turn())

    def test_append_turn_context_summary_updates(self) -> None:
        store = SessionStore()
        session_id = store.create()
        store.append_turn(session_id, _turn(question="Revenue by country?", insight="Germany leads."))
        conv = store.get(session_id)
        assert conv is not None
        summary = conv.context_summary()
        assert "Revenue by country?" in summary
        assert "Germany leads." in summary


# ── SessionStore — TTL eviction ────────────────────────────────────────────────


class TestSessionStoreTTL:
    def test_session_present_before_ttl(self) -> None:
        store = SessionStore(ttl_seconds=60)
        session_id = store.create()
        assert store.get(session_id) is not None

    def test_session_evicted_after_ttl(self) -> None:
        store = SessionStore(ttl_seconds=0)
        session_id = store.create()
        # TTL=0 means created_at is already expired.
        # Sleep 1ms to ensure the timestamp comparison crosses the boundary.
        time.sleep(0.01)
        assert store.get(session_id) is None

    def test_multiple_sessions_only_expired_evicted(self) -> None:
        store = SessionStore(ttl_seconds=0)
        expired_id = store.create()
        # Inject a fresh session with a future created_at by creating it
        # after a tiny sleep (still TTL=0, but we need a long-lived session
        # to test partial eviction — use a second store with normal TTL).
        store2 = SessionStore(ttl_seconds=3600)
        live_id = store2.create()

        time.sleep(0.01)
        assert store.get(expired_id) is None
        assert store2.get(live_id) is not None
