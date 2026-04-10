"""In-process session store for multi-turn conversation history.

Each HTTP session is identified by a UUID session_id that the client echoes
back on subsequent requests. The server stores the prior Q&A turns in memory
so Claude receives conversation context when the user asks a follow-up like
"What about Q4?" or "Show me the same for products."

Sessions expire after a configurable TTL (default 1 hour). Eviction runs
lazily on every get() call — no background thread needed.

This is an in-process store, so state is lost on ECS task restart. That is
acceptable for the test-and-destroy workflow. A Redis-backed store could be
substituted later without changing the interface.
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar


@dataclass
class Turn:
    """One question-answer exchange within a conversation.

    Attributes:
        question: The plain-English question the user asked.
        sql: The validated SQL that was executed.
        insight: The plain-English insight returned to the user.
        assumptions: Assumption strings from the SQL generator.
        timestamp: ISO-8601 UTC timestamp of when this turn was recorded.
    """

    question: str
    sql: str
    insight: str
    assumptions: list[str]
    timestamp: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )


@dataclass
class Conversation:
    """A sequence of turns belonging to one session.

    Attributes:
        session_id: Opaque UUID string. Returned to the client and echoed
            back on subsequent requests.
        turns: Ordered list of prior Q&A exchanges.
        created_at: ISO-8601 UTC timestamp of session creation.
    """

    session_id: str
    turns: list[Turn] = field(default_factory=list)
    created_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )

    # Maximum number of prior turns to include in the context summary.
    # Older turns are dropped to keep the prompt size bounded.
    _MAX_CONTEXT_TURNS: ClassVar[int] = 5

    def context_summary(self) -> str:
        """Build a plain-text summary of prior Q&A for the Claude system prompt.

        Returns an empty string for a fresh session (no turns yet). For
        subsequent turns, returns the last _MAX_CONTEXT_TURNS exchanges
        formatted as:

            Prior conversation:
            Q: Which country has the highest revenue?
            A: Germany leads with £432k revenue.
            SQL: SELECT country, total_revenue FROM ...

        Claude uses this to resolve pronouns and relative references like
        "What about for Q4?" or "Show me the same breakdown for products."
        """
        if not self.turns:
            return ""

        recent = self.turns[-self._MAX_CONTEXT_TURNS :]
        lines = ["Prior conversation:"]
        for turn in recent:
            lines.append(f"Q: {turn.question}")
            lines.append(f"A: {turn.insight}")
            lines.append(f"SQL: {turn.sql}")
            lines.append("")
        return "\n".join(lines).rstrip()


class SessionStore:
    """In-memory store for multi-turn conversation sessions.

    Thread-safe for single-threaded async FastAPI workloads. For multi-worker
    deployments, replace with a Redis-backed implementation that shares the
    same interface.

    Usage:
        store = SessionStore()
        session_id = store.create()
        conv = store.get(session_id)          # Conversation or None
        store.append_turn(session_id, turn)
    """

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._ttl_seconds = ttl_seconds
        self._store: dict[str, Conversation] = {}

    def create(self) -> str:
        """Create a new empty conversation and return its session_id."""
        session_id = str(uuid.uuid4())
        self._store[session_id] = Conversation(session_id=session_id)
        return session_id

    def get(self, session_id: str) -> Conversation | None:
        """Return the Conversation for session_id, or None if not found / expired.

        Runs lazy TTL eviction before returning.
        """
        self._evict_expired()
        return self._store.get(session_id)

    def append_turn(self, session_id: str, turn: Turn) -> None:
        """Append a turn to an existing conversation.

        Silently does nothing if session_id is not found (e.g. expired between
        the get() and append_turn() calls).
        """
        conversation = self._store.get(session_id)
        if conversation is not None:
            conversation.turns.append(turn)

    # ── Private helpers ────────────────────────────────────────────────────────

    def _evict_expired(self) -> None:
        """Remove sessions that have exceeded the TTL."""
        now = datetime.now(UTC)
        expired = [
            sid
            for sid, conv in self._store.items()
            if (now - datetime.fromisoformat(conv.created_at)).total_seconds()
            > self._ttl_seconds
        ]
        for sid in expired:
            del self._store[sid]
