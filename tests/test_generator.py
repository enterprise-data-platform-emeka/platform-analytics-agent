"""Tests for SQLGenerator in agent/generator.py.

All tests mock ClaudeClient and SQLValidator directly — no Anthropic API
calls, no AWS calls, no sqlparse parsing. The generator's job is purely
to drive the correction loop, so we test the loop logic in isolation.
"""

from unittest.mock import MagicMock

import pytest

from agent.exceptions import SQLGenerationError, SQLValidationError
from agent.generator import MAX_ATTEMPTS, GeneratedSQL, SQLGenerator

# ── Helpers ────────────────────────────────────────────────────────────────────


def _validation_error(reason: str = "Forbidden keyword DROP.") -> SQLValidationError:
    return SQLValidationError(f"Validation failed: {reason}", reason=reason)


def _mock_client(
    responses: list[tuple[str, list[str]]],
) -> MagicMock:
    """Build a ClaudeClient mock that returns (sql, assumptions) in sequence."""
    client = MagicMock()
    client.generate_sql.side_effect = list(responses)
    return client


def _mock_validator(
    side_effects: list[str | None],
) -> MagicMock:
    """Build a SQLValidator mock.

    side_effects: list where each entry is either:
      - None: validate() returns the sql unchanged (success)
      - str: validate() raises SQLValidationError with that reason
    """
    validator = MagicMock()
    effects = []
    for effect in side_effects:
        if effect is None:
            # Success: return the argument as-is
            effects.append(lambda sql, _r=None: sql)
        else:
            effects.append(SQLValidationError(f"failed: {effect}", reason=effect))

    # validate() is called with sql; side_effect items are either callables or exceptions
    def _validate(sql: str) -> str:
        item = effects.pop(0)
        if isinstance(item, Exception):
            raise item
        return item(sql)

    validator.validate.side_effect = _validate
    return validator


def _generator(
    client: MagicMock,
    validator: MagicMock,
) -> SQLGenerator:
    return SQLGenerator(client=client, validator=validator)


QUESTION = "What is the total revenue by country?"
SYSTEM = "system prompt text"
VALID_SQL = "SELECT country, total_revenue FROM revenue_by_country LIMIT 10"


# ── GeneratedSQL dataclass ─────────────────────────────────────────────────────


class TestGeneratedSQLDataclass:
    def test_stores_sql_assumptions_attempts(self) -> None:
        result = GeneratedSQL(
            sql=VALID_SQL,
            assumptions=["Table: revenue_by_country"],
            attempts=1,
        )
        assert result.sql == VALID_SQL
        assert result.assumptions == ["Table: revenue_by_country"]
        assert result.attempts == 1

    def test_empty_assumptions_allowed(self) -> None:
        result = GeneratedSQL(sql=VALID_SQL, assumptions=[], attempts=2)
        assert result.assumptions == []


# ── Success on first attempt ───────────────────────────────────────────────────


class TestSuccessOnFirstAttempt:
    def test_returns_generated_sql(self) -> None:
        assumptions = ["Table: revenue_by_country — best match"]
        client = _mock_client([(VALID_SQL, assumptions)])
        validator = _mock_validator([None])

        result = _generator(client, validator).generate(QUESTION, SYSTEM)

        assert result.sql == VALID_SQL
        assert result.assumptions == assumptions
        assert result.attempts == 1

    def test_client_called_once(self) -> None:
        client = _mock_client([(VALID_SQL, [])])
        validator = _mock_validator([None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        client.generate_sql.assert_called_once()

    def test_validator_called_with_raw_sql(self) -> None:
        client = _mock_client([(VALID_SQL, [])])
        validator = _mock_validator([None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        validator.validate.assert_called_once_with(VALID_SQL)

    def test_initial_messages_are_user_message(self) -> None:
        client = _mock_client([(VALID_SQL, [])])
        validator = _mock_validator([None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        call_kwargs = client.generate_sql.call_args
        messages = call_kwargs[1]["messages"] if call_kwargs[1] else call_kwargs[0][0]
        assert messages[0]["role"] == "user"
        assert QUESTION in messages[0]["content"]

    def test_system_prompt_passed_through(self) -> None:
        client = _mock_client([(VALID_SQL, [])])
        validator = _mock_validator([None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        call_kwargs = client.generate_sql.call_args
        system = call_kwargs[1].get("system_prompt") or call_kwargs[0][1]
        assert system == SYSTEM


# ── Success on second attempt ──────────────────────────────────────────────────


class TestSuccessOnSecondAttempt:
    def test_returns_second_attempt_sql(self) -> None:
        bad_sql = "DROP TABLE revenue_by_country"
        good_sql = "SELECT country, total_revenue FROM revenue_by_country LIMIT 10"

        client = _mock_client([
            (bad_sql, ["Table: revenue_by_country"]),
            (good_sql, ["Table: revenue_by_country — corrected"]),
        ])
        validator = _mock_validator(["Forbidden keyword DROP.", None])

        result = _generator(client, validator).generate(QUESTION, SYSTEM)

        assert result.sql == good_sql
        assert result.attempts == 2

    def test_client_called_twice(self) -> None:
        client = _mock_client([
            ("DROP TABLE foo", []),
            (VALID_SQL, []),
        ])
        validator = _mock_validator(["DROP not allowed.", None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        assert client.generate_sql.call_count == 2

    def test_correction_messages_extended_with_prior_sql(self) -> None:
        """The second call must receive a messages list that includes the failed SQL."""
        bad_sql = "DELETE FROM revenue_by_country"
        client = _mock_client([
            (bad_sql, ["assumption one"]),
            (VALID_SQL, []),
        ])
        validator = _mock_validator(["DELETE not allowed.", None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        second_call = client.generate_sql.call_args_list[1]
        messages = second_call[1].get("messages") or second_call[0][0]
        # The correction messages list must include the bad SQL in the assistant turn.
        full_content = " ".join(
            str(m.get("content", "")) for m in messages
        )
        assert bad_sql in full_content

    def test_correction_messages_include_validation_reason(self) -> None:
        """The correction user message must include the guardrail reason."""
        reason = "Only SELECT statements are allowed."
        client = _mock_client([
            ("INSERT INTO foo VALUES (1)", ["assumption"]),
            (VALID_SQL, []),
        ])
        validator = _mock_validator([reason, None])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        second_call = client.generate_sql.call_args_list[1]
        messages = second_call[1].get("messages") or second_call[0][0]
        full_content = " ".join(str(m.get("content", "")) for m in messages)
        assert reason in full_content

    def test_second_attempt_assumptions_returned(self) -> None:
        client = _mock_client([
            ("DROP TABLE foo", ["wrong assumption"]),
            (VALID_SQL, ["corrected assumption"]),
        ])
        validator = _mock_validator(["DROP not allowed.", None])

        result = _generator(client, validator).generate(QUESTION, SYSTEM)

        assert result.assumptions == ["corrected assumption"]


# ── Success on third attempt ───────────────────────────────────────────────────


class TestSuccessOnThirdAttempt:
    def test_returns_third_attempt_sql(self) -> None:
        client = _mock_client([
            ("DROP TABLE foo", []),
            ("DELETE FROM foo", []),
            (VALID_SQL, ["final assumption"]),
        ])
        validator = _mock_validator([
            "DROP not allowed.",
            "DELETE not allowed.",
            None,
        ])

        result = _generator(client, validator).generate(QUESTION, SYSTEM)

        assert result.sql == VALID_SQL
        assert result.attempts == 3

    def test_client_called_three_times(self) -> None:
        client = _mock_client([
            ("DROP TABLE foo", []),
            ("DELETE FROM foo", []),
            (VALID_SQL, []),
        ])
        validator = _mock_validator([
            "DROP not allowed.",
            "DELETE not allowed.",
            None,
        ])

        _generator(client, validator).generate(QUESTION, SYSTEM)

        assert client.generate_sql.call_count == 3


# ── All attempts exhausted ─────────────────────────────────────────────────────


class TestAllAttemptsExhausted:
    def test_raises_sql_generation_error(self) -> None:
        client = _mock_client([
            ("DROP TABLE foo", []),
            ("DELETE FROM foo", []),
            ("INSERT INTO foo VALUES (1)", []),
        ])
        validator = _mock_validator([
            "DROP not allowed.",
            "DELETE not allowed.",
            "INSERT not allowed.",
        ])

        with pytest.raises(SQLGenerationError):
            _generator(client, validator).generate(QUESTION, SYSTEM)

    def test_error_message_contains_last_validation_reason(self) -> None:
        last_reason = "INSERT not allowed — only SELECT statements."
        client = _mock_client([
            ("DROP TABLE foo", []),
            ("DELETE FROM foo", []),
            ("INSERT INTO foo VALUES (1)", []),
        ])
        validator = _mock_validator([
            "DROP not allowed.",
            "DELETE not allowed.",
            last_reason,
        ])

        with pytest.raises(SQLGenerationError) as exc_info:
            _generator(client, validator).generate(QUESTION, SYSTEM)

        assert last_reason in str(exc_info.value)

    def test_error_message_contains_last_generated_sql(self) -> None:
        last_sql = "INSERT INTO foo VALUES (1)"
        client = _mock_client([
            ("DROP TABLE foo", []),
            ("DELETE FROM foo", []),
            (last_sql, []),
        ])
        validator = _mock_validator([
            "DROP not allowed.",
            "DELETE not allowed.",
            "INSERT not allowed.",
        ])

        with pytest.raises(SQLGenerationError) as exc_info:
            _generator(client, validator).generate(QUESTION, SYSTEM)

        assert last_sql in str(exc_info.value)

    def test_error_mentions_max_attempts(self) -> None:
        client = _mock_client([("bad sql", [])] * MAX_ATTEMPTS)
        validator = _mock_validator(["bad reason"] * MAX_ATTEMPTS)

        with pytest.raises(SQLGenerationError) as exc_info:
            _generator(client, validator).generate(QUESTION, SYSTEM)

        assert str(MAX_ATTEMPTS) in str(exc_info.value)

    def test_sql_generation_error_is_agent_error(self) -> None:
        from agent.exceptions import AgentError

        client = _mock_client([("bad", [])] * MAX_ATTEMPTS)
        validator = _mock_validator(["bad reason"] * MAX_ATTEMPTS)

        with pytest.raises(AgentError):
            _generator(client, validator).generate(QUESTION, SYSTEM)

    def test_client_called_exactly_max_attempts_times(self) -> None:
        client = _mock_client([("bad", [])] * MAX_ATTEMPTS)
        validator = _mock_validator(["bad reason"] * MAX_ATTEMPTS)

        with pytest.raises(SQLGenerationError):
            _generator(client, validator).generate(QUESTION, SYSTEM)

        assert client.generate_sql.call_count == MAX_ATTEMPTS

    def test_no_correction_message_on_final_attempt(self) -> None:
        """The third (final) call must not append another correction turn.
        The messages list for attempt 3 should end with the attempt-2 correction,
        not gain an additional correction after attempt-3 fails."""
        call_message_lengths: list[int] = []

        client = MagicMock()

        def capture_and_return(**kwargs: object) -> tuple[str, list[str]]:
            msgs = kwargs.get("messages", [])
            call_message_lengths.append(len(msgs))  # type: ignore[arg-type]
            return ("bad sql", [])

        client.generate_sql.side_effect = capture_and_return
        validator = _mock_validator(["bad reason"] * MAX_ATTEMPTS)

        with pytest.raises(SQLGenerationError):
            _generator(client, validator).generate(QUESTION, SYSTEM)

        # Attempt 1: 1 message (initial user)
        # Attempt 2: 3 messages (user + assistant + correction user)
        # Attempt 3: 5 messages (user + assistant + correction + assistant + correction)
        assert call_message_lengths[0] == 1
        assert call_message_lengths[1] == 3
        assert call_message_lengths[2] == 5


# ── Claude API failure propagation ────────────────────────────────────────────


class TestClaudeApiFailure:
    def test_sql_generation_error_from_client_propagates(self) -> None:
        """SQLGenerationError from ClaudeClient (e.g. exhausted retries) must
        propagate immediately without being wrapped or swallowed."""
        client = MagicMock()
        client.generate_sql.side_effect = SQLGenerationError(
            "API unavailable after 3 retries."
        )
        validator = _mock_validator([])

        with pytest.raises(SQLGenerationError, match="API unavailable"):
            _generator(client, validator).generate(QUESTION, SYSTEM)

    def test_unexpected_error_from_client_propagates(self) -> None:
        """Unexpected errors (e.g. boto3 misconfiguration) must not be swallowed."""
        client = MagicMock()
        client.generate_sql.side_effect = RuntimeError("unexpected")
        validator = _mock_validator([])

        with pytest.raises(RuntimeError, match="unexpected"):
            _generator(client, validator).generate(QUESTION, SYSTEM)

    def test_client_not_called_again_after_api_error(self) -> None:
        client = MagicMock()
        client.generate_sql.side_effect = SQLGenerationError("API down.")
        validator = _mock_validator([])

        with pytest.raises(SQLGenerationError):
            _generator(client, validator).generate(QUESTION, SYSTEM)

        client.generate_sql.assert_called_once()


# ── MAX_ATTEMPTS constant ──────────────────────────────────────────────────────


class TestMaxAttemptsConstant:
    def test_max_attempts_is_three(self) -> None:
        assert MAX_ATTEMPTS == 3
