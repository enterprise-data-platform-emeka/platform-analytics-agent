"""SQL generator: drives the generation-validation-correction loop.

This module is the glue between ClaudeClient and SQLValidator. It owns the
3-attempt correction loop and is the only place in the codebase that knows
about both components simultaneously.

Flow for each generate() call:

  Attempt 1:
    ClaudeClient.generate_sql(initial_messages, system_prompt)
      -> raw_sql, assumptions
    SQLValidator.validate(raw_sql)
      -> validated_sql              (success: return immediately)
      -> SQLValidationError         (failure: build correction messages, try again)

  Attempt 2:
    ClaudeClient.generate_sql(correction_messages, system_prompt)
      -> raw_sql, assumptions
    SQLValidator.validate(raw_sql)
      -> validated_sql              (success: return)
      -> SQLValidationError         (failure: build another correction, try again)

  Attempt 3 (final):
    ClaudeClient.generate_sql(correction_messages, system_prompt)
      -> raw_sql, assumptions
    SQLValidator.validate(raw_sql)
      -> validated_sql              (success: return)
      -> SQLValidationError         (failure: raise SQLGenerationError — give up)

The return value is a GeneratedSQL dataclass that carries the validated SQL,
the assumptions list, and the number of attempts needed. The executor and
audit log both consume this structure.
"""

import logging
from dataclasses import dataclass

from agent.claude_client import ClaudeClient
from agent.exceptions import SQLGenerationError, SQLValidationError
from agent.prompts import (
    build_sql_correction_messages,
    build_sql_request_messages,
    build_verdict_retry_messages,
)
from agent.validator import SQLValidator

logger = logging.getLogger(__name__)

MAX_ATTEMPTS: int = 3


@dataclass
class GeneratedSQL:
    """The result of a successful generate() call.

    Attributes:
        sql: Validated SQL ready to send to Athena. Has a LIMIT clause.
            Never contains forbidden keywords or non-Gold database references.
        assumptions: List of assumption strings from Claude. Empty list if
            Claude did not produce an <assumptions> block.
        attempts: Number of generation attempts needed (1, 2, or 3).
    """

    sql: str
    assumptions: list[str]
    attempts: int


class SQLGenerator:
    """Drives the Claude generation + validator correction loop.

    Instantiate once per agent session alongside ClaudeClient and SQLValidator.

    Usage:
        generator = SQLGenerator(client, validator)
        result = generator.generate(question, system_prompt)
        # result.sql is validated and ready for Athena
    """

    def __init__(self, client: ClaudeClient, validator: SQLValidator) -> None:
        self._client = client
        self._validator = validator

    def generate(
        self,
        question: str,
        system_prompt: str,
        verdict_feedback: str = "",
    ) -> GeneratedSQL:
        """Generate validated SQL for a plain-English question.

        Drives up to MAX_ATTEMPTS (3) rounds of generation and validation.
        On each validation failure the reason is fed back to Claude as a
        correction message. If all attempts are exhausted, raises
        SQLGenerationError with the last validation failure reason.

        Args:
            question: Plain-English analytical question from the user.
            system_prompt: System prompt with embedded Gold schemas from
                prompts.build_system_prompt().
            verdict_feedback: If non-empty, the verdict discrepancy detail from
                a prior attempt. Injects the mismatch description into the
                initial user message so Claude corrects its approach. Used for
                the post-Athena reflection retry (max one call per question).

        Returns:
            GeneratedSQL with validated sql, assumptions, and attempt count.

        Raises:
            SQLGenerationError: if Claude cannot produce valid SQL after
                MAX_ATTEMPTS attempts, or if the Claude API is unavailable.
        """
        if verdict_feedback:
            messages = build_verdict_retry_messages(question, verdict_feedback)
        else:
            messages = build_sql_request_messages(question)
        last_validation_error: SQLValidationError | None = None
        last_sql: str = ""

        for attempt in range(1, MAX_ATTEMPTS + 1):
            logger.info(
                "SQL generation attempt %d/%d for question: %r",
                attempt,
                MAX_ATTEMPTS,
                question[:80],
            )

            raw_sql, assumptions = self._client.generate_sql(
                messages=messages,
                system_prompt=system_prompt,
            )

            try:
                validated_sql = self._validator.validate(raw_sql)
            except SQLValidationError as exc:
                last_validation_error = exc
                last_sql = raw_sql

                logger.warning(
                    "Attempt %d/%d: validation failed — %s",
                    attempt,
                    MAX_ATTEMPTS,
                    exc.reason,
                )

                if attempt < MAX_ATTEMPTS:
                    # Extend the conversation with the failed SQL and the
                    # guardrail reason so Claude can correct itself.
                    messages = build_sql_correction_messages(
                        prior_messages=messages,
                        prior_sql=raw_sql,
                        prior_assumptions=assumptions,
                        validation_reason=exc.reason,
                    )
                continue

            logger.info(
                "SQL generation succeeded on attempt %d/%d.",
                attempt,
                MAX_ATTEMPTS,
            )
            return GeneratedSQL(
                sql=validated_sql,
                assumptions=assumptions,
                attempts=attempt,
            )

        # All attempts exhausted.
        assert last_validation_error is not None  # always set if loop ran
        raise SQLGenerationError(
            f"Could not generate valid SQL after {MAX_ATTEMPTS} attempts. "
            f"Last validation failure: {last_validation_error.reason}\n"
            f"Last generated SQL:\n{last_sql}"
        ) from last_validation_error
