"""Insight generator: produces a plain-English answer from query results.

Wraps the second Claude call. Receives the question, validated SQL,
result markdown, and validation flags, and returns a structured
InsightResponse that the CLI and FastAPI endpoint both consume.

The insight text is 2-3 sentences written for a non-technical business
stakeholder. If the result has zero rows the insight acknowledges that
explicitly rather than fabricating numbers. If the ValidationReport has
flags they are appended to the InsightResponse so the caller can choose
whether to surface them.
"""

import logging
from dataclasses import dataclass, field

from agent.claude_client import ClaudeClient
from agent.executor import QueryResult
from agent.result_validator import ValidationReport

logger = logging.getLogger(__name__)

# Max rows of the result sent to Claude for insight generation.
# Sending the full result set for large queries would waste tokens.
# The insight model only needs a representative sample to answer the question.
_INSIGHT_SAMPLE_ROWS: int = 20


@dataclass
class InsightResponse:
    """Structured output from the insight generator.

    Attributes:
        insight: 2-3 sentence plain-English answer to the question.
        assumptions: List of assumption strings from the SQL generator.
            Passed through so the full response includes them.
        validation_flags: List of anomaly flag strings from ResultValidator.
            Empty when the result is clean.
        execution_id: Athena QueryExecutionId for traceability.
        bytes_scanned: Actual bytes scanned by Athena.
        cost_usd: Estimated cost in USD for this question.
    """

    insight: str
    assumptions: list[str]
    chart_title: str = ""
    validation_flags: list[str] = field(default_factory=list)
    execution_id: str = ""
    bytes_scanned: int = 0
    cost_usd: float = 0.0

    def format_for_display(self) -> str:
        """Format the full response for CLI or plain-text output.

        Includes the insight, assumptions, any validation flags, and the
        cost breakdown. Order mirrors what a business analyst would want
        to read first.
        """
        lines: list[str] = [self.insight, ""]

        if self.assumptions:
            lines.append("Assumptions:")
            for assumption in self.assumptions:
                lines.append(f"  {assumption}")
            lines.append("")

        if self.validation_flags:
            lines.append("Data quality notices:")
            for flag in self.validation_flags:
                lines.append(f"  {flag}")
            lines.append("")

        lines.append(
            f"Query: {self.execution_id} | "
            f"Scanned: {self.bytes_scanned / (1024 * 1024):.2f} MB | "
            f"Cost: ${self.cost_usd:.6f}"
        )

        return "\n".join(lines)


class InsightGenerator:
    """Generates a plain-English insight from query results via Claude.

    Instantiate once per agent session alongside ClaudeClient.

    Usage:
        generator = InsightGenerator(client)
        response = generator.generate(
            question=question,
            sql=validated_sql,
            query_result=query_result,
            assumptions=assumptions,
            validation_report=report,
        )
    """

    def __init__(self, client: ClaudeClient) -> None:
        self._client = client

    def generate(
        self,
        question: str,
        sql: str,
        query_result: QueryResult,
        assumptions: list[str],
        validation_report: ValidationReport,
    ) -> InsightResponse:
        """Generate a plain-English insight for the query result.

        If the result has zero rows, a zero-row insight is generated
        without making a Claude call (Claude has nothing to reason about).

        Args:
            question: Original plain-English question from the user.
            sql: Validated SQL that was executed.
            query_result: The QueryResult from AthenaExecutor.
            assumptions: Assumption strings from the SQL generator.
            validation_report: ValidationReport from result_validator.validate().

        Returns:
            InsightResponse with insight text, assumptions, flags, and cost.

        Raises:
            InsightGenerationError: if Claude returns an empty or malformed
                response. Propagates from ClaudeClient.generate_insight().
        """
        if validation_report.zero_rows:
            insight, chart_title = self._client.generate_insight(
                question=question,
                sql=sql,
                result_markdown="(no rows returned)",
            )
        else:
            result_markdown = self._sample_markdown(query_result)
            insight, chart_title = self._client.generate_insight(
                question=question,
                sql=sql,
                result_markdown=result_markdown,
            )
            logger.info(
                "Insight generated for execution_id=%s (%d chars).",
                query_result.execution_id,
                len(insight),
            )

        return InsightResponse(
            insight=insight,
            chart_title=chart_title,
            assumptions=assumptions,
            validation_flags=validation_report.flags,
            execution_id=query_result.execution_id,
            bytes_scanned=query_result.bytes_scanned,
            cost_usd=query_result.cost_usd,
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _sample_markdown(result: QueryResult) -> str:
        """Return a markdown table of the first _INSIGHT_SAMPLE_ROWS rows.

        Slices the result before passing to to_markdown() so that large
        results don't send thousands of tokens to Claude.
        """
        from agent.executor import QueryResult as QR  # local import avoids cycle

        sampled = QR(
            execution_id=result.execution_id,
            columns=result.columns,
            rows=result.rows[:_INSIGHT_SAMPLE_ROWS],
            bytes_scanned=result.bytes_scanned,
            cost_usd=result.cost_usd,
        )
        return sampled.to_markdown()
