"""Tests for insight.py — InsightGenerator and InsightResponse."""

from unittest.mock import MagicMock

from agent.executor import QueryResult
from agent.insight import _INSIGHT_SAMPLE_ROWS, InsightGenerator, InsightResponse
from agent.result_validator import ValidationReport

# ── Helpers ────────────────────────────────────────────────────────────────────


def _query_result(
    rows: list[dict[str, str]] | None = None,
    columns: list[str] | None = None,
) -> QueryResult:
    return QueryResult(
        execution_id="exec-insight-test",
        columns=columns or ["country", "total_revenue"],
        rows=rows or [{"country": "Germany", "total_revenue": "432701.55"}],
        bytes_scanned=20 * 1024 * 1024,
        cost_usd=0.000095,
    )


def _clean_report() -> ValidationReport:
    return ValidationReport(flags=[], zero_rows=False)


def _zero_rows_report() -> ValidationReport:
    return ValidationReport(
        flags=["Zero rows returned."],
        zero_rows=True,
    )


def _flagged_report() -> ValidationReport:
    return ValidationReport(
        flags=["Negative value in 'total_revenue'."],
        zero_rows=False,
    )


def _mock_client(insight_text: str = "Germany leads with £432k revenue.") -> MagicMock:
    client = MagicMock()
    client.generate_insight.return_value = insight_text
    return client


def _generator(client: MagicMock | None = None) -> InsightGenerator:
    return InsightGenerator(client=client or _mock_client())


QUESTION = "Which country has the highest revenue?"
SQL = "SELECT country, total_revenue FROM revenue_by_country ORDER BY total_revenue DESC LIMIT 1"


# ── InsightResponse ────────────────────────────────────────────────────────────


class TestInsightResponse:
    def _response(self) -> InsightResponse:
        return InsightResponse(
            insight="Germany leads with £432k.",
            assumptions=["Table: revenue_by_country — best match"],
            validation_flags=[],
            execution_id="exec-123",
            bytes_scanned=20 * 1024 * 1024,
            cost_usd=0.000095,
        )

    def test_format_includes_insight(self) -> None:
        assert "Germany leads" in self._response().format_for_display()

    def test_format_includes_assumptions(self) -> None:
        text = self._response().format_for_display()
        assert "revenue_by_country" in text

    def test_format_includes_execution_id(self) -> None:
        text = self._response().format_for_display()
        assert "exec-123" in text

    def test_format_includes_cost(self) -> None:
        text = self._response().format_for_display()
        assert "Cost:" in text

    def test_format_includes_mb_scanned(self) -> None:
        text = self._response().format_for_display()
        assert "MB" in text

    def test_format_no_flags_section_when_clean(self) -> None:
        text = self._response().format_for_display()
        assert "Data quality" not in text

    def test_format_flags_section_when_flagged(self) -> None:
        response = InsightResponse(
            insight="Some insight.",
            assumptions=[],
            validation_flags=["Negative value detected."],
            execution_id="exec-123",
            bytes_scanned=0,
            cost_usd=0.0,
        )
        text = response.format_for_display()
        assert "Data quality notices" in text
        assert "Negative value detected." in text

    def test_format_no_assumptions_section_when_empty(self) -> None:
        response = InsightResponse(
            insight="Some insight.",
            assumptions=[],
            execution_id="exec-123",
            bytes_scanned=0,
            cost_usd=0.0,
        )
        text = response.format_for_display()
        assert "Assumptions" not in text


# ── InsightGenerator — normal result ──────────────────────────────────────────


class TestInsightGeneratorNormalResult:
    def test_returns_insight_response(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert isinstance(result, InsightResponse)

    def test_insight_text_from_client(self) -> None:
        client = _mock_client("Germany leads with £432k revenue.")
        gen = _generator(client)
        result = gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert result.insight == "Germany leads with £432k revenue."

    def test_assumptions_passed_through(self) -> None:
        gen = _generator()
        assumptions = ["Table: revenue_by_country", "Filter: none"]
        result = gen.generate(QUESTION, SQL, _query_result(), assumptions, _clean_report())
        assert result.assumptions == assumptions

    def test_validation_flags_passed_through(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(), [], _flagged_report())
        assert result.validation_flags == _flagged_report().flags

    def test_execution_id_from_query_result(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert result.execution_id == "exec-insight-test"

    def test_bytes_scanned_from_query_result(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert result.bytes_scanned == 20 * 1024 * 1024

    def test_cost_usd_from_query_result(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert result.cost_usd == 0.000095

    def test_client_generate_insight_called_with_question(self) -> None:
        client = _mock_client()
        gen = _generator(client)
        gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        call_kwargs = client.generate_insight.call_args[1]
        assert call_kwargs.get("question") == QUESTION or QUESTION in str(
            client.generate_insight.call_args
        )

    def test_client_generate_insight_called_with_sql(self) -> None:
        client = _mock_client()
        gen = _generator(client)
        gen.generate(QUESTION, SQL, _query_result(), [], _clean_report())
        assert SQL in str(client.generate_insight.call_args)

    def test_result_sampled_to_insight_sample_rows(self) -> None:
        # Build a result with more rows than the sample limit.
        many_rows = [{"country": f"country_{i}", "total_revenue": str(i)} for i in range(50)]
        qr = _query_result(rows=many_rows, columns=["country", "total_revenue"])
        client = _mock_client()
        gen = _generator(client)
        gen.generate(QUESTION, SQL, qr, [], _clean_report())

        # The markdown passed to the client should contain at most _INSIGHT_SAMPLE_ROWS rows.
        call_args = client.generate_insight.call_args
        markdown = call_args[1].get("result_markdown") or call_args[0][2]
        # Count data rows in markdown (lines that start with '|' and contain values, not header/sep)
        data_lines = [
            line
            for line in markdown.splitlines()
            if line.startswith("|") and "---" not in line and "country" not in line  # skip header
        ]
        assert len(data_lines) <= _INSIGHT_SAMPLE_ROWS


# ── InsightGenerator — zero rows ──────────────────────────────────────────────


class TestInsightGeneratorZeroRows:
    def test_zero_rows_calls_claude_with_no_rows_marker(self) -> None:
        client = _mock_client()
        gen = _generator(client)
        gen.generate(QUESTION, SQL, _query_result(rows=[]), [], _zero_rows_report())
        client.generate_insight.assert_called_once()
        call_kwargs = client.generate_insight.call_args[1]
        assert call_kwargs.get("result_markdown") == "(no rows returned)"

    def test_zero_rows_returns_claude_insight(self) -> None:
        client = _mock_client(insight_text="No data matched the requested filters.")
        gen = _generator(client)
        result = gen.generate(QUESTION, SQL, _query_result(rows=[]), [], _zero_rows_report())
        assert result.insight == "No data matched the requested filters."

    def test_zero_rows_passes_question_to_claude(self) -> None:
        client = _mock_client()
        gen = _generator(client)
        gen.generate(QUESTION, SQL, _query_result(rows=[]), [], _zero_rows_report())
        call_kwargs = client.generate_insight.call_args[1]
        assert call_kwargs.get("question") == QUESTION

    def test_zero_rows_flags_still_included(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(rows=[]), [], _zero_rows_report())
        assert result.validation_flags == _zero_rows_report().flags

    def test_zero_rows_execution_id_preserved(self) -> None:
        gen = _generator()
        result = gen.generate(QUESTION, SQL, _query_result(rows=[]), [], _zero_rows_report())
        assert result.execution_id == "exec-insight-test"
