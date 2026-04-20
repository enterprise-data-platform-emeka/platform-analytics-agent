"""Tests for charts.py — ChartGenerator."""

from unittest.mock import MagicMock, patch

from agent.charts import (
    _CHARTS_PREFIX,
    _PRESIGNED_URL_EXPIRY,
    ChartGenerator,
    ChartOutput,
)
from agent.config import AWSConfig
from agent.executor import QueryResult

# ── Helpers ────────────────────────────────────────────────────────────────────


def _aws_config() -> AWSConfig:
    return AWSConfig(
        region="eu-central-1",
        environment="dev",
        bronze_bucket="edp-dev-123456789012-bronze",
        gold_bucket="edp-dev-123456789012-gold",
        athena_results_bucket="edp-dev-123456789012-athena-results",
        athena_workgroup="edp-dev-workgroup",
        glue_gold_database="edp_dev_gold",
        ssm_api_key_param="/edp/dev/anthropic_api_key",
    )


def _result(
    columns: list[str],
    rows: list[dict[str, str]],
    execution_id: str = "exec-chart-test",
) -> QueryResult:
    return QueryResult(
        execution_id=execution_id,
        columns=columns,
        rows=rows,
        bytes_scanned=10 * 1024 * 1024,
        cost_usd=0.000095,
    )


def _generator() -> tuple[ChartGenerator, MagicMock]:
    """Return (ChartGenerator, mock_s3_client)."""
    mock_s3 = MagicMock()
    mock_s3.put_object.return_value = {}
    mock_s3.generate_presigned_url.return_value = "https://s3.example.com/chart.png"
    with patch("agent.charts.boto3.client", return_value=mock_s3):
        gen = ChartGenerator(config=_aws_config())
    return gen, mock_s3


REVENUE_BY_COUNTRY_COLS = ["country", "total_revenue"]
REVENUE_BY_COUNTRY_ROWS = [
    {"country": "Germany", "total_revenue": "432701.55"},
    {"country": "France", "total_revenue": "301245.20"},
    {"country": "Spain", "total_revenue": "198432.10"},
    {"country": "Italy", "total_revenue": "175123.45"},
]

MONTHLY_TREND_COLS = ["order_year", "order_month", "total_revenue"]
MONTHLY_TREND_ROWS = [
    {"order_year": "2025", "order_month": "1", "total_revenue": "80000"},
    {"order_year": "2025", "order_month": "2", "total_revenue": "95000"},
    {"order_year": "2025", "order_month": "3", "total_revenue": "110000"},
]

PRODUCT_COLS = ["product_name", "category", "total_orders", "total_revenue"]
PRODUCT_ROWS = [
    {
        "product_name": "Widget A",
        "category": "Electronics",
        "total_orders": "120",
        "total_revenue": "12000.00",
    },
    {
        "product_name": "Widget B",
        "category": "Clothing",
        "total_orders": "95",
        "total_revenue": "9500.00",
    },
]

TEXT_ONLY_COLS = ["country", "customer_frequency_band"]
TEXT_ONLY_ROWS = [
    {"country": "Germany", "customer_frequency_band": "vip"},
    {"country": "France", "customer_frequency_band": "core"},
]

# ── New chart type fixtures ────────────────────────────────────────────────────

# Scatter: payment method volume vs revenue lost (correlation question)
SCATTER_COLS = ["payment_method", "total_transactions", "revenue_lost"]
SCATTER_ROWS = [
    {"payment_method": "Credit Card", "total_transactions": "450", "revenue_lost": "12500"},
    {"payment_method": "PayPal", "total_transactions": "230", "revenue_lost": "8200"},
    {"payment_method": "Bank Transfer", "total_transactions": "180", "revenue_lost": "3100"},
    {"payment_method": "Klarna", "total_transactions": "95", "revenue_lost": "4500"},
    {"payment_method": "Apple Pay", "total_transactions": "310", "revenue_lost": "9800"},
]
SCATTER_QUESTION = "Is there a correlation between payment volume and revenue lost?"

# Pie: payment method revenue share (proportion question, ≤8 rows)
PIE_COLS = ["payment_method", "total_revenue"]
PIE_ROWS = [
    {"payment_method": "Credit Card", "total_revenue": "453702"},
    {"payment_method": "PayPal", "total_revenue": "231654"},
    {"payment_method": "Bank Transfer", "total_revenue": "156466"},
    {"payment_method": "Klarna", "total_revenue": "89211"},
    {"payment_method": "Apple Pay", "total_revenue": "73818"},
]
PIE_QUESTION = "What is the revenue breakdown by payment method?"

# Multi-line: two metrics over the same time dimension
MULTILINE_COLS = ["order_year", "order_month", "total_revenue", "total_orders"]
MULTILINE_ROWS = [
    {"order_year": "2025", "order_month": "1", "total_revenue": "80000", "total_orders": "120"},
    {"order_year": "2025", "order_month": "2", "total_revenue": "95000", "total_orders": "145"},
    {"order_year": "2025", "order_month": "3", "total_revenue": "110000", "total_orders": "160"},
    {"order_year": "2025", "order_month": "4", "total_revenue": "88000", "total_orders": "132"},
]
MULTILINE_QUESTION = "Show me both revenue and order volume trends over the last year"


# ── ChartOutput dataclass ──────────────────────────────────────────────────────


class TestChartOutput:
    def test_default_all_none(self) -> None:
        output = ChartOutput()
        assert output.png_bytes is None
        assert output.html is None
        assert output.presigned_url is None
        assert output.error is None

    def test_default_chart_type_none(self) -> None:
        output = ChartOutput()
        assert output.chart_type == "none"


# ── Chart type detection ───────────────────────────────────────────────────────


class TestDetectChartType:
    def test_bar_for_categorical_plus_numeric(self) -> None:
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "bar"

    def test_line_for_year_month_plus_numeric(self) -> None:
        result = _result(MONTHLY_TREND_COLS, MONTHLY_TREND_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "line"

    def test_line_for_single_date_column(self) -> None:
        result = _result(
            ["order_date", "total_revenue"],
            [{"order_date": "2025-01-01", "total_revenue": "50000"}],
        )
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "line"

    def test_table_for_no_numeric_columns(self) -> None:
        result = _result(TEXT_ONLY_COLS, TEXT_ONLY_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "table"

    def test_table_for_all_empty_numeric_column(self) -> None:
        result = _result(
            ["country", "total_revenue"],
            [{"country": "Germany", "total_revenue": ""}],
        )
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "table"

    def test_bar_for_mixed_columns_no_time(self) -> None:
        result = _result(PRODUCT_COLS, PRODUCT_ROWS)
        # product_name is categorical, total_orders + total_revenue are numeric
        # no time column -> bar
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "bar"

    def test_line_for_week_column(self) -> None:
        result = _result(
            ["order_week", "total_orders"],
            [{"order_week": "2025-W01", "total_orders": "50"}],
        )
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "line"

    def test_line_for_quarter_column(self) -> None:
        result = _result(
            ["order_quarter", "total_revenue"],
            [{"order_quarter": "Q1", "total_revenue": "100000"}],
        )
        chart_type = ChartGenerator._detect_chart_type(result)
        assert chart_type == "line"


# ── Numeric column detection ───────────────────────────────────────────────────


class TestNumericColumns:
    def test_detects_numeric_column(self) -> None:
        result = _result(["country", "total_revenue"], REVENUE_BY_COUNTRY_ROWS)
        numeric = ChartGenerator._numeric_columns(result)
        assert "total_revenue" in numeric
        assert "country" not in numeric

    def test_ignores_empty_column(self) -> None:
        result = _result(
            ["country", "total_revenue"],
            [{"country": "Germany", "total_revenue": ""}],
        )
        numeric = ChartGenerator._numeric_columns(result)
        assert "total_revenue" not in numeric

    def test_detects_integer_values(self) -> None:
        result = _result(
            ["product", "total_orders"],
            [{"product": "Widget A", "total_orders": "120"}],
        )
        numeric = ChartGenerator._numeric_columns(result)
        assert "total_orders" in numeric

    def test_mixed_numeric_and_non_numeric_not_detected(self) -> None:
        # If even one value is non-numeric, the column is not numeric.
        result = _result(
            ["country", "value"],
            [
                {"country": "Germany", "value": "100"},
                {"country": "France", "value": "N/A"},
            ],
        )
        numeric = ChartGenerator._numeric_columns(result)
        assert "value" not in numeric

    def test_negative_values_are_numeric(self) -> None:
        result = _result(
            ["product", "delta"],
            [{"product": "Widget", "delta": "-50.5"}],
        )
        numeric = ChartGenerator._numeric_columns(result)
        assert "delta" in numeric


# ── Zero rows ──────────────────────────────────────────────────────────────────


class TestZeroRows:
    def test_zero_rows_returns_none_chart_type(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, [])
        output = gen.generate(result, "Any question")
        assert output.chart_type == "none"

    def test_zero_rows_returns_no_png(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, [])
        output = gen.generate(result, "Any question")
        assert output.png_bytes is None

    def test_zero_rows_no_s3_upload(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, [])
        gen.generate(result, "Any question")
        mock_s3.put_object.assert_not_called()


# ── PNG generation ─────────────────────────────────────────────────────────────


class TestPngGeneration:
    def test_bar_chart_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Which country has the highest revenue?")
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0

    def test_png_starts_with_png_signature(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Revenue by country")
        assert output.png_bytes is not None
        # PNG files start with the 8-byte PNG signature.
        assert output.png_bytes[:4] == b"\x89PNG"

    def test_line_chart_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(MONTHLY_TREND_COLS, MONTHLY_TREND_ROWS)
        output = gen.generate(result, "Monthly revenue trend")
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0

    def test_table_fallback_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(TEXT_ONLY_COLS, TEXT_ONLY_ROWS)
        output = gen.generate(result, "Customer segments")
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0


# ── HTML generation ────────────────────────────────────────────────────────────


class TestHtmlGeneration:
    def test_bar_chart_returns_html_string(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Revenue by country")
        assert output.html is not None
        assert isinstance(output.html, str)
        assert len(output.html) > 0

    def test_html_contains_plotlyjs_reference(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Revenue by country")
        assert output.html is not None
        assert "plotly" in output.html.lower()

    def test_line_chart_returns_html(self) -> None:
        gen, _ = _generator()
        result = _result(MONTHLY_TREND_COLS, MONTHLY_TREND_ROWS)
        output = gen.generate(result, "Monthly trend")
        assert output.html is not None

    def test_table_fallback_returns_html(self) -> None:
        gen, _ = _generator()
        result = _result(TEXT_ONLY_COLS, TEXT_ONLY_ROWS)
        output = gen.generate(result, "Text only result")
        assert output.html is not None


# ── S3 upload ──────────────────────────────────────────────────────────────────


class TestS3Upload:
    def test_put_object_called_with_gold_bucket(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS, execution_id="exec-123")
        gen.generate(result, "Revenue by country")
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "edp-dev-123456789012-gold"

    def test_put_object_key_contains_execution_id(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS, execution_id="my-exec")
        gen.generate(result, "Revenue by country")
        call_kwargs = mock_s3.put_object.call_args[1]
        assert "my-exec" in call_kwargs["Key"]

    def test_put_object_key_starts_with_charts_prefix(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        gen.generate(result, "Revenue by country")
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Key"].startswith(_CHARTS_PREFIX)

    def test_put_object_content_type_is_image_png(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        gen.generate(result, "Revenue by country")
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "image/png"

    def test_presigned_url_called_with_correct_expiry(self) -> None:
        gen, mock_s3 = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        gen.generate(result, "Revenue by country")
        call_kwargs = mock_s3.generate_presigned_url.call_args[1]
        assert call_kwargs["ExpiresIn"] == _PRESIGNED_URL_EXPIRY

    def test_presigned_url_returned_in_output(self) -> None:
        gen, mock_s3 = _generator()
        mock_s3.generate_presigned_url.return_value = "https://presigned.example.com/chart.png"
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Revenue by country")
        assert output.presigned_url == "https://presigned.example.com/chart.png"

    def test_s3_upload_failure_does_not_raise(self) -> None:
        from botocore.exceptions import ClientError

        gen, mock_s3 = _generator()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        # Must not raise
        output = gen.generate(result, "Revenue by country")
        assert output.presigned_url is None

    def test_s3_upload_failure_png_bytes_still_returned(self) -> None:
        from botocore.exceptions import ClientError

        gen, mock_s3 = _generator()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        output = gen.generate(result, "Revenue by country")
        # PNG is still generated even if S3 upload fails
        assert output.png_bytes is not None


# ── Non-fatal rendering errors ─────────────────────────────────────────────────


class TestNonFatalErrors:
    def test_rendering_exception_does_not_raise(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        with patch.object(gen, "_render", side_effect=RuntimeError("render failed")):
            output = gen.generate(result, "Any question")
        assert output.error is not None

    def test_rendering_exception_sets_error_field(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        with patch.object(gen, "_render", side_effect=RuntimeError("render failed")):
            output = gen.generate(result, "Any question")
        assert output.error is not None and "render failed" in output.error

    def test_rendering_exception_no_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        with patch.object(gen, "_render", side_effect=RuntimeError("render failed")):
            output = gen.generate(result, "Any question")
        assert output.png_bytes is None

    def test_generate_returns_chart_output_on_error(self) -> None:
        gen, _ = _generator()
        result = _result(REVENUE_BY_COUNTRY_COLS, REVENUE_BY_COUNTRY_ROWS)
        with patch.object(gen, "_render", side_effect=RuntimeError("render failed")):
            output = gen.generate(result, "Any question")
        assert isinstance(output, ChartOutput)


# ── Scatter chart detection ────────────────────────────────────────────────────


class TestScatterDetection:
    def test_scatter_for_correlation_question(self) -> None:
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, SCATTER_QUESTION)
        assert chart_type == "scatter"

    def test_scatter_requires_correlation_hint(self) -> None:
        # Same data, no correlation keyword in question → bar (not scatter).
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, "Show payment method stats")
        assert chart_type == "bar"

    def test_scatter_requires_two_numeric_columns(self) -> None:
        # Only one numeric column → bar even with correlation hint.
        result = _result(
            ["payment_method", "total_transactions"],
            [{"payment_method": "Credit Card", "total_transactions": "450"}],
        )
        chart_type = ChartGenerator._detect_chart_type(result, SCATTER_QUESTION)
        assert chart_type == "bar"

    def test_scatter_vs_hint_triggers_scatter(self) -> None:
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        chart_type = ChartGenerator._detect_chart_type(
            result, "Does order volume vs revenue show a pattern?"
        )
        assert chart_type == "scatter"

    def test_scatter_relationship_hint_triggers_scatter(self) -> None:
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        chart_type = ChartGenerator._detect_chart_type(
            result, "What is the relationship between transactions and revenue lost?"
        )
        assert chart_type == "scatter"


# ── Pie chart detection ────────────────────────────────────────────────────────


class TestPieDetection:
    def test_pie_for_proportion_question(self) -> None:
        result = _result(PIE_COLS, PIE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, PIE_QUESTION)
        assert chart_type == "pie"

    def test_pie_for_share_question(self) -> None:
        result = _result(PIE_COLS, PIE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(
            result, "What share of revenue does each payment method contribute?"
        )
        assert chart_type == "pie"

    def test_pie_for_percentage_question(self) -> None:
        result = _result(PIE_COLS, PIE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(
            result, "What percentage of total revenue comes from each country?"
        )
        assert chart_type == "pie"

    def test_no_pie_without_proportion_hint(self) -> None:
        # Same data, no proportion keyword → bar.
        result = _result(PIE_COLS, PIE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, "Show revenue by payment method")
        assert chart_type == "bar"

    def test_no_pie_when_more_than_eight_rows(self) -> None:
        # >8 rows even with proportion keyword → bar.
        many_rows = [
            {"payment_method": f"Method{i}", "total_revenue": str(i * 1000)} for i in range(9)
        ]
        result = _result(PIE_COLS, many_rows)
        chart_type = ChartGenerator._detect_chart_type(result, PIE_QUESTION)
        assert chart_type == "bar"

    def test_no_pie_with_time_dimension(self) -> None:
        # Time column present → not pie (line takes priority).
        result = _result(
            ["order_month", "total_revenue"],
            [{"order_month": "1", "total_revenue": "50000"}],
        )
        chart_type = ChartGenerator._detect_chart_type(result, PIE_QUESTION)
        assert chart_type != "pie"


# ── Multi-line chart detection ─────────────────────────────────────────────────


class TestMultilineDetection:
    def test_multiline_for_time_plus_two_numerics(self) -> None:
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, MULTILINE_QUESTION)
        assert chart_type == "multiline"

    def test_line_not_multiline_for_single_metric(self) -> None:
        # Only 1 non-time numeric → line, not multiline.
        result = _result(MONTHLY_TREND_COLS, MONTHLY_TREND_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, "Show monthly revenue trend")
        assert chart_type == "line"

    def test_multiline_without_question_hint(self) -> None:
        # multiline is data-driven (no question keyword needed).
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        chart_type = ChartGenerator._detect_chart_type(result, "Show me the data")
        assert chart_type == "multiline"

    def test_multiline_three_metrics(self) -> None:
        cols = ["order_year", "order_month", "total_revenue", "total_orders", "unique_customers"]
        rows = [
            {
                "order_year": "2025",
                "order_month": str(m),
                "total_revenue": str(m * 10000),
                "total_orders": str(m * 15),
                "unique_customers": str(m * 8),
            }
            for m in range(1, 4)
        ]
        result = _result(cols, rows)
        chart_type = ChartGenerator._detect_chart_type(result, "Compare metrics over time")
        assert chart_type == "multiline"


# ── Scatter PNG and HTML generation ───────────────────────────────────────────


class TestScatterGeneration:
    def test_scatter_chart_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        output = gen.generate(result, SCATTER_QUESTION)
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0

    def test_scatter_png_signature(self) -> None:
        gen, _ = _generator()
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        output = gen.generate(result, SCATTER_QUESTION)
        assert output.png_bytes is not None
        assert output.png_bytes[:4] == b"\x89PNG"

    def test_scatter_chart_type_field(self) -> None:
        gen, _ = _generator()
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        output = gen.generate(result, SCATTER_QUESTION)
        assert output.chart_type == "scatter"

    def test_scatter_returns_html(self) -> None:
        gen, _ = _generator()
        result = _result(SCATTER_COLS, SCATTER_ROWS)
        output = gen.generate(result, SCATTER_QUESTION)
        assert output.html is not None
        assert "plotly" in output.html.lower()

    def test_scatter_with_two_point_minimum_no_trend_line(self) -> None:
        # 2 rows: trend line requires >=3 points — should still render without error.
        two_rows = SCATTER_ROWS[:2]
        gen, _ = _generator()
        result = _result(SCATTER_COLS, two_rows)
        output = gen.generate(result, SCATTER_QUESTION)
        assert output.png_bytes is not None
        assert output.error is None


# ── Pie PNG and HTML generation ────────────────────────────────────────────────


class TestPieGeneration:
    def test_pie_chart_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(PIE_COLS, PIE_ROWS)
        output = gen.generate(result, PIE_QUESTION)
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0

    def test_pie_png_signature(self) -> None:
        gen, _ = _generator()
        result = _result(PIE_COLS, PIE_ROWS)
        output = gen.generate(result, PIE_QUESTION)
        assert output.png_bytes is not None
        assert output.png_bytes[:4] == b"\x89PNG"

    def test_pie_chart_type_field(self) -> None:
        gen, _ = _generator()
        result = _result(PIE_COLS, PIE_ROWS)
        output = gen.generate(result, PIE_QUESTION)
        assert output.chart_type == "pie"

    def test_pie_returns_html(self) -> None:
        gen, _ = _generator()
        result = _result(PIE_COLS, PIE_ROWS)
        output = gen.generate(result, PIE_QUESTION)
        assert output.html is not None
        assert "plotly" in output.html.lower()

    def test_pie_single_row_renders(self) -> None:
        result = _result(PIE_COLS, PIE_ROWS[:1])
        gen, _ = _generator()
        output = gen.generate(result, PIE_QUESTION)
        assert output.png_bytes is not None
        assert output.error is None


# ── Multi-line PNG and HTML generation ────────────────────────────────────────


class TestMultilineGeneration:
    def test_multiline_chart_returns_png_bytes(self) -> None:
        gen, _ = _generator()
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        output = gen.generate(result, MULTILINE_QUESTION)
        assert output.png_bytes is not None
        assert len(output.png_bytes) > 0

    def test_multiline_png_signature(self) -> None:
        gen, _ = _generator()
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        output = gen.generate(result, MULTILINE_QUESTION)
        assert output.png_bytes is not None
        assert output.png_bytes[:4] == b"\x89PNG"

    def test_multiline_chart_type_field(self) -> None:
        gen, _ = _generator()
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        output = gen.generate(result, MULTILINE_QUESTION)
        assert output.chart_type == "multiline"

    def test_multiline_returns_html(self) -> None:
        gen, _ = _generator()
        result = _result(MULTILINE_COLS, MULTILINE_ROWS)
        output = gen.generate(result, MULTILINE_QUESTION)
        assert output.html is not None
        assert "plotly" in output.html.lower()

    def test_multiline_with_three_metrics(self) -> None:
        cols = ["order_year", "order_month", "total_revenue", "total_orders", "unique_customers"]
        rows = [
            {
                "order_year": "2025",
                "order_month": str(m),
                "total_revenue": str(m * 10000),
                "total_orders": str(m * 15),
                "unique_customers": str(m * 8),
            }
            for m in range(1, 5)
        ]
        gen, _ = _generator()
        result = _result(cols, rows)
        output = gen.generate(result, "Compare all metrics over time")
        assert output.chart_type == "multiline"
        assert output.png_bytes is not None
        assert output.error is None


# ── Rank-column exclusion (regression) ────────────────────────────────────────


class TestRankColumnExclusion:
    def test_revenue_rank_not_selected_as_metric(self) -> None:
        # Simulates "Rank all countries by total revenue" result shape.
        cols = ["country", "total_revenue", "revenue_rank"]
        rows = [
            {"country": "Germany", "total_revenue": "453702", "revenue_rank": "1"},
            {"country": "France", "total_revenue": "431654", "revenue_rank": "2"},
            {"country": "UK", "total_revenue": "356466", "revenue_rank": "3"},
        ]
        result = _result(cols, rows)
        gen, _ = _generator()
        output = gen.generate(result, "Rank all countries by total revenue")
        # Chart should be bar (no correlation hint) and should render without error.
        assert output.chart_type == "bar"
        assert output.png_bytes is not None
        assert output.error is None

    def test_is_rank_col_detects_suffix(self) -> None:
        assert ChartGenerator._is_rank_col("revenue_rank") is True
        assert ChartGenerator._is_rank_col("customer_position") is True
        # Detection is case-insensitive (col.lower() applied internally).
        assert ChartGenerator._is_rank_col("REVENUE_RANK") is True

    def test_is_rank_col_exact_match(self) -> None:
        assert ChartGenerator._is_rank_col("rank") is True
        assert ChartGenerator._is_rank_col("dense_rank") is True
        assert ChartGenerator._is_rank_col("total_revenue") is False
