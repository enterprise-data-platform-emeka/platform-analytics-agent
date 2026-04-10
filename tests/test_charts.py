"""Tests for charts.py — ChartGenerator."""

from unittest.mock import MagicMock, patch

from agent.charts import _CHARTS_PREFIX, _PRESIGNED_URL_EXPIRY, ChartGenerator, ChartOutput
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
        assert "render failed" in output.error

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
