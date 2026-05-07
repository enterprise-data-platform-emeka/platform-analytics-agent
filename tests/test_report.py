"""Tests for backend PDF report generation."""

import base64

from agent.prompts import build_system_prompt
from agent.report import ReportInput, _detect_period, _extract_kpi_tiles, build_pdf_report


def test_build_pdf_report_returns_pdf_bytes() -> None:
    pdf = build_pdf_report(
        ReportInput(
            question="Which country has the highest revenue?",
            insight="Germany had the highest revenue in the result set.",
            assumptions=["Revenue means completed payments."],
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": "432701.55"}],
            chart_type="bar",
            cost_usd=0.001234,
            bytes_scanned=2048,
            sql="SELECT country, total_revenue FROM edp_dev_gold.revenue_by_country LIMIT 10",
            request_id="req-123",
        )
    )

    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 1000


def test_build_pdf_report_ignores_invalid_png() -> None:
    pdf = build_pdf_report(
        ReportInput(
            question="Show revenue by country",
            insight="Germany leads.",
            png_b64=base64.b64encode(b"not-a-png").decode("utf-8"),
            chart_type="bar",
        )
    )

    assert pdf.startswith(b"%PDF")


def test_period_detection_uses_chronological_bounds_for_unsorted_rows() -> None:
    rows = [
        {"year_month": "2025-10", "total_revenue": "151128.00"},
        {"year_month": "2026-03", "total_revenue": "41376.55"},
        {"year_month": "2025-05", "total_revenue": "89311.02"},
        {"year_month": "2025-06", "total_revenue": "91566.00"},
    ]

    assert _detect_period(["year_month", "total_revenue"], rows) == "May 2025 - Mar 2026"


def test_time_series_kpis_use_latest_and_prior_chronologically() -> None:
    rows = [
        {"year_month": "2025-10", "total_revenue": "151128.00"},
        {"year_month": "2026-03", "total_revenue": "41376.55"},
        {"year_month": "2025-05", "total_revenue": "89311.02"},
        {"year_month": "2026-02", "total_revenue": "109200.00"},
    ]

    tiles = _extract_kpi_tiles(["year_month", "total_revenue"], rows)

    assert tiles[0] == ("Total Revenue", "€41,376.55", "Mar 2026", "-62.1% vs prior")
    assert tiles[2] == ("Total Revenue (All)", "€391,015.57", "May 2025 - Mar 2026", "")


def test_sql_prompt_anchors_relative_month_windows_to_latest_available_month() -> None:
    prompt = build_system_prompt(schemas={}, gold_database="edp_dev_gold")

    assert "anchor the window to the latest available month in the table" in prompt
    assert "not CURRENT_DATE" in prompt
    assert "never compute MAX(order_year) and MAX(order_month) independently" in prompt
    assert "ORDER BY order_year, order_month ASC" in prompt
