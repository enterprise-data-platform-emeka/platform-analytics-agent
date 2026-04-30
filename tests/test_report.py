"""Tests for backend PDF report generation."""

import base64

from agent.report import ReportInput, build_pdf_report


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
