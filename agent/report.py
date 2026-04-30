"""Stakeholder PDF report generation shared by non-UI clients."""

from __future__ import annotations

import base64
import io
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class ReportInput:
    question: str
    insight: str
    assumptions: list[str] = field(default_factory=list)
    validation_flags: list[str] = field(default_factory=list)
    png_b64: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, str]] = field(default_factory=list)
    chart_type: str = "none"
    cost_usd: float = 0.0
    bytes_scanned: int = 0
    sql: str = ""
    inferred_question: str = ""
    verdict: str = "No"
    discrepancy_detail: str = "None"
    request_id: str = ""


def build_pdf_report(report: ReportInput) -> bytes:
    """Build a branded stakeholder PDF report.

    This mirrors the Streamlit report contract closely enough for non-browser
    clients such as Slack: question, KPI tiles, chart, insight, snapshot,
    assumptions, intent check, cost, and request metadata.
    """

    from fpdf import FPDF

    class PDFReport(FPDF):
        def header(self) -> None:
            self.set_fill_color(75, 83, 32)
            self.rect(0, 0, self.w, 25, style="F")
            _draw_e_mark(self, 7, 6.5, 12)

            self.set_xy(22, 7)
            self.set_font("Helvetica", "B", 9)
            self.set_text_color(255, 255, 255)
            self.cell(55, 5, "EMEKA EDEH")
            self.set_xy(22, 13)
            self.set_font("Helvetica", "", 7)
            self.set_text_color(180, 210, 230)
            self.cell(55, 4, "DATA ENGINEER")

            self.set_font("Helvetica", "B", 10)
            self.set_text_color(255, 255, 255)
            self.set_xy(self.w / 2 - 35, 9)
            self.cell(70, 6, "EDP Analytics Report", align="C")

            generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
            self.set_font("Helvetica", "", 7)
            self.set_text_color(180, 210, 230)
            self.set_xy(self.w - 62, 7)
            self.cell(57, 5, "INTERNAL | CONFIDENTIAL", align="R")
            self.set_xy(self.w - 62, 13)
            self.set_font("Helvetica", "", 6)
            self.cell(57, 4, f"Generated: {generated}", align="R")
            self.set_text_color(0, 0, 0)
            self.set_y(28)

        def footer(self) -> None:
            self.set_y(-13)
            self.set_font("Helvetica", "", 7)
            self.set_text_color(148, 163, 184)
            col_w = self.epw / 3
            self.cell(col_w, 5, "Source: Gold Layer . Athena", align="L")
            self.cell(col_w, 5, "Confidential - Internal Use Only", align="C")
            self.cell(col_w, 5, f"Page {self.page_no()} of {{nb}}", align="R")

    pdf = PDFReport()
    pdf.alias_nb_pages()
    pdf.set_margins(15, 28, 15)
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    _section(pdf, "QUESTION")
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(15, 23, 42)
    pdf.multi_cell(pdf.epw, 7, _safe(report.question))
    pdf.ln(3)

    _draw_kpi_tiles(pdf, report)
    _draw_chart(pdf, report)
    _draw_summary(pdf, report.insight)
    _draw_snapshot(pdf, report)

    pdf.add_page()
    _draw_methodology(pdf, report)

    return bytes(pdf.output())


def _draw_e_mark(pdf: Any, x: float, y: float, size: float) -> None:
    pdf.set_fill_color(255, 255, 255)
    bar_h = size * 0.18
    bar_w = size * 0.68
    stem_w = size * 0.18
    pdf.rect(x, y, stem_w, size, style="F")
    pdf.rect(x, y, bar_w, bar_h, style="F")
    pdf.rect(x, y + size * 0.41, bar_w * 0.82, bar_h, style="F")
    pdf.rect(x, y + size - bar_h, bar_w, bar_h, style="F")


def _section(pdf: Any, title: str) -> None:
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(75, 83, 32)
    pdf.cell(pdf.epw, 5, title, new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + pdf.epw, pdf.get_y())
    pdf.ln(3)


def _draw_kpi_tiles(pdf: Any, report: ReportInput) -> None:
    tiles = [
        ("ATHENA COST", f"${report.cost_usd:.6f}", "per question"),
        ("DATA SCANNED", _format_bytes(report.bytes_scanned), "Athena scan"),
        ("CHART TYPE", report.chart_type or "none", "rendered output"),
    ]
    tile_w = pdf.epw / len(tiles)
    tile_h = 22.0
    y = pdf.get_y()
    for index, (label, value, sublabel) in enumerate(tiles):
        x = pdf.l_margin + index * tile_w
        pdf.set_fill_color(240, 247, 255)
        pdf.rect(x, y, tile_w - 2, tile_h, style="F")
        pdf.set_fill_color(75, 83, 32)
        pdf.rect(x, y, tile_w - 2, 3, style="F")
        pdf.set_xy(x + 3, y + 5)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(75, 83, 32)
        pdf.cell(tile_w - 5, 4, _safe(label))
        pdf.set_xy(x + 3, y + 10)
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(tile_w - 5, 6, _safe(value))
        pdf.set_xy(x + 3, y + 17)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(75, 83, 32)
        pdf.cell(tile_w - 5, 4, _safe(sublabel))
    pdf.set_y(y + tile_h + 6)


def _draw_chart(pdf: Any, report: ReportInput) -> None:
    if not report.png_b64 or report.chart_type == "table":
        return
    try:
        png_bytes = base64.b64decode(report.png_b64)
    except Exception:
        return
    if not png_bytes.startswith(b"\x89PNG"):
        return

    max_h = 82.0
    png_w = int.from_bytes(png_bytes[16:20], "big")
    png_h = int.from_bytes(png_bytes[20:24], "big")
    natural_h = pdf.epw * (png_h / png_w) if png_w else max_h
    if natural_h > max_h:
        embed_w = pdf.epw * (max_h / natural_h)
        pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin + (pdf.epw - embed_w) / 2, w=embed_w)
    else:
        pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin, w=pdf.epw)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(148, 163, 184)
    pdf.cell(pdf.epw, 4, "Source: Gold Layer . Athena", align="R", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)


def _draw_summary(pdf: Any, insight: str) -> None:
    _section(pdf, "SUMMARY")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(30, 41, 59)
    y = pdf.get_y()
    pdf.set_x(pdf.l_margin + 6)
    pdf.multi_cell(pdf.epw - 6, 7, _safe(_strip_markdown(insight)))
    pdf.set_fill_color(75, 83, 32)
    pdf.rect(pdf.l_margin, y, 3, max(4, pdf.get_y() - y), style="F")
    pdf.ln(3)


def _draw_snapshot(pdf: Any, report: ReportInput) -> None:
    if not report.columns or not report.rows:
        return
    columns = [col for col in report.columns if not col.lower().endswith("_id")][:4]
    rows = report.rows[:5]
    if not columns or not rows:
        return

    if pdf.h - pdf.get_y() - pdf.b_margin < 38:
        pdf.add_page()
    _section(pdf, "DATA SNAPSHOT")
    col_w = pdf.epw / len(columns)
    row_h = 6.5
    y = pdf.get_y()
    pdf.set_fill_color(75, 83, 32)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 8)
    for index, col in enumerate(columns):
        pdf.set_xy(pdf.l_margin + index * col_w, y)
        pdf.cell(col_w, row_h, _safe(_label(col)), fill=True)
    pdf.set_y(y + row_h)

    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(30, 41, 59)
    for row_index, row in enumerate(rows):
        y = pdf.get_y()
        pdf.set_fill_color(243, 244, 236) if row_index % 2 else pdf.set_fill_color(255, 255, 255)
        for index, col in enumerate(columns):
            value = _safe(str(row.get(col, "")))[:28]
            pdf.set_xy(pdf.l_margin + index * col_w, y)
            pdf.cell(col_w, row_h, value, fill=True)
        pdf.set_y(y + row_h)
    pdf.ln(4)


def _draw_methodology(pdf: Any, report: ReportInput) -> None:
    _section(pdf, "ASSUMPTIONS")
    _bullet_list(pdf, report.assumptions or ["No assumptions were returned."])

    if report.validation_flags:
        _section(pdf, "DATA QUALITY NOTICES")
        _bullet_list(pdf, report.validation_flags)

    _section(pdf, "QUERY INTENT CHECK")
    lines = [
        f"Intent discrepancy: {report.verdict}",
        f"Inferred question: {report.inferred_question or 'Not available'}",
        f"Detail: {report.discrepancy_detail or 'None'}",
    ]
    if report.request_id:
        lines.append(f"Request ID: {report.request_id}")
    _bullet_list(pdf, lines)

    if report.sql:
        _section(pdf, "SQL QUERY")
        pdf.set_font("Courier", "", 8)
        pdf.set_text_color(30, 41, 59)
        pdf.multi_cell(pdf.epw, 4.5, _safe(report.sql[:3500]))


def _bullet_list(pdf: Any, items: list[str]) -> None:
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(30, 41, 59)
    for item in items[:8]:
        pdf.multi_cell(pdf.epw, 6, f"- {_safe(_strip_markdown(item))}")
    pdf.ln(3)


def _strip_markdown(value: str) -> str:
    return re.sub(r"\*{1,3}([^*\n]+)\*{1,3}", r"\1", value).replace("\r", "")


def _label(value: str) -> str:
    return value.replace("_", " ").title()


def _safe(value: str) -> str:
    replacements = {
        "\u2013": "-",
        "\u2014": "--",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
        "\u2022": "-",
        "\u00b7": ".",
    }
    for src, dest in replacements.items():
        value = value.replace(src, dest)
    return "".join(ch if 0x20 <= ord(ch) <= 0xFF or ch in "\n\t" else "?" for ch in value)


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"

