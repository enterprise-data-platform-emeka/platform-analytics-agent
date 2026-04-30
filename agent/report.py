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


# ---------------------------------------------------------------------------
# Font resolution
# ---------------------------------------------------------------------------

_DEJAVU_REG_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
]
_DEJAVU_BOLD_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
]


def _resolve_font() -> tuple[str, str | None, str | None]:
    import os
    reg = next((p for p in _DEJAVU_REG_PATHS if os.path.exists(p)), None)
    bold = next((p for p in _DEJAVU_BOLD_PATHS if os.path.exists(p)), None)
    if reg:
        return "DejaVu", reg, bold or reg
    return "Helvetica", None, None


# ---------------------------------------------------------------------------
# Text sanitizer
# ---------------------------------------------------------------------------

# Unicode escapes keep the source file free of ambiguous smart-quote characters.
_COMMON_REPLACEMENTS: dict[str, str] = {
    "–": "-",    # en dash
    "—": "--",   # em dash
    "‘": "'",    # left single quotation mark
    "’": "'",    # right single quotation mark
    "“": '"',    # left double quotation mark
    "”": '"',    # right double quotation mark
    "…": "...",  # horizontal ellipsis
    "•": "-",    # bullet
    "·": ".",    # middle dot
}


def _make_safe(font_name: str):  # type: ignore[return]
    """Return a text sanitizer matched to the active font."""
    if font_name != "Helvetica":
        def _safe_unicode(value: str) -> str:
            for src, dest in _COMMON_REPLACEMENTS.items():
                value = value.replace(src, dest)
            return value
        return _safe_unicode

    def _safe_latin(value: str) -> str:
        for src, dest in _COMMON_REPLACEMENTS.items():
            value = value.replace(src, dest)
        value = value.replace("€", "EUR")  # euro sign -> EUR for Helvetica
        return "".join(ch if 0x20 <= ord(ch) <= 0xFF else "?" for ch in value)
    return _safe_latin


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_pdf_report(report: ReportInput) -> bytes:
    """Build a single-page branded stakeholder PDF report.

    Matches the layout produced by the Streamlit UI download:
    question, period label, KPI tiles (with MoM badge), chart,
    summary, and data snapshot. Methodology goes to the engineer log.
    """
    from fpdf import FPDF

    font_name, font_reg, font_bold = _resolve_font()
    safe = _make_safe(font_name)

    try:
        import zoneinfo
        _tz = zoneinfo.ZoneInfo("Europe/Berlin")
        _now = datetime.now(_tz)
        _months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        generated = (
            f"{_now.day:02d} {_months[_now.month - 1]} {_now.year},"
            f" {_now.strftime('%H:%M %Z')}"
        )
    except Exception:
        generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    _fn = font_name

    class PDFReport(FPDF):
        def header(self) -> None:
            self.set_fill_color(75, 83, 32)
            self.rect(0, 0, self.w, 25, style="F")
            _draw_logo(self, 7, 6.5, 12)

            self.set_xy(22, 7)
            self.set_font(_fn, "B", 9)
            self.set_text_color(255, 255, 255)
            self.cell(55, 5, "EMEKA EDEH")
            self.set_xy(22, 13)
            self.set_font(_fn, "", 7)
            self.set_text_color(180, 210, 230)
            self.cell(55, 4, "DATA ENGINEER")

            self.set_font(_fn, "B", 10)
            self.set_text_color(255, 255, 255)
            self.set_xy(self.w / 2 - 35, 9)
            self.cell(70, 6, "EDP Analytics Report", align="C")

            self.set_font(_fn, "", 7)
            self.set_text_color(180, 210, 230)
            self.set_xy(self.w - 62, 7)
            self.cell(57, 5, "INTERNAL | CONFIDENTIAL", align="R")
            self.set_xy(self.w - 62, 13)
            self.set_font(_fn, "", 6)
            self.cell(57, 4, f"Generated: {generated}", align="R")
            self.set_text_color(0, 0, 0)
            self.set_y(28)

        def footer(self) -> None:
            self.set_y(-13)
            self.set_font(_fn, "", 7)
            self.set_text_color(148, 163, 184)
            col_w = self.epw / 3
            self.cell(col_w, 5, "Source: Gold Layer . Athena", align="L")
            self.cell(col_w, 5, "Confidential - Internal Use Only", align="C")
            self.cell(col_w, 5, f"Page {self.page_no()} of {{nb}}", align="R")

    pdf = PDFReport()
    pdf.alias_nb_pages()
    pdf.set_margins(15, 28, 15)
    pdf.set_auto_page_break(auto=True, margin=18)

    if font_reg:
        try:
            pdf.add_font("DejaVu", fname=font_reg)
            pdf.add_font("DejaVu", style="B", fname=font_bold)
        except Exception:
            font_name = "Helvetica"
            _fn = "Helvetica"
            safe = _make_safe("Helvetica")

    pdf.add_page()
    W = pdf.epw

    _section(pdf, "QUESTION", _fn)
    pdf.set_font(_fn, "B", 13)
    pdf.set_text_color(15, 23, 42)
    pdf.multi_cell(W, 7, safe(report.question))
    pdf.ln(3)

    period_label = _detect_period(report.columns, report.rows)
    if period_label:
        pdf.set_font(_fn, "", 8)
        pdf.set_text_color(100, 116, 139)
        pdf.cell(W, 5, safe(f"Period: {period_label}"), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)
    else:
        pdf.ln(5)

    _draw_kpi_tiles(pdf, report, _fn, safe)
    _draw_chart(pdf, report)
    _draw_summary(pdf, report.insight, _fn, safe)
    _draw_snapshot(pdf, report, _fn, safe)

    return bytes(pdf.output())


# ---------------------------------------------------------------------------
# Logo mark
# ---------------------------------------------------------------------------

def _draw_logo(pdf: Any, x: float, y: float, size: float) -> None:
    pdf.set_fill_color(75, 83, 32)
    pdf.rect(x, y, size, size, style="F")
    pad_x = size * 0.18
    pad_top = size * 0.14
    pad_bot = size * 0.20
    usable_w = size - 2 * pad_x
    usable_h = size - pad_top - pad_bot
    n_bars = 3
    gap = usable_w * 0.12
    bar_w = (usable_w - (n_bars - 1) * gap) / n_bars
    baseline_y = y + size - pad_bot
    pdf.set_fill_color(255, 255, 255)
    for i, frac in enumerate([0.40, 0.70, 1.00]):
        bh = usable_h * frac
        bx = x + pad_x + i * (bar_w + gap)
        pdf.rect(bx, baseline_y - bh, bar_w, bh, style="F")
    pdf.rect(x + pad_x, baseline_y, usable_w, size * 0.06, style="F")
    pdf.set_fill_color(255, 255, 255)


# ---------------------------------------------------------------------------
# KPI extraction
# ---------------------------------------------------------------------------

_TIME_HINTS = {"year", "month", "date", "week", "quarter", "period"}
_KPI_PRIORITY = {"revenue", "amount", "sales", "profit", "income", "spend", "price", "value", "payment"}
_KPI_ANY = {"revenue", "total", "amount", "sales", "profit", "sum", "value", "orders", "avg", "average"}
_KPI_COUNTS = {"count", "customers", "users", "visitors", "quantity", "qty"}
_RANK_EXACT = frozenset({"rank", "position", "pos", "row_num", "row_number", "rn", "ntile", "dense_rank"})
_MON_HINTS = {"revenue", "amount", "sales", "profit", "spend", "cost", "price", "income", "value", "payment", "volume", "lifetime"}


def _is_rank_col(col: str) -> bool:
    cl = col.lower()
    return cl in _RANK_EXACT or cl.endswith("_rank") or cl.endswith("_position") or cl.endswith("_pos")


def _pick_metric(cols: list[str]) -> str:
    non_rank = [c for c in cols if not _is_rank_col(c)]
    cands = non_rank if non_rank else cols
    for c in cands:
        if any(h in c.lower() for h in _KPI_PRIORITY):
            return c
    for c in cands:
        if any(h in c.lower() for h in _KPI_ANY) and not any(h in c.lower() for h in _KPI_COUNTS):
            return c
    for c in cands:
        if any(h in c.lower() for h in _KPI_ANY):
            return c
    return cands[-1]


def _is_mon_col(col: str) -> bool:
    return any(h in col.lower() for h in _MON_HINTS)


def _fmt_val(val: str, col: str = "") -> str:
    try:
        f = float(str(val).replace(",", "").replace("€", "").replace("$", ""))
        prefix = "€" if _is_mon_col(col) else ""
        if f == int(f):
            return f"{prefix}{int(f):,}"
        return f"{prefix}{f:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_period(val: str) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    m = re.match(r"^(\d{4})-(\d{2})(?:-\d{2})?$", str(val).strip())
    if m:
        mo = int(m.group(2)) - 1
        if 0 <= mo <= 11:
            return f"{months[mo]} {m.group(1)}"
    return val


def _fmt_snap(val: str, col: str) -> str:
    try:
        f = float(str(val).replace(",", "").replace("€", "").replace("$", ""))
        if _is_mon_col(col):
            return f"€{f:,.0f}"
        if f == int(f):
            return f"{int(f):,}"
        return f"{f:,.2f}"
    except (ValueError, TypeError):
        s = str(val)
        cleaned = s.replace("_", " ")
        return cleaned.title() if s == s.lower() else cleaned


def _detect_period(columns: list[str], rows: list[dict]) -> str:
    if not rows:
        return ""
    for col in columns:
        if any(h in col.lower() for h in _TIME_HINTS):
            first_v = str(rows[0].get(col, "")).strip()
            last_v = str(rows[-1].get(col, "")).strip()
            if re.match(r"^\d{4}", first_v):
                first_fmt = _fmt_period(first_v)
                last_fmt = _fmt_period(last_v)
                if first_fmt and first_fmt != last_fmt:
                    return f"{first_fmt} - {last_fmt}"
                elif first_fmt:
                    return first_fmt
    return ""


def _extract_kpi_tiles(columns: list[str], rows: list[dict]) -> list[tuple[str, str, str, str]]:
    """Return up to 3 (label, value, sub_label, badge) KPI tuples."""
    if not rows or not columns:
        return []

    numeric_cols: list[str] = []
    cat_cols: list[str] = []
    for col in columns:
        if any(hint in col.lower() for hint in _TIME_HINTS):
            cat_cols.append(col)
            continue
        raw = str(rows[0].get(col, "") or "")
        try:
            float(raw.replace(",", "").replace("$", ""))
            numeric_cols.append(col)
        except ValueError:
            cat_cols.append(col)

    tiles: list[tuple[str, str, str, str]] = []

    if cat_cols and numeric_cols:
        metric_col = _pick_metric(numeric_cols)
        cat_col = cat_cols[0]
        metric_label = _label(metric_col)

        _year_col = next((c for c in cat_cols if "year" in c.lower()), None)
        _month_col = next((c for c in cat_cols if "month" in c.lower() and c != _year_col), None)

        def _period_str(row: dict) -> str:
            if _year_col and _month_col:
                y = str(row.get(_year_col, "") or "")
                mo = str(row.get(_month_col, "") or "")
                if y.isdigit() and mo.isdigit():
                    return f"{y}-{mo.zfill(2)}-01"
            return str(row.get(cat_col, ""))

        is_time_cat = any(hint in cat_col.lower() for hint in _TIME_HINTS)
        display_row = rows[-1] if is_time_cat else rows[0]

        if is_time_cat:
            top_cat = _fmt_period(_period_str(display_row))
        else:
            top_raw = str(display_row.get(cat_col, ""))
            cleaned = top_raw.replace("_", " ")
            top_cat = cleaned.title() if top_raw == top_raw.lower() else cleaned

        top_val = _fmt_val(str(display_row.get(metric_col, "")), col=metric_col)

        badge = ""
        if is_time_cat and len(rows) >= 2:
            try:
                cur = float(str(rows[-1].get(metric_col, 0) or 0).replace(",", ""))
                prv = float(str(rows[-2].get(metric_col, 0) or 0).replace(",", ""))
                if prv != 0:
                    pct = (cur - prv) / abs(prv) * 100
                    sign = "+" if pct >= 0 else ""
                    badge = f"{sign}{pct:.1f}% vs prior"
            except (ValueError, TypeError):
                pass

        tiles.append((metric_label, top_val, top_cat, badge))

        col_l = cat_col.lower()
        if is_time_cat:
            if "month" in col_l or _month_col:
                cat_plural = "Months"
            elif "week" in col_l:
                cat_plural = "Weeks"
            elif "quarter" in col_l:
                cat_plural = "Quarters"
            elif "year" in col_l:
                cat_plural = "Years"
            else:
                cat_plural = "Periods"
            tile2_label = "Periods Covered"
        else:
            cat_label = _label(cat_col)
            if cat_label.endswith("y"):
                cat_plural = cat_label[:-1] + "ies"
            elif cat_label.endswith("s"):
                cat_plural = cat_label
            else:
                cat_plural = cat_label + "s"
            tile2_label = "Total Entries"

        tiles.append((tile2_label, str(len(rows)), cat_plural, ""))

        try:
            total = sum(float(str(r.get(metric_col, 0) or 0).replace(",", "")) for r in rows)
            _base_col = re.sub(r"^total_", "", metric_col, flags=re.IGNORECASE)
            _base_col = re.sub(r"_total$", "", _base_col, flags=re.IGNORECASE)
            _base_label = _label(_base_col) if _base_col != metric_col else metric_label
            tile3_label = f"Total {_base_label} (All)"
            if is_time_cat and len(rows) >= 2:
                first_fmt = _fmt_period(_period_str(rows[0]))
                last_fmt = _fmt_period(_period_str(rows[-1]))
                tile3_sub = f"{first_fmt} - {last_fmt}"
            else:
                tile3_sub = "All Entries"
            tiles.append((tile3_label, _fmt_val(str(total), col=metric_col), tile3_sub, ""))
        except (ValueError, TypeError):
            pass

    elif numeric_cols:
        for col in numeric_cols[:3]:
            tiles.append((_label(col), _fmt_val(str(rows[0].get(col, "")), col=col), "", ""))
    else:
        tiles.append(("Total Results", str(len(rows)), "", ""))

    return tiles[:3]


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def _section(pdf: Any, title: str, font_name: str) -> None:
    pdf.set_font(font_name, "B", 11)
    pdf.set_text_color(75, 83, 32)
    pdf.cell(pdf.epw, 5, title, new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + pdf.epw, pdf.get_y())
    pdf.ln(3)


def _draw_kpi_tiles(pdf: Any, report: ReportInput, font_name: str, safe: Any) -> None:
    tiles = _extract_kpi_tiles(report.columns, report.rows)
    if not tiles:
        return

    tile_w = pdf.epw / len(tiles)
    tile_h = 26.0
    tile_y = pdf.get_y()

    for i, (label, value, sub_label, badge) in enumerate(tiles):
        tx = pdf.l_margin + i * tile_w
        pdf.set_fill_color(240, 247, 255)
        pdf.rect(tx, tile_y, tile_w - 2, tile_h, style="F")
        pdf.set_fill_color(75, 83, 32)
        pdf.rect(tx, tile_y, tile_w - 2, 3, style="F")

        pdf.set_xy(tx + 3, tile_y + 4)
        pdf.set_font(font_name, "", 7)
        pdf.set_text_color(75, 83, 32)
        pdf.cell(tile_w - 5, 4, safe(label.upper()))

        pdf.set_xy(tx + 3, tile_y + 9)
        pdf.set_font(font_name, "B", 13)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(tile_w - 5, 7, safe(value))

        pdf.set_xy(tx + 3, tile_y + 17)
        pdf.set_font(font_name, "", 7)
        pdf.set_text_color(75, 83, 32)
        pdf.cell(tile_w - 5, 4, safe(sub_label))

        if badge:
            positive = badge.startswith("+")
            r, g, b = (34, 197, 94) if positive else (239, 68, 68)
            pdf.set_xy(tx + 3, tile_y + 21)
            pdf.set_font(font_name, "B", 7)
            pdf.set_text_color(r, g, b)
            pdf.cell(tile_w - 5, 4, safe(badge))

    pdf.set_y(tile_y + tile_h + 5)
    pdf.set_text_color(0, 0, 0)
    pdf.set_fill_color(255, 255, 255)


def _draw_chart(pdf: Any, report: ReportInput) -> None:
    if not report.png_b64 or report.chart_type == "table":
        return
    try:
        png_bytes = base64.b64decode(report.png_b64)
    except Exception:
        return
    if not png_bytes.startswith(b"\x89PNG"):
        return

    _chart_reserve = 105.0
    max_h = max(40.0, (pdf.h - 18.0) - pdf.get_y() - _chart_reserve)
    png_w = int.from_bytes(png_bytes[16:20], "big")
    png_h = int.from_bytes(png_bytes[20:24], "big")
    natural_h = pdf.epw * (png_h / png_w) if png_w else max_h

    if natural_h > max_h:
        embed_w = pdf.epw * (max_h / natural_h)
        pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin + (pdf.epw - embed_w) / 2, w=embed_w, h=max_h)
    else:
        pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin, w=pdf.epw)

    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(148, 163, 184)
    pdf.cell(pdf.epw, 4, "Source: Gold Layer . Athena", align="R", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)


def _draw_summary(pdf: Any, insight: str, font_name: str, safe: Any) -> None:
    _section(pdf, "SUMMARY", font_name)
    pdf.set_font(font_name, "", 11)
    pdf.set_text_color(30, 41, 59)
    y = pdf.get_y()
    pdf.set_x(pdf.l_margin + 6)
    pdf.multi_cell(pdf.epw - 6, 7, safe(_strip_markdown(insight)))
    pdf.set_fill_color(75, 83, 32)
    pdf.rect(pdf.l_margin, y, 3, max(4, pdf.get_y() - y), style="F")
    pdf.ln(3)


def _draw_snapshot(pdf: Any, report: ReportInput, font_name: str, safe: Any) -> None:
    if not report.columns or not report.rows:
        return
    columns = [col for col in report.columns if not col.lower().endswith("_id")][:4]
    rows = report.rows[:5]
    if not columns or not rows:
        return

    if pdf.h - pdf.get_y() - pdf.b_margin < 38:
        pdf.add_page()
    _section(pdf, "DATA SNAPSHOT", font_name)

    col_w = pdf.epw / len(columns)
    row_h = 6.5
    y = pdf.get_y()

    pdf.set_fill_color(75, 83, 32)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font(font_name, "B", 8)
    for i, col in enumerate(columns):
        pdf.set_xy(pdf.l_margin + i * col_w, y)
        pdf.cell(col_w, row_h, safe(_label(col)), fill=True)
    pdf.set_y(y + row_h)

    pdf.set_font(font_name, "", 8)
    pdf.set_text_color(30, 41, 59)
    for row_i, row in enumerate(rows):
        y = pdf.get_y()
        pdf.set_fill_color(243, 244, 236) if row_i % 2 else pdf.set_fill_color(255, 255, 255)
        for i, col in enumerate(columns):
            value = safe(_fmt_snap(str(row.get(col, "")), col))[:28]
            pdf.set_xy(pdf.l_margin + i * col_w, y)
            pdf.cell(col_w, row_h, value, fill=True)
        pdf.set_y(y + row_h)
    pdf.ln(4)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _strip_markdown(value: str) -> str:
    return re.sub(r"\*{1,3}([^*\n]+)\*{1,3}", r"\1", value).replace("\r", "")


def _label(value: str) -> str:
    return value.replace("_", " ").title()


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"
