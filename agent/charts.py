"""Chart generator: produces matplotlib PNG and Plotly HTML from query results.

Two output formats per chart:

  PNG  — static image rendered by matplotlib (Agg backend, no display needed).
         Uploaded to S3 and exposed via a presigned URL so clients can embed
         it in a web page or download it without needing AWS credentials.

  HTML — interactive Plotly chart as a self-contained HTML fragment.
         The CDN-loaded plotly.js means the fragment can be embedded in any
         page without bundling the full library.

Chart type is chosen automatically from the column names and data values:

  line  — result contains a time dimension column (year, month, date, week,
           quarter) and at least one numeric column. Monthly trend data always
           renders as a line chart.

  bar   — result contains at least one categorical (string) column and at least
           one numeric column, with no time dimension. Revenue by country,
           product ranking, payment method breakdown.

  table — fallback when the result has no numeric columns or only one row.
          A text table is rendered as a plain matplotlib figure.

  none  — zero rows. No chart is produced.

Chart generation is non-fatal. If matplotlib or Plotly raise for any reason
(e.g. non-numeric string values that slipped through, empty column names), the
error is logged at WARNING level and ChartOutput is returned with None fields.
The caller (main.py / FastAPI endpoint) always receives a ChartOutput, never
an exception from this module.
"""

import glob as _glob
import logging
import os as _os
from dataclasses import dataclass
from typing import Any

import boto3
import matplotlib.patches as mpatches
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.executor import QueryResult

logger = logging.getLogger(__name__)


def _setup_matplotlib_cjk() -> None:
    """Register a CJK-capable font with matplotlib so chart titles render in any language.

    fpdf2 and matplotlib have independent font stacks. Without this, a Chinese chart
    title set via ax.set_title() renders as boxes in the PNG even if the PDF text is fine.
    Runs once at import time; failures are silent so chart generation never crashes.
    """
    try:
        import matplotlib
        import matplotlib.font_manager as fm

        candidates = [
            # Linux: Debian/Ubuntu (installed by fonts-noto-cjk)
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
            # macOS: built-in CJK-capable system fonts (local dev)
            "/System/Library/Fonts/STHeiti Medium.ttc",
            "/System/Library/Fonts/Hiragino Sans GB.ttc",
        ] + _glob.glob("/usr/share/fonts/**/*[Nn]oto*[Cc][Jj][Kk]*", recursive=True)

        for path in candidates:
            if not _os.path.exists(path):
                continue
            try:
                fm.fontManager.addfont(path)
                prop = fm.FontProperties(fname=path)
                cjk_name = prop.get_name()
                current = list(matplotlib.rcParams.get("font.sans-serif", []))
                if cjk_name not in current:
                    matplotlib.rcParams["font.sans-serif"] = [cjk_name] + current
                    matplotlib.rcParams["font.family"] = "sans-serif"
                logger.debug("CJK font registered with matplotlib: %s (%s)", cjk_name, path)
                break
            except Exception:
                continue
    except Exception:
        pass


_setup_matplotlib_cjk()

# S3 key prefix for chart PNGs.
_CHARTS_PREFIX = "charts"

# Presigned URL expiry in seconds (1 hour).
_PRESIGNED_URL_EXPIRY = 3600

# Time dimension column name fragments (case-insensitive).
_TIME_HINTS: frozenset[str] = frozenset({"year", "month", "date", "week", "quarter"})

# Metric column name hints: prefer these over raw IDs when picking the y-axis for bar charts.
_METRIC_HINTS: frozenset[str] = frozenset(
    {
        "revenue",
        "total",
        "amount",
        "sales",
        "count",
        "sum",
        "value",
        "profit",
        "margin",
        "spend",
        "cost",
        "quantity",
        "qty",
        "orders",
        "avg",
        "average",
    }
)

# High-priority monetary hints — always preferred over count/quantity columns.
# When a result has both total_revenue and total_customers, revenue wins.
_PRIORITY_METRIC_HINTS: frozenset[str] = frozenset(
    {"revenue", "amount", "sales", "profit", "income", "spend", "price", "value", "payment"}
)

# Count-like hints — deprioritised when a monetary column is also available.
_COUNT_HINTS: frozenset[str] = frozenset(
    {"count", "customers", "users", "visitors", "quantity", "qty", "num_", "number"}
)

# Rank/ordinal column name patterns — never chosen as the primary metric.
# Checked via endswith("_rank") / endswith("_position") plus exact matches.
_RANK_COL_EXACT: frozenset[str] = frozenset(
    {"rank", "position", "pos", "row_num", "row_number", "rn", "ntile", "dense_rank"}
)

# EDP brand colour used for matplotlib charts.
_BRAND_COLOUR = "#4B5320"  # EDP army olive (primary)

# Column name hints that suggest monetary values (used to format callout labels with $).
_MONETARY_HINTS: frozenset[str] = frozenset(
    {"revenue", "amount", "sales", "profit", "spend", "cost", "price", "total", "income"}
)

# Question-level keyword hints that signal a scatter / correlation chart.
_SCATTER_HINTS: frozenset[str] = frozenset(
    {
        "correlation",
        "correlate",
        "relationship",
        "relate",
        " vs ",
        "versus",
        "against",
        "scatter",
        "affect",
        "predict",
        "linked to",
        "tied to",
        "does.*have",
    }
)

# Question-level keyword hints that signal a pie / donut chart.
_PIE_HINTS: frozenset[str] = frozenset(
    {
        "share",
        "proportion",
        "distribution",
        "breakdown",
        "percentage",
        "split",
        "mix",
        "composition",
        "portion",
        "percent",
        "make up",
        "makeup",
    }
)

# Olive-family colour palette for multi-series charts (pie slices, multi-line traces).
_OLIVE_PALETTE: list[str] = [
    "#4B5320",  # brand olive (darkest)
    "#6B7A3F",
    "#8B9A5B",
    "#A8BA7A",
    "#C8D4A8",
    "#566117",
    "#D4DDB5",
    "#3A4D0E",
]


# Month abbreviations for axis label formatting ("Apr-2025" style).
_MONTH_ABBR: tuple[str, ...] = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def _fmt_month_axis(label: str) -> str:
    """Reformat an ISO year-month label from '2025-04' to 'Apr-2025'.

    Pass through unchanged if the label does not match YYYY-MM.
    This keeps x-axis ticks readable without losing sort order (sorting is
    done on the raw ISO label before this function is called).
    """
    import re as _re

    m = _re.match(r"^(\d{4})-(\d{1,2})$", str(label).strip())
    if m:
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return f"{_MONTH_ABBR[mo - 1]}-{m.group(1)}"
    return label


def _is_monetary(col: str) -> bool:
    """Return True if the column name suggests a monetary metric."""
    col_lower = col.lower()
    return any(hint in col_lower for hint in _MONETARY_HINTS)


def _bar_gradient_colors(n: int) -> list[str]:
    """Return n hex colors interpolated from dark olive (highest) to light olive (lowest).

    Bars are sorted descending before rendering, so index 0 is the highest value
    and receives the darkest shade. The last bar receives the lightest shade.
    The two endpoints are:
      dark  = #4B5320  (brand olive, RGB 75, 83, 32)
      light = #B8C88A  (pale olive, RGB 184, 200, 138)

    With only one bar, the single color is the brand dark olive.
    """
    if n <= 0:
        return []
    dark = (75, 83, 32)
    light = (184, 200, 138)
    colors: list[str] = []
    for i in range(n):
        t = i / max(n - 1, 1)  # 0.0 at top (darkest) → 1.0 at bottom (lightest)
        r = int(dark[0] + t * (light[0] - dark[0]))
        g = int(dark[1] + t * (light[1] - dark[1]))
        b = int(dark[2] + t * (light[2] - dark[2]))
        colors.append(f"#{r:02x}{g:02x}{b:02x}")
    return colors


def _fmt_axis(value: float, monetary: bool) -> str:
    """Format an axis tick value with K/M suffix and optional € prefix.

    Examples: 140000 → '€140K', 1_500_000 → '€1.5M', 42 → '42'.
    """
    abs_v = abs(value)
    if abs_v >= 1_000_000:
        s = f"{value / 1_000_000:.1f}M"
    elif abs_v >= 1_000:
        s = f"{value / 1_000:.0f}K"
    else:
        s = f"{value:.0f}"
    return f"€{s}" if monetary else s


def _catmull_rom_smooth(
    x_indices: list[int],
    y_values: list[float],
    n_points: int = 300,
) -> tuple[Any, Any]:
    """Return smoothed (x, y) arrays using Catmull-Rom spline interpolation.

    Passes through every data point. Requires numpy only — no scipy.
    Falls back to straight-line arrays when fewer than 3 points are given.
    """
    import numpy as np

    n = len(x_indices)
    if n < 3:
        return np.array(x_indices, float), np.array(y_values, float)

    # Pad endpoints by reflecting so the spline has well-defined tangents at both ends.
    xs = [2 * x_indices[0] - x_indices[1]] + list(x_indices) + [2 * x_indices[-1] - x_indices[-2]]
    ys = [2 * y_values[0] - y_values[1]] + list(y_values) + [2 * y_values[-1] - y_values[-2]]

    result_x: list[float] = []
    result_y: list[float] = []
    segments = n - 1
    pts_per_seg = max(2, n_points // segments)

    for i in range(1, n):
        p0x, p0y = xs[i - 1], ys[i - 1]
        p1x, p1y = xs[i], ys[i]
        p2x, p2y = xs[i + 1], ys[i + 1]
        p3x, p3y = xs[i + 2], ys[i + 2]

        is_last = i == n - 1
        for t in np.linspace(0, 1, pts_per_seg, endpoint=is_last):
            t2 = t * t
            t3 = t2 * t
            rx = 0.5 * (
                (2 * p1x)
                + (-p0x + p2x) * t
                + (2 * p0x - 5 * p1x + 4 * p2x - p3x) * t2
                + (-p0x + 3 * p1x - 3 * p2x + p3x) * t3
            )
            ry = 0.5 * (
                (2 * p1y)
                + (-p0y + p2y) * t
                + (2 * p0y - 5 * p1y + 4 * p2y - p3y) * t2
                + (-p0y + 3 * p1y - 3 * p2y + p3y) * t3
            )
            result_x.append(rx)
            result_y.append(ry)

    return np.array(result_x), np.array(result_y)


def _display_label(value: str) -> str:
    """Title-case a label only if it is already all-lowercase.

    Preserves mixed-case brand names (UrbanEdge, TechPlus) while capitalising
    database-stored lowercase values (germany -> Germany, united kingdom -> United Kingdom).
    """
    return value.title() if value == value.lower() else value


@dataclass
class ChartOutput:
    """The result of one chart generation call.

    All fields are optional — any may be None if generation failed or was
    skipped (e.g. zero rows, non-numeric result).

    Attributes:
        png_bytes: Raw PNG image bytes from matplotlib. None on failure.
        html: Plotly chart as an HTML fragment (no <html>/<body> wrapper).
            Includes a CDN <script> tag for plotly.js. None on failure.
        presigned_url: S3 presigned URL for the PNG, valid for 1 hour.
            None if the S3 upload failed or was skipped.
        chart_type: The auto-detected chart type: "bar", "line", "table",
            or "none" (zero rows).
        chart_height: Pixel height the Plotly chart occupies, including
            title and axis padding. Used by the UI to size the iframe exactly,
            eliminating the dead whitespace below small charts.
        error: Human-readable error message if generation failed. None on
            success.
    """

    png_bytes: bytes | None = None
    html: str | None = None
    presigned_url: str | None = None
    chart_type: str = "none"
    chart_height: int = 0
    error: str | None = None


class ChartGenerator:
    """Produces matplotlib PNG and Plotly HTML charts from query results.

    Instantiate once per agent session.

    Usage:
        generator = ChartGenerator(config.aws)
        output = generator.generate(query_result, question="...")
        # output.png_bytes, output.html, output.presigned_url
    """

    def __init__(self, config: AWSConfig) -> None:
        self._config = config
        self._s3 = boto3.client("s3", region_name=config.region)

    def generate(
        self,
        result: QueryResult,
        question: str,
        title: str = "",
        forced_chart_type: str = "",
    ) -> ChartOutput:
        """Generate a PNG and HTML chart for a query result.

        Non-fatal: catches all exceptions, logs at WARNING, and returns a
        ChartOutput with error set. The caller never needs to handle
        exceptions from this method.

        Args:
            result: The QueryResult from AthenaExecutor.execute().
            question: Original plain-English question (used only for logging).
            title: Short chart title from the insight generator, in the same
                language as the question. Displayed above the chart. If empty,
                no title is shown.

        Returns:
            ChartOutput with png_bytes, html, presigned_url, and chart_type.
        """
        if not result.rows:
            logger.info(
                "Zero rows for execution_id=%s — skipping chart generation.",
                result.execution_id,
            )
            return ChartOutput(chart_type="none")

        chart_type = (
            forced_chart_type if forced_chart_type else self._detect_chart_type(result, question)
        )
        logger.info(
            "Chart type detected: %s for execution_id=%s",
            chart_type,
            result.execution_id,
        )

        try:
            png_bytes, html, chart_height = self._render(result, title, chart_type)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Chart rendering failed for execution_id=%s: %s",
                result.execution_id,
                exc,
                exc_info=True,
            )
            return ChartOutput(chart_type=chart_type, error=str(exc))

        presigned_url = self._upload_and_presign(png_bytes, result.execution_id)

        return ChartOutput(
            png_bytes=png_bytes,
            html=html,
            presigned_url=presigned_url,
            chart_type=chart_type,
            chart_height=chart_height,
        )

    # ── Chart type detection ───────────────────────────────────────────────────

    @staticmethod
    def _detect_chart_type(result: QueryResult, question: str = "") -> str:
        """Choose chart type from data shape and optional question keywords.

        Detection order (first match wins):
          1. scatter  — 2+ numeric cols, 1+ categorical label col, question
                        contains a correlation/relationship hint
          2. multiline — time dimension col + 2+ non-time numeric cols,
                         no non-time categorical col
          3. line     — time dimension col + 1 non-time numeric col,
                         no non-time categorical col
          4. pie      — question contains a proportion/share hint,
                         ≤8 rows, 1 categorical + 1 numeric (no time)
          5. bar      — at least 1 categorical + 1 numeric col
          6. table    — fallback (no numeric cols, or nothing else matches)
        """
        numeric_cols = ChartGenerator._numeric_columns(result)
        if not numeric_cols:
            return "table"

        time_cols = [
            col for col in result.columns if any(hint in col.lower() for hint in _TIME_HINTS)
        ]
        non_time_numeric = [c for c in numeric_cols if c not in time_cols]
        categorical_cols = [c for c in result.columns if c not in numeric_cols]
        non_time_categorical = [c for c in categorical_cols if c not in time_cols]

        ql = question.lower()

        # 1. Scatter: correlation/relationship question + 2+ numerics + categorical labels.
        if (
            len(non_time_numeric) >= 2
            and non_time_categorical
            and any(h in ql for h in _SCATTER_HINTS)
        ):
            return "scatter"

        # 2. Multi-line: time axis + 2+ separate metric columns, no string grouping.
        if time_cols and len(non_time_numeric) >= 2 and not non_time_categorical:
            return "multiline"

        # 3. Single-metric line chart over time.
        if time_cols and non_time_numeric and not non_time_categorical:
            return "line"

        # 4. Pie/donut: proportion question + ≤8 rows + exactly one category + one metric.
        if (
            non_time_categorical
            and non_time_numeric
            and not time_cols
            and len(result.rows) <= 8
            and any(h in ql for h in _PIE_HINTS)
        ):
            return "pie"

        # 5. Bar: any categorical grouping with a numeric metric.
        if categorical_cols:
            return "bar"

        return "table"

    @staticmethod
    def _numeric_columns(result: QueryResult) -> list[str]:
        """Return column names whose non-empty values are all parseable as float."""
        numeric: list[str] = []
        for col in result.columns:
            values = [row.get(col, "").strip() for row in result.rows]
            non_empty = [v for v in values if v]
            if not non_empty:
                continue
            try:
                for v in non_empty:
                    float(v)
                numeric.append(col)
            except ValueError:
                pass
        return numeric

    @staticmethod
    def _is_integer_like(value: str) -> bool:
        """Return True if value is a whole number (e.g. 1, 12, 2024).

        Used to distinguish time dimension columns (month=1..12, year=2024)
        from metric columns whose names happen to contain a time hint
        (e.g. monthly_revenue=133528.69).
        """
        try:
            f = float(value)
            return f == int(f)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _is_rank_col(col: str) -> bool:
        """Return True if the column is a rank/ordinal position, not a metric.

        Catches columns like revenue_rank, customer_rank, position, row_num.
        These should never be used as the primary chart metric even when their
        names contain monetary hints (e.g. 'revenue' inside 'revenue_rank').
        """
        col_l = col.lower()
        return (
            col_l in _RANK_COL_EXACT
            or col_l.endswith("_rank")
            or col_l.endswith("_position")
            or col_l.endswith("_pos")
        )

    @staticmethod
    def _best_metric_column(numeric_cols: list[str]) -> str:
        """Pick the most likely metric column from a list of numeric columns.

        Rank/ordinal columns (revenue_rank, position, row_num …) are excluded
        first so that monetary-hint matching cannot accidentally pick them.

        Three-pass priority so that a revenue column always beats a customer
        count column when both are present (e.g. total_revenue vs total_customers):

          Pass 1 — monetary/value hints (revenue, amount, sales, profit, payment...)
          Pass 2 — any metric hint that is NOT a count/quantity hint
          Pass 3 — any metric hint (last resort before raw fallback)

        Each pass iterates in reverse so the last matching column wins when
        multiple columns tie within the same priority tier. Claude puts
        general counts early in the SELECT list and specific metrics last.

        Falls back to the last non-rank column, or the last column overall.
        """
        non_rank = [c for c in numeric_cols if not ChartGenerator._is_rank_col(c)]
        candidates = non_rank if non_rank else numeric_cols

        for col in reversed(candidates):
            if any(hint in col.lower() for hint in _PRIORITY_METRIC_HINTS):
                return col
        for col in reversed(candidates):
            col_lower = col.lower()
            if any(hint in col_lower for hint in _METRIC_HINTS) and not any(
                h in col_lower for h in _COUNT_HINTS
            ):
                return col
        for col in reversed(candidates):
            if any(hint in col.lower() for hint in _METRIC_HINTS):
                return col
        return candidates[-1]

    # ── Rendering ─────────────────────────────────────────────────────────────

    def _render(
        self,
        result: QueryResult,
        title: str,
        chart_type: str,
    ) -> tuple[bytes, str, int]:
        """Dispatch to the correct renderer and return (png_bytes, html, height)."""
        if chart_type == "line":
            return self._render_line(result, title)
        if chart_type == "bar":
            return self._render_bar(result, title)
        if chart_type == "scatter":
            return self._render_scatter(result, title)
        if chart_type == "pie":
            return self._render_pie(result, title)
        if chart_type == "multiline":
            return self._render_multiline(result, title)
        return self._render_table(result, title)

    def _render_bar(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a horizontal bar chart sorted descending by the metric column."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        numeric_cols = self._numeric_columns(result)
        categorical_cols = [c for c in result.columns if c not in numeric_cols]
        y_col = self._best_metric_column(numeric_cols)

        # Combine first_name + last_name into a readable full-name label when both exist.
        if "first_name" in result.columns and "last_name" in result.columns:
            x_col = "customer"
            labels: list[str] = [
                _display_label(f"{row.get('first_name', '')} {row.get('last_name', '')}".strip())
                for row in result.rows
            ]
        else:
            x_col = categorical_cols[0]
            labels = [_display_label(str(row.get(x_col, ""))) for row in result.rows]
        values: list[float] = [float(row.get(y_col, 0) or 0) for row in result.rows]

        # Sort descending so the largest bar is at the top.
        sorted_pairs = sorted(zip(values, labels, strict=False), reverse=True)
        if sorted_pairs:
            values = [v for v, _ in sorted_pairs]
            labels = [lbl for _, lbl in sorted_pairs]

        import matplotlib.ticker as mticker

        _bar_monetary = _is_monetary(y_col)
        # Cap height at 7 inches so charts with many bars (e.g. 15 countries)
        # still fit on a single PDF page alongside the summary and data snapshot.
        fig_h = min(max(4, len(labels) * 0.6), 7.0)
        fig, ax = plt.subplots(figsize=(10, fig_h))
        bar_colors = _bar_gradient_colors(len(values))
        bars = ax.barh(labels, values, color=bar_colors)
        # Bar end labels formatted with $ and K/M.
        bar_labels = [_fmt_axis(v, _bar_monetary) for v in values]
        ax.bar_label(bars, labels=bar_labels, padding=4, fontsize=9)
        ax.set_xlabel(y_col.replace("_", " ").title())
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _p: _fmt_axis(v, _bar_monetary))
        )
        ax.invert_yaxis()
        if title:
            ax.set_title(title, fontsize=11, pad=12)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        plotly_height = max(300, len(labels) * 30)
        html = _plotly_bar(
            labels, values, x_col, y_col, title, height=plotly_height, colors=bar_colors
        )
        # Add padding for title, axis labels, and iframe border.
        return png_bytes, html, plotly_height + 80

    def _render_scatter(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a scatter plot with an optional linear trend line.

        The y-axis is the primary metric column (revenue-like). The x-axis is the
        secondary numeric column (volume / count-like). Each point is labelled with
        the categorical dimension value (payment method, carrier, country…).
        A dashed trend line is added when there are 3 or more data points.
        """
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        import numpy as np

        numeric_cols = self._numeric_columns(result)
        categorical_cols = [c for c in result.columns if c not in numeric_cols]
        non_rank = [c for c in numeric_cols if not self._is_rank_col(c)]
        candidates = non_rank if len(non_rank) >= 2 else numeric_cols

        y_col = self._best_metric_column(candidates)
        x_candidates = [c for c in candidates if c != y_col]
        x_col = x_candidates[0] if x_candidates else candidates[0]
        label_col = categorical_cols[0] if categorical_cols else ""

        labels = (
            [_display_label(str(row.get(label_col, ""))) for row in result.rows]
            if label_col
            else [""] * len(result.rows)
        )
        x_vals = [float(row.get(x_col, 0) or 0) for row in result.rows]
        y_vals = [float(row.get(y_col, 0) or 0) for row in result.rows]

        _x_mon = _is_monetary(x_col)
        _y_mon = _is_monetary(y_col)

        fig, ax = plt.subplots(figsize=(9, 6))
        ax.scatter(x_vals, y_vals, color=_BRAND_COLOUR, s=80, zorder=5)

        # Label each point slightly offset from the marker.
        for lbl, xv, yv in zip(labels, x_vals, y_vals, strict=False):
            ax.annotate(
                lbl,
                (xv, yv),
                textcoords="offset points",
                xytext=(7, 4),
                fontsize=8,
                color="#333333",
            )

        # Dashed trend line via linear regression (numpy polyfit).
        if len(x_vals) >= 3:
            z = np.polyfit(x_vals, y_vals, 1)
            p = np.poly1d(z)
            x_line = [min(x_vals), max(x_vals)]
            ax.plot(
                x_line,
                [p(v) for v in x_line],
                color=_BRAND_COLOUR,
                linewidth=1.5,
                linestyle="--",
                alpha=0.55,
                label="Trend",
            )

        ax.set_xlabel(x_col.replace("_", " ").title())
        ax.set_ylabel(y_col.replace("_", " ").title())
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _p: _fmt_axis(v, _x_mon)))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _p: _fmt_axis(v, _y_mon)))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(color="#e0e0e0", linewidth=0.7, linestyle="--")
        if title:
            ax.set_title(title, fontsize=11, pad=12)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_scatter(labels, x_vals, y_vals, x_col, y_col, title)
        return png_bytes, html, 500

    def _render_pie(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a donut / pie chart for proportion and share questions.

        Each slice shows its label and percentage. The donut hole keeps the chart
        readable when there are many thin slices.
        """
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        numeric_cols = self._numeric_columns(result)
        categorical_cols = [c for c in result.columns if c not in numeric_cols]
        x_col = categorical_cols[0] if categorical_cols else result.columns[0]
        y_col = self._best_metric_column(numeric_cols)

        labels = [_display_label(str(row.get(x_col, ""))) for row in result.rows]
        values = [float(row.get(y_col, 0) or 0) for row in result.rows]
        colours = (_OLIVE_PALETTE * 2)[: len(labels)]

        fig, ax = plt.subplots(figsize=(8, 6))
        _wedges, _texts, autotexts = ax.pie(  # type: ignore[misc]
            values,
            labels=labels,
            autopct="%1.1f%%",
            colors=colours,
            startangle=90,
            pctdistance=0.78,
            wedgeprops={"linewidth": 1.5, "edgecolor": "white"},
        )
        for at in autotexts:
            at.set_fontsize(9)
            at.set_color("white")
            at.set_fontweight("bold")
        # Draw the donut hole.
        centre_circle = mpatches.Circle((0, 0), 0.55, fc="white")
        ax.add_patch(centre_circle)
        ax.axis("equal")
        if title:
            ax.set_title(title, fontsize=11, pad=16)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_pie(labels, values, x_col, y_col, title)
        return png_bytes, html, 450

    def _render_multiline(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a multi-line chart: shared time axis, one line per metric column.

        Up to 3 metric columns are plotted. Each line uses a distinct olive-palette
        colour and a legend entry. Catmull-Rom spline smoothing is applied per line.
        """
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        numeric_cols = self._numeric_columns(result)
        time_cols = [
            col for col in result.columns if any(hint in col.lower() for hint in _TIME_HINTS)
        ]
        non_time_numeric = [c for c in numeric_cols if c not in time_cols]

        x_dim_cols = [
            tc
            for tc in time_cols
            if all(
                ChartGenerator._is_integer_like(str(row.get(tc, "")))
                for row in result.rows
                if row.get(tc, "")
            )
        ]
        if not x_dim_cols:
            x_dim_cols = time_cols[:1]

        if len(x_dim_cols) >= 2:
            row_label_pairs = [
                (
                    "-".join(str(row.get(tc, "")).zfill(2) for tc in x_dim_cols[:2]),
                    row,
                )
                for row in result.rows
            ]
            x_title = " / ".join(tc.replace("_", " ").title() for tc in x_dim_cols[:2])
        else:
            row_label_pairs = [(str(row.get(x_dim_cols[0], "")), row) for row in result.rows]
            x_title = x_dim_cols[0].replace("_", " ").title()

        row_label_pairs.sort(key=lambda p: p[0])
        raw_labels = [p[0] for p in row_label_pairs]
        sorted_rows_ml = [p[1] for p in row_label_pairs]
        x_labels = [_fmt_month_axis(lbl) for lbl in raw_labels]

        metric_cols = non_time_numeric[:3]
        colours = _OLIVE_PALETTE[: len(metric_cols)]

        x_indices = list(range(len(x_labels)))
        fig, ax = plt.subplots(figsize=(12, 5))

        for col, colour in zip(metric_cols, colours, strict=False):
            y_values = [float(row.get(col, 0) or 0) for row in sorted_rows_ml]
            x_smooth, y_smooth = _catmull_rom_smooth(x_indices, y_values)
            ax.fill_between(x_smooth, y_smooth, alpha=0.06, color=colour)
            ax.plot(
                x_smooth,
                y_smooth,
                color=colour,
                linewidth=2.5,
                label=col.replace("_", " ").title(),
            )
            ax.scatter(x_indices, y_values, color=colour, s=35, zorder=5)

        ax.set_xticks(x_indices)
        ax.set_xticklabels(x_labels, rotation=45, ha="right")
        ax.set_xlabel(x_title)
        # Format y-axis with K/M if any metric is monetary.
        _ml_monetary = any(_is_monetary(c) for c in metric_cols)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _p: _fmt_axis(v, _ml_monetary))
        )
        ax.legend(loc="upper left", fontsize=9, framealpha=0.7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.7, linestyle="--")
        if title:
            ax.set_title(title, fontsize=11, pad=20)
        fig.tight_layout()
        fig.subplots_adjust(top=0.88)

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_multiline(x_labels, metric_cols, sorted_rows_ml, x_title, title)
        return png_bytes, html, 480

    def _render_line(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a line chart: time axis vs first numeric column."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        numeric_cols = self._numeric_columns(result)
        time_cols = [
            col for col in result.columns if any(hint in col.lower() for hint in _TIME_HINTS)
        ]
        non_time_numeric = [c for c in numeric_cols if c not in time_cols]

        # Filter time_cols to only true time dimensions (integer-like values, e.g. month=1..12,
        # year=2024). Columns like "monthly_revenue" match "month" in their name but contain
        # large float metric values — these must stay on the y-axis, not become x-axis labels.
        x_dim_cols = [
            tc
            for tc in time_cols
            if all(
                ChartGenerator._is_integer_like(str(row.get(tc, "")))
                for row in result.rows
                if row.get(tc, "")
            )
        ]
        if not x_dim_cols:
            # Fallback: use first time_col as-is (avoids blank x-axis on unusual queries).
            x_dim_cols = time_cols[:1]

        # Build (raw_ISO_label, row) pairs so we can sort chronologically.
        # Raw labels are in YYYY-MM format so lexicographic sort = chronological.
        if len(x_dim_cols) >= 2:
            row_label_pairs = [
                (
                    "-".join(str(row.get(tc, "")).zfill(2) for tc in x_dim_cols[:2]),
                    row,
                )
                for row in result.rows
            ]
            x_title = " / ".join(tc.replace("_", " ").title() for tc in x_dim_cols[:2])
        else:
            row_label_pairs = [(str(row.get(x_dim_cols[0], "")), row) for row in result.rows]
            x_title = x_dim_cols[0].replace("_", " ").title()

        # Sort ascending by raw label (ISO YYYY-MM sorts correctly as a string).
        row_label_pairs.sort(key=lambda p: p[0])
        raw_labels = [p[0] for p in row_label_pairs]
        sorted_rows = [p[1] for p in row_label_pairs]

        # Reformat "2025-04" → "Apr-2025" for human-readable tick labels.
        x_labels = [_fmt_month_axis(lbl) for lbl in raw_labels]

        y_col = non_time_numeric[0]
        y_values = [float(row.get(y_col, 0) or 0) for row in sorted_rows]

        import numpy as np

        x_indices = list(range(len(x_labels)))
        x_smooth, y_smooth = _catmull_rom_smooth(x_indices, y_values)

        fig, ax = plt.subplots(figsize=(12, 5))

        # Gradient area fill under the curve.
        ax.fill_between(x_smooth, y_smooth, alpha=0.13, color=_BRAND_COLOUR)

        # Smooth line.
        ax.plot(x_smooth, y_smooth, color=_BRAND_COLOUR, linewidth=2.5)

        # Data point markers on top of the smooth line.
        ax.scatter(x_indices, y_values, color=_BRAND_COLOUR, s=40, zorder=5)

        # Callout pill labels for the peak and valley.
        if len(y_values) >= 2:
            max_i = int(np.argmax(y_values))
            min_i = int(np.argmin(y_values))
            monetary = _is_monetary(y_col)
            y_min_val = min(y_values)
            y_range = max(y_values) - y_min_val or 1.0
            for idx, default_offset in [(max_i, 22), (min_i, -22)]:
                val = y_values[idx]
                # Flip label direction when near the chart edge to avoid title overlap.
                # Peak in top 25% of range -> label below; valley in bottom 25% -> label above.
                normalized = (val - y_min_val) / y_range
                if default_offset > 0 and normalized > 0.75:
                    offset = -22
                elif default_offset < 0 and normalized < 0.25:
                    offset = 22
                else:
                    offset = default_offset
                lbl = f"€{val:,.0f}" if monetary else f"{val:,.0f}"
                ax.annotate(
                    lbl,
                    xy=(idx, val),
                    xytext=(0, offset),
                    textcoords="offset points",
                    ha="center",
                    va="bottom" if offset > 0 else "top",
                    fontsize=9,
                    color="white",
                    fontweight="bold",
                    bbox={"boxstyle": "round,pad=0.35", "fc": _BRAND_COLOUR, "ec": "none"},
                    arrowprops={"arrowstyle": "-", "color": _BRAND_COLOUR, "lw": 1},
                )

        # Axis labels, ticks, and formatted y-axis.
        import matplotlib.ticker as mticker

        ax.set_xticks(x_indices)
        ax.set_xticklabels(x_labels, rotation=45, ha="right")
        ax.set_xlabel(x_title)
        ax.set_ylabel(y_col.replace("_", " ").title())
        _line_monetary = _is_monetary(y_col)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _p: _fmt_axis(v, _line_monetary))
        )

        # Clean up chart borders.
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.7, linestyle="--")

        # Extra top padding so the title never overlaps callout annotations.
        if title:
            ax.set_title(title, fontsize=11, pad=20)
        fig.tight_layout()
        fig.subplots_adjust(top=0.88)

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_line(x_labels, y_values, x_title, y_col, title)
        return png_bytes, html, 480

    def _render_table(self, result: QueryResult, title: str) -> tuple[bytes, str, int]:
        """Render a plain text table as a matplotlib figure (fallback)."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        col_labels = result.columns
        cell_data = [
            [str(row.get(col, "")) for col in col_labels]
            for row in result.rows[:20]  # cap at 20 rows for readability
        ]

        fig, ax = plt.subplots(
            figsize=(max(8, len(col_labels) * 1.8), max(3, len(cell_data) * 0.45))
        )
        ax.axis("off")
        table = ax.table(
            cellText=cell_data,
            colLabels=col_labels,
            loc="center",
            cellLoc="left",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.auto_set_column_width(col=list(range(len(col_labels))))
        if title:
            ax.set_title(title, fontsize=11, pad=12)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_table(col_labels, cell_data, title)
        table_height = max(200, len(cell_data) * 35) + 100
        return png_bytes, html, table_height

    # ── S3 upload and presigned URL ────────────────────────────────────────────

    def _upload_and_presign(
        self,
        png_bytes: bytes,
        execution_id: str,
    ) -> str | None:
        """Upload PNG to S3 and return a presigned URL. Non-fatal."""
        key = f"{_CHARTS_PREFIX}/{execution_id}.png"
        try:
            self._s3.put_object(
                Bucket=self._config.gold_bucket,
                Key=key,
                Body=png_bytes,
                ContentType="image/png",
            )
            url: str = self._s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._config.gold_bucket, "Key": key},
                ExpiresIn=_PRESIGNED_URL_EXPIRY,
            )
            logger.info(
                "Chart uploaded: s3://%s/%s (presigned URL generated)",
                self._config.gold_bucket,
                key,
            )
            return url
        except ClientError as exc:
            logger.warning(
                "Chart S3 upload failed for execution_id=%s: %s",
                execution_id,
                exc,
            )
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Unexpected error during chart upload for execution_id=%s: %s",
                execution_id,
                exc,
            )
            return None


# ── Private rendering helpers ──────────────────────────────────────────────────


def _fig_to_png(fig) -> bytes:  # type: ignore[no-untyped-def]
    """Render a matplotlib figure to PNG bytes without writing a file."""
    import io

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    buf.seek(0)
    return buf.read()


def _plotly_bar(
    labels: list[str],
    values: list[float],
    x_col: str,
    y_col: str,
    title: str = "",
    height: int = 300,
    colors: list[str] | None = None,
) -> str:
    """Return a Plotly horizontal bar chart as an HTML fragment."""
    import plotly.graph_objects as go

    bar_colors = colors if colors else _bar_gradient_colors(len(values))
    fig = go.Figure(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker_color=bar_colors,
        )
    )
    fig.update_layout(
        title=title or None,
        xaxis_title=y_col.replace("_", " ").title(),
        yaxis_title=x_col.replace("_", " ").title(),
        yaxis_autorange="reversed",
        margin={"l": 150, "r": 20, "t": 50 if title else 20, "b": 40},
        height=height,
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_line(
    x_labels: list[str],
    y_values: list[float],
    x_title: str,
    y_col: str,
    title: str = "",
) -> str:
    """Return a Plotly line chart as an HTML fragment.

    Uses spline smoothing, a translucent area fill, and pill callout labels on
    the peak and valley data points.
    """
    import plotly.graph_objects as go

    monetary = _is_monetary(y_col)

    # Build callout annotations for peak and valley.
    annotations = []
    if len(y_values) >= 2:
        max_i = y_values.index(max(y_values))
        min_i = y_values.index(min(y_values))
        for idx, ay_offset in [(max_i, -40), (min_i, 40)]:
            val = y_values[idx]
            lbl = f"€{val:,.0f}" if monetary else f"{val:,.0f}"
            annotations.append(
                {
                    "x": x_labels[idx],
                    "y": val,
                    "text": f"<b>·{lbl}</b>",
                    "showarrow": True,
                    "arrowhead": 0,
                    "arrowwidth": 1.5,
                    "arrowcolor": _BRAND_COLOUR,
                    "ax": 0,
                    "ay": ay_offset,
                    "bgcolor": _BRAND_COLOUR,
                    "font": {"color": "white", "size": 11},
                    "borderpad": 5,
                    "bordercolor": _BRAND_COLOUR,
                    "borderwidth": 0,
                    "opacity": 1.0,
                }
            )

    fig = go.Figure(
        go.Scatter(
            x=x_labels,
            y=y_values,
            mode="lines+markers",
            line={"color": _BRAND_COLOUR, "width": 2.5, "shape": "spline", "smoothing": 1.0},
            fill="tozeroy",
            fillcolor="rgba(75, 83, 32, 0.10)",
            marker={"size": 6, "color": _BRAND_COLOUR},
        )
    )
    fig.update_layout(
        title=title or None,
        xaxis_title=x_title,
        xaxis={"type": "category", "showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        yaxis_title=y_col.replace("_", " ").title(),
        yaxis={"showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        margin={"l": 60, "r": 20, "t": 50 if title else 20, "b": 60},
        height=420,
        plot_bgcolor="white",
        paper_bgcolor="white",
        annotations=annotations,
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_table(
    col_labels: list[str],
    cell_data: list[list[str]],
    title: str = "",
) -> str:
    """Return a Plotly table as an HTML fragment."""
    import plotly.graph_objects as go

    # Transpose rows-of-columns into columns-of-values for Plotly Table.
    columns_of_values = [[row[i] for row in cell_data] for i in range(len(col_labels))]

    fig = go.Figure(
        go.Table(
            header={"values": col_labels, "fill_color": _BRAND_COLOUR, "font_color": "white"},
            cells={"values": columns_of_values},
        )
    )
    fig.update_layout(
        title=title or None,
        margin={"l": 20, "r": 20, "t": 50 if title else 20, "b": 20},
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_scatter(
    labels: list[str],
    x_vals: list[float],
    y_vals: list[float],
    x_col: str,
    y_col: str,
    title: str = "",
) -> str:
    """Return a Plotly scatter chart with point labels and a linear trend line."""
    import numpy as np
    import plotly.graph_objects as go

    fig = go.Figure()

    # Data points with category labels.
    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="markers+text",
            marker={"size": 12, "color": _BRAND_COLOUR, "opacity": 0.85},
            text=labels,
            textposition="top right",
            textfont={"size": 10},
            name="Data",
            hovertemplate=(
                f"<b>%{{text}}</b><br>"
                f"{x_col.replace('_', ' ').title()}: %{{x:,.0f}}<br>"
                f"{y_col.replace('_', ' ').title()}: %{{y:,.0f}}<extra></extra>"
            ),
        )
    )

    # Dashed trend line.
    if len(x_vals) >= 3:
        z = np.polyfit(x_vals, y_vals, 1)
        p = np.poly1d(z)
        x_line = sorted(x_vals)
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=[float(p(v)) for v in x_line],
                mode="lines",
                line={"color": _BRAND_COLOUR, "width": 1.5, "dash": "dash"},
                name="Trend",
                showlegend=False,
                hoverinfo="skip",
            )
        )

    fig.update_layout(
        title=title or None,
        xaxis_title=x_col.replace("_", " ").title(),
        yaxis_title=y_col.replace("_", " ").title(),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis={"showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        yaxis={"showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        margin={"l": 60, "r": 20, "t": 50 if title else 20, "b": 60},
        height=450,
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_pie(
    labels: list[str],
    values: list[float],
    x_col: str,
    y_col: str,
    title: str = "",
) -> str:
    """Return a Plotly donut chart as an HTML fragment."""
    import plotly.graph_objects as go

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.38,
            marker={
                "colors": (_OLIVE_PALETTE * 2)[: len(labels)],
                "line": {"color": "white", "width": 1.5},
            },
            textinfo="percent+label",
            hovertemplate="%{label}: %{value:,.0f} (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        title=title or None,
        margin={"l": 20, "r": 20, "t": 50 if title else 20, "b": 20},
        height=420,
        showlegend=True,
        legend={"orientation": "v", "x": 1.02, "y": 0.5},
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_multiline(
    x_labels: list[str],
    metric_cols: list[str],
    rows: list[dict[str, str]],
    x_title: str,
    title: str = "",
) -> str:
    """Return a Plotly multi-line chart as an HTML fragment.

    Each metric column becomes a separate trace with its own olive-palette colour.
    Spline smoothing is applied. No callout annotations (too crowded with multiple lines).
    """
    import plotly.graph_objects as go

    colours = _OLIVE_PALETTE[: len(metric_cols)]
    fig = go.Figure()

    for col, colour in zip(metric_cols, colours, strict=False):
        y_vals = [float(row.get(col, 0) or 0) for row in rows]
        fig.add_trace(
            go.Scatter(
                x=x_labels,
                y=y_vals,
                mode="lines+markers",
                name=col.replace("_", " ").title(),
                line={"color": colour, "width": 2.5, "shape": "spline", "smoothing": 0.9},
                marker={"size": 6, "color": colour},
                hovertemplate=f"{col.replace('_', ' ').title()}: %{{y:,.0f}}<extra></extra>",
            )
        )

    fig.update_layout(
        title=title or None,
        xaxis_title=x_title,
        xaxis={"type": "category", "showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        yaxis={"showgrid": True, "gridcolor": "rgba(0,0,0,0.07)"},
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin={"l": 60, "r": 20, "t": 50 if title else 20, "b": 60},
        height=420,
        legend={"orientation": "h", "y": -0.2},
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))
