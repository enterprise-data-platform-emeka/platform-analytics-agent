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

import boto3
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

# EDP brand colour used for matplotlib charts.
_BRAND_COLOUR = "#0D2137"  # EDP deep navy (primary)


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

    def generate(self, result: QueryResult, question: str, title: str = "") -> ChartOutput:
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

        chart_type = self._detect_chart_type(result)
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
    def _detect_chart_type(result: QueryResult) -> str:
        """Choose bar, line, or table based on column names and values.

        Rules (in order):
          1. If any column name contains a time hint (year/month/date/week/quarter)
             AND at least one other column is numeric
             AND there are no non-time categorical columns -> line
             (time is the primary grouping dimension, e.g. monthly revenue trend)
          2. If at least one column is categorical and one is numeric -> bar
             (categorical grouping beats time when both exist, e.g. customers with
             first_order_date metadata columns should still render as a bar chart)
          3. Fallback -> table
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

        # Only use line when time is the sole grouping dimension (no other string cols).
        if time_cols and non_time_numeric and not non_time_categorical:
            return "line"

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
    def _best_metric_column(numeric_cols: list[str]) -> str:
        """Pick the most likely metric column from a list of numeric columns.

        Iterates in reverse so the last matching column wins. Claude puts
        general counts (total_orders, rank) early in the SELECT list and
        the specific requested metric (avg_revenue_per_unit) last. Reversing
        means a specific metric beats a general one when both match a hint.

        Falls back to the last numeric column — identifiers appear first,
        metrics appear last.
        """
        for col in reversed(numeric_cols):
            col_lower = col.lower()
            if any(hint in col_lower for hint in _METRIC_HINTS):
                return col
        # Last column is the safest fallback: IDs come first, metrics come last.
        return numeric_cols[-1]

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

        fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.6)))
        bars = ax.barh(labels, values, color=_BRAND_COLOUR)
        ax.bar_label(bars, fmt="%.0f", padding=4, fontsize=9)
        ax.set_xlabel(y_col.replace("_", " ").title())
        ax.invert_yaxis()
        if title:
            ax.set_title(title, fontsize=11, pad=12)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        plotly_height = max(300, len(labels) * 30)
        html = _plotly_bar(labels, values, x_col, y_col, title, height=plotly_height)
        # Add padding for title, axis labels, and iframe border.
        return png_bytes, html, plotly_height + 80

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

        # Build x-axis labels from time dimension columns, joined if there are two
        # (e.g. order_year + order_month -> "2025-01").
        if len(x_dim_cols) >= 2:
            x_labels = [
                "-".join(str(row.get(tc, "")).zfill(2) for tc in x_dim_cols[:2])
                for row in result.rows
            ]
            x_title = " / ".join(tc.replace("_", " ").title() for tc in x_dim_cols[:2])
        else:
            x_labels = [str(row.get(x_dim_cols[0], "")) for row in result.rows]
            x_title = x_dim_cols[0].replace("_", " ").title()

        y_col = non_time_numeric[0]
        y_values = [float(row.get(y_col, 0) or 0) for row in result.rows]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(x_labels, y_values, marker="o", color=_BRAND_COLOUR, linewidth=2)
        ax.set_xlabel(x_title)
        ax.set_ylabel(y_col.replace("_", " ").title())
        ax.tick_params(axis="x", rotation=45)
        if title:
            ax.set_title(title, fontsize=11, pad=12)
        fig.tight_layout()

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
) -> str:
    """Return a Plotly horizontal bar chart as an HTML fragment."""
    import plotly.graph_objects as go

    fig = go.Figure(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker_color=_BRAND_COLOUR,
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
    """Return a Plotly line chart as an HTML fragment."""
    import plotly.graph_objects as go

    fig = go.Figure(
        go.Scatter(
            x=x_labels,
            y=y_values,
            mode="lines+markers",
            line={"color": _BRAND_COLOUR, "width": 2},
        )
    )
    fig.update_layout(
        title=title or None,
        xaxis_title=x_title,
        xaxis={"type": "category"},
        yaxis_title=y_col.replace("_", " ").title(),
        margin={"l": 60, "r": 20, "t": 50 if title else 20, "b": 60},
        height=400,
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
