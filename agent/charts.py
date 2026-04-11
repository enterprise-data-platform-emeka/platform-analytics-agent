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

import logging
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.executor import QueryResult

logger = logging.getLogger(__name__)

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
_BRAND_COLOUR = "#2563EB"  # blue-600


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
        error: Human-readable error message if generation failed. None on
            success.
    """

    png_bytes: bytes | None = None
    html: str | None = None
    presigned_url: str | None = None
    chart_type: str = "none"
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

    def generate(self, result: QueryResult, question: str) -> ChartOutput:
        """Generate a PNG and HTML chart for a query result.

        Non-fatal: catches all exceptions, logs at WARNING, and returns a
        ChartOutput with error set. The caller never needs to handle
        exceptions from this method.

        Args:
            result: The QueryResult from AthenaExecutor.execute().
            question: Original plain-English question, used as chart title.

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
            png_bytes, html = self._render(result, question, chart_type)
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
        )

    # ── Chart type detection ───────────────────────────────────────────────────

    @staticmethod
    def _detect_chart_type(result: QueryResult) -> str:
        """Choose bar, line, or table based on column names and values.

        Rules (in order):
          1. If any column name contains a time hint (year/month/date/week/quarter)
             AND at least one other column is numeric -> line
          2. If at least one column is categorical and one is numeric -> bar
          3. Fallback -> table
        """
        numeric_cols = ChartGenerator._numeric_columns(result)
        if not numeric_cols:
            return "table"

        time_cols = [
            col for col in result.columns if any(hint in col.lower() for hint in _TIME_HINTS)
        ]
        non_time_numeric = [c for c in numeric_cols if c not in time_cols]

        if time_cols and non_time_numeric:
            return "line"

        categorical_cols = [c for c in result.columns if c not in numeric_cols]
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
    def _best_metric_column(numeric_cols: list[str]) -> str:
        """Pick the most likely metric column from a list of numeric columns.

        Prefers columns whose names contain metric hints (revenue, total, count,
        etc.) over raw identifier columns (id, key). Falls back to the last
        numeric column — identifiers typically appear first in SELECT lists.
        """
        for col in numeric_cols:
            col_lower = col.lower()
            if any(hint in col_lower for hint in _METRIC_HINTS):
                return col
        # Last column is the safest fallback: IDs come first, metrics come last.
        return numeric_cols[-1]

    # ── Rendering ─────────────────────────────────────────────────────────────

    def _render(
        self,
        result: QueryResult,
        question: str,
        chart_type: str,
    ) -> tuple[bytes, str]:
        """Dispatch to the correct renderer and return (png_bytes, html)."""
        if chart_type == "line":
            return self._render_line(result, question)
        if chart_type == "bar":
            return self._render_bar(result, question)
        return self._render_table(result, question)

    def _render_bar(self, result: QueryResult, question: str) -> tuple[bytes, str]:
        """Render a horizontal bar chart sorted descending by the metric column."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        numeric_cols = self._numeric_columns(result)
        categorical_cols = [c for c in result.columns if c not in numeric_cols]
        x_col = categorical_cols[0]
        y_col = self._best_metric_column(numeric_cols)

        labels: list[str] = [str(row.get(x_col, "")) for row in result.rows]
        values: list[float] = [float(row.get(y_col, 0) or 0) for row in result.rows]

        # Sort descending so the largest bar is at the top.
        sorted_pairs = sorted(zip(values, labels, strict=False), reverse=True)
        if sorted_pairs:
            values = [v for v, _ in sorted_pairs]
            labels = [lbl for _, lbl in sorted_pairs]

        fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.45)))
        bars = ax.barh(labels, values, color=_BRAND_COLOUR)
        ax.bar_label(bars, fmt="%.0f", padding=4, fontsize=9)
        ax.set_xlabel(y_col.replace("_", " ").title())
        ax.set_title(question, fontsize=11, pad=12)
        ax.invert_yaxis()
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_bar(labels, values, x_col, y_col, question)
        return png_bytes, html

    def _render_line(self, result: QueryResult, question: str) -> tuple[bytes, str]:
        """Render a line chart: time axis vs first numeric column."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        numeric_cols = self._numeric_columns(result)
        time_cols = [
            col for col in result.columns if any(hint in col.lower() for hint in _TIME_HINTS)
        ]
        non_time_numeric = [c for c in numeric_cols if c not in time_cols]

        # Build x-axis labels from time columns, joined if there are two
        # (e.g. order_year + order_month -> "2025-01").
        if len(time_cols) >= 2:
            x_labels = [
                "-".join(str(row.get(tc, "")).zfill(2) for tc in time_cols[:2])
                for row in result.rows
            ]
            x_title = " / ".join(tc.replace("_", " ").title() for tc in time_cols[:2])
        else:
            x_labels = [str(row.get(time_cols[0], "")) for row in result.rows]
            x_title = time_cols[0].replace("_", " ").title()

        y_col = non_time_numeric[0]
        y_values = [float(row.get(y_col, 0) or 0) for row in result.rows]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(x_labels, y_values, marker="o", color=_BRAND_COLOUR, linewidth=2)
        ax.set_xlabel(x_title)
        ax.set_ylabel(y_col.replace("_", " ").title())
        ax.set_title(question, fontsize=11, pad=12)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_line(x_labels, y_values, x_title, y_col, question)
        return png_bytes, html

    def _render_table(self, result: QueryResult, question: str) -> tuple[bytes, str]:
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
        ax.set_title(question, fontsize=11, pad=12)
        fig.tight_layout()

        png_bytes = _fig_to_png(fig)
        plt.close(fig)

        html = _plotly_table(col_labels, cell_data, question)
        return png_bytes, html

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
    title: str,
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
        title=title,
        xaxis_title=y_col.replace("_", " ").title(),
        yaxis_title=x_col.replace("_", " ").title(),
        yaxis_autorange="reversed",
        margin={"l": 150, "r": 20, "t": 50, "b": 40},
        height=max(300, len(labels) * 30),
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_line(
    x_labels: list[str],
    y_values: list[float],
    x_title: str,
    y_col: str,
    title: str,
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
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_col.replace("_", " ").title(),
        margin={"l": 60, "r": 20, "t": 50, "b": 60},
        height=400,
    )
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))


def _plotly_table(
    col_labels: list[str],
    cell_data: list[list[str]],
    title: str,
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
    fig.update_layout(title=title, margin={"l": 20, "r": 20, "t": 50, "b": 20})
    return str(fig.to_html(full_html=False, include_plotlyjs="cdn"))
