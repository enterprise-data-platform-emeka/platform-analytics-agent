"""Result validator: sanity checks on Athena query results.

Runs after the executor returns a QueryResult and before the insight
generator sees the data. The validator flags anomalies rather than
raising — a flagged result is still surfaced to the user, with the flags
included in the output and the audit log.

The one exception is ResultValidationError, which is reserved for
unrecoverable structural problems (e.g. the result CSV from S3 is
malformed). In practice every Athena result this agent receives is
well-formed; the error exists for completeness.

Checks performed:
  - Zero rows:        valid for Gold aggregations — a correct empty result.
                      Flagged so the insight generator knows to say "no data
                      matched" rather than inventing numbers.
  - Negative revenue: any column whose name contains 'revenue', 'amount',
                      'value', or 'price' should not have negative values.
                      Negative means the data pipeline produced something
                      unexpected.
  - High null rate:   if more than 50% of values in a column are empty
                      string (Athena's NULL representation in CSV), flag it.
                      This catches join failures or missing upstream data.

All flags are plain strings suitable for embedding directly in the audit
log and the user-facing output.
"""

import logging
from dataclasses import dataclass, field

from agent.executor import QueryResult

logger = logging.getLogger(__name__)

# Column name substrings that indicate a monetary/value column.
_REVENUE_COLUMN_HINTS: frozenset[str] = frozenset(
    {
        "revenue",
        "amount",
        "value",
        "price",
    }
)

# Null rate above this threshold triggers a flag (0.0 – 1.0 scale).
_HIGH_NULL_THRESHOLD: float = 0.50


@dataclass
class ValidationReport:
    """The result of validate(). Always produced, even for clean results.

    Attributes:
        flags: List of human-readable warning strings. Empty means clean.
        zero_rows: True when the result set contains no data rows. The
            insight generator uses this to avoid fabricating numbers.
    """

    flags: list[str] = field(default_factory=list)
    zero_rows: bool = False

    @property
    def is_clean(self) -> bool:
        """True when no anomalies were detected."""
        return not self.flags


def validate(result: QueryResult) -> ValidationReport:
    """Run all sanity checks on result and return a ValidationReport.

    Never raises. All anomalies are recorded as flags. A result with flags
    is still valid — it is returned to the user with the flags appended so
    the business stakeholder can decide whether to investigate.

    Args:
        result: The QueryResult from AthenaExecutor.execute().

    Returns:
        ValidationReport with zero or more flags and a zero_rows indicator.
    """
    report = ValidationReport()

    if not result.rows:
        report.zero_rows = True
        report.flags.append(
            "Zero rows returned. The query executed successfully but no data "
            "matched the filters. This is a valid result for a Gold aggregation "
            "table — the underlying data may not yet exist for the requested period."
        )
        logger.info(
            "Result validation: zero rows for execution_id=%s.",
            result.execution_id,
        )
        return report

    _check_negative_revenue(result, report)
    _check_high_null_rate(result, report)

    if report.flags:
        logger.warning(
            "Result validation flagged %d issue(s) for execution_id=%s: %s",
            len(report.flags),
            result.execution_id,
            "; ".join(report.flags),
        )
    else:
        logger.debug(
            "Result validation clean for execution_id=%s (%d rows).",
            result.execution_id,
            len(result.rows),
        )

    return report


# ── Private checks ─────────────────────────────────────────────────────────────


def _is_revenue_column(name: str) -> bool:
    """Return True if the column name suggests a monetary value."""
    lower = name.lower()
    return any(hint in lower for hint in _REVENUE_COLUMN_HINTS)


def _check_negative_revenue(result: QueryResult, report: ValidationReport) -> None:
    """Flag any monetary column that contains a negative value."""
    revenue_cols = [c for c in result.columns if _is_revenue_column(c)]

    for col in revenue_cols:
        for row in result.rows:
            raw = row.get(col, "").strip()
            if not raw:
                continue
            try:
                val = float(raw)
            except ValueError:
                continue
            if val < 0:
                report.flags.append(
                    f"Negative value detected in column '{col}' (value: {raw}). "
                    f"Revenue and amount columns should not be negative. "
                    f"Check the upstream Glue or dbt transformation for this table."
                )
                # One flag per column is enough — don't flood with per-row flags.
                break


def _check_high_null_rate(result: QueryResult, report: ValidationReport) -> None:
    """Flag any column where more than 50% of values are empty (NULL)."""
    total_rows = len(result.rows)
    if total_rows == 0:
        return

    for col in result.columns:
        null_count = sum(1 for row in result.rows if not row.get(col, "").strip())
        null_rate = null_count / total_rows
        if null_rate > _HIGH_NULL_THRESHOLD:
            report.flags.append(
                f"High null rate in column '{col}': "
                f"{null_count}/{total_rows} rows ({null_rate:.0%}) are empty. "
                f"This may indicate a join failure or missing upstream data."
            )
