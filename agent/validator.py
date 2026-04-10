"""SQL validator: guardrail checks before every Athena execution.

Every SQL query passes through validate() before being sent to Athena.
The validator rejects unsafe queries immediately and injects a LIMIT clause
if Claude omitted one.

Rules enforced, in order:
  1. Non-empty, single statement
  2. No forbidden DDL/DML keywords (checked before statement type so CTEs
     containing write operations are still caught)
  3. SELECT statement only (WITH ... SELECT ... CTEs are permitted)
  4. No references to non-Gold databases
  5. LIMIT present and within max_rows (injected or capped as needed)

The validator does not know about table or column names. That is the job of the
SQL generator. The validator only enforces structural safety rules.
"""

import logging
import re
from typing import Final

import sqlparse

from agent.exceptions import SQLValidationError

logger = logging.getLogger(__name__)

# Keywords that must never appear anywhere in the query, including inside CTEs
# and subqueries. Checked against the uppercased raw SQL with word-boundary
# anchors so column names like 'last_updated' or 'created_at' do not trigger
# false positives ('_' is a word character in Python regex, so UPDATE does not
# match inside LAST_UPDATED or UPDATE_AT).
_FORBIDDEN_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "CREATE",
        "ALTER",
        "TRUNCATE",
    }
)

# Matches "database"."table" in Athena double-quoted identifier style.
# Captures the database name in group 1.
# Example: "edp_dev_bronze"."orders" -> group 1 = "edp_dev_bronze"
_QUOTED_DB_RE = re.compile(r'"([^"]+)"\s*\.\s*"[^"]+"', re.IGNORECASE)

# Matches unquoted EDP database names for the non-gold layers only.
# This avoids table-alias false positives (e.g. a table alias named 'revenue')
# by only matching identifiers that follow the edp_{env}_{layer} naming pattern
# where layer is a known non-gold layer.
# Example: edp_dev_bronze.orders -> match
# Example: revenue.order_date   -> no match (not an EDP database pattern)
_NON_GOLD_UNQUOTED_RE = re.compile(
    r"\b(edp_[a-z0-9]+_(?:bronze|silver|quarantine|athena_results))\s*\.",
    re.IGNORECASE,
)

# Matches a trailing LIMIT clause at the end of the statement.
# Using $ (end of string) so subquery LIMITs inside CTEs are not matched.
_TRAILING_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\s*$", re.IGNORECASE)


class SQLValidator:
    """Validates and normalises SQL before Athena execution.

    Instantiate once with the Gold database name and row cap, then call
    validate() on every query. All validation failures raise SQLValidationError
    with a reason string that is suitable for feeding directly back to Claude
    in the correction loop.

    Usage:
        validator = SQLValidator(
            gold_database=config.aws.glue_gold_database,
            max_rows=config.agent.max_rows,
        )
        safe_sql = validator.validate(raw_sql_from_claude)
    """

    def __init__(self, gold_database: str, max_rows: int) -> None:
        # Strip any accidental surrounding quotes so comparisons always work
        # against the bare database name regardless of how the config was set.
        self._gold_database = gold_database.strip('"').lower()
        self._max_rows = max_rows

    def validate(self, sql: str) -> str:
        """Validate sql and return it with a LIMIT clause guaranteed present.

        Args:
            sql: Raw SQL string from the SQL generator.

        Returns:
            Cleaned SQL with a trailing LIMIT <= max_rows.

        Raises:
            SQLValidationError: on any guardrail failure. The exception carries
                a 'reason' attribute with a description suitable for Claude.
        """
        sql = sql.strip().rstrip(";").strip()

        if not sql:
            raise SQLValidationError(
                "Empty SQL query.",
                reason="The query is empty. Generate a valid SELECT statement.",
            )

        self._check_forbidden_keywords(sql)
        self._check_is_select(sql)
        self._check_database_references(sql)
        sql = self._ensure_limit(sql)

        logger.debug("SQL passed all guardrail checks.")
        return sql

    # ── Private validators ─────────────────────────────────────────────────────

    def _check_forbidden_keywords(self, sql: str) -> None:
        """Reject queries that contain any DDL or write DML keyword.

        Checks the full raw SQL so that keywords hidden inside CTEs, subqueries,
        or string literals are still caught. Word-boundary anchors prevent
        column names that contain these words from triggering false positives.
        """
        upper = sql.upper()
        for keyword in _FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", upper):
                raise SQLValidationError(
                    f"Query contains forbidden keyword '{keyword}'.",
                    reason=(
                        f"The keyword '{keyword}' is not permitted. "
                        "Only read-only SELECT statements may be submitted."
                    ),
                )

    def _check_is_select(self, sql: str) -> None:
        """Reject anything that is not a single SELECT or WITH...SELECT statement.

        sqlparse returns None as the statement type for CTEs (WITH ... SELECT ...).
        Because forbidden keywords are already checked before this method is
        called, a statement that starts with WITH and has no forbidden keywords
        is safely assumed to be a CTE-based SELECT.
        """
        statements = sqlparse.parse(sql)

        if len(statements) != 1:
            raise SQLValidationError(
                f"Expected exactly one statement, got {len(statements)}.",
                reason="Submit exactly one SQL SELECT statement per request.",
            )

        stmt = statements[0]
        stmt_type = stmt.get_type()

        if stmt_type == "SELECT":
            return

        # sqlparse returns None for CTEs — treat WITH...SELECT as allowed.
        if stmt_type is None and sql.upper().lstrip().startswith("WITH"):
            return

        raise SQLValidationError(
            f"Query is not a SELECT statement (type: {stmt_type!r}).",
            reason=(
                f"Only SELECT statements are allowed. "
                f"The submitted statement appears to be of type '{stmt_type}'. "
                "Rewrite it as a SELECT query against a Gold table."
            ),
        )

    def _check_database_references(self, sql: str) -> None:
        """Reject queries that reference any database other than the Gold catalog.

        Two patterns are checked:
          - Athena double-quoted style: "database"."table"
          - Unquoted EDP non-gold names: edp_dev_bronze.table

        Unqualified table names (no database prefix) are always allowed because
        the Athena workgroup is configured to default to the Gold catalog.
        Short table aliases followed by a dot (t.column_name) are not matched
        by either pattern.
        """
        for match in _QUOTED_DB_RE.finditer(sql):
            db = match.group(1).lower()
            if db != self._gold_database:
                raise SQLValidationError(
                    f"Query references non-Gold database '{db}'.",
                    reason=(
                        f"Database '{db}' is not allowed. "
                        f"Only '{self._gold_database}' may be queried. "
                        f"Use unqualified table names or prefix with "
                        f'"{self._gold_database}"."table_name".'
                    ),
                )

        match = _NON_GOLD_UNQUOTED_RE.search(sql)
        if match:
            db = match.group(1).lower()
            raise SQLValidationError(
                f"Query references non-Gold database '{db}'.",
                reason=(
                    f"Database '{db}' is not allowed. "
                    f"Only '{self._gold_database}' may be queried. "
                    "Use unqualified table names for Gold tables."
                ),
            )

    def _ensure_limit(self, sql: str) -> str:
        """Inject LIMIT max_rows if absent, or cap an existing LIMIT that exceeds it.

        Uses a trailing-match regex so that LIMIT clauses inside subqueries or
        CTEs do not satisfy the top-level LIMIT requirement.
        """
        match = _TRAILING_LIMIT_RE.search(sql)

        if match is None:
            logger.debug("No LIMIT found; injecting LIMIT %d.", self._max_rows)
            return f"{sql}\nLIMIT {self._max_rows}"

        existing = int(match.group(1))
        if existing > self._max_rows:
            logger.debug("LIMIT %d exceeds max_rows %d; capping.", existing, self._max_rows)
            return _TRAILING_LIMIT_RE.sub(f"LIMIT {self._max_rows}", sql)

        return sql
