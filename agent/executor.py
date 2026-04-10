"""Athena query executor.

Submits validated SQL to Athena, polls until completion, reads results,
and records the actual bytes scanned and cost.

One public entry point: AthenaExecutor.execute(sql) -> QueryResult.

Polling strategy:
  - First 5 polls: 1 second apart (catches fast Gold table queries)
  - Subsequent polls: 3 seconds apart (avoids hammering the API)
  - Hard timeout: 120 seconds (Gold queries on pre-aggregated tables
    should never take this long; if they do, something is wrong)

Error handling:
  - Transient throttling during polling: retry silently with backoff
  - FAILED query state: raise ExecutionError immediately — Athena already
    retried internally; these are permanent failures
  - CANCELLED query state: raise ExecutionError — external cancellation
  - Timeout: raise ExecutionError after 120 seconds

Results are read via GetQueryResults pagination (up to max_rows rows).
The first result page from Athena always contains a header row; this is
stripped before building the dict list.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.cost import bytes_to_usd
from agent.exceptions import ExecutionError

logger = logging.getLogger(__name__)

# Polling intervals in seconds. The first _FAST_POLL_COUNT polls use
# _FAST_POLL_INTERVAL; after that _SLOW_POLL_INTERVAL is used.
_FAST_POLL_COUNT: int = 5
_FAST_POLL_INTERVAL: float = 1.0
_SLOW_POLL_INTERVAL: float = 3.0
_TIMEOUT_SECONDS: float = 120.0

# Athena terminal query states.
_TERMINAL_STATES: frozenset[str] = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


@dataclass
class QueryResult:
    """The result of a successful Athena query execution.

    Attributes:
        execution_id: Athena QueryExecutionId. Stored in the audit log and
            useful for debugging — paste it into the Athena console to see
            the full query history entry.
        columns: Ordered list of column names from the result set.
        rows: List of dicts mapping column name to string value. Athena
            returns all values as strings; callers cast as needed.
        bytes_scanned: Actual DataScannedInBytes from Athena statistics.
        cost_usd: Estimated cost calculated from bytes_scanned with the
            10 MB minimum floor applied.
    """

    execution_id: str
    columns: list[str]
    rows: list[dict[str, str]]
    bytes_scanned: int
    cost_usd: float

    def to_markdown(self) -> str:
        """Format the result as a GitHub-flavoured markdown table.

        Used by the insight generator to give Claude a compact, readable
        view of the data. Returns a placeholder string for empty results
        so the insight generator always receives non-empty input.
        """
        if not self.rows:
            return "_No rows returned._"

        header = "| " + " | ".join(self.columns) + " |"
        separator = "| " + " | ".join("---" for _ in self.columns) + " |"
        data_rows = [
            "| " + " | ".join(str(row.get(col, "")) for col in self.columns) + " |"
            for row in self.rows
        ]
        return "\n".join([header, separator, *data_rows])


class AthenaExecutor:
    """Executes validated SQL against Athena and returns structured results.

    Instantiate once per agent session.

    Usage:
        executor = AthenaExecutor(config.aws, max_rows=1000)
        result = executor.execute(validated_sql)
    """

    def __init__(self, config: AWSConfig, max_rows: int) -> None:
        self._config = config
        self._max_rows = max_rows
        self._athena = boto3.client("athena", region_name=config.region)

    def execute(self, sql: str) -> QueryResult:
        """Submit sql to Athena and return the full result.

        Args:
            sql: Validated SQL from SQLValidator. Must be a SELECT with a
                LIMIT clause. The validator guarantees this.

        Returns:
            QueryResult with rows, columns, bytes_scanned, and cost_usd.

        Raises:
            ExecutionError: if the query fails, is cancelled, or times out.
                Not raised on transient throttling during polling — those
                are retried silently.
        """
        execution_id = self._start(sql)
        logger.info("Athena query submitted: %s", execution_id)

        stats = self._poll(execution_id)

        bytes_scanned = stats.get("DataScannedInBytes", 0)
        cost = bytes_to_usd(bytes_scanned)
        logger.info(
            "Query %s completed: %d bytes scanned, $%.6f estimated cost.",
            execution_id,
            bytes_scanned,
            cost,
        )

        columns, rows = self._fetch_results(execution_id)
        logger.info(
            "Query %s returned %d rows across %d columns.",
            execution_id,
            len(rows),
            len(columns),
        )

        return QueryResult(
            execution_id=execution_id,
            columns=columns,
            rows=rows,
            bytes_scanned=bytes_scanned,
            cost_usd=cost,
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    def _start(self, sql: str) -> str:
        """Submit the query and return the QueryExecutionId."""
        try:
            response = self._athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={
                    "Database": self._config.glue_gold_database,
                },
                ResultConfiguration={
                    "OutputLocation": (
                        f"s3://{self._config.athena_results_bucket}/query-results/"
                    ),
                },
                WorkGroup=self._config.athena_workgroup,
            )
        except ClientError as exc:
            raise ExecutionError(
                f"Failed to start Athena query: {exc}"
            ) from exc

        return str(response["QueryExecutionId"])

    def _poll(self, execution_id: str) -> dict[str, Any]:
        """Poll GetQueryExecution until the query reaches a terminal state.

        Returns the QueryExecutionStatistics dict on SUCCEEDED.
        Raises ExecutionError on FAILED, CANCELLED, or timeout.
        """
        start_time = time.monotonic()
        poll_number = 0

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > _TIMEOUT_SECONDS:
                # Cancel the dangling query so it doesn't consume bytes.
                self._cancel(execution_id)
                raise ExecutionError(
                    f"Athena query {execution_id} timed out after "
                    f"{_TIMEOUT_SECONDS:.0f} seconds and was cancelled."
                )

            try:
                response = self._athena.get_query_execution(
                    QueryExecutionId=execution_id
                )
            except ClientError as exc:
                # Throttling during polling is transient. Sleep and retry.
                code = exc.response["Error"]["Code"]
                if code == "ThrottlingException":
                    logger.debug(
                        "Throttled polling query %s; backing off.", execution_id
                    )
                    time.sleep(_SLOW_POLL_INTERVAL)
                    continue
                raise ExecutionError(
                    f"Error polling Athena query {execution_id}: {exc}"
                ) from exc

            execution = response["QueryExecution"]
            state = execution["Status"]["State"]

            if state not in _TERMINAL_STATES:
                poll_number += 1
                interval = (
                    _FAST_POLL_INTERVAL
                    if poll_number <= _FAST_POLL_COUNT
                    else _SLOW_POLL_INTERVAL
                )
                logger.debug(
                    "Query %s state: %s (poll %d, %.1fs elapsed)",
                    execution_id,
                    state,
                    poll_number,
                    elapsed,
                )
                time.sleep(interval)
                continue

            if state == "SUCCEEDED":
                return dict(execution.get("Statistics", {}))

            # FAILED or CANCELLED
            reason = (
                execution["Status"].get("StateChangeReason", "no reason provided")
            )
            raise ExecutionError(
                f"Athena query {execution_id} {state.lower()}: {reason}"
            )

    def _cancel(self, execution_id: str) -> None:
        """Best-effort query cancellation. Logs but does not raise on failure."""
        try:
            self._athena.stop_query_execution(QueryExecutionId=execution_id)
            logger.info("Cancelled timed-out query %s.", execution_id)
        except ClientError as exc:
            logger.warning(
                "Could not cancel query %s: %s", execution_id, exc
            )

    def _fetch_results(self, execution_id: str) -> tuple[list[str], list[dict[str, str]]]:
        """Paginate through GetQueryResults and return (columns, rows).

        Athena's first result page always contains a header row as the
        first data item. This is stripped; subsequent pages do not include
        a header row.

        Caps at self._max_rows rows. Any rows beyond the cap are discarded
        (the SQL already has a LIMIT, so this is a safety net only).
        """
        paginator = self._athena.get_paginator("get_query_results")
        columns: list[str] = []
        rows: list[dict[str, str]] = []
        first_page = True

        try:
            for page in paginator.paginate(QueryExecutionId=execution_id):
                result_set = page["ResultSet"]

                if first_page:
                    columns = [
                        col["Label"]
                        for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                    ]
                    first_page = False

                for row in result_set["Rows"]:
                    # Skip the header row that Athena includes on the first page.
                    values = [datum.get("VarCharValue", "") for datum in row["Data"]]
                    if values == columns:
                        continue

                    if len(rows) >= self._max_rows:
                        logger.debug(
                            "Result cap of %d rows reached; discarding remaining.",
                            self._max_rows,
                        )
                        return columns, rows

                    rows.append(dict(zip(columns, values)))

        except ClientError as exc:
            raise ExecutionError(
                f"Failed to fetch results for query {execution_id}: {exc}"
            ) from exc

        return columns, rows
