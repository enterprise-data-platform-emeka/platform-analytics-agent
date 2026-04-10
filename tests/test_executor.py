"""Tests for cost.py and executor.py.

All Athena boto3 calls are mocked — no real AWS calls.
"""

from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.cost import _MIN_BYTES, _PRICE_PER_BYTE, bytes_to_usd
from agent.exceptions import ExecutionError
from agent.executor import AthenaExecutor, QueryResult

# ── Helpers ────────────────────────────────────────────────────────────────────

GOLD_DB = "edp_dev_gold"


def _aws_config() -> AWSConfig:
    return AWSConfig(
        region="eu-central-1",
        environment="dev",
        bronze_bucket="edp-dev-123456789012-bronze",
        gold_bucket="edp-dev-123456789012-gold",
        athena_results_bucket="edp-dev-123456789012-athena-results",
        athena_workgroup="edp-dev-workgroup",
        glue_gold_database=GOLD_DB,
        ssm_api_key_param="/edp/dev/anthropic_api_key",
    )


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "operation",
    )


def _execution_response(
    state: str,
    reason: str = "",
    bytes_scanned: int = 1024 * 1024 * 20,  # 20 MB
) -> dict[str, Any]:
    """Build a minimal GetQueryExecution response."""
    status: dict[str, Any] = {"State": state}
    if reason:
        status["StateChangeReason"] = reason
    stats: dict[str, Any] = {}
    if state == "SUCCEEDED":
        stats["DataScannedInBytes"] = bytes_scanned
    return {
        "QueryExecution": {
            "QueryExecutionId": "test-exec-id",
            "Status": status,
            "Statistics": stats,
        }
    }


def _column_info(names: list[str]) -> list[dict[str, str]]:
    return [{"Label": name, "Name": name, "Type": "varchar"} for name in names]


def _result_page(
    columns: list[str],
    data_rows: list[list[str]],
    include_header: bool = False,
) -> dict[str, Any]:
    """Build a minimal GetQueryResults page."""
    rows: list[dict[str, Any]] = []
    if include_header:
        rows.append({"Data": [{"VarCharValue": c} for c in columns]})
    for data_row in data_rows:
        rows.append({"Data": [{"VarCharValue": v} for v in data_row]})
    return {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": _column_info(columns)},
            "Rows": rows,
        }
    }


def _executor(max_rows: int = 1000) -> tuple[AthenaExecutor, MagicMock]:
    """Return (executor, mock_athena_client) with the boto3 client patched."""
    mock_athena = MagicMock()
    with patch("agent.executor.boto3.client", return_value=mock_athena):
        executor = AthenaExecutor(config=_aws_config(), max_rows=max_rows)
    return executor, mock_athena


# ═══════════════════════════════════════════════════════════════════════════════
# cost.py
# ═══════════════════════════════════════════════════════════════════════════════


class TestBytesToUsd:
    def test_zero_bytes_charged_at_minimum(self) -> None:
        result = bytes_to_usd(0)
        expected = round(_MIN_BYTES * _PRICE_PER_BYTE, 6)
        assert result == expected

    def test_below_minimum_charged_at_minimum(self) -> None:
        # 5 MB < 10 MB minimum
        result_5mb = bytes_to_usd(5 * 1024 * 1024)
        result_0 = bytes_to_usd(0)
        assert result_5mb == result_0

    def test_exactly_minimum_bytes(self) -> None:
        result = bytes_to_usd(_MIN_BYTES)
        expected = round(_MIN_BYTES * _PRICE_PER_BYTE, 6)
        assert result == expected

    def test_above_minimum_uses_actual_bytes(self) -> None:
        # 100 MB — well above the 10 MB floor
        bytes_100mb = 100 * 1024 * 1024
        result = bytes_to_usd(bytes_100mb)
        expected = round(bytes_100mb * _PRICE_PER_BYTE, 6)
        assert result == expected

    def test_above_minimum_costs_more_than_minimum(self) -> None:
        assert bytes_to_usd(100 * 1024 * 1024) > bytes_to_usd(0)

    def test_result_is_rounded_to_6_decimal_places(self) -> None:
        result = bytes_to_usd(50 * 1024 * 1024)
        assert result == round(result, 6)

    def test_negative_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match=">= 0"):
            bytes_to_usd(-1)

    def test_one_tb_costs_five_dollars(self) -> None:
        one_tb = 1024**4
        result = bytes_to_usd(one_tb)
        assert abs(result - 5.00) < 0.01  # within 1 cent of $5.00

    def test_typical_gold_query_cost_is_tiny(self) -> None:
        # A typical Gold table query scans ~4 MB.
        # Should cost less than $0.001 (charged as 10 MB minimum).
        result = bytes_to_usd(4 * 1024 * 1024)
        assert result < 0.001


# ═══════════════════════════════════════════════════════════════════════════════
# QueryResult
# ═══════════════════════════════════════════════════════════════════════════════


class TestQueryResult:
    def _result(
        self,
        columns: list[str] | None = None,
        rows: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        return QueryResult(
            execution_id="exec-123",
            columns=columns or ["country", "total_revenue"],
            rows=rows or [
                {"country": "Germany", "total_revenue": "432701.55"},
                {"country": "France", "total_revenue": "301245.20"},
            ],
            bytes_scanned=20 * 1024 * 1024,
            cost_usd=0.000095,
        )

    def test_to_markdown_has_header_row(self) -> None:
        md = self._result().to_markdown()
        assert "| country | total_revenue |" in md

    def test_to_markdown_has_separator_row(self) -> None:
        md = self._result().to_markdown()
        assert "| --- | --- |" in md

    def test_to_markdown_has_data_rows(self) -> None:
        md = self._result().to_markdown()
        assert "| Germany | 432701.55 |" in md
        assert "| France | 301245.20 |" in md

    def test_to_markdown_empty_rows_returns_placeholder(self) -> None:
        md = self._result(rows=[]).to_markdown()
        assert "No rows" in md

    def test_to_markdown_column_order_preserved(self) -> None:
        result = self._result(
            columns=["z_col", "a_col"],
            rows=[{"z_col": "z_val", "a_col": "a_val"}],
        )
        md = result.to_markdown()
        # z_col must appear before a_col in the header
        assert md.index("z_col") < md.index("a_col")

    def test_to_markdown_missing_value_rendered_as_empty(self) -> None:
        result = self._result(
            columns=["a", "b"],
            rows=[{"a": "val_a"}],  # 'b' missing
        )
        md = result.to_markdown()
        assert "| val_a |  |" in md


# ═══════════════════════════════════════════════════════════════════════════════
# AthenaExecutor._start
# ═══════════════════════════════════════════════════════════════════════════════


class TestAthenaExecutorStart:
    def test_calls_start_query_execution(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "exec-abc"
        }
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["col"], [["val"]], include_header=True)
        ]

        executor.execute("SELECT 1 LIMIT 1")

        mock_athena.start_query_execution.assert_called_once()

    def test_passes_sql_to_athena(self) -> None:
        sql = "SELECT country, total_revenue FROM revenue_by_country LIMIT 10"
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["country"], [["Germany"]], include_header=True)
        ]

        executor.execute(sql)

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == sql

    def test_passes_gold_database_context(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["c"], [["v"]], include_header=True)
        ]

        executor.execute("SELECT 1 LIMIT 1")

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["QueryExecutionContext"]["Database"] == GOLD_DB

    def test_passes_correct_workgroup(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["c"], [["v"]], include_header=True)
        ]

        executor.execute("SELECT 1 LIMIT 1")

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        assert call_kwargs["WorkGroup"] == "edp-dev-workgroup"

    def test_passes_results_bucket_output_location(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["c"], [["v"]], include_header=True)
        ]

        executor.execute("SELECT 1 LIMIT 1")

        call_kwargs = mock_athena.start_query_execution.call_args[1]
        output = call_kwargs["ResultConfiguration"]["OutputLocation"]
        assert "edp-dev-123456789012-athena-results" in output

    def test_start_failure_raises_execution_error(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.side_effect = _client_error(
            "InvalidRequestException", "Invalid SQL"
        )

        with pytest.raises(ExecutionError, match="start Athena query"):
            executor.execute("SELECT 1 LIMIT 1")


# ═══════════════════════════════════════════════════════════════════════════════
# AthenaExecutor._poll
# ═══════════════════════════════════════════════════════════════════════════════


class TestAthenaExecutorPoll:
    def _setup(
        self,
        poll_responses: list[dict[str, Any]],
        result_pages: list[dict[str, Any]] | None = None,
    ) -> tuple[AthenaExecutor, MagicMock]:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "exec-poll"
        }
        mock_athena.get_query_execution.side_effect = poll_responses
        pages = result_pages or [
            _result_page(["col"], [["val"]], include_header=True)
        ]
        mock_athena.get_paginator.return_value.paginate.return_value = pages
        return executor, mock_athena

    def test_succeeds_after_one_running_poll(self) -> None:
        executor, mock_athena = self._setup([
            _execution_response("RUNNING"),
            _execution_response("SUCCEEDED"),
        ])
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.execution_id == "exec-poll"

    def test_polls_until_terminal_state(self) -> None:
        executor, mock_athena = self._setup([
            _execution_response("QUEUED"),
            _execution_response("RUNNING"),
            _execution_response("RUNNING"),
            _execution_response("SUCCEEDED"),
        ])
        with patch("agent.executor.time.sleep"):
            executor.execute("SELECT 1 LIMIT 1")
        assert mock_athena.get_query_execution.call_count == 4

    def test_failed_state_raises_execution_error(self) -> None:
        executor, mock_athena = self._setup([
            _execution_response("FAILED", reason="Table not found"),
        ])
        with patch("agent.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="failed"):
                executor.execute("SELECT 1 LIMIT 1")

    def test_failed_error_includes_reason(self) -> None:
        executor, mock_athena = self._setup([
            _execution_response("FAILED", reason="Column 'xyz' does not exist"),
        ])
        with patch("agent.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="xyz"):
                executor.execute("SELECT 1 LIMIT 1")

    def test_cancelled_state_raises_execution_error(self) -> None:
        executor, mock_athena = self._setup([
            _execution_response("CANCELLED"),
        ])
        with patch("agent.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="cancelled"):
                executor.execute("SELECT 1 LIMIT 1")

    def test_throttling_during_poll_retried(self) -> None:
        executor, mock_athena = self._setup([
            _client_error("ThrottlingException"),
            _execution_response("SUCCEEDED"),
        ])
        mock_athena.get_query_execution.side_effect = [
            _client_error("ThrottlingException"),
            _execution_response("SUCCEEDED"),
        ]
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.execution_id == "exec-poll"

    def test_non_throttle_poll_error_raises(self) -> None:
        executor, mock_athena = self._setup([])
        mock_athena.get_query_execution.side_effect = _client_error(
            "AccessDeniedException"
        )
        with patch("agent.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="polling"):
                executor.execute("SELECT 1 LIMIT 1")

    def test_timeout_raises_execution_error(self) -> None:
        executor, mock_athena = self._setup([])
        mock_athena.get_query_execution.return_value = _execution_response("RUNNING")
        mock_athena.stop_query_execution.return_value = {}

        # Simulate elapsed time exceeding timeout on first check.
        with patch("agent.executor.time.monotonic") as mock_time:
            mock_time.side_effect = [0.0, 200.0]  # start, then past timeout
            with patch("agent.executor.time.sleep"):
                with pytest.raises(ExecutionError, match="timed out"):
                    executor.execute("SELECT 1 LIMIT 1")

    def test_timeout_attempts_cancellation(self) -> None:
        executor, mock_athena = self._setup([])
        mock_athena.get_query_execution.return_value = _execution_response("RUNNING")
        mock_athena.stop_query_execution.return_value = {}

        with patch("agent.executor.time.monotonic") as mock_time:
            mock_time.side_effect = [0.0, 200.0]
            with patch("agent.executor.time.sleep"):
                with pytest.raises(ExecutionError):
                    executor.execute("SELECT 1 LIMIT 1")
        mock_athena.stop_query_execution.assert_called_once()

    def test_bytes_scanned_recorded_from_statistics(self) -> None:
        executor, mock_athena = self._setup(
            [_execution_response("SUCCEEDED", bytes_scanned=50 * 1024 * 1024)],
            result_pages=[_result_page(["c"], [["v"]], include_header=True)],
        )
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.bytes_scanned == 50 * 1024 * 1024

    def test_cost_usd_calculated_from_bytes_scanned(self) -> None:
        bytes_scanned = 50 * 1024 * 1024
        executor, mock_athena = self._setup(
            [_execution_response("SUCCEEDED", bytes_scanned=bytes_scanned)],
            result_pages=[_result_page(["c"], [["v"]], include_header=True)],
        )
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.cost_usd == bytes_to_usd(bytes_scanned)

    def test_missing_statistics_defaults_to_zero_bytes(self) -> None:
        executor, mock_athena = self._setup([])
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": "exec-poll",
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {},  # DataScannedInBytes absent
            }
        }
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["c"], [["v"]], include_header=True)
        ]
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.bytes_scanned == 0


# ═══════════════════════════════════════════════════════════════════════════════
# AthenaExecutor._fetch_results
# ═══════════════════════════════════════════════════════════════════════════════


class TestAthenaExecutorFetchResults:
    def _run(
        self,
        pages: list[dict[str, Any]],
        max_rows: int = 1000,
    ) -> QueryResult:
        executor, mock_athena = _executor(max_rows=max_rows)
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-r"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = pages
        with patch("agent.executor.time.sleep"):
            return executor.execute("SELECT 1 LIMIT 1")

    def test_columns_extracted_from_metadata(self) -> None:
        result = self._run([
            _result_page(["country", "total_revenue"], [["Germany", "432701.55"]], include_header=True)
        ])
        assert result.columns == ["country", "total_revenue"]

    def test_rows_are_dicts_keyed_by_column_name(self) -> None:
        result = self._run([
            _result_page(
                ["country", "total_revenue"],
                [["Germany", "432701.55"], ["France", "301245.20"]],
                include_header=True,
            )
        ])
        assert result.rows[0] == {"country": "Germany", "total_revenue": "432701.55"}
        assert result.rows[1] == {"country": "France", "total_revenue": "301245.20"}

    def test_header_row_is_stripped(self) -> None:
        # Athena includes column names as first data row on page 1.
        result = self._run([
            _result_page(["a", "b"], [["1", "2"]], include_header=True)
        ])
        # Should have only 1 data row, not 2 (one being the header).
        assert len(result.rows) == 1
        assert result.rows[0] == {"a": "1", "b": "2"}

    def test_empty_result_set_returns_empty_rows(self) -> None:
        result = self._run([
            _result_page(["a", "b"], [], include_header=True)
        ])
        assert result.rows == []
        assert result.columns == ["a", "b"]

    def test_multi_page_results_merged(self) -> None:
        result = self._run([
            _result_page(["country"], [["Germany"], ["France"]], include_header=True),
            _result_page(["country"], [["Spain"], ["Italy"]]),
        ])
        countries = [r["country"] for r in result.rows]
        assert countries == ["Germany", "France", "Spain", "Italy"]

    def test_rows_capped_at_max_rows(self) -> None:
        # max_rows=2 but 5 data rows provided.
        result = self._run(
            [
                _result_page(
                    ["val"],
                    [["a"], ["b"], ["c"], ["d"], ["e"]],
                    include_header=True,
                )
            ],
            max_rows=2,
        )
        assert len(result.rows) == 2

    def test_fetch_results_client_error_raises(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-r"}
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.side_effect = _client_error(
            "AccessDeniedException"
        )
        with patch("agent.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="fetch results"):
                executor.execute("SELECT 1 LIMIT 1")

    def test_execution_id_in_result(self) -> None:
        executor, mock_athena = _executor()
        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "unique-exec-id-999"
        }
        mock_athena.get_query_execution.return_value = _execution_response("SUCCEEDED")
        mock_athena.get_paginator.return_value.paginate.return_value = [
            _result_page(["c"], [["v"]], include_header=True)
        ]
        with patch("agent.executor.time.sleep"):
            result = executor.execute("SELECT 1 LIMIT 1")
        assert result.execution_id == "unique-exec-id-999"

    def test_execution_error_is_agent_error(self) -> None:
        from agent.exceptions import AgentError

        executor, mock_athena = _executor()
        mock_athena.start_query_execution.side_effect = _client_error("AccessDenied")
        with pytest.raises(AgentError):
            executor.execute("SELECT 1 LIMIT 1")
