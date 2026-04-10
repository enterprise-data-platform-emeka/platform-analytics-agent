"""Tests for audit.py — AuditLogger."""

import json
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from agent.audit import _AUDIT_PREFIX, AuditLogger
from agent.config import AWSConfig
from agent.insight import InsightResponse

# ── Helpers ────────────────────────────────────────────────────────────────────


def _aws_config() -> AWSConfig:
    return AWSConfig(
        region="eu-central-1",
        environment="dev",
        bronze_bucket="edp-dev-123456789012-bronze",
        gold_bucket="edp-dev-123456789012-gold",
        athena_results_bucket="edp-dev-123456789012-athena-results",
        athena_workgroup="edp-dev-workgroup",
        glue_gold_database="edp_dev_gold",
        ssm_api_key_param="/edp/dev/anthropic_api_key",
    )


def _response(
    insight: str = "Germany leads with £432k revenue.",
    assumptions: list[str] | None = None,
    flags: list[str] | None = None,
    execution_id: str = "exec-audit-test",
    bytes_scanned: int = 20 * 1024 * 1024,
    cost_usd: float = 0.000095,
) -> InsightResponse:
    return InsightResponse(
        insight=insight,
        assumptions=assumptions or ["Table: revenue_by_country"],
        validation_flags=flags or [],
        execution_id=execution_id,
        bytes_scanned=bytes_scanned,
        cost_usd=cost_usd,
    )


def _logger() -> tuple[AuditLogger, MagicMock]:
    """Return (AuditLogger, mock_s3_client)."""
    mock_s3 = MagicMock()
    mock_s3.put_object.return_value = {}
    with patch("agent.audit.boto3.client", return_value=mock_s3):
        logger = AuditLogger(config=_aws_config())
    return logger, mock_s3


QUESTION = "Which country has the highest revenue?"
SQL = "SELECT country, total_revenue FROM revenue_by_country ORDER BY total_revenue DESC LIMIT 1"


# ── S3 key format ──────────────────────────────────────────────────────────────


class TestS3KeyFormat:
    def test_key_starts_with_audit_prefix(self) -> None:
        from agent.audit import AuditLogger
        key = AuditLogger._s3_key("exec-123", "2026-04-09T14:23:01+00:00")
        assert key.startswith(_AUDIT_PREFIX)

    def test_key_contains_year_month_day(self) -> None:
        from agent.audit import AuditLogger
        key = AuditLogger._s3_key("exec-123", "2026-04-09T14:23:01+00:00")
        assert "2026/04/09" in key

    def test_key_ends_with_execution_id_json(self) -> None:
        from agent.audit import AuditLogger
        key = AuditLogger._s3_key("my-exec-id", "2026-04-09T14:23:01+00:00")
        assert key.endswith("my-exec-id.json")

    def test_key_full_format(self) -> None:
        from agent.audit import AuditLogger
        key = AuditLogger._s3_key("exec-xyz", "2026-04-09T00:00:00+00:00")
        assert key == f"{_AUDIT_PREFIX}/2026/04/09/exec-xyz.json"


# ── Record structure ───────────────────────────────────────────────────────────


class TestRecordStructure:
    def _get_written_record(self) -> dict:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        body_bytes = mock_s3.put_object.call_args[1]["Body"]
        return json.loads(body_bytes.decode("utf-8"))

    def test_record_has_timestamp(self) -> None:
        record = self._get_written_record()
        assert "timestamp" in record
        assert record["timestamp"]

    def test_record_has_question(self) -> None:
        record = self._get_written_record()
        assert record["question"] == QUESTION

    def test_record_has_sql(self) -> None:
        record = self._get_written_record()
        assert record["sql"] == SQL

    def test_record_has_assumptions(self) -> None:
        record = self._get_written_record()
        assert record["assumptions"] == ["Table: revenue_by_country"]

    def test_record_has_execution_id(self) -> None:
        record = self._get_written_record()
        assert record["execution_id"] == "exec-audit-test"

    def test_record_has_bytes_scanned(self) -> None:
        record = self._get_written_record()
        assert record["bytes_scanned"] == 20 * 1024 * 1024

    def test_record_has_cost_usd(self) -> None:
        record = self._get_written_record()
        assert record["cost_usd"] == 0.000095

    def test_record_has_validation_flags(self) -> None:
        record = self._get_written_record()
        assert "validation_flags" in record
        assert isinstance(record["validation_flags"], list)

    def test_record_has_insight(self) -> None:
        record = self._get_written_record()
        assert record["insight"] == "Germany leads with £432k revenue."

    def test_validation_flags_populated_when_present(self) -> None:
        audit_logger, mock_s3 = _logger()
        resp = _response(flags=["Negative revenue detected."])
        audit_logger.write(QUESTION, SQL, resp)
        body = json.loads(mock_s3.put_object.call_args[1]["Body"].decode())
        assert "Negative revenue detected." in body["validation_flags"]

    def test_record_is_valid_json(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        body_bytes = mock_s3.put_object.call_args[1]["Body"]
        # Should not raise
        parsed = json.loads(body_bytes.decode("utf-8"))
        assert isinstance(parsed, dict)


# ── S3 put_object call ─────────────────────────────────────────────────────────


class TestS3PutObjectCall:
    def test_put_object_called_once(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        mock_s3.put_object.assert_called_once()

    def test_put_to_bronze_bucket(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "edp-dev-123456789012-bronze"

    def test_content_type_is_json(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/json"

    def test_key_contains_execution_id(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response(execution_id="my-unique-exec"))
        call_kwargs = mock_s3.put_object.call_args[1]
        assert "my-unique-exec" in call_kwargs["Key"]

    def test_key_is_date_partitioned(self) -> None:
        audit_logger, mock_s3 = _logger()
        audit_logger.write(QUESTION, SQL, _response())
        call_kwargs = mock_s3.put_object.call_args[1]
        # Key must contain at least one date segment (YYYY/MM/DD pattern)
        import re
        assert re.search(r"\d{4}/\d{2}/\d{2}", call_kwargs["Key"])


# ── Non-fatal error handling ───────────────────────────────────────────────────


class TestNonFatalErrors:
    def test_s3_client_error_does_not_raise(self) -> None:
        audit_logger, mock_s3 = _logger()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        # Must not raise
        audit_logger.write(QUESTION, SQL, _response())

    def test_unexpected_exception_does_not_raise(self) -> None:
        audit_logger, mock_s3 = _logger()
        mock_s3.put_object.side_effect = RuntimeError("network failure")
        # Must not raise
        audit_logger.write(QUESTION, SQL, _response())

    def test_returns_none_on_success(self) -> None:
        audit_logger, _ = _logger()
        result = audit_logger.write(QUESTION, SQL, _response())
        assert result is None

    def test_returns_none_on_failure(self) -> None:
        audit_logger, mock_s3 = _logger()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
            "PutObject",
        )
        result = audit_logger.write(QUESTION, SQL, _response())
        assert result is None
