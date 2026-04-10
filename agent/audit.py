"""Audit log writer: records every query as a structured JSON record in S3.

Every question the agent answers writes one JSON object to:

    s3://{bronze_bucket}/metadata/agent-audit/YYYY/MM/DD/{execution_id}.json

Partitioned by date so the audit log is itself queryable via Athena using
a simple partition projection — no crawler needed.

The audit log is non-fatal. If the S3 write fails the agent logs the error
at ERROR level and returns the result to the user anyway. A broken audit
trail should never prevent the user from getting their answer.

Schema of each JSON record:
    timestamp       ISO-8601 UTC timestamp of the write
    question        The original plain-English question
    sql             The validated SQL that was executed
    assumptions     List of assumption strings from the SQL generator
    execution_id    Athena QueryExecutionId
    rows_returned   Number of rows in the result
    bytes_scanned   DataScannedInBytes from Athena statistics
    cost_usd        Estimated cost for this query
    validation_flags List of flag strings from result_validator (may be empty)
    insight         The plain-English insight string
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.exceptions import AuditLogError
from agent.insight import InsightResponse

logger = logging.getLogger(__name__)

# S3 key prefix for all audit records.
_AUDIT_PREFIX = "metadata/agent-audit"


class AuditLogger:
    """Writes structured JSON audit records to S3.

    Instantiate once per agent session.

    Usage:
        audit = AuditLogger(config.aws)
        audit.write(question, sql, insight_response)
        # Never raises — logs ERROR on failure and continues.
    """

    def __init__(self, config: AWSConfig) -> None:
        self._config = config
        self._s3 = boto3.client("s3", region_name=config.region)

    def write(
        self,
        question: str,
        sql: str,
        response: InsightResponse,
    ) -> None:
        """Write one audit record to S3.

        Non-fatal: catches all exceptions, logs at ERROR level, and returns
        normally. The caller (main.py / FastAPI endpoint) never needs to
        handle AuditLogError directly — it is raised internally and caught
        here so it can be included in the log message with full context.

        Args:
            question: The original plain-English question from the user.
            sql: The validated SQL that was executed.
            response: The InsightResponse from InsightGenerator.generate().
        """
        record = self._build_record(question, sql, response)
        key = self._s3_key(response.execution_id, record["timestamp"])

        try:
            self._s3.put_object(
                Bucket=self._config.bronze_bucket,
                Key=key,
                Body=json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(
                "Audit record written: s3://%s/%s",
                self._config.bronze_bucket,
                key,
            )
        except ClientError as exc:
            error = AuditLogError(
                f"Failed to write audit record to s3://{self._config.bronze_bucket}/{key}: {exc}"
            )
            logger.error(
                "Audit log write failed (non-fatal): %s",
                error,
                exc_info=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Unexpected error writing audit log (non-fatal): %s",
                exc,
                exc_info=True,
            )

    # ── Private helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _build_record(
        question: str,
        sql: str,
        response: InsightResponse,
    ) -> dict[str, Any]:
        """Build the JSON-serialisable audit record dict."""
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "question": question,
            "sql": sql,
            "assumptions": response.assumptions,
            "execution_id": response.execution_id,
            "rows_returned": 0,  # caller may not pass rows; execution_id is enough
            "bytes_scanned": response.bytes_scanned,
            "cost_usd": response.cost_usd,
            "validation_flags": response.validation_flags,
            "insight": response.insight,
        }

    @staticmethod
    def _s3_key(execution_id: str, timestamp: str) -> str:
        """Build the date-partitioned S3 key for an audit record.

        Format: metadata/agent-audit/YYYY/MM/DD/{execution_id}.json

        Args:
            execution_id: Athena QueryExecutionId (used as the filename).
            timestamp: ISO-8601 UTC timestamp string from the record.
        """
        # Parse just the date part from the ISO timestamp.
        # datetime.fromisoformat handles '2026-04-09T14:23:01.123456+00:00'.
        dt = datetime.fromisoformat(timestamp)
        date_path = dt.strftime("%Y/%m/%d")
        return f"{_AUDIT_PREFIX}/{date_path}/{execution_id}.json"
