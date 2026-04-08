"""Named exception hierarchy for the Analytics Agent.

Every failure in the agent raises one of these exceptions. Callers catch
only what they expect and let everything else crash loudly. Silent failures
hide bugs — this hierarchy makes the failure category explicit.
"""


class AgentError(Exception):
    """Base class for all agent errors. Never raise this directly."""


class ConfigurationError(AgentError):
    """A required environment variable is missing or has an invalid value.

    Raised at startup before any AWS or Claude calls are made. If this
    is raised, nothing works and the process should exit immediately.
    """


class SchemaResolutionError(AgentError):
    """The schema resolver could not load Gold table metadata.

    Raised when the Glue Catalog is unreachable, the Gold database does
    not exist, or the dbt catalog.json is malformed.
    """


class SQLValidationError(AgentError):
    """The generated SQL failed a guardrail check.

    Includes the reason string so it can be sent back to Claude for
    correction. Raised when the SQL is not a SELECT, references a
    non-Gold database, contains DDL keywords, or is missing a LIMIT.
    """

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class SQLGenerationError(AgentError):
    """Claude could not produce valid SQL after the maximum number of attempts.

    Raised when the validation feedback loop exhausts all retries without
    producing a query that passes guardrail checks.
    """


class ExecutionError(AgentError):
    """Athena rejected or failed the query.

    Raised on query syntax errors, permission denied, or Athena internal
    errors that are not transient. Transient errors (throttling, timeout)
    are retried inside the executor and never surface as ExecutionError.
    """


class ResultValidationError(AgentError):
    """The query result failed a sanity check so severe it cannot be surfaced.

    In practice, result validation flags anomalies rather than raising.
    This exception is reserved for unrecoverable result problems (for
    example, the result CSV from S3 is malformed and cannot be parsed).
    """


class InsightGenerationError(AgentError):
    """Claude returned a malformed or empty insight response.

    Raised when the insight prompt produces output that cannot be parsed
    into a structured insight string.
    """


class AuditLogError(AgentError):
    """The audit log could not be written to S3.

    Non-fatal in production — the agent returns the result to the user
    even if the audit write fails. Logged at ERROR level.
    """
