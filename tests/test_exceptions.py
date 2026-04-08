"""Tests for the exception hierarchy."""

import pytest

from agent.exceptions import (
    AgentError,
    AuditLogError,
    ConfigurationError,
    ExecutionError,
    InsightGenerationError,
    ResultValidationError,
    SQLGenerationError,
    SQLValidationError,
    SchemaResolutionError,
)


def test_all_exceptions_inherit_from_agent_error() -> None:
    for exc_class in (
        ConfigurationError,
        SchemaResolutionError,
        SQLValidationError,
        SQLGenerationError,
        ExecutionError,
        ResultValidationError,
        InsightGenerationError,
        AuditLogError,
    ):
        assert issubclass(exc_class, AgentError)


def test_agent_error_is_exception() -> None:
    assert issubclass(AgentError, Exception)


def test_sql_validation_error_carries_reason() -> None:
    err = SQLValidationError("SQL failed guardrail", reason="DDL keyword DROP found")
    assert err.reason == "DDL keyword DROP found"
    assert "SQL failed guardrail" in str(err)


def test_sql_validation_error_is_catchable_as_agent_error() -> None:
    with pytest.raises(AgentError):
        raise SQLValidationError("bad sql", reason="not a SELECT")


def test_configuration_error_message() -> None:
    err = ConfigurationError("Missing: ['BRONZE_BUCKET']")
    assert "BRONZE_BUCKET" in str(err)
