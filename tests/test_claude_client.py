"""Tests for ClaudeClient in agent/claude_client.py.

All tests mock the Anthropic SDK and boto3 — no real API calls are made.
The fixtures create a minimal ClaudeClient by passing api_key directly to
skip the SSM Parameter Store fetch.
"""

from typing import Any
from unittest.mock import MagicMock, patch

import anthropic
import pytest

from agent.claude_client import MODEL, ClaudeClient
from agent.config import AgentConfig, AWSConfig, Config
from agent.exceptions import (
    ConfigurationError,
    InsightGenerationError,
    SQLGenerationError,
)
from agent.schema import ColumnSchema, SchemaResolver, TableSchema

# ── Fixtures and helpers ───────────────────────────────────────────────────────

GOLD_DB = "edp_dev_gold"


def _config() -> Config:
    return Config(
        aws=AWSConfig(
            region="eu-central-1",
            environment="dev",
            bronze_bucket="edp-dev-123456789012-bronze",
            gold_bucket="edp-dev-123456789012-gold",
            athena_results_bucket="edp-dev-123456789012-athena-results",
            athena_workgroup="edp-dev-workgroup",
            glue_gold_database=GOLD_DB,
            ssm_api_key_param="/edp/dev/anthropic_api_key",
        ),
        agent=AgentConfig(
            cost_threshold_usd=0.10,
            max_rows=1000,
        ),
    )


def _schema_resolver() -> MagicMock:
    resolver = MagicMock(spec=SchemaResolver)
    resolver.get_schema.return_value = TableSchema(
        name="monthly_revenue_trend",
        database=GOLD_DB,
        description="Monthly revenue",
        columns=[ColumnSchema("order_year", "bigint", "")],
        partition_keys=[],
    )
    return resolver


def _client(resolver: MagicMock | None = None) -> ClaudeClient:
    """Create a ClaudeClient with a mocked Anthropic SDK."""
    return ClaudeClient(
        config=_config(),
        schema_resolver=resolver or _schema_resolver(),
        api_key="test-api-key",
    )


def _text_block(text: str) -> MagicMock:
    """Create a mock TextBlock."""
    block = MagicMock()
    block.type = "text"
    block.text = text
    return block


def _tool_use_block(tool_id: str, table_name: str) -> MagicMock:
    """Create a mock ToolUseBlock."""
    block = MagicMock()
    block.type = "tool_use"
    block.id = tool_id
    block.name = "get_schema"
    block.input = {"table_name": table_name}
    return block


def _message(
    content: list[Any],
    stop_reason: str = "end_turn",
) -> MagicMock:
    """Create a mock anthropic.types.Message."""
    msg = MagicMock(spec=anthropic.types.Message)
    msg.content = content
    msg.stop_reason = stop_reason
    return msg


def _sql_response(sql: str, assumptions: list[str]) -> MagicMock:
    assumptions_text = "\n".join(f"- {a}" for a in assumptions)
    text = f"<sql>\n{sql}\n</sql>\n<assumptions>\n{assumptions_text}\n</assumptions>"
    return _message([_text_block(text)])


# ── SSM key fetch ──────────────────────────────────────────────────────────────


class TestFetchApiKey:
    def test_fetches_from_ssm(self) -> None:
        with patch("agent.claude_client.boto3.client") as mock_boto:
            ssm = MagicMock()
            ssm.get_parameter.return_value = {"Parameter": {"Value": "sk-real-key"}}
            mock_boto.return_value = ssm
            with patch("agent.claude_client.anthropic.Anthropic"):
                ClaudeClient(
                    config=_config(),
                    schema_resolver=_schema_resolver(),
                )
            ssm.get_parameter.assert_called_once_with(
                Name="/edp/dev/anthropic_api_key",
                WithDecryption=True,
            )

    def test_parameter_not_found_raises_configuration_error(self) -> None:
        from botocore.exceptions import ClientError

        with patch("agent.claude_client.boto3.client") as mock_boto:
            ssm = MagicMock()
            ssm.get_parameter.side_effect = ClientError(
                {"Error": {"Code": "ParameterNotFound", "Message": "not found"}},
                "GetParameter",
            )
            mock_boto.return_value = ssm
            with pytest.raises(ConfigurationError, match="anthropic_api_key"):
                ClaudeClient(
                    config=_config(),
                    schema_resolver=_schema_resolver(),
                )

    def test_api_key_override_skips_ssm(self) -> None:
        with patch("agent.claude_client.boto3.client") as mock_boto:
            with patch("agent.claude_client.anthropic.Anthropic"):
                ClaudeClient(
                    config=_config(),
                    schema_resolver=_schema_resolver(),
                    api_key="override-key",
                )
            mock_boto.assert_not_called()


# ── generate_sql — happy path ──────────────────────────────────────────────────


class TestGenerateSqlHappyPath:
    def test_returns_sql_and_assumptions(self) -> None:
        client = _client()
        sql = "SELECT order_year, total_revenue FROM monthly_revenue_trend LIMIT 10"
        assumptions = ["Table: monthly_revenue_trend — best match for revenue over time"]

        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _sql_response(sql, assumptions)
            result_sql, result_assumptions = client.generate_sql(
                messages=[{"role": "user", "content": "Show revenue by year"}],
                system_prompt="system",
            )

        assert result_sql == sql
        assert result_assumptions == assumptions

    def test_calls_claude_with_correct_model(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _sql_response("SELECT 1 LIMIT 1", [])
            client.generate_sql(
                messages=[{"role": "user", "content": "test"}],
                system_prompt="sys",
            )
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["model"] == MODEL

    def test_tools_are_passed_to_api(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _sql_response("SELECT 1 LIMIT 1", [])
            client.generate_sql(
                messages=[{"role": "user", "content": "test"}],
                system_prompt="sys",
            )
        call_kwargs = mock_create.call_args[1]
        assert "tools" in call_kwargs
        assert call_kwargs["tools"][0]["name"] == "get_schema"

    def test_multiline_sql_is_parsed(self) -> None:
        client = _client()
        sql = "SELECT\n  order_year,\n  total_revenue\nFROM monthly_revenue_trend\nLIMIT 10"
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _sql_response(sql, [])
            result_sql, _ = client.generate_sql(
                messages=[{"role": "user", "content": "test"}],
                system_prompt="sys",
            )
        assert result_sql == sql

    def test_missing_assumptions_returns_empty_list(self) -> None:
        client = _client()
        text = "<sql>\nSELECT 1 LIMIT 1\n</sql>"
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block(text)])
            _, assumptions = client.generate_sql(
                messages=[{"role": "user", "content": "test"}],
                system_prompt="sys",
            )
        assert assumptions == []


# ── generate_sql — parsing failures ───────────────────────────────────────────


class TestGenerateSqlParsingFailures:
    def test_missing_sql_tag_raises(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("No SQL here at all.")])
            with pytest.raises(SQLGenerationError, match="<sql>"):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )

    def test_empty_sql_tag_raises(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("<sql>\n   \n</sql>")])
            with pytest.raises(SQLGenerationError, match="empty"):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )

    def test_sql_generation_error_is_agent_error(self) -> None:
        from agent.exceptions import AgentError

        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("no tags")])
            with pytest.raises(AgentError):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )


# ── generate_sql — retry on transient errors ──────────────────────────────────


class TestGenerateSqlRetry:
    def test_retries_on_rate_limit_then_succeeds(self) -> None:
        client = _client()
        success_response = _sql_response("SELECT 1 LIMIT 1", [])
        call_count = 0

        def side_effect(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise anthropic.RateLimitError(
                    message="rate limited",
                    response=MagicMock(status_code=429),
                    body={},
                )
            return success_response

        with patch.object(client._client.messages, "create", side_effect=side_effect):
            with patch("agent.claude_client.time.sleep"):
                sql, _ = client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )
        assert sql == "SELECT 1 LIMIT 1"
        assert call_count == 3

    def test_exhausted_retries_raises_sql_generation_error(self) -> None:
        client = _client()

        def always_rate_limit(**kwargs: Any) -> Any:
            raise anthropic.RateLimitError(
                message="rate limited",
                response=MagicMock(status_code=429),
                body={},
            )

        with patch.object(client._client.messages, "create", side_effect=always_rate_limit):
            with patch("agent.claude_client.time.sleep"):
                with pytest.raises(SQLGenerationError, match="retries"):
                    client.generate_sql(
                        messages=[{"role": "user", "content": "test"}],
                        system_prompt="sys",
                    )

    def test_non_transient_error_not_retried(self) -> None:
        client = _client()
        call_count = 0

        def bad_request(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            raise anthropic.BadRequestError(
                message="bad request",
                response=MagicMock(status_code=400),
                body={},
            )

        with patch.object(client._client.messages, "create", side_effect=bad_request):
            with pytest.raises(anthropic.BadRequestError):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )
        # Must only call once — no retry on non-transient errors.
        assert call_count == 1

    def test_sleep_called_between_retries(self) -> None:
        client = _client()
        success = _sql_response("SELECT 1 LIMIT 1", [])
        call_count = 0

        def side_effect(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise anthropic.RateLimitError(
                    message="rate limited",
                    response=MagicMock(status_code=429),
                    body={},
                )
            return success

        with patch.object(client._client.messages, "create", side_effect=side_effect):
            with patch("agent.claude_client.time.sleep") as mock_sleep:
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )
        mock_sleep.assert_called_once()


# ── generate_sql — tool-use handling ──────────────────────────────────────────


class TestToolUse:
    def test_tool_use_resolved_then_sql_returned(self) -> None:
        resolver = _schema_resolver()
        client = _client(resolver)

        tool_response = _message(
            [_tool_use_block("tool_123", "monthly_revenue_trend")],
            stop_reason="tool_use",
        )
        final_response = _sql_response(
            "SELECT order_year FROM monthly_revenue_trend LIMIT 10",
            ["Table: monthly_revenue_trend"],
        )
        responses = iter([tool_response, final_response])

        with patch.object(client._client.messages, "create", side_effect=lambda **kw: next(responses)):
            sql, assumptions = client.generate_sql(
                messages=[{"role": "user", "content": "show revenue by year"}],
                system_prompt="sys",
            )

        assert "monthly_revenue_trend" in sql
        resolver.get_schema.assert_called_once_with("monthly_revenue_trend")

    def test_unknown_tool_name_raises(self) -> None:
        client = _client()
        unknown_tool = MagicMock()
        unknown_tool.type = "tool_use"
        unknown_tool.id = "tool_999"
        unknown_tool.name = "execute_sql"  # not allowed
        unknown_tool.input = {}

        tool_response = _message([unknown_tool], stop_reason="tool_use")

        with patch.object(client._client.messages, "create", return_value=tool_response):
            with pytest.raises(SQLGenerationError, match="execute_sql"):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )

    def test_schema_resolver_failure_returns_error_text_not_raises(self) -> None:
        """A get_schema failure returns an error string so Claude can gracefully recover."""
        resolver = _schema_resolver()
        resolver.get_schema.side_effect = Exception("Glue unreachable")
        client = _client(resolver)

        tool_response = _message(
            [_tool_use_block("tool_abc", "nonexistent_table")],
            stop_reason="tool_use",
        )
        final_response = _sql_response("SELECT 1 LIMIT 1", [])
        responses = iter([tool_response, final_response])

        with patch.object(client._client.messages, "create", side_effect=lambda **kw: next(responses)):
            # Should not raise even though get_schema failed.
            sql, _ = client.generate_sql(
                messages=[{"role": "user", "content": "test"}],
                system_prompt="sys",
            )
        assert sql == "SELECT 1 LIMIT 1"

    def test_excessive_tool_rounds_raises(self) -> None:
        client = _client()

        def always_tool(**kwargs: Any) -> Any:
            return _message(
                [_tool_use_block("tool_loop", "monthly_revenue_trend")],
                stop_reason="tool_use",
            )

        with patch.object(client._client.messages, "create", side_effect=always_tool):
            with pytest.raises(SQLGenerationError, match="tool-use rounds"):
                client.generate_sql(
                    messages=[{"role": "user", "content": "test"}],
                    system_prompt="sys",
                )


# ── generate_insight ───────────────────────────────────────────────────────────


class TestGenerateInsight:
    def test_returns_insight_string(self) -> None:
        client = _client()
        insight = "Germany generated the highest revenue at £432,701, followed by France."

        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block(insight)])
            result = client.generate_insight(
                question="Which country has the most revenue?",
                sql="SELECT country, total_revenue FROM revenue_by_country ORDER BY total_revenue DESC LIMIT 1",
                result_markdown="| country | total_revenue |\n|---|---|\n| Germany | 432701.55 |",
            )

        assert result == insight

    def test_tools_not_passed_for_insight(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("An insight.")])
            client.generate_insight("question", "SELECT 1", "results")
        call_kwargs = mock_create.call_args[1]
        # tools should be absent or empty for insight calls
        assert not call_kwargs.get("tools")

    def test_empty_insight_raises(self) -> None:
        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("   ")])
            with pytest.raises(InsightGenerationError):
                client.generate_insight("question", "SELECT 1", "results")

    def test_insight_generation_error_is_agent_error(self) -> None:
        from agent.exceptions import AgentError

        client = _client()
        with patch.object(client._client.messages, "create") as mock_create:
            mock_create.return_value = _message([_text_block("")])
            with pytest.raises(AgentError):
                client.generate_insight("q", "SELECT 1", "r")

    def test_retries_on_timeout_for_insight(self) -> None:
        client = _client()
        call_count = 0

        def side_effect(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise anthropic.APITimeoutError(request=MagicMock())
            return _message([_text_block("Insight after retry.")])

        with patch.object(client._client.messages, "create", side_effect=side_effect):
            with patch("agent.claude_client.time.sleep"):
                result = client.generate_insight("q", "SELECT 1", "r")
        assert result == "Insight after retry."
        assert call_count == 2
