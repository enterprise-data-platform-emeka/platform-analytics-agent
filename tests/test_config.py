"""Tests for Config.from_env()."""

import dataclasses

import pytest

from agent.config import Config
from agent.exceptions import ConfigurationError


def test_config_loads_from_env(agent_env_vars: None) -> None:
    config = Config.from_env()
    assert config.aws.environment == "dev"
    assert config.aws.region == "eu-central-1"
    assert config.aws.bronze_bucket == "edp-dev-123456789012-bronze"
    assert config.aws.glue_gold_database == "edp_dev_gold"
    assert config.aws.claude_provider == "anthropic_api_key"
    assert config.agent.max_rows == 1000
    assert config.agent.cost_threshold_usd == 0.10


def test_config_fails_on_missing_required_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRONZE_BUCKET", raising=False)
    with pytest.raises(ConfigurationError, match="BRONZE_BUCKET"):
        Config.from_env()


def test_config_fails_on_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    with pytest.raises(ConfigurationError, match="dev.*staging.*prod"):
        Config.from_env()


def test_config_fails_on_invalid_cost_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COST_THRESHOLD_USD", "not-a-number")
    with pytest.raises(ConfigurationError, match="COST_THRESHOLD_USD"):
        Config.from_env()


def test_config_fails_on_invalid_max_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_ROWS", "lots")
    with pytest.raises(ConfigurationError, match="MAX_ROWS"):
        Config.from_env()


def test_config_fails_on_invalid_claude_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLAUDE_PROVIDER", "bedrock")
    with pytest.raises(ConfigurationError, match="CLAUDE_PROVIDER"):
        Config.from_env()


def test_config_requires_workspace_for_aws_claude_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLAUDE_PROVIDER", "aws_claude_platform")
    monkeypatch.delenv("ANTHROPIC_AWS_WORKSPACE_ID", raising=False)
    with pytest.raises(ConfigurationError, match="ANTHROPIC_AWS_WORKSPACE_ID"):
        Config.from_env()


def test_config_loads_aws_claude_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLAUDE_PROVIDER", "aws_claude_platform")
    monkeypatch.setenv("ANTHROPIC_AWS_WORKSPACE_ID", "wrkspc_test")
    monkeypatch.setenv("ANTHROPIC_AWS_INFERENCE_GEO", "us")
    monkeypatch.delenv("SSM_API_KEY_PARAM", raising=False)

    config = Config.from_env()

    assert config.aws.claude_provider == "aws_claude_platform"
    assert config.aws.claude_workspace_id == "wrkspc_test"
    assert config.aws.claude_inference_geo == "us"
    assert config.aws.ssm_api_key_param == ""


def test_config_repr_does_not_expose_bucket_names(agent_env_vars: None) -> None:
    config = Config.from_env()
    repr_str = repr(config)
    assert "bronze" not in repr_str
    assert "gold" not in repr_str
    assert "environment='dev'" in repr_str


def test_config_is_frozen(agent_env_vars: None) -> None:
    config = Config.from_env()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.aws = config.aws  # type: ignore[misc]


def test_config_defaults_region_when_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    config = Config.from_env()
    assert config.aws.region == "eu-central-1"
