"""Shared pytest fixtures for the Analytics Agent test suite.

Unit test fixtures use moto for AWS and mock for Claude API.
Integration test fixtures (marked @pytest.mark.integration) use
real AWS credentials and require deployed dev infrastructure.
"""

import pytest


@pytest.fixture(autouse=True)
def agent_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables for all unit tests.

    Applied automatically to every test. Integration tests that need
    real env vars should override individual variables as needed.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("BRONZE_BUCKET", "edp-dev-123456789012-bronze")
    monkeypatch.setenv("GOLD_BUCKET", "edp-dev-123456789012-gold")
    monkeypatch.setenv("ATHENA_RESULTS_BUCKET", "edp-dev-123456789012-athena-results")
    monkeypatch.setenv("ATHENA_WORKGROUP", "edp-dev-workgroup")
    monkeypatch.setenv("GLUE_GOLD_DATABASE", "edp_dev_gold")
    monkeypatch.setenv("SSM_API_KEY_PARAM", "/edp/dev/anthropic_api_key")
    monkeypatch.setenv("COST_THRESHOLD_USD", "0.10")
    monkeypatch.setenv("MAX_ROWS", "1000")
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    # Prevent boto3 from trying to use real credentials in unit tests
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
