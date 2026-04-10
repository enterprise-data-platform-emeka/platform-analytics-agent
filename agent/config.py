"""Agent configuration loaded from environment variables.

All config is validated at startup before any AWS or Claude calls are made.
If any required variable is missing the process exits immediately with a
clear error message. No scattered os.getenv() calls in business logic.
"""

import os
from dataclasses import dataclass

from agent.exceptions import ConfigurationError


@dataclass(frozen=True)
class AWSConfig:
    region: str
    environment: str
    bronze_bucket: str
    gold_bucket: str
    athena_results_bucket: str
    athena_workgroup: str
    glue_gold_database: str
    ssm_api_key_param: str


@dataclass(frozen=True)
class AgentConfig:
    cost_threshold_usd: float
    max_rows: int


@dataclass(frozen=True)
class Config:
    aws: AWSConfig
    agent: AgentConfig

    def __repr__(self) -> str:
        # Never expose bucket names or SSM paths in logs accidentally.
        return f"Config(environment={self.aws.environment!r}, " f"region={self.aws.region!r})"

    @classmethod
    def from_env(cls) -> "Config":
        required = [
            "ENVIRONMENT",
            "BRONZE_BUCKET",
            "GOLD_BUCKET",
            "ATHENA_RESULTS_BUCKET",
            "ATHENA_WORKGROUP",
            "GLUE_GOLD_DATABASE",
            "SSM_API_KEY_PARAM",
        ]
        missing = [v for v in required if not os.getenv(v)]
        if missing:
            raise ConfigurationError(
                f"Missing required environment variables: {missing}. "
                f"Copy .env.example to .env and fill in the values."
            )

        environment = os.environ["ENVIRONMENT"]
        if environment not in ("dev", "staging", "prod"):
            raise ConfigurationError(
                f"ENVIRONMENT must be 'dev', 'staging', or 'prod', got {environment!r}."
            )

        try:
            cost_threshold = float(os.getenv("COST_THRESHOLD_USD", "0.10"))
        except ValueError as exc:
            raise ConfigurationError(
                f"COST_THRESHOLD_USD must be a number, got {os.getenv('COST_THRESHOLD_USD')!r}."
            ) from exc

        try:
            max_rows = int(os.getenv("MAX_ROWS", "1000"))
        except ValueError as exc:
            raise ConfigurationError(
                f"MAX_ROWS must be an integer, got {os.getenv('MAX_ROWS')!r}."
            ) from exc

        return cls(
            aws=AWSConfig(
                region=os.getenv("AWS_REGION", "eu-central-1"),
                environment=environment,
                bronze_bucket=os.environ["BRONZE_BUCKET"],
                gold_bucket=os.environ["GOLD_BUCKET"],
                athena_results_bucket=os.environ["ATHENA_RESULTS_BUCKET"],
                athena_workgroup=os.environ["ATHENA_WORKGROUP"],
                glue_gold_database=os.environ["GLUE_GOLD_DATABASE"],
                ssm_api_key_param=os.environ["SSM_API_KEY_PARAM"],
            ),
            agent=AgentConfig(
                cost_threshold_usd=cost_threshold,
                max_rows=max_rows,
            ),
        )
