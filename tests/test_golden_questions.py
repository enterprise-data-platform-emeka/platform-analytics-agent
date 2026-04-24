"""Golden question test suite for the Analytics Agent SQL generation pipeline.

Each test case is a fixed plain-English question paired with:
  - The Gold table name expected in the generated SQL
  - One or more column names expected in the generated SQL

Tests are skipped when ANTHROPIC_API_KEY is not set. When the key is available,
the suite makes real Claude API calls using the static GOLD_TABLE_CATALOG as
the schema source (no Glue, no Athena). This catches prompt regressions and
schema description changes offline, before deploying to AWS.

Run with:
    ANTHROPIC_API_KEY=sk-ant-... pytest tests/test_golden_questions.py -v
"""

import os
from unittest.mock import MagicMock

import pytest

from agent.claude_client import ClaudeClient
from agent.config import AgentConfig, AWSConfig, Config
from agent.generator import SQLGenerator
from agent.prompts import build_system_prompt
from agent.schema import SchemaResolver
from agent.validator import SQLValidator

# ---------------------------------------------------------------------------
# Skip guard
# ---------------------------------------------------------------------------

_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

needs_api_key = pytest.mark.skipif(
    not _API_KEY,
    reason="ANTHROPIC_API_KEY not set — golden suite requires a real Claude API key",
)

_GOLD_DB = "edp_dev_gold"

# ---------------------------------------------------------------------------
# 25 golden cases: (question, expected_table, expected_columns)
#
# expected_columns: at least one key column that must appear in the SQL.
# Assertions are lowercase substring checks — not exact SQL matching.
# ---------------------------------------------------------------------------

GOLDEN_CASES = [
    # monthly_revenue_trend — 4 cases
    pytest.param(
        "Show me monthly revenue trends for the last year.",
        "monthly_revenue_trend",
        ["total_revenue"],
        id="monthly-revenue-trend",
    ),
    pytest.param(
        "How many orders were placed each month?",
        "monthly_revenue_trend",
        ["total_orders"],
        id="monthly-order-count",
    ),
    pytest.param(
        "What was total revenue in 2024?",
        "monthly_revenue_trend",
        ["total_revenue"],
        id="annual-revenue-2024",
    ),
    pytest.param(
        "How has unique customer count changed over time?",
        "monthly_revenue_trend",
        ["unique_customers"],
        id="unique-customers-over-time",
    ),
    # customer_segments — 3 cases
    pytest.param(
        "Who are the top 5 customers by lifetime value?",
        "customer_segments",
        ["lifetime_value"],
        id="top-customers-ltv",
    ),
    pytest.param(
        "How many VIP customers are there?",
        "customer_segments",
        ["customer_frequency_band"],
        id="vip-customer-count",
    ),
    pytest.param(
        "Which country has the most customers?",
        "customer_segments",
        ["country"],
        id="country-most-customers",
    ),
    # payment_method_performance — 4 cases
    pytest.param(
        "Which payment method has the highest success rate?",
        "payment_method_performance",
        ["success_per_pct"],
        id="payment-success-rate",
    ),
    pytest.param(
        "How many transactions failed for each payment method?",
        "payment_method_performance",
        ["failed"],
        id="payment-failed-transactions",
    ),
    pytest.param(
        "What revenue was captured by credit card?",
        "payment_method_performance",
        ["revenue_captured"],
        id="credit-card-revenue",
    ),
    pytest.param(
        "Which payment method has the most refunded transactions?",
        "payment_method_performance",
        ["refunded"],
        id="payment-refunds",
    ),
    # carrier_delivery_performance — 4 cases
    pytest.param(
        "Which carrier has the fastest average delivery time?",
        "carrier_delivery_performance",
        ["avg_delivery_days"],
        id="fastest-carrier",
    ),
    pytest.param(
        "What is the delivery success rate for each carrier?",
        "carrier_delivery_performance",
        ["delivery_success_pct"],
        id="carrier-success-rate",
    ),
    pytest.param(
        "How many total shipments does each carrier handle?",
        "carrier_delivery_performance",
        ["total_shipments"],
        id="carrier-shipment-count",
    ),
    pytest.param(
        "Which carrier has the slowest maximum delivery time?",
        "carrier_delivery_performance",
        ["slowest_delivery_days"],
        id="slowest-carrier",
    ),
    # product_category_performance — 3 cases
    pytest.param(
        "Which brand generates the most revenue?",
        "product_category_performance",
        ["total_revenue"],
        id="brand-revenue",
    ),
    pytest.param(
        "What is the average revenue per unit for each brand?",
        "product_category_performance",
        ["avg_revenue_per_unit"],
        id="brand-avg-revenue-per-unit",
    ),
    pytest.param(
        "How many total units were sold per brand?",
        "product_category_performance",
        ["total_units_sold"],
        id="brand-units-sold",
    ),
    # revenue_by_country — 4 cases
    pytest.param(
        "Which country has the highest total revenue?",
        "revenue_by_country",
        ["total_revenue"],
        id="country-highest-revenue",
    ),
    pytest.param(
        "What is the average order value by country?",
        "revenue_by_country",
        ["avg_order_value"],
        id="avg-order-value-by-country",
    ),
    pytest.param(
        "How many orders came from Germany?",
        "revenue_by_country",
        ["total_orders"],
        id="germany-order-count",
    ),
    pytest.param(
        "What are the top 5 countries by number of customers?",
        "revenue_by_country",
        ["total_customers"],
        id="top-countries-by-customers",
    ),
    # top_selling_products — 3 cases
    pytest.param(
        "What are the top 10 best-selling products by revenue?",
        "top_selling_products",
        ["total_revenue"],
        id="top-products-by-revenue",
    ),
    pytest.param(
        "Which product has the highest average revenue per unit?",
        "top_selling_products",
        ["avg_revenue_per_unit"],
        id="product-highest-avg-revenue",
    ),
    pytest.param(
        "Which product is ranked number 1 by revenue?",
        "top_selling_products",
        ["rank"],
        id="product-rank-1",
    ),
]


# ---------------------------------------------------------------------------
# Fixtures — built once per module to avoid repeated SSM/schema fetches
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def golden_generator() -> SQLGenerator:
    """Real SQLGenerator backed by Claude, using the static schema catalog.

    Constructs config directly (no env vars needed) and passes the API key
    to ClaudeClient to skip the SSM Parameter Store fetch.
    Skips the module if ANTHROPIC_API_KEY is not set.
    """
    if not _API_KEY:
        pytest.skip("ANTHROPIC_API_KEY not set")

    config = Config(
        aws=AWSConfig(
            region="eu-central-1",
            environment="dev",
            bronze_bucket="test-bronze",
            gold_bucket="test-gold",
            athena_results_bucket="test-athena-results",
            athena_workgroup="test-workgroup",
            glue_gold_database=_GOLD_DB,
            ssm_api_key_param="/test/key",
        ),
        agent=AgentConfig(cost_threshold_usd=1.0, max_rows=1000),
    )
    resolver = MagicMock(spec=SchemaResolver)
    client = ClaudeClient(config=config, schema_resolver=resolver, api_key=_API_KEY)
    validator = SQLValidator(gold_database=_GOLD_DB, max_rows=1000)
    return SQLGenerator(client=client, validator=validator)


@pytest.fixture(scope="module")
def golden_system_prompt() -> str:
    """System prompt built from the static GOLD_TABLE_CATALOG (no live Glue).

    Passing an empty schema dict triggers the static-catalog fallback in
    build_system_prompt, which is what the agent uses when Glue is unreachable.
    This ensures golden tests exercise the same prompt that runs in production
    when schemas are embedded from the catalog.
    """
    return build_system_prompt({}, gold_database=_GOLD_DB, max_rows=1000)


# ---------------------------------------------------------------------------
# Golden question assertions
# ---------------------------------------------------------------------------

_FORBIDDEN_KEYWORDS = ["drop", "delete", "insert", "update", "create", "alter", "truncate"]


@needs_api_key
@pytest.mark.parametrize("question,expected_table,expected_columns", GOLDEN_CASES)
def test_golden_question(
    question: str,
    expected_table: str,
    expected_columns: list[str],
    golden_generator: SQLGenerator,
    golden_system_prompt: str,
) -> None:
    """Assert that Claude generates structurally correct SQL for each question.

    Checks:
    - Expected table name appears in the SQL
    - Each expected column name appears in the SQL
    - SQL starts with SELECT
    - SQL contains a LIMIT clause
    - No DDL keywords present
    """
    result = golden_generator.generate(question, golden_system_prompt)
    sql_lower = result.sql.lower()

    assert expected_table in sql_lower, (
        f"Expected table '{expected_table}' not in SQL for: {question!r}\n\nSQL:\n{result.sql}"
    )

    for col in expected_columns:
        assert col in sql_lower, (
            f"Expected column '{col}' not in SQL for: {question!r}\n\nSQL:\n{result.sql}"
        )

    assert "limit" in sql_lower, (
        f"LIMIT clause missing from SQL for: {question!r}\n\nSQL:\n{result.sql}"
    )

    assert sql_lower.lstrip().startswith("select"), (
        f"SQL must start with SELECT for: {question!r}\n\nSQL:\n{result.sql}"
    )

    for kw in _FORBIDDEN_KEYWORDS:
        assert kw not in sql_lower, (
            f"Forbidden keyword '{kw.upper()}' found in SQL for: {question!r}\n\nSQL:\n{result.sql}"
        )
