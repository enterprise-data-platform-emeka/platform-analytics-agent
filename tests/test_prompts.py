"""Tests for prompt builders in agent/prompts.py.

Verifies that the system prompt includes all 7 Gold tables, that schema
descriptions are merged correctly, and that message builders produce the
expected conversation structure for SQL generation and insight generation.
"""

from agent.prompts import (
    GET_SCHEMA_TOOL,
    GOLD_TABLE_CATALOG,
    build_insight_messages,
    build_sql_correction_messages,
    build_sql_request_messages,
    build_system_prompt,
)
from agent.schema import ColumnSchema, TableSchema

# ── Helpers ────────────────────────────────────────────────────────────────────

GOLD_DB = "edp_dev_gold"
ALL_SEVEN_TABLES = [
    "monthly_revenue_trend",
    "customer_segments",
    "payment_method_performance",
    "carrier_delivery_performance",
    "product_category_performance",
    "revenue_by_country",
    "top_selling_products",
]


def _make_schemas(
    tables: list[str] | None = None,
    include_description: bool = True,
) -> dict[str, TableSchema]:
    """Build a minimal AllSchemas dict for testing."""
    target = tables or ALL_SEVEN_TABLES
    schemas = {}
    for name in target:
        schemas[name] = TableSchema(
            name=name,
            database=GOLD_DB,
            description=f"Test description for {name}" if include_description else "",
            columns=[
                ColumnSchema(name="col_a", data_type="bigint", description="Column A"),
                ColumnSchema(name="col_b", data_type="varchar", description=""),
            ],
            partition_keys=["col_a"] if name == "monthly_revenue_trend" else [],
        )
    return schemas


# ── GOLD_TABLE_CATALOG static content ─────────────────────────────────────────


class TestGoldTableCatalog:
    def test_all_seven_tables_present(self) -> None:
        for table in ALL_SEVEN_TABLES:
            assert table in GOLD_TABLE_CATALOG, f"Missing: {table}"

    def test_each_table_has_table_description(self) -> None:
        for table in ALL_SEVEN_TABLES:
            assert (
                "__table__" in GOLD_TABLE_CATALOG[table]
            ), f"{table} missing '__table__' description"
            assert GOLD_TABLE_CATALOG[table][
                "__table__"
            ], f"{table} '__table__' description is empty"

    def test_each_table_has_column_hints(self) -> None:
        for table in ALL_SEVEN_TABLES:
            non_meta = {k: v for k, v in GOLD_TABLE_CATALOG[table].items() if k != "__table__"}
            assert non_meta, f"{table} has no column hints"

    def test_payment_method_accepted_values_documented(self) -> None:
        hint = GOLD_TABLE_CATALOG["payment_method_performance"]["payment_method"]
        for method in ("credit_card", "debit_card", "paypal", "apple_pay", "crypto"):
            assert method in hint, f"Missing payment method '{method}' in hint"

    def test_carrier_accepted_values_documented(self) -> None:
        hint = GOLD_TABLE_CATALOG["carrier_delivery_performance"]["carrier"]
        for carrier in ("Royal Mail", "UPS", "FedEx", "DHL"):
            assert carrier in hint, f"Missing carrier '{carrier}' in hint"

    def test_customer_frequency_band_values_documented(self) -> None:
        hint = GOLD_TABLE_CATALOG["customer_segments"]["customer_frequency_band"]
        for band in ("vip", "core", "occasional", "new"):
            assert band in hint, f"Missing band '{band}' in hint"


# ── GET_SCHEMA_TOOL structure ──────────────────────────────────────────────────


class TestGetSchemaTool:
    def test_tool_has_required_keys(self) -> None:
        assert "name" in GET_SCHEMA_TOOL
        assert "description" in GET_SCHEMA_TOOL
        assert "input_schema" in GET_SCHEMA_TOOL

    def test_tool_name_is_get_schema(self) -> None:
        assert GET_SCHEMA_TOOL["name"] == "get_schema"

    def test_tool_requires_table_name(self) -> None:
        schema = GET_SCHEMA_TOOL["input_schema"]
        assert "table_name" in schema["properties"]
        assert "table_name" in schema["required"]


# ── build_system_prompt ────────────────────────────────────────────────────────


class TestBuildSystemPrompt:
    def test_includes_gold_database_name(self) -> None:
        prompt = build_system_prompt(_make_schemas(), gold_database=GOLD_DB)
        assert GOLD_DB in prompt

    def test_includes_max_rows(self) -> None:
        prompt = build_system_prompt(_make_schemas(), gold_database=GOLD_DB, max_rows=500)
        assert "500" in prompt

    def test_includes_all_seven_table_names(self) -> None:
        prompt = build_system_prompt(_make_schemas(), gold_database=GOLD_DB)
        for table in ALL_SEVEN_TABLES:
            assert table in prompt, f"Table '{table}' missing from system prompt"

    def test_includes_live_table_description(self) -> None:
        schemas = _make_schemas(tables=["monthly_revenue_trend"])
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        assert "Test description for monthly_revenue_trend" in prompt

    def test_includes_column_names(self) -> None:
        schemas = _make_schemas(tables=["revenue_by_country"])
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        assert "col_a" in prompt
        assert "col_b" in prompt

    def test_column_description_from_live_schema(self) -> None:
        schemas = _make_schemas(tables=["revenue_by_country"])
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        assert "Column A" in prompt

    def test_static_hint_used_when_live_column_description_empty(self) -> None:
        # col_b has no live description; static hint for 'country' should fill in.
        schemas: dict[str, TableSchema] = {
            "revenue_by_country": TableSchema(
                name="revenue_by_country",
                database=GOLD_DB,
                description="",
                columns=[
                    ColumnSchema(name="country", data_type="varchar", description=""),
                ],
                partition_keys=[],
            )
        }
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        # Static hint for 'country' in revenue_by_country mentions Germany
        assert "Germany" in prompt

    def test_static_table_description_used_when_live_description_empty(self) -> None:
        schemas = _make_schemas(tables=["monthly_revenue_trend"], include_description=False)
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        # Static hint mentions 'revenue trends' — from GOLD_TABLE_CATALOG
        assert "revenue trend" in prompt.lower() or "Monthly" in prompt

    def test_partition_keys_included(self) -> None:
        # monthly_revenue_trend has partition_keys=["col_a"] in the test schema
        schemas = _make_schemas(tables=["monthly_revenue_trend"])
        prompt = build_system_prompt(schemas, gold_database=GOLD_DB)
        assert "Partition keys" in prompt
        assert "col_a" in prompt

    def test_fallback_to_static_catalog_when_schemas_empty(self) -> None:
        prompt = build_system_prompt({}, gold_database=GOLD_DB)
        for table in ALL_SEVEN_TABLES:
            assert table in prompt, f"Fallback missing table '{table}'"

    def test_select_only_rule_present(self) -> None:
        prompt = build_system_prompt(_make_schemas(), gold_database=GOLD_DB)
        assert "SELECT" in prompt
        assert "DROP" in prompt  # forbidden keywords are listed

    def test_output_format_tags_present(self) -> None:
        prompt = build_system_prompt(_make_schemas(), gold_database=GOLD_DB)
        assert "<sql>" in prompt
        assert "<assumptions>" in prompt


# ── build_sql_request_messages ─────────────────────────────────────────────────


class TestBuildSqlRequestMessages:
    def test_returns_single_user_message(self) -> None:
        msgs = build_sql_request_messages("What is the total revenue?")
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    def test_message_contains_question(self) -> None:
        question = "Which country has the highest average order value?"
        msgs = build_sql_request_messages(question)
        assert question in msgs[0]["content"]


# ── build_sql_correction_messages ─────────────────────────────────────────────


class TestBuildSqlCorrectionMessages:
    def _initial(self) -> list[dict[str, str]]:
        return build_sql_request_messages("Show me revenue by country")

    def test_appends_two_messages_to_prior(self) -> None:
        prior = self._initial()
        result = build_sql_correction_messages(
            prior_messages=prior,
            prior_sql="DROP TABLE foo",
            prior_assumptions=["Table: revenue_by_country"],
            validation_reason="DROP is not allowed.",
        )
        # original user + assistant + correction user = 3 total
        assert len(result) == 3

    def test_assistant_message_contains_prior_sql(self) -> None:
        prior = self._initial()
        result = build_sql_correction_messages(
            prior_messages=prior,
            prior_sql="SELECT * FROM revenue_by_country",
            prior_assumptions=["Table: revenue_by_country — best match"],
            validation_reason="LIMIT is missing.",
        )
        assistant_msg = result[1]
        assert assistant_msg["role"] == "assistant"
        assert "SELECT * FROM revenue_by_country" in assistant_msg["content"]

    def test_assistant_message_contains_assumptions(self) -> None:
        prior = self._initial()
        result = build_sql_correction_messages(
            prior_messages=prior,
            prior_sql="SELECT * FROM revenue_by_country",
            prior_assumptions=["Table: revenue_by_country — best match"],
            validation_reason="LIMIT is missing.",
        )
        assert "revenue_by_country — best match" in result[1]["content"]

    def test_correction_message_contains_reason(self) -> None:
        prior = self._initial()
        result = build_sql_correction_messages(
            prior_messages=prior,
            prior_sql="SELECT 1",
            prior_assumptions=[],
            validation_reason="Only Gold database is allowed.",
        )
        correction_msg = result[2]
        assert correction_msg["role"] == "user"
        assert "Only Gold database is allowed." in correction_msg["content"]

    def test_empty_assumptions_does_not_crash(self) -> None:
        prior = self._initial()
        result = build_sql_correction_messages(
            prior_messages=prior,
            prior_sql="SELECT 1",
            prior_assumptions=[],
            validation_reason="Missing LIMIT.",
        )
        assert len(result) == 3


# ── build_insight_messages ─────────────────────────────────────────────────────


class TestBuildInsightMessages:
    def test_returns_single_user_message(self) -> None:
        msgs = build_insight_messages(
            question="What is the top country by revenue?",
            sql="SELECT country, total_revenue FROM revenue_by_country ORDER BY total_revenue DESC LIMIT 1",
            result_markdown="| country | total_revenue |\n|---|---|\n| Germany | 432701.55 |",
        )
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    def test_message_contains_question(self) -> None:
        msgs = build_insight_messages(
            question="Which carrier has the best delivery rate?",
            sql="SELECT carrier, delivery_success_pct FROM carrier_delivery_performance ORDER BY delivery_success_pct DESC LIMIT 1",
            result_markdown="| carrier | delivery_success_pct |\n|---|---|\n| FedEx | 55.2 |",
        )
        assert "Which carrier has the best delivery rate?" in msgs[0]["content"]

    def test_message_contains_sql(self) -> None:
        sql = "SELECT country FROM revenue_by_country LIMIT 5"
        msgs = build_insight_messages("question", sql, "| a | b |")
        assert sql in msgs[0]["content"]

    def test_message_contains_result(self) -> None:
        result = "| Germany | 432701.55 |"
        msgs = build_insight_messages("question", "SELECT 1", result)
        assert result in msgs[0]["content"]
