"""Tests for SQLValidator.

Covers all five guardrail rules:
  1. Non-empty, single statement
  2. Forbidden keywords (DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE)
  3. SELECT-only enforcement (including CTE allowance)
  4. Database reference whitelist (Gold only)
  5. LIMIT injection and capping
"""

import pytest

from agent.exceptions import SQLValidationError
from agent.validator import SQLValidator


def _validator(
    gold_database: str = "edp_dev_gold",
    max_rows: int = 1000,
) -> SQLValidator:
    return SQLValidator(gold_database=gold_database, max_rows=max_rows)


# ── Empty and malformed input ──────────────────────────────────────────────────


class TestEmptyInput:
    def test_empty_string_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("")
        assert "empty" in exc_info.value.reason.lower()

    def test_whitespace_only_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("   \n  ")
        assert "empty" in exc_info.value.reason.lower()

    def test_trailing_semicolon_is_stripped(self) -> None:
        result = _validator().validate("SELECT order_year FROM monthly_revenue_trend LIMIT 10;")
        assert not result.endswith(";")

    def test_multiple_statements_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate(
                "SELECT 1 FROM monthly_revenue_trend; SELECT 2 FROM customer_segments"
            )
        reason = exc_info.value.reason.lower()
        assert "one" in reason or "exactly" in reason


# ── Forbidden keywords ─────────────────────────────────────────────────────────


class TestForbiddenKeywords:
    @pytest.mark.parametrize(
        "keyword",
        ["DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER", "TRUNCATE"],
    )
    def test_forbidden_keyword_uppercase_raises(self, keyword: str) -> None:
        sql = f"{keyword} TABLE monthly_revenue_trend"
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate(sql)
        assert keyword in exc_info.value.reason

    def test_forbidden_keyword_lowercase_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("delete from monthly_revenue_trend")
        assert "DELETE" in exc_info.value.reason

    def test_forbidden_keyword_mixedcase_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("Delete From monthly_revenue_trend")
        assert "DELETE" in exc_info.value.reason

    def test_forbidden_keyword_inside_subquery_raises(self) -> None:
        # DROP hidden inside a subquery must still be caught.
        sql = "SELECT * FROM (DROP TABLE monthly_revenue_trend) t"
        with pytest.raises(SQLValidationError):
            _validator().validate(sql)

    def test_forbidden_keyword_inside_cte_raises(self) -> None:
        # UPDATE inside a CTE must still be caught.
        sql = (
            "WITH bad AS (UPDATE monthly_revenue_trend SET total_revenue = 0) " "SELECT * FROM bad"
        )
        with pytest.raises(SQLValidationError):
            _validator().validate(sql)

    def test_column_named_updated_at_does_not_raise(self) -> None:
        # 'UPDATE' is a word-boundary match; 'updated_at' must not trigger it.
        result = _validator().validate("SELECT updated_at FROM customer_segments LIMIT 10")
        assert "updated_at" in result

    def test_column_named_created_at_does_not_raise(self) -> None:
        # 'CREATE' must not match inside 'created_at'.
        result = _validator().validate("SELECT created_at FROM customer_segments LIMIT 10")
        assert "created_at" in result

    def test_column_named_last_update_does_not_raise(self) -> None:
        # 'UPDATE' must not match inside 'last_update'.
        result = _validator().validate("SELECT last_update FROM monthly_revenue_trend LIMIT 10")
        assert "last_update" in result


# ── SELECT-only enforcement ────────────────────────────────────────────────────


class TestSelectOnly:
    def test_plain_select_passes(self) -> None:
        result = _validator().validate("SELECT * FROM monthly_revenue_trend LIMIT 10")
        assert result

    def test_select_with_where_passes(self) -> None:
        result = _validator().validate(
            "SELECT order_year, total_revenue FROM monthly_revenue_trend "
            "WHERE order_year = 2024 LIMIT 10"
        )
        assert result

    def test_select_with_aggregation_passes(self) -> None:
        result = _validator().validate(
            "SELECT country, SUM(total_revenue) AS revenue "
            "FROM revenue_by_country "
            "GROUP BY country "
            "ORDER BY revenue DESC "
            "LIMIT 10"
        )
        assert result

    def test_cte_with_select_passes(self) -> None:
        sql = (
            "WITH monthly AS (\n"
            "    SELECT order_year, total_revenue FROM monthly_revenue_trend\n"
            ")\n"
            "SELECT * FROM monthly\n"
            "LIMIT 20"
        )
        result = _validator().validate(sql)
        assert "WITH" in result.upper()

    def test_table_alias_dot_column_passes(self) -> None:
        # t.column_name must not trigger the database reference check.
        result = _validator().validate(
            "SELECT t.order_year, t.total_revenue "
            'FROM "edp_dev_gold"."monthly_revenue_trend" t '
            "LIMIT 10"
        )
        assert result


# ── Database reference whitelist ───────────────────────────────────────────────


class TestDatabaseReferences:
    def test_unqualified_table_passes(self) -> None:
        result = _validator().validate("SELECT * FROM monthly_revenue_trend LIMIT 10")
        assert result

    def test_gold_database_double_quoted_passes(self) -> None:
        result = _validator().validate(
            'SELECT * FROM "edp_dev_gold"."monthly_revenue_trend" LIMIT 10'
        )
        assert "edp_dev_gold" in result

    def test_bronze_database_quoted_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate('SELECT * FROM "edp_dev_bronze"."orders" LIMIT 10')
        assert "edp_dev_bronze" in exc_info.value.reason

    def test_silver_database_quoted_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate('SELECT * FROM "edp_dev_silver"."fact_orders" LIMIT 10')
        assert "edp_dev_silver" in exc_info.value.reason

    def test_unquoted_bronze_reference_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("SELECT * FROM edp_dev_bronze.orders LIMIT 10")
        assert "edp_dev_bronze" in exc_info.value.reason

    def test_unquoted_silver_reference_raises(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("SELECT * FROM edp_dev_silver.fact_orders LIMIT 10")
        assert "edp_dev_silver" in exc_info.value.reason

    def test_staging_bronze_quoted_raises(self) -> None:
        # Cross-environment references must be rejected regardless of env.
        with pytest.raises(SQLValidationError) as exc_info:
            _validator(gold_database="edp_dev_gold").validate(
                'SELECT * FROM "edp_staging_bronze"."orders" LIMIT 10'
            )
        assert "edp_staging_bronze" in exc_info.value.reason

    def test_wrong_gold_environment_raises(self) -> None:
        # edp_prod_gold is not the dev gold database.
        with pytest.raises(SQLValidationError) as exc_info:
            _validator(gold_database="edp_dev_gold").validate(
                'SELECT * FROM "edp_prod_gold"."monthly_revenue_trend" LIMIT 10'
            )
        assert "edp_prod_gold" in exc_info.value.reason

    def test_gold_database_with_surrounding_quotes_in_config_passes(self) -> None:
        # Config may pass the database name with surrounding quotes; the
        # validator strips them before comparing.
        result = _validator(gold_database='"edp_dev_gold"').validate(
            'SELECT * FROM "edp_dev_gold"."monthly_revenue_trend" LIMIT 10'
        )
        assert result

    def test_multiple_gold_tables_joined_passes(self) -> None:
        sql = (
            "SELECT r.country, r.total_revenue, c.total_customers "
            'FROM "edp_dev_gold"."revenue_by_country" r '
            'JOIN "edp_dev_gold"."customer_segments" c ON r.country = c.country '
            "LIMIT 10"
        )
        result = _validator().validate(sql)
        assert result


# ── LIMIT injection and capping ────────────────────────────────────────────────


class TestLimit:
    def test_missing_limit_is_injected(self) -> None:
        result = _validator(max_rows=500).validate("SELECT * FROM monthly_revenue_trend")
        assert "LIMIT 500" in result.upper()

    def test_limit_within_bounds_is_unchanged(self) -> None:
        result = _validator(max_rows=1000).validate("SELECT * FROM monthly_revenue_trend LIMIT 10")
        assert "LIMIT 10" in result.upper()
        assert "LIMIT 1000" not in result.upper()

    def test_limit_exactly_at_max_is_unchanged(self) -> None:
        result = _validator(max_rows=1000).validate(
            "SELECT * FROM monthly_revenue_trend LIMIT 1000"
        )
        assert "LIMIT 1000" in result.upper()
        # Must not be doubled.
        assert result.upper().count("LIMIT") == 1

    def test_limit_exceeding_max_is_capped(self) -> None:
        result = _validator(max_rows=1000).validate(
            "SELECT * FROM monthly_revenue_trend LIMIT 5000"
        )
        assert "LIMIT 1000" in result.upper()
        assert "LIMIT 5000" not in result.upper()

    def test_limit_zero_not_replaced(self) -> None:
        # LIMIT 0 is valid Athena SQL and within bounds (0 <= max_rows).
        result = _validator(max_rows=1000).validate("SELECT * FROM monthly_revenue_trend LIMIT 0")
        assert "LIMIT 0" in result.upper()

    def test_cte_without_outer_limit_gets_injected(self) -> None:
        sql = (
            "WITH monthly AS (\n"
            "    SELECT order_year, total_revenue FROM monthly_revenue_trend\n"
            ")\n"
            "SELECT * FROM monthly"
        )
        result = _validator(max_rows=1000).validate(sql)
        assert "LIMIT 1000" in result.upper()

    def test_cte_with_inner_limit_only_gets_outer_injected(self) -> None:
        # LIMIT inside the CTE does not count as the top-level LIMIT.
        sql = (
            "WITH monthly AS (\n"
            "    SELECT order_year, total_revenue FROM monthly_revenue_trend LIMIT 5\n"
            ")\n"
            "SELECT * FROM monthly"
        )
        result = _validator(max_rows=1000).validate(sql)
        # The outer LIMIT must be injected.
        assert result.upper().endswith("LIMIT 1000")

    def test_cte_with_outer_limit_unchanged(self) -> None:
        sql = (
            "WITH monthly AS (\n"
            "    SELECT order_year, total_revenue FROM monthly_revenue_trend\n"
            ")\n"
            "SELECT * FROM monthly\n"
            "LIMIT 50"
        )
        result = _validator(max_rows=1000).validate(sql)
        assert "LIMIT 50" in result.upper()
        assert "LIMIT 1000" not in result.upper()


# ── Error structure ────────────────────────────────────────────────────────────


class TestErrorStructure:
    def test_error_has_non_empty_reason(self) -> None:
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("DROP TABLE monthly_revenue_trend")
        error = exc_info.value
        assert isinstance(error.reason, str)
        assert len(error.reason) > 0

    def test_error_message_and_reason_are_distinct(self) -> None:
        # message is the concise summary; reason is the Claude-facing explanation.
        with pytest.raises(SQLValidationError) as exc_info:
            _validator().validate("DROP TABLE monthly_revenue_trend")
        error = exc_info.value
        assert str(error) != error.reason

    def test_error_is_agent_error_subclass(self) -> None:
        from agent.exceptions import AgentError

        with pytest.raises(AgentError):
            _validator().validate("DELETE FROM monthly_revenue_trend")
