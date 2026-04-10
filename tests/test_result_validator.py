"""Tests for result_validator.py."""


from agent.executor import QueryResult
from agent.result_validator import (
    ValidationReport,
    validate,
)

# ── Helpers ────────────────────────────────────────────────────────────────────


def _result(
    columns: list[str],
    rows: list[dict[str, str]],
    execution_id: str = "exec-test",
) -> QueryResult:
    return QueryResult(
        execution_id=execution_id,
        columns=columns,
        rows=rows,
        bytes_scanned=1024,
        cost_usd=0.000046,
    )


# ── ValidationReport ───────────────────────────────────────────────────────────


class TestValidationReport:
    def test_is_clean_when_no_flags(self) -> None:
        report = ValidationReport()
        assert report.is_clean

    def test_not_clean_when_flags_present(self) -> None:
        report = ValidationReport(flags=["something is wrong"])
        assert not report.is_clean

    def test_zero_rows_default_false(self) -> None:
        report = ValidationReport()
        assert not report.zero_rows


# ── Zero rows ──────────────────────────────────────────────────────────────────


class TestZeroRows:
    def test_zero_rows_sets_flag(self) -> None:
        result = _result(columns=["country", "total_revenue"], rows=[])
        report = validate(result)
        assert report.zero_rows
        assert len(report.flags) == 1

    def test_zero_rows_flag_mentions_valid_result(self) -> None:
        result = _result(columns=["country"], rows=[])
        report = validate(result)
        assert "valid" in report.flags[0].lower()

    def test_zero_rows_skips_other_checks(self) -> None:
        # Even with suspicious column names, zero rows only gets one flag.
        result = _result(columns=["total_revenue"], rows=[])
        report = validate(result)
        assert len(report.flags) == 1


# ── Negative revenue ───────────────────────────────────────────────────────────


class TestNegativeRevenue:
    def test_negative_revenue_column_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": "-100.00"}],
        )
        report = validate(result)
        assert not report.is_clean
        assert any("total_revenue" in f for f in report.flags)

    def test_negative_amount_column_flagged(self) -> None:
        result = _result(
            columns=["payment_method", "total_amount"],
            rows=[{"payment_method": "credit_card", "total_amount": "-5.00"}],
        )
        report = validate(result)
        assert any("total_amount" in f for f in report.flags)

    def test_negative_value_column_flagged(self) -> None:
        result = _result(
            columns=["product", "lifetime_value"],
            rows=[{"product": "Widget", "lifetime_value": "-1.00"}],
        )
        report = validate(result)
        assert any("lifetime_value" in f for f in report.flags)

    def test_negative_price_column_flagged(self) -> None:
        result = _result(
            columns=["item", "avg_revenue_per_unit"],
            rows=[{"item": "A", "avg_revenue_per_unit": "-0.01"}],
        )
        report = validate(result)
        assert any("avg_revenue_per_unit" in f for f in report.flags)

    def test_positive_revenue_not_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": "432701.55"}],
        )
        report = validate(result)
        assert report.is_clean

    def test_zero_revenue_not_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": "0.0"}],
        )
        report = validate(result)
        assert report.is_clean

    def test_non_revenue_column_with_negative_not_flagged(self) -> None:
        # 'rank' or 'count' columns can legitimately have values < 0 in theory.
        result = _result(
            columns=["country", "rank"],
            rows=[{"country": "Germany", "rank": "-1"}],
        )
        report = validate(result)
        assert report.is_clean

    def test_one_flag_per_revenue_column_not_per_row(self) -> None:
        # Multiple negative rows in the same column should produce only one flag.
        result = _result(
            columns=["country", "total_revenue"],
            rows=[
                {"country": "Germany", "total_revenue": "-100.00"},
                {"country": "France", "total_revenue": "-200.00"},
            ],
        )
        report = validate(result)
        revenue_flags = [f for f in report.flags if "total_revenue" in f]
        assert len(revenue_flags) == 1

    def test_empty_revenue_value_not_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": ""}],
        )
        report = validate(result)
        # Empty is handled by null rate check, not negative check.
        negative_flags = [f for f in report.flags if "Negative" in f]
        assert not negative_flags

    def test_non_numeric_revenue_value_not_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[{"country": "Germany", "total_revenue": "N/A"}],
        )
        report = validate(result)
        negative_flags = [f for f in report.flags if "Negative" in f]
        assert not negative_flags

    def test_multiple_revenue_columns_each_flagged_independently(self) -> None:
        result = _result(
            columns=["total_revenue", "avg_revenue_per_unit"],
            rows=[
                {"total_revenue": "-1.00", "avg_revenue_per_unit": "-0.50"},
            ],
        )
        report = validate(result)
        assert len(report.flags) == 2


# ── High null rate ─────────────────────────────────────────────────────────────


class TestHighNullRate:
    def test_majority_null_column_flagged(self) -> None:
        # 3 out of 4 rows null = 75% > 50% threshold
        result = _result(
            columns=["country", "total_revenue"],
            rows=[
                {"country": "Germany", "total_revenue": ""},
                {"country": "France", "total_revenue": ""},
                {"country": "Spain", "total_revenue": ""},
                {"country": "Italy", "total_revenue": "432701.55"},
            ],
        )
        report = validate(result)
        assert any("total_revenue" in f and "null rate" in f.lower() for f in report.flags)

    def test_exactly_at_threshold_not_flagged(self) -> None:
        # 50% null — threshold is *above* 50%, so exactly 50% should not flag.
        result = _result(
            columns=["country"],
            rows=[
                {"country": ""},
                {"country": "Germany"},
            ],
        )
        report = validate(result)
        null_flags = [f for f in report.flags if "null rate" in f.lower()]
        assert not null_flags

    def test_low_null_rate_not_flagged(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[
                {"country": "Germany", "total_revenue": "432701.55"},
                {"country": "France", "total_revenue": "301245.20"},
                {"country": "Spain", "total_revenue": ""},
            ],
        )
        report = validate(result)
        null_flags = [f for f in report.flags if "null rate" in f.lower()]
        assert not null_flags

    def test_null_flag_mentions_column_name(self) -> None:
        result = _result(
            columns=["mystery_col"],
            rows=[{"mystery_col": ""}, {"mystery_col": ""}, {"mystery_col": "x"}],
        )
        report = validate(result)
        null_flags = [f for f in report.flags if "null rate" in f.lower()]
        if null_flags:
            assert "mystery_col" in null_flags[0]

    def test_all_nulls_flagged(self) -> None:
        result = _result(
            columns=["country"],
            rows=[{"country": ""}, {"country": ""}],
        )
        report = validate(result)
        null_flags = [f for f in report.flags if "null rate" in f.lower()]
        assert null_flags

    def test_whitespace_only_value_treated_as_null(self) -> None:
        result = _result(
            columns=["country"],
            rows=[{"country": "  "}, {"country": "  "}, {"country": "Germany"}],
        )
        report = validate(result)
        # 2/3 = 66% — above threshold
        null_flags = [f for f in report.flags if "null rate" in f.lower()]
        assert null_flags


# ── Combined checks ────────────────────────────────────────────────────────────


class TestCombinedChecks:
    def test_multiple_flags_accumulated(self) -> None:
        result = _result(
            columns=["country", "total_revenue"],
            rows=[
                {"country": "", "total_revenue": "-100.00"},
                {"country": "", "total_revenue": "200.00"},
                {"country": "", "total_revenue": "300.00"},
            ],
        )
        report = validate(result)
        # Negative revenue + high null rate on 'country' = at least 2 flags
        assert len(report.flags) >= 2

    def test_clean_result_produces_no_flags(self) -> None:
        result = _result(
            columns=["country", "total_revenue", "total_orders"],
            rows=[
                {"country": "Germany", "total_revenue": "432701.55", "total_orders": "321"},
                {"country": "France", "total_revenue": "301245.20", "total_orders": "225"},
            ],
        )
        report = validate(result)
        assert report.is_clean
        assert not report.zero_rows
