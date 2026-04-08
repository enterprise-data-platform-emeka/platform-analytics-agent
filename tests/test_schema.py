"""Tests for SchemaResolver."""

import json
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.exceptions import SchemaResolutionError
from agent.schema import ColumnSchema, SchemaResolver, TableSchema


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "operation",
    )


def _aws_config() -> AWSConfig:
    return AWSConfig(
        region="eu-central-1",
        environment="dev",
        bronze_bucket="edp-dev-123456789012-bronze",
        gold_bucket="edp-dev-123456789012-gold",
        athena_results_bucket="edp-dev-123456789012-athena-results",
        athena_workgroup="edp-dev-workgroup",
        glue_gold_database="edp_dev_gold",
        ssm_api_key_param="/edp/dev/anthropic_api_key",
    )


def _glue_table(
    name: str,
    columns: list[dict[str, str]],
    partition_keys: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Minimal Glue table dict matching the shape returned by get_table / get_tables."""
    return {
        "Name": name,
        "DatabaseName": "edp_dev_gold",
        "StorageDescriptor": {"Columns": columns},
        "PartitionKeys": partition_keys or [],
    }


def _dbt_catalog(*table_names: str) -> dict[str, Any]:
    """Build a minimal dbt catalog.json dict with one node per table name."""
    nodes: dict[str, Any] = {}
    for name in table_names:
        nodes[f"model.edp.{name}"] = {
            "description": f"{name} model description",
            "columns": {
                "id": {"description": "Primary key"},
                "value": {"description": "Aggregated metric value"},
            },
        }
    return {"nodes": nodes, "sources": {}}


def _make_resolver(
    glue_tables: list[dict[str, Any]],
    dbt_catalog: dict[str, Any] | None = None,
    s3_error_code: str | None = None,
    glue_error_code: str | None = None,
) -> SchemaResolver:
    """Build a SchemaResolver with fully mocked boto3 clients."""
    config = _aws_config()
    resolver = SchemaResolver(config)

    # Mock Glue paginator
    mock_glue = MagicMock()
    if glue_error_code:
        mock_glue.get_paginator.return_value.paginate.side_effect = _client_error(
            glue_error_code
        )
        mock_glue.get_table.side_effect = _client_error(glue_error_code)
    else:
        mock_page = {"TableList": glue_tables}
        mock_glue.get_paginator.return_value.paginate.return_value = [mock_page]
        mock_glue.get_table.return_value = {"Table": glue_tables[0]} if glue_tables else {}

    # Mock S3
    mock_s3 = MagicMock()
    if s3_error_code:
        mock_s3.get_object.side_effect = _client_error(s3_error_code)
    elif dbt_catalog is not None:
        body = BytesIO(json.dumps(dbt_catalog).encode())
        mock_s3.get_object.return_value = {"Body": body}
    else:
        mock_s3.get_object.side_effect = _client_error("NoSuchKey")

    resolver._glue = mock_glue
    resolver._s3 = mock_s3
    return resolver


# ── load_all_schemas ──────────────────────────────────────────────────────────


def test_load_all_schemas_returns_all_tables(agent_env_vars: None) -> None:
    tables = [
        _glue_table(
            "monthly_revenue_trend",
            [
                {"Name": "id", "Type": "string"},
                {"Name": "value", "Type": "double"},
            ],
        ),
        _glue_table(
            "daily_order_volume",
            [{"Name": "id", "Type": "string"}],
        ),
    ]
    catalog = _dbt_catalog("monthly_revenue_trend", "daily_order_volume")
    resolver = _make_resolver(tables, dbt_catalog=catalog)

    schemas = resolver.load_all_schemas()

    assert set(schemas.keys()) == {"monthly_revenue_trend", "daily_order_volume"}


def test_load_all_schemas_merges_dbt_descriptions(agent_env_vars: None) -> None:
    tables = [
        _glue_table(
            "monthly_revenue_trend",
            [{"Name": "id", "Type": "string"}],
        )
    ]
    catalog = _dbt_catalog("monthly_revenue_trend")
    resolver = _make_resolver(tables, dbt_catalog=catalog)

    schemas = resolver.load_all_schemas()

    schema = schemas["monthly_revenue_trend"]
    assert schema.description == "monthly_revenue_trend model description"
    assert schema.columns[0].description == "Primary key"


def test_load_all_schemas_includes_partition_keys(agent_env_vars: None) -> None:
    tables = [
        _glue_table(
            "daily_order_volume",
            [{"Name": "order_count", "Type": "bigint"}],
            partition_keys=[{"Name": "order_date", "Type": "date"}],
        )
    ]
    resolver = _make_resolver(tables)

    schemas = resolver.load_all_schemas()

    assert schemas["daily_order_volume"].partition_keys == ["order_date"]


def test_load_all_schemas_falls_back_when_catalog_missing(
    agent_env_vars: None,
) -> None:
    """When catalog.json is absent, schema loads from Glue only, no exception raised."""
    tables = [_glue_table("fct_orders", [{"Name": "order_id", "Type": "string"}])]
    resolver = _make_resolver(tables, s3_error_code="NoSuchKey")

    schemas = resolver.load_all_schemas()

    assert "fct_orders" in schemas
    # No dbt descriptions available in fallback path
    assert schemas["fct_orders"].columns[0].description == ""


def test_load_all_schemas_raises_when_glue_db_missing(agent_env_vars: None) -> None:
    resolver = _make_resolver([], glue_error_code="EntityNotFoundException")

    with pytest.raises(SchemaResolutionError, match="not found"):
        resolver.load_all_schemas()


def test_load_all_schemas_raises_when_no_tables(agent_env_vars: None) -> None:
    """An empty Glue database is treated as a configuration error."""
    config = _aws_config()
    resolver = SchemaResolver(config)

    mock_glue = MagicMock()
    mock_glue.get_paginator.return_value.paginate.return_value = [{"TableList": []}]
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = _client_error("NoSuchKey")
    resolver._glue = mock_glue
    resolver._s3 = mock_s3

    with pytest.raises(SchemaResolutionError, match="No tables found"):
        resolver.load_all_schemas()


def test_load_all_schemas_raises_on_unexpected_s3_error(agent_env_vars: None) -> None:
    tables = [_glue_table("fct_orders", [{"Name": "order_id", "Type": "string"}])]
    resolver = _make_resolver(tables, s3_error_code="AccessDenied")

    with pytest.raises(SchemaResolutionError, match="catalog.json"):
        resolver.load_all_schemas()


# ── get_schema ────────────────────────────────────────────────────────────────


def test_get_schema_returns_single_table(agent_env_vars: None) -> None:
    tables = [
        _glue_table(
            "monthly_revenue_trend",
            [{"Name": "revenue", "Type": "double"}],
        )
    ]
    catalog = _dbt_catalog("monthly_revenue_trend")
    resolver = _make_resolver(tables, dbt_catalog=catalog)

    schema = resolver.get_schema("monthly_revenue_trend")

    assert schema.name == "monthly_revenue_trend"
    assert schema.database == "edp_dev_gold"
    assert schema.columns[0].name == "revenue"


def test_get_schema_raises_on_missing_table(agent_env_vars: None) -> None:
    resolver = _make_resolver([], glue_error_code="EntityNotFoundException")

    with pytest.raises(SchemaResolutionError, match="not found"):
        resolver.get_schema("nonexistent_table")


def test_get_schema_raises_on_unexpected_glue_error(agent_env_vars: None) -> None:
    config = _aws_config()
    resolver = SchemaResolver(config)

    mock_glue = MagicMock()
    mock_glue.get_table.side_effect = _client_error("InternalServiceException")
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = _client_error("NoSuchKey")
    resolver._glue = mock_glue
    resolver._s3 = mock_s3

    with pytest.raises(SchemaResolutionError, match="Glue error"):
        resolver.get_schema("some_table")


# ── TableSchema.to_prompt_text ────────────────────────────────────────────────


def test_to_prompt_text_includes_table_name() -> None:
    schema = TableSchema(name="fct_orders", database="edp_dev_gold")
    assert "edp_dev_gold.fct_orders" in schema.to_prompt_text()


def test_to_prompt_text_includes_column_with_description() -> None:
    schema = TableSchema(
        name="fct_orders",
        database="edp_dev_gold",
        columns=[ColumnSchema(name="order_id", data_type="string", description="Unique order ID")],
    )
    text = schema.to_prompt_text()
    assert "order_id (string)" in text
    assert "Unique order ID" in text


def test_to_prompt_text_includes_partition_keys() -> None:
    schema = TableSchema(
        name="fct_orders",
        database="edp_dev_gold",
        partition_keys=["order_date"],
    )
    assert "Partition keys: order_date" in schema.to_prompt_text()


def test_to_prompt_text_omits_empty_description() -> None:
    schema = TableSchema(
        name="fct_orders",
        database="edp_dev_gold",
        columns=[ColumnSchema(name="id", data_type="string")],
    )
    text = schema.to_prompt_text()
    assert "id (string)" in text
    # No em-dash separator when description is empty
    assert " — " not in text


def test_to_prompt_text_omits_partition_section_when_none() -> None:
    schema = TableSchema(name="fct_orders", database="edp_dev_gold", partition_keys=[])
    assert "Partition keys" not in schema.to_prompt_text()


# ── dbt node matching ─────────────────────────────────────────────────────────


def test_dbt_node_matches_on_suffix_not_full_key(agent_env_vars: None) -> None:
    """Node key is 'model.some_project.table_name' — match by suffix only."""
    tables = [_glue_table("fct_orders", [{"Name": "id", "Type": "string"}])]
    catalog = {
        "nodes": {
            "model.my_project.fct_orders": {
                "description": "Order fact table",
                "columns": {"id": {"description": "Order ID"}},
            }
        },
        "sources": {},
    }
    body = BytesIO(json.dumps(catalog).encode())

    config = _aws_config()
    resolver = SchemaResolver(config)
    mock_glue = MagicMock()
    mock_glue.get_paginator.return_value.paginate.return_value = [{"TableList": tables}]
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": body}
    resolver._glue = mock_glue
    resolver._s3 = mock_s3

    schemas = resolver.load_all_schemas()

    assert schemas["fct_orders"].description == "Order fact table"
    assert schemas["fct_orders"].columns[0].description == "Order ID"


def test_dbt_node_also_checks_sources_section(agent_env_vars: None) -> None:
    """dbt source tables appear under 'sources', not 'nodes'."""
    tables = [_glue_table("raw_orders", [{"Name": "id", "Type": "string"}])]
    catalog = {
        "nodes": {},
        "sources": {
            "source.edp.raw_orders": {
                "description": "Raw order source",
                "columns": {"id": {"description": "Source order ID"}},
            }
        },
    }
    body = BytesIO(json.dumps(catalog).encode())

    config = _aws_config()
    resolver = SchemaResolver(config)
    mock_glue = MagicMock()
    mock_glue.get_paginator.return_value.paginate.return_value = [{"TableList": tables}]
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": body}
    resolver._glue = mock_glue
    resolver._s3 = mock_s3

    schemas = resolver.load_all_schemas()

    assert schemas["raw_orders"].description == "Raw order source"
