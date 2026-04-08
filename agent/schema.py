"""Schema resolver: loads all Gold table schemas at startup.

Merges two sources for each table:
  - Glue Data Catalog: physical schema (column names, data types, partition keys)
  - dbt catalog.json (from S3): business context (column descriptions, model docs)

All schemas are loaded once at startup via load_all_schemas() and embedded
in the Claude system prompt. This eliminates the list_tables/get_schema
tool-call round trips from the common case.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from agent.config import AWSConfig
from agent.exceptions import SchemaResolutionError

logger = logging.getLogger(__name__)

# Type alias for the full schema dict returned by load_all_schemas().
AllSchemas = dict[str, "TableSchema"]


@dataclass
class ColumnSchema:
    name: str
    data_type: str
    description: str = ""


@dataclass
class TableSchema:
    name: str
    database: str
    description: str = ""
    columns: list[ColumnSchema] = field(default_factory=list)
    partition_keys: list[str] = field(default_factory=list)

    def to_prompt_text(self) -> str:
        """Format the schema as a readable block for the Claude system prompt."""
        lines = [f"Table: {self.database}.{self.name}"]
        if self.description:
            lines.append(f"Description: {self.description}")
        if self.partition_keys:
            lines.append(f"Partition keys: {', '.join(self.partition_keys)}")
        lines.append("Columns:")
        for col in self.columns:
            suffix = f" — {col.description}" if col.description else ""
            lines.append(f"  {col.name} ({col.data_type}){suffix}")
        return "\n".join(lines)


class SchemaResolver:
    """Loads and merges Gold table schemas from Glue Catalog and dbt catalog.json.

    Instantiate once at startup. Call load_all_schemas() to get the full merged
    schema dict. Pass the result to build_system_prompt() in prompts.py so Claude
    starts every question with complete schema awareness.
    """

    def __init__(self, config: AWSConfig) -> None:
        self._config = config
        self._glue = boto3.client("glue", region_name=config.region)
        self._s3 = boto3.client("s3", region_name=config.region)

    def load_all_schemas(self) -> AllSchemas:
        """Load and merge schemas for every Gold table.

        Reads Glue Catalog for physical schema, then overlays dbt catalog.json
        for business context. If catalog.json is absent (pipeline hasn't run
        yet), falls back to Glue-only schema with a warning logged.

        Returns:
            Dict mapping table name to TableSchema for every Gold table.

        Raises:
            SchemaResolutionError: if the Glue Catalog is unreachable or the
                Gold database doesn't exist.
        """
        glue_tables = self._fetch_glue_tables()
        dbt_catalog = self._fetch_dbt_catalog()
        schemas = self._merge(glue_tables, dbt_catalog)
        logger.info(
            "Loaded %d Gold table schemas from Glue Catalog (%s dbt descriptions)",
            len(schemas),
            "with" if dbt_catalog else "without",
        )
        return schemas

    def get_schema(self, table_name: str) -> TableSchema:
        """Fetch the merged schema for a single Gold table.

        Available as a Claude tool for edge cases where Claude needs to
        re-examine one specific table during reasoning. Not called in normal
        operation because all schemas are already embedded in the system prompt.

        Raises:
            SchemaResolutionError: if the table doesn't exist in Glue Catalog.
        """
        dbt_catalog = self._fetch_dbt_catalog()
        try:
            response = self._glue.get_table(
                DatabaseName=self._config.glue_gold_database,
                Name=table_name,
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "EntityNotFoundException":
                raise SchemaResolutionError(
                    f"Table '{table_name}' not found in '{self._config.glue_gold_database}'. "
                    f"Check the table name against the Gold database."
                ) from exc
            raise SchemaResolutionError(
                f"Glue error fetching table '{table_name}': {exc}"
            ) from exc

        return self._merge_table(response["Table"], dbt_catalog)

    # ── Private helpers ────────────────────────────────────────────────────────

    def _fetch_glue_tables(self) -> list[dict[str, Any]]:
        """Paginate through all tables in the Gold Glue database."""
        tables: list[dict[str, Any]] = []
        paginator = self._glue.get_paginator("get_tables")
        try:
            for page in paginator.paginate(DatabaseName=self._config.glue_gold_database):
                tables.extend(page["TableList"])
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "EntityNotFoundException":
                raise SchemaResolutionError(
                    f"Glue database '{self._config.glue_gold_database}' not found. "
                    f"Has the platform infrastructure been applied?"
                ) from exc
            raise SchemaResolutionError(
                f"Failed to list Glue tables: {exc}"
            ) from exc

        if not tables:
            raise SchemaResolutionError(
                f"No tables found in '{self._config.glue_gold_database}'. "
                f"Has the dbt pipeline run successfully?"
            )
        return tables

    def _fetch_dbt_catalog(self) -> dict[str, Any]:
        """Download dbt catalog.json from S3.

        Returns an empty dict if the file doesn't exist yet (pipeline hasn't
        run). Logs a warning so upstream callers know the fallback is active.
        """
        key = "metadata/dbt/catalog.json"
        try:
            response = self._s3.get_object(
                Bucket=self._config.bronze_bucket,
                Key=key,
            )
            catalog: dict[str, Any] = json.loads(response["Body"].read())
            logger.debug(
                "Loaded dbt catalog.json from s3://%s/%s",
                self._config.bronze_bucket,
                key,
            )
            return catalog
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(
                    "dbt catalog.json not found at s3://%s/%s — falling back to "
                    "Glue-only schema (no column descriptions). Run the MWAA "
                    "pipeline to populate business context.",
                    self._config.bronze_bucket,
                    key,
                )
                return {}
            raise SchemaResolutionError(
                f"Failed to fetch dbt catalog.json from s3://{self._config.bronze_bucket}/{key}: {exc}"
            ) from exc

    def _merge(
        self,
        glue_tables: list[dict[str, Any]],
        dbt_catalog: dict[str, Any],
    ) -> AllSchemas:
        return {
            table["Name"]: self._merge_table(table, dbt_catalog)
            for table in glue_tables
        }

    def _merge_table(
        self,
        glue_table: dict[str, Any],
        dbt_catalog: dict[str, Any],
    ) -> TableSchema:
        """Build a TableSchema by merging one Glue table dict with dbt catalog."""
        table_name = glue_table["Name"]

        # dbt catalog.json structure:
        # {"nodes": {"model.{project}.{table_name}": {"columns": {...}, "description": "..."}}}
        # Sources use a "sources" key with the same shape. We match on the
        # suffix to avoid hard-coding the dbt project name.
        dbt_node = self._find_dbt_node(table_name, dbt_catalog)
        dbt_cols: dict[str, Any] = dbt_node.get("columns", {}) if dbt_node else {}
        dbt_description: str = dbt_node.get("description", "") if dbt_node else ""

        storage_desc = glue_table.get("StorageDescriptor", {})
        glue_columns: list[dict[str, Any]] = storage_desc.get("Columns", [])
        partition_col_defs: list[dict[str, Any]] = glue_table.get("PartitionKeys", [])

        # dbt column keys are lowercased in catalog.json
        columns = [
            ColumnSchema(
                name=col["Name"],
                data_type=col["Type"],
                description=dbt_cols.get(col["Name"].lower(), {}).get("description", ""),
            )
            for col in glue_columns
        ]

        return TableSchema(
            name=table_name,
            database=self._config.glue_gold_database,
            description=dbt_description,
            columns=columns,
            partition_keys=[p["Name"] for p in partition_col_defs],
        )

    def _find_dbt_node(
        self,
        table_name: str,
        dbt_catalog: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Find the dbt catalog entry for a given table name.

        Checks both 'nodes' (dbt models) and 'sources' (dbt source tables).
        Matches on the node key suffix to avoid needing the dbt project name.
        """
        if not dbt_catalog:
            return None
        for section in ("nodes", "sources"):
            for key, node in dbt_catalog.get(section, {}).items():
                if key.endswith(f".{table_name}"):
                    return node  # type: ignore[return-value]
        return None
