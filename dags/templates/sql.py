"""SQL Blueprint templates backed by the provider-agnostic ``common.sql`` package.

Every template here talks to the warehouse through an Airflow connection
(``conn_id``), so a single Blueprint works against Snowflake, Postgres,
Redshift, BigQuery, MySQL, Databricks, and any other DB-API connection without
a warehouse-specific variant.

Blueprints:
    - ``run_sql``             -> RunSQL
    - ``data_quality_check``  -> DataQualityCheck
    - ``load_file_to_table``  -> LoadFileToTable
"""

from __future__ import annotations

from typing import Literal

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)

from blueprint import (
    BaseModel,
    Blueprint,
    Field,
    TaskOrGroup,
    model_validator,
)


# --- Run SQL: execute arbitrary SQL against any connection ---------------------


class RunSQLConfig(BaseModel):
    conn_id: str = Field(
        description="Airflow connection ID for the target database, "
        "e.g. `snowflake_default`.",
    )
    sql: str | None = Field(
        default=None,
        description="SQL to run inline. Provide either `sql` or `sql_file`.",
    )
    sql_file: str | None = Field(
        default=None,
        description="Path to a `.sql` file (resolved via the DAG template "
        "search path). Provide either `sql` or `sql_file`.",
    )
    autocommit: bool = Field(
        default=True,
        description="Commit each statement automatically.",
    )
    split_statements: bool = Field(
        default=True,
        description="Split the SQL on `;` and run each statement separately.",
    )

    @model_validator(mode="after")
    def _exactly_one_source(self) -> "RunSQLConfig":
        if bool(self.sql) == bool(self.sql_file):
            raise ValueError("Provide exactly one of `sql` or `sql_file`.")
        return self


class RunSQL(Blueprint[RunSQLConfig]):
    """Run one or more SQL statements against any SQL connection.

    The single most common Airflow task — transformations, DDL, ad-hoc
    statements — expressed once and reused across every warehouse.
    """

    def render(self, config: RunSQLConfig) -> TaskOrGroup:
        return SQLExecuteQueryOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.sql or config.sql_file,
            autocommit=config.autocommit,
            split_statements=config.split_statements,
        )


# --- Data Quality Check: assertions that build trust in the data ---------------


class DataQualityCheckConfig(BaseModel):
    conn_id: str = Field(
        description="Airflow connection ID for the database to check.",
    )
    check_type: Literal["row_count", "not_null", "unique", "custom_sql"] = Field(
        description="`row_count`: table has at least `min_rows`. "
        "`not_null`: `column` has no NULLs. "
        "`unique`: `column` has no duplicates. "
        "`custom_sql`: `custom_sql` returns a truthy single row.",
    )
    table: str | None = Field(
        default=None,
        description="Fully-qualified table name. Required for row_count, "
        "not_null, and unique checks.",
    )
    column: str | None = Field(
        default=None,
        description="Column to check. Required for not_null and unique checks.",
    )
    min_rows: int = Field(
        default=1,
        ge=0,
        description="Minimum acceptable row count (row_count check).",
    )
    custom_sql: str | None = Field(
        default=None,
        description="SQL returning a single row; the check passes if the value "
        "is truthy. Required for the custom_sql check.",
    )

    @model_validator(mode="after")
    def _validate_required(self) -> "DataQualityCheckConfig":
        if self.check_type in ("row_count", "not_null", "unique") and not self.table:
            raise ValueError(f"`table` is required for the {self.check_type} check.")
        if self.check_type in ("not_null", "unique") and not self.column:
            raise ValueError(f"`column` is required for the {self.check_type} check.")
        if self.check_type == "custom_sql" and not self.custom_sql:
            raise ValueError("`custom_sql` is required for the custom_sql check.")
        return self


class DataQualityCheck(Blueprint[DataQualityCheckConfig]):
    """Assert a data-quality condition and fail the task if it is not met.

    Cheap to run and the fastest way to earn trust in a pipeline. Backed by the
    ``common.sql`` check operators, so it works against any SQL connection.
    """

    def render(self, config: DataQualityCheckConfig) -> TaskOrGroup:
        if config.check_type == "row_count":
            return SQLTableCheckOperator(
                task_id=self.step_id,
                conn_id=config.conn_id,
                table=config.table,
                checks={
                    "row_count_check": {
                        "check_statement": f"COUNT(*) >= {config.min_rows}",
                    },
                },
            )
        if config.check_type == "not_null":
            return SQLColumnCheckOperator(
                task_id=self.step_id,
                conn_id=config.conn_id,
                table=config.table,
                column_mapping={config.column: {"null_check": {"equal_to": 0}}},
            )
        if config.check_type == "unique":
            return SQLColumnCheckOperator(
                task_id=self.step_id,
                conn_id=config.conn_id,
                table=config.table,
                column_mapping={
                    config.column: {"distinct_check": {"equal_to": "unique"}}
                },
            )
        # custom_sql
        return SQLCheckOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.custom_sql,
        )


# --- Load File to Table: COPY data from object storage into a warehouse --------


class LoadFileToTableConfig(BaseModel):
    conn_id: str = Field(
        description="Airflow connection ID for the target warehouse.",
    )
    source_uri: str = Field(
        description="Location of the source data, e.g. "
        "`s3://my-bucket/path/` or a named stage.",
    )
    target_table: str = Field(
        description="Fully-qualified destination table, e.g. `analytics.orders`.",
    )
    dialect: Literal["snowflake", "redshift", "postgres"] = Field(
        default="snowflake",
        description="Warehouse SQL dialect used to build the COPY statement.",
    )
    file_format: Literal["csv", "parquet", "json"] = Field(
        default="csv",
        description="Format of the source files.",
    )
    mode: Literal["append", "overwrite"] = Field(
        default="append",
        description="`overwrite` truncates the table before loading.",
    )


class LoadFileToTable(Blueprint[LoadFileToTableConfig]):
    """Load files from object storage into a warehouse table via COPY.

    Builds the appropriate ``COPY``/``COPY INTO`` statement for the chosen
    dialect and runs it through the ``common.sql`` executor.
    """

    def _build_sql(self, config: LoadFileToTableConfig) -> str:
        statements: list[str] = []
        if config.mode == "overwrite":
            statements.append(f"TRUNCATE TABLE {config.target_table};")

        if config.dialect == "snowflake":
            statements.append(
                f"COPY INTO {config.target_table} FROM '{config.source_uri}' "
                f"FILE_FORMAT = (TYPE = '{config.file_format.upper()}');"
            )
        elif config.dialect == "redshift":
            fmt = "CSV" if config.file_format == "csv" else config.file_format.upper()
            statements.append(
                f"COPY {config.target_table} FROM '{config.source_uri}' "
                f"FORMAT AS {fmt};"
            )
        else:  # postgres
            fmt = "csv" if config.file_format == "csv" else config.file_format
            statements.append(
                f"COPY {config.target_table} FROM '{config.source_uri}' "
                f"WITH (FORMAT {fmt});"
            )
        return "\n".join(statements)

    def render(self, config: LoadFileToTableConfig) -> TaskOrGroup:
        return SQLExecuteQueryOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=self._build_sql(config),
            split_statements=True,
            autocommit=True,
        )
