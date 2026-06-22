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

from airflow.providers.common.sql.operators.generic_transfer import (
    GenericTransfer as GenericTransferOp,
)
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLInsertRowsOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLThresholdCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.common.sql.sensors.sql import SqlSensor as SqlSensorOp

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


# --- SQL Check: assert a query returns all-truthy values -----------------------


class SqlCheckConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    sql: str = Field(
        description="SQL returning a single row. The check fails if any value in "
        "that row is falsy (0, empty, NULL).",
    )


class SqlCheck(Blueprint[SqlCheckConfig]):
    """Run a SQL statement and fail unless every value in the first row is truthy.

    Wraps SQLCheckOperator — the simplest data-quality gate.
    """

    def render(self, config: SqlCheckConfig) -> TaskOrGroup:
        return SQLCheckOperator(
            task_id=self.step_id, conn_id=config.conn_id, sql=config.sql
        )


# --- SQL Value Check: compare a query result to an expected value --------------


class SqlValueCheckConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    sql: str = Field(description="SQL returning a single value to check.")
    pass_value: str = Field(
        description="Expected value. The check passes if the result equals this "
        "(within `tolerance`, if set).",
    )
    tolerance: float | None = Field(
        default=None,
        description="Optional fractional tolerance, e.g. 0.05 allows ±5%.",
    )


class SqlValueCheck(Blueprint[SqlValueCheckConfig]):
    """Assert a query result equals an expected value (with optional tolerance).

    Wraps SQLValueCheckOperator.
    """

    def render(self, config: SqlValueCheckConfig) -> TaskOrGroup:
        return SQLValueCheckOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.sql,
            pass_value=config.pass_value,
            tolerance=config.tolerance,
        )


# --- SQL Interval Check: compare a metric to a previous period -----------------


class SqlIntervalCheckConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    table: str = Field(description="Table the metric is computed over.")
    metric: str = Field(
        description="SQL expression for the metric, e.g. `COUNT(*)` or "
        "`SUM(amount)`.",
    )
    threshold: float = Field(
        gt=0,
        description="Max allowed ratio change vs. the comparison period "
        "(per `ratio_formula`).",
    )
    date_filter_column: str = Field(
        default="ds",
        description="Date column used to select each period's rows.",
    )
    days_back: int = Field(
        default=7,
        ge=1,
        description="How many days back the comparison period is.",
    )
    ratio_formula: Literal["max_over_min", "relative_diff"] = Field(
        default="max_over_min",
        description="How the ratio between periods is computed.",
    )
    ignore_zero: bool = Field(
        default=True,
        description="Ignore rows where the metric is zero.",
    )


class SqlIntervalCheck(Blueprint[SqlIntervalCheckConfig]):
    """Compare a metric today vs. N days ago and fail if it drifts too far.

    Wraps SQLIntervalCheckOperator. Configures a single metric; multi-metric
    checks require a custom template.
    """

    def render(self, config: SqlIntervalCheckConfig) -> TaskOrGroup:
        return SQLIntervalCheckOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            table=config.table,
            metrics_thresholds={config.metric: config.threshold},
            date_filter_column=config.date_filter_column,
            days_back=-abs(config.days_back),
            ratio_formula=config.ratio_formula,
            ignore_zero=config.ignore_zero,
        )


# --- SQL Threshold Check: assert a value falls within a range ------------------


class SqlThresholdCheckConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    sql: str = Field(description="SQL returning a single numeric value.")
    min_threshold: float = Field(description="Minimum acceptable value (inclusive).")
    max_threshold: float = Field(description="Maximum acceptable value (inclusive).")


class SqlThresholdCheck(Blueprint[SqlThresholdCheckConfig]):
    """Assert a query result falls between a min and max threshold.

    Wraps SQLThresholdCheckOperator.
    """

    def render(self, config: SqlThresholdCheckConfig) -> TaskOrGroup:
        return SQLThresholdCheckOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.sql,
            min_threshold=config.min_threshold,
            max_threshold=config.max_threshold,
        )


# --- Branch SQL: choose downstream steps based on a SQL result -----------------


class BranchSqlConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    sql: str = Field(
        description="SQL returning a single boolean-ish value used to branch.",
    )
    follow_task_ids_if_true: list[str] = Field(
        description="Step IDs to run when the query is true.",
    )
    follow_task_ids_if_false: list[str] = Field(
        description="Step IDs to run when the query is false.",
    )


class BranchSql(Blueprint[BranchSqlConfig]):
    """Branch the DAG down one path or another based on a SQL result.

    Wraps BranchSQLOperator. The follow lists must reference other step IDs in
    the same workflow.
    """

    def render(self, config: BranchSqlConfig) -> TaskOrGroup:
        return BranchSQLOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.sql,
            follow_task_ids_if_true=config.follow_task_ids_if_true,
            follow_task_ids_if_false=config.follow_task_ids_if_false,
        )


# --- SQL Insert Rows: insert rows (typically from an upstream task) ------------


class SqlInsertRowsConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the target database.")
    table_name: str = Field(description="Destination table name.")
    rows: str = Field(
        description="Rows to insert. This field is templated — usually an XCom "
        "reference resolving to a list of row tuples, e.g. "
        "`{{ ti.xcom_pull(task_ids='extract') }}`.",
    )
    db_schema: str | None = Field(
        default=None, description="Optional schema for the destination table."
    )
    columns: list[str] | None = Field(
        default=None, description="Optional explicit column names for the insert."
    )


class SqlInsertRows(Blueprint[SqlInsertRowsConfig]):
    """Insert rows into a table, typically piping data from an upstream task.

    Wraps SQLInsertRowsOperator.
    """

    def render(self, config: SqlInsertRowsConfig) -> TaskOrGroup:
        return SQLInsertRowsOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            table_name=config.table_name,
            rows=config.rows,
            schema=config.db_schema,
            columns=config.columns,
        )


# --- Generic Transfer: copy query results from one connection to another -------


class GenericTransferConfig(BaseModel):
    source_conn_id: str = Field(description="Connection to read from.")
    destination_conn_id: str = Field(description="Connection to write to.")
    destination_table: str = Field(description="Table to insert the results into.")
    sql: str = Field(description="Query run against the source connection.")
    preoperator: str | None = Field(
        default=None,
        description="Optional SQL run on the destination before loading, "
        "e.g. a TRUNCATE.",
    )


class GenericTransfer(Blueprint[GenericTransferConfig]):
    """Move the results of a query from a source connection into a destination table.

    Wraps GenericTransfer, a database-agnostic transfer operator.
    """

    def render(self, config: GenericTransferConfig) -> TaskOrGroup:
        return GenericTransferOp(
            task_id=self.step_id,
            sql=config.sql,
            destination_table=config.destination_table,
            source_conn_id=config.source_conn_id,
            destination_conn_id=config.destination_conn_id,
            preoperator=config.preoperator,
        )


# --- SQL Sensor: wait until a query returns a truthy result --------------------


class SqlSensorConfig(BaseModel):
    conn_id: str = Field(description="Airflow connection ID for the database.")
    sql: str = Field(
        description="SQL polled until its first cell is truthy (non-zero, "
        "non-empty, non-NULL).",
    )
    fail_on_empty: bool = Field(
        default=False,
        description="Fail immediately if the query returns no rows.",
    )
    poke_interval: int = Field(
        default=60, ge=1, description="Seconds between checks."
    )
    timeout: int = Field(
        default=60 * 60 * 24,
        ge=1,
        description="Seconds to wait before the sensor times out and fails.",
    )
    mode: Literal["poke", "reschedule"] = Field(
        default="reschedule",
        description="`reschedule` frees the worker slot between checks.",
    )


class SqlSensor(Blueprint[SqlSensorConfig]):
    """Wait until a SQL query returns a truthy result before continuing.

    Wraps SqlSensor.
    """

    def render(self, config: SqlSensorConfig) -> TaskOrGroup:
        return SqlSensorOp(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=config.sql,
            fail_on_empty=config.fail_on_empty,
            poke_interval=config.poke_interval,
            timeout=config.timeout,
            mode=config.mode,
        )
