# Blueprint Starter Library

A set of **out-of-the-box Blueprint templates** for Astronomer's no-code DAG
authoring experience. Drop this into an Astro project and teams can start
composing production Airflow DAGs in the Astro IDE — by dragging templates onto
a canvas and filling in forms — without writing any Airflow code first.

These templates are intentionally **basic, high-value, and provider-agnostic**
so a team can validate Blueprint end to end before investing in custom
templates of their own.

## What's included

**General** (no connection required):

| Blueprint (`name`) | Class | What it does | Backed by |
|---|---|---|---|
| `run_bash` | `RunBash` | Run a shell command | `BashOperator` |
| `run_python` | `RunPython` | Run a Python snippet | TaskFlow `@task` |

**Ingestion & notifications:**

| Blueprint (`name`) | Class | What it does | Backed by |
|---|---|---|---|
| `http_api_extract` | `HttpApiExtract` | Call a REST endpoint, push response to XCom | `HttpOperator` |
| `wait_for_file` | `WaitForFile` | Wait for a file to land before continuing | `FileSensor` |
| `send_slack_notification` | `SendSlackNotification` | Post a message to Slack | `SlackWebhookOperator` |

**Common SQL provider** — every operator/sensor, provider-agnostic via `conn_id`:

| Blueprint (`name`) | Class | Backed by (`common.sql`) |
|---|---|---|
| `run_sql` | `RunSQL` | `SQLExecuteQueryOperator` |
| `data_quality_check` | `DataQualityCheck` | `SQLColumnCheckOperator` / `SQLTableCheckOperator` / `SQLCheckOperator` |
| `load_file_to_table` | `LoadFileToTable` | `SQLExecuteQueryOperator` (COPY) |
| `sql_check` | `SqlCheck` | `SQLCheckOperator` |
| `sql_value_check` | `SqlValueCheck` | `SQLValueCheckOperator` |
| `sql_interval_check` | `SqlIntervalCheck` | `SQLIntervalCheckOperator` |
| `sql_threshold_check` | `SqlThresholdCheck` | `SQLThresholdCheckOperator` |
| `branch_sql` | `BranchSql` | `BranchSQLOperator` |
| `sql_insert_rows` | `SqlInsertRows` | `SQLInsertRowsOperator` |
| `generic_transfer` | `GenericTransfer` | `GenericTransfer` |
| `sql_sensor` | `SqlSensor` | `SqlSensor` |

> Not included: `AnalyticsOperator` — its config is a nested list of datasource
> objects, which the IDE's flat form fields can't represent. Use a custom
> template if you need it.

**Common AI provider** (`apache-airflow-providers-common-ai`) — LLM/agent
operators; each uses a `pydanticai*` connection (`llm_conn_id`) for credentials:

| Blueprint (`name`) | Class | Backed by (`common.ai`) |
|---|---|---|
| `llm` | `Llm` | `LLMOperator` |
| `ai_agent` | `AiAgent` | `AgentOperator` |
| `llm_branch` | `LlmBranch` | `LLMBranchOperator` |
| `llm_sql_query` | `LlmSqlQuery` | `LLMSQLQueryOperator` |
| `llm_file_analysis` | `LlmFileAnalysis` | `LLMFileAnalysisOperator` |
| `llm_schema_compare` | `LlmSchemaCompare` | `LLMSchemaCompareOperator` |

The SQL templates use the provider-agnostic `common.sql` package, so a single
template works against Snowflake, Postgres, Redshift, BigQuery, MySQL, and any
other DB-API connection — no warehouse-specific variants required.

## Project layout

```
dags/
  templates/              # Blueprint definitions (Python classes)
    general.py            #   run_bash, run_python
    sql.py               #   run_sql + all Common SQL operators/sensors
    ai.py                #   all Common AI (LLM/agent) operators
    ingest.py            #   http_api_extract, wait_for_file
    notify.py            #   send_slack_notification
  loader.py               # build_all_dags() — turns *.dag.yaml into DAGs
  hello_blueprint.dag.yaml # zero-setup smoke test (no connections)
  customer_elt.dag.yaml    # realistic ELT loop composed from these templates
blueprint/
  generated-schemas/      # JSON schemas the Astro IDE reads to render forms
```

## Getting started

1. Install dependencies (already listed in `requirements.txt`): `airflow-blueprint`
   plus the `slack`, `http`, and `common-ai` providers (`common.sql` and
   `standard` ship with Astro Runtime).
2. Start Airflow locally with `astro dev start`.
3. Trigger **`hello_blueprint`** — it needs no connections and proves the wiring
   works end to end.
4. Configure the connections referenced in `customer_elt.dag.yaml`
   (`snowflake_default`, `fs_default`, `slack_default`) to run the full loop.

## Authoring in the Astro IDE

Platform teams maintain the templates in `dags/templates/`; everyone else
composes them in the Blueprint canvas. After changing a template's config
schema, regenerate its JSON schema so the IDE forms stay in sync:

```bash
blueprint schema <name> > blueprint/generated-schemas/<name>.schema.json
```

Useful CLI commands:

```bash
blueprint list                 # list available blueprints
blueprint describe run_sql      # show a blueprint's config schema
blueprint lint dags/customer_elt.dag.yaml   # validate a workflow YAML
```

## Design conventions

- **Flat config only.** The IDE renders `str`, `int`, `float`, `bool`,
  `Literal` enums, and typed lists — not nested models, dicts, or unions. Every
  config model here is flat by design.
- **Connections, not secrets.** Templates reference an Airflow `conn_id`; they
  never embed credentials.
- **Rich field descriptions.** Each `Field(description=...)` becomes the help
  text shown next to the form input in the IDE.
