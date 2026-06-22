# Blueprint Starter Library

A set of **out-of-the-box Blueprint templates** for Astronomer's no-code DAG
authoring experience. Drop this into an Astro project and teams can start
composing production Airflow DAGs in the Astro IDE — by dragging templates onto
a canvas and filling in forms — without writing any Airflow code first.

These templates are intentionally **basic, high-value, and provider-agnostic**
so a team can validate Blueprint end to end before investing in custom
templates of their own.

## What's included

| Blueprint (`name`) | Class | What it does | Backed by | Needs a connection? |
|---|---|---|---|---|
| `run_bash` | `RunBash` | Run a shell command | `BashOperator` | No |
| `run_python` | `RunPython` | Run a Python snippet | TaskFlow `@task` | No |
| `run_sql` | `RunSQL` | Execute SQL against any warehouse | `SQLExecuteQueryOperator` | Yes |
| `data_quality_check` | `DataQualityCheck` | Row-count / not-null / unique / custom-SQL assertions | `common.sql` check operators | Yes |
| `load_file_to_table` | `LoadFileToTable` | COPY files from object storage into a table | `SQLExecuteQueryOperator` | Yes |
| `dbt_build` | `DbtBuild` | Run a dbt command (`build`/`run`/`test`/…) | `BashOperator` + dbt CLI | dbt + adapter |
| `http_api_extract` | `HttpApiExtract` | Call a REST endpoint, push response to XCom | `HttpOperator` | HTTP conn |
| `wait_for_file` | `WaitForFile` | Wait for a file to land before continuing | `FileSensor` | fs conn |
| `send_slack_notification` | `SendSlackNotification` | Post a message to Slack | `SlackWebhookOperator` | Slack conn |

The SQL templates use the provider-agnostic `common.sql` package, so a single
template works against Snowflake, Postgres, Redshift, BigQuery, MySQL, and any
other DB-API connection — no warehouse-specific variants required.

## Project layout

```
dags/
  templates/              # Blueprint definitions (Python classes)
    general.py            #   run_bash, run_python
    sql.py               #   run_sql, data_quality_check, load_file_to_table
    transform.py         #   dbt_build
    ingest.py            #   http_api_extract, wait_for_file
    notify.py            #   send_slack_notification
  loader.py               # build_all_dags() — turns *.dag.yaml into DAGs
  hello_blueprint.dag.yaml # zero-setup smoke test (no connections)
  customer_elt.dag.yaml    # realistic ELT loop composed from these templates
blueprint/
  generated-schemas/      # JSON schemas the Astro IDE reads to render forms
```

## Getting started

1. Install dependencies (already listed in `requirements.txt`):
   `airflow-blueprint>=0.2.0` and `apache-airflow-providers-slack`.
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
