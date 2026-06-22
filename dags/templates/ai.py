"""Common AI Blueprint templates (apache-airflow-providers-common-ai).

LLM and agent operators built on Pydantic AI. Each talks to a model provider
through an Airflow connection (``llm_conn_id``) of a ``pydanticai*`` type, where
credentials live — no keys in code. The model is taken from the connection and
can be overridden per step with ``model_id`` (e.g. ``openai:gpt-5`` or
``anthropic:claude-sonnet-4-20250514``).

Blueprints:
    - ``llm``                 -> Llm
    - ``ai_agent``            -> AiAgent
    - ``llm_branch``          -> LlmBranch
    - ``llm_sql_query``       -> LlmSqlQuery
    - ``llm_file_analysis``   -> LlmFileAnalysis
    - ``llm_schema_compare``  -> LlmSchemaCompare
"""

from __future__ import annotations

from typing import Literal

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.ai.operators.llm_file_analysis import (
    LLMFileAnalysisOperator,
)
from airflow.providers.common.ai.operators.llm_schema_compare import (
    LLMSchemaCompareOperator,
)
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup, model_validator


# --- LLM: a single, stateless model call ---------------------------------------


class LlmConfig(BaseModel):
    prompt: str = Field(
        description="The user prompt sent to the model. Templated, e.g. "
        "`Summarize: {% raw %}{{ ti.xcom_pull(task_ids='fetch') }}{% endraw %}`.",
    )
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider (a "
        "`pydanticai*` connection holding the credentials).",
    )
    system_prompt: str = Field(
        default="",
        description="Optional instructions that steer the model's behavior.",
    )
    model_id: str | None = Field(
        default=None,
        description="Override the connection's model, e.g. `openai:gpt-5`.",
    )


class Llm(Blueprint[LlmConfig]):
    """Make a single LLM call and return its output.

    Best for classification, summarization, extraction — any one-shot prompt.
    Wraps LLMOperator.
    """

    def render(self, config: LlmConfig) -> TaskOrGroup:
        return LLMOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            system_prompt=config.system_prompt,
            model_id=config.model_id,
        )


# --- Agent: multi-step reasoning ------------------------------------------------


class AiAgentConfig(BaseModel):
    prompt: str = Field(description="The task/question given to the agent.")
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider.",
    )
    system_prompt: str = Field(
        default="",
        description="Instructions defining the agent's behavior.",
    )
    model_id: str | None = Field(
        default=None, description="Override the connection's model."
    )
    durable: bool = Field(
        default=False,
        description="Cache tool results and model responses so retries skip "
        "completed steps.",
    )


class AiAgent(Blueprint[AiAgentConfig]):
    """Run a multi-step agent that reasons until it answers the prompt.

    Wraps AgentOperator. Tool/toolset wiring requires a custom template; this
    template exposes the prompt-driven configuration.
    """

    def render(self, config: AiAgentConfig) -> TaskOrGroup:
        return AgentOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            system_prompt=config.system_prompt,
            model_id=config.model_id,
            durable=config.durable,
        )


# --- LLM Branch: let the model pick downstream steps ---------------------------


class LlmBranchConfig(BaseModel):
    prompt: str = Field(
        description="What to classify/decide. The model chooses among the "
        "downstream step IDs.",
    )
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider.",
    )
    system_prompt: str = Field(
        default="",
        description="Optional classification instructions.",
    )
    model_id: str | None = Field(
        default=None, description="Override the connection's model."
    )
    allow_multiple_branches: bool = Field(
        default=False,
        description="Allow the model to select more than one downstream branch.",
    )


class LlmBranch(Blueprint[LlmBranchConfig]):
    """Ask an LLM which downstream step(s) to run next.

    Wraps LLMBranchOperator. Downstream steps become the available branches.
    """

    def render(self, config: LlmBranchConfig) -> TaskOrGroup:
        return LLMBranchOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            system_prompt=config.system_prompt,
            model_id=config.model_id,
            allow_multiple_branches=config.allow_multiple_branches,
        )


# --- LLM SQL Query: natural language to validated SQL --------------------------


class LlmSqlQueryConfig(BaseModel):
    prompt: str = Field(
        description="Natural-language question to turn into a SQL query.",
    )
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider.",
    )
    db_conn_id: str = Field(
        description="Airflow connection ID for the database the SQL targets.",
    )
    table_names: list[str] = Field(
        description="Tables the generated query is allowed to reference.",
    )
    dialect: str | None = Field(
        default=None,
        description="SQL dialect for generation/validation, e.g. `postgres`.",
    )
    validate_sql: bool = Field(
        default=True,
        description="Validate generated SQL by parsing its AST (sqlglot).",
    )
    model_id: str | None = Field(
        default=None, description="Override the connection's model."
    )


class LlmSqlQuery(Blueprint[LlmSqlQueryConfig]):
    """Generate (and optionally validate) SQL from a natural-language prompt.

    Wraps LLMSQLQueryOperator.
    """

    def render(self, config: LlmSqlQueryConfig) -> TaskOrGroup:
        return LLMSQLQueryOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            model_id=config.model_id,
            db_conn_id=config.db_conn_id,
            table_names=config.table_names,
            dialect=config.dialect,
            validate_sql=config.validate_sql,
        )


# --- LLM File Analysis: analyze files from storage -----------------------------


class LlmFileAnalysisConfig(BaseModel):
    prompt: str = Field(description="What to extract or analyze from the file(s).")
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider.",
    )
    file_path: str = Field(
        description="File location, e.g. `s3://bucket/data.csv` or a local path.",
    )
    file_conn_id: str | None = Field(
        default=None,
        description="Connection ID for the object store holding the file.",
    )
    multi_modal: bool = Field(
        default=False,
        description="Attach images/PDFs as binary for multi-modal analysis.",
    )
    sample_rows: int = Field(
        default=10,
        ge=1,
        description="Rows to sample from tabular files.",
    )
    max_files: int = Field(
        default=20, ge=1, description="Maximum number of files to analyze."
    )
    system_prompt: str = Field(
        default="", description="Optional instructions for the analysis."
    )
    model_id: str | None = Field(
        default=None, description="Override the connection's model."
    )


class LlmFileAnalysis(Blueprint[LlmFileAnalysisConfig]):
    """Analyze files from object storage with a single LLM call.

    Wraps LLMFileAnalysisOperator.
    """

    def render(self, config: LlmFileAnalysisConfig) -> TaskOrGroup:
        return LLMFileAnalysisOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            model_id=config.model_id,
            system_prompt=config.system_prompt,
            file_path=config.file_path,
            file_conn_id=config.file_conn_id,
            multi_modal=config.multi_modal,
            sample_rows=config.sample_rows,
            max_files=config.max_files,
        )


# --- LLM Schema Compare: detect schema drift across databases ------------------


class LlmSchemaCompareConfig(BaseModel):
    llm_conn_id: str = Field(
        description="Airflow connection ID for the model provider.",
    )
    db_conn_ids: list[str] = Field(
        description="Database connection IDs whose schemas are compared.",
    )
    table_names: list[str] = Field(
        description="Tables to compare across the given connections.",
    )
    context_strategy: Literal["basic", "full"] = Field(
        default="full",
        description="How much schema context to give the model.",
    )
    prompt: str = Field(
        default="Compare the database schemas and report any mismatches.",
        description="Instruction for the comparison.",
    )
    model_id: str | None = Field(
        default=None, description="Override the connection's model."
    )

    @model_validator(mode="after")
    def _enough_targets(self) -> "LlmSchemaCompareConfig":
        if not self.db_conn_ids or not self.table_names:
            raise ValueError(
                "Provide at least one db_conn_id and one table_name to compare."
            )
        if len(self.db_conn_ids) * len(self.table_names) < 2:
            raise ValueError(
                "Schema compare needs at least two connection/table combinations "
                "(e.g. two db_conn_ids, or one db_conn_id with two table_names)."
            )
        return self


class LlmSchemaCompare(Blueprint[LlmSchemaCompareConfig]):
    """Compare schemas across databases and flag drift using LLM reasoning.

    Wraps LLMSchemaCompareOperator. Needs at least two table/connection
    combinations to compare.
    """

    def render(self, config: LlmSchemaCompareConfig) -> TaskOrGroup:
        return LLMSchemaCompareOperator(
            task_id=self.step_id,
            prompt=config.prompt,
            llm_conn_id=config.llm_conn_id,
            model_id=config.model_id,
            db_conn_ids=config.db_conn_ids,
            table_names=config.table_names,
            context_strategy=config.context_strategy,
        )
