"""Transformation Blueprint templates.

Blueprints:
    - ``dbt_build`` -> DbtBuild
"""

from __future__ import annotations

import shlex
from typing import Literal

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class DbtBuildConfig(BaseModel):
    project_dir: str = Field(
        description="Path to the dbt project, e.g. `/usr/local/airflow/dbt/my_project`.",
    )
    command: Literal["build", "run", "test", "seed", "snapshot"] = Field(
        default="build",
        description="dbt command to execute. `build` runs models, tests, seeds, "
        "and snapshots together.",
    )
    select: str | None = Field(
        default=None,
        description="dbt `--select` selector, e.g. `tag:nightly` or `my_model+`.",
    )
    target: str | None = Field(
        default=None,
        description="dbt target/profile to use, e.g. `prod`.",
    )
    profiles_dir: str | None = Field(
        default=None,
        description="Path to the dbt profiles directory. Defaults to the "
        "project directory.",
    )
    full_refresh: bool = Field(
        default=False,
        description="Pass `--full-refresh` to rebuild incremental models.",
    )


class DbtBuild(Blueprint[DbtBuildConfig]):
    """Run a dbt command via the dbt CLI.

    Lightweight wrapper around the dbt CLI — requires `dbt-core` plus the
    relevant adapter (e.g. `dbt-snowflake`) to be installed in the project.
    """

    def _build_command(self, config: DbtBuildConfig) -> str:
        parts = ["dbt", config.command]
        profiles_dir = config.profiles_dir or config.project_dir
        parts += ["--project-dir", config.project_dir]
        parts += ["--profiles-dir", profiles_dir]
        if config.target:
            parts += ["--target", config.target]
        if config.select:
            parts += ["--select", config.select]
        if config.full_refresh:
            parts.append("--full-refresh")
        return " ".join(shlex.quote(p) for p in parts)

    def render(self, config: DbtBuildConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=self._build_command(config),
        )
