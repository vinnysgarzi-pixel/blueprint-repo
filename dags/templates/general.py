"""General-purpose Blueprint templates.

These run with **no external connection configured**, which makes them the
fastest way to validate that Blueprint is wired up correctly end to end: drop
one on the canvas, trigger the DAG, and watch it succeed.

Blueprints:
    - ``run_bash``   -> RunBash
    - ``run_python`` -> RunPython
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class RunBashConfig(BaseModel):
    bash_command: str = Field(
        description="The shell command to run, e.g. `echo 'hello world'`.",
    )
    cwd: str | None = Field(
        default=None,
        description="Working directory to run the command in. Defaults to a "
        "temporary directory.",
    )


class RunBash(Blueprint[RunBashConfig]):
    """Run an arbitrary shell command.

    A dependency-free escape hatch and the quickest smoke test for a new
    Blueprint setup.
    """

    def render(self, config: RunBashConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=config.bash_command,
            cwd=config.cwd,
        )


class RunPythonConfig(BaseModel):
    code: str = Field(
        description="Python source to execute. Runs in a fresh namespace; use "
        "`print(...)` for output. Example: `print(sum(range(10)))`.",
    )


class RunPython(Blueprint[RunPythonConfig]):
    """Run an arbitrary snippet of Python.

    The most flexible escape hatch — useful for quick custom logic that does not
    yet have a dedicated Blueprint. Because it executes arbitrary code, platform
    teams should only expose this template to trusted authors.
    """

    def render(self, config: RunPythonConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:

            @task(task_id="run")
            def run() -> None:
                exec(config.code, {"__name__": "__blueprint__"})

            run()
        return group
