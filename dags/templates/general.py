"""General-purpose Blueprint templates.

These run with **no external connection configured**, which makes them the
fastest way to validate that Blueprint is wired up correctly end to end: drop
one on the canvas, trigger the DAG, and watch it succeed.

Blueprints:
    - ``run_bash``   -> RunBash
    - ``run_python`` -> RunPython
"""

from __future__ import annotations

import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup

# run_python executes arbitrary code, so it is disabled unless a deployment
# explicitly opts in via this environment variable.
_ALLOW_PYTHON_ENV = "BLUEPRINT_ALLOW_ARBITRARY_PYTHON"


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
    yet have a dedicated Blueprint. Because it executes arbitrary code it is
    **disabled by default**: set ``BLUEPRINT_ALLOW_ARBITRARY_PYTHON=true`` on the
    deployment to enable it, and only expose it to trusted authors.
    """

    def render(self, config: RunPythonConfig) -> TaskOrGroup:
        if os.environ.get(_ALLOW_PYTHON_ENV, "").lower() not in ("1", "true", "yes"):
            raise ValueError(
                "run_python executes arbitrary Python and is disabled by default. "
                f"Set {_ALLOW_PYTHON_ENV}=true on the deployment to enable it."
            )

        code = config.code

        def _run() -> None:
            exec(code, {"__name__": "__blueprint__"})

        return PythonOperator(task_id=self.step_id, python_callable=_run)
