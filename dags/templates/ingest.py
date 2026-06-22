"""Ingestion and orchestration-utility Blueprint templates.

Blueprints:
    - ``http_api_extract`` -> HttpApiExtract
    - ``wait_for_file``    -> WaitForFile
"""

from __future__ import annotations

from typing import Literal

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class HttpApiExtractConfig(BaseModel):
    http_conn_id: str = Field(
        default="http_default",
        description="Airflow HTTP connection ID holding the base URL.",
    )
    endpoint: str = Field(
        description="Path appended to the connection's base URL, e.g. `/v1/orders`.",
    )
    method: Literal["GET", "POST", "PUT", "DELETE"] = Field(
        default="GET",
        description="HTTP method.",
    )
    data: str | None = Field(
        default=None,
        description="Request body (for POST/PUT) or query string (for GET).",
    )
    log_response: bool = Field(
        default=True,
        description="Log the response body. The response is also pushed to XCom.",
    )


class HttpApiExtract(Blueprint[HttpApiExtractConfig]):
    """Call a REST API endpoint and push the response to XCom.

    Works against any endpoint reachable via an Airflow HTTP connection; point
    it at a public API for a connection-free smoke test.
    """

    def render(self, config: HttpApiExtractConfig) -> TaskOrGroup:
        return HttpOperator(
            task_id=self.step_id,
            http_conn_id=config.http_conn_id,
            endpoint=config.endpoint,
            method=config.method,
            data=config.data,
            log_response=config.log_response,
        )


class WaitForFileConfig(BaseModel):
    filepath: str = Field(
        description="Path (or glob) to wait for, relative to the connection's root.",
    )
    fs_conn_id: str = Field(
        default="fs_default",
        description="Filesystem connection ID defining the root path.",
    )
    poke_interval: int = Field(
        default=60,
        ge=1,
        description="Seconds between checks.",
    )
    timeout: int = Field(
        default=60 * 60 * 24,
        ge=1,
        description="Seconds to wait before the sensor times out and fails.",
    )
    mode: Literal["poke", "reschedule"] = Field(
        default="reschedule",
        description="`reschedule` frees the worker slot between checks "
        "(recommended for long waits).",
    )


class WaitForFile(Blueprint[WaitForFileConfig]):
    """Wait until a file appears before letting downstream steps run.

    A common gating pattern for file-driven pipelines.
    """

    def render(self, config: WaitForFileConfig) -> TaskOrGroup:
        return FileSensor(
            task_id=self.step_id,
            filepath=config.filepath,
            fs_conn_id=config.fs_conn_id,
            poke_interval=config.poke_interval,
            timeout=config.timeout,
            mode=config.mode,
        )
