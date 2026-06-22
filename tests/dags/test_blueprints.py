"""Hardening test: assert the full Blueprint inventory is discoverable.

Blueprint discovery silently skips any template module that fails to import
(e.g. a missing optional dependency or a renamed provider symbol), which makes
its blueprints disappear without an error. This test turns that into a loud,
debuggable failure and guards against provider drift.

Run with ``astro dev pytest`` or in CI.
"""

from __future__ import annotations

from blueprint.loaders import discover_blueprints

EXPECTED_BLUEPRINTS = {
    # general
    "run_bash",
    "run_python",
    # ingest / notify
    "http_api_extract",
    "wait_for_file",
    "send_slack_notification",
    # common.sql
    "run_sql",
    "data_quality_check",
    "load_file_to_table",
    "sql_check",
    "sql_value_check",
    "sql_interval_check",
    "sql_threshold_check",
    "branch_sql",
    "generic_transfer",
    "sql_sensor",
    # common.ai
    "llm",
    "ai_agent",
    "llm_branch",
    "llm_sql_query",
    "llm_file_analysis",
    "llm_schema_compare",
}


def test_all_expected_blueprints_are_discovered():
    discovered = {bp["name"] for bp in discover_blueprints()}
    missing = EXPECTED_BLUEPRINTS - discovered
    assert not missing, (
        f"Expected blueprints missing from discovery: {sorted(missing)}. "
        "A template module likely failed to import — check provider/optional "
        "dependencies in requirements.txt."
    )
