"""Generate JSON schemas for every Blueprint in ``dags/templates/``.

This mirrors what ``blueprint schema <name>`` produces, so the output can be
committed to ``blueprint/generated-schemas/`` for the Astro IDE to read. In a
real Airflow runtime you would simply run::

    blueprint schema <name> > blueprint/generated-schemas/<name>.schema.json

This standalone generator exists so schemas can be produced in environments
where Airflow itself is not installed: it stubs the operator imports, loads the
real Pydantic config models from the template modules (no duplication), and
applies the exact same schema envelope as ``blueprint/cli.py``.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import re
import sys
import types
from pathlib import Path
from typing import Any, get_args, get_origin

import pydantic

REPO = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = REPO / "dags" / "templates"
OUT_DIR = REPO / "blueprint" / "generated-schemas"

# Airflow 3.x TriggerRule values, sorted (matches CLI's sorted(...) output).
TRIGGER_RULE_VALUES = [
    "all_done",
    "all_done_setup_success",
    "all_failed",
    "all_skipped",
    "all_success",
    "always",
    "none_failed",
    "none_failed_min_one_success",
    "none_skipped",
    "one_done",
    "one_failed",
    "one_success",
]


# --- Stub out the airflow imports the template modules do at import time -------


class _StubModule(types.ModuleType):
    def __getattr__(self, name: str) -> Any:  # noqa: D105
        return type(name, (), {})


def _install_stubs() -> None:
    for mod_name in (
        "airflow",
        "airflow.sdk",
        "airflow.utils",
        "airflow.utils.task_group",
        "airflow.providers",
        "airflow.providers.standard",
        "airflow.providers.standard.operators",
        "airflow.providers.standard.operators.bash",
        "airflow.providers.standard.sensors",
        "airflow.providers.standard.sensors.filesystem",
        "airflow.providers.common",
        "airflow.providers.common.sql",
        "airflow.providers.common.sql.operators",
        "airflow.providers.common.sql.operators.sql",
        "airflow.providers.http",
        "airflow.providers.http.operators",
        "airflow.providers.http.operators.http",
        "airflow.providers.slack",
        "airflow.providers.slack.operators",
        "airflow.providers.slack.operators.slack_webhook",
    ):
        sys.modules.setdefault(mod_name, _StubModule(mod_name))


# --- Stub `blueprint` package mapping to pydantic + the real Blueprint logic ---


def _resolve_refs(schema: dict) -> dict:
    """Resolve all $ref/$defs, inlining definitions (verbatim from core.py)."""
    defs = schema.get("$defs", {})
    if not defs:
        result = copy.deepcopy(schema)
        result.pop("$defs", None)
        return result

    def _resolve(node: Any, resolving: set[str] | None = None) -> Any:
        if resolving is None:
            resolving = set()
        if isinstance(node, dict):
            if "$ref" in node and len(node) == 1:
                ref_path = node["$ref"]
                prefix = "#/$defs/"
                if ref_path.startswith(prefix):
                    def_name = ref_path[len(prefix):]
                    if def_name in resolving:
                        return copy.deepcopy(node)
                    if def_name in defs:
                        resolving = resolving | {def_name}
                        return _resolve(copy.deepcopy(defs[def_name]), resolving)
                return copy.deepcopy(node)
            return {k: _resolve(v, resolving) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [_resolve(item, resolving) for item in node]
        return node

    return _resolve(schema)


class _Blueprint:
    """Minimal stand-in for blueprint.Blueprint with the real schema/name logic."""

    name: str | None = None
    version: int | None = None

    def __class_getitem__(cls, item):  # capture the config type from Blueprint[Cfg]
        new = type(cls.__name__, (cls,), {"_config_type": item})
        return new

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            args = get_args(base)
            if args and isinstance(args[0], type) and issubclass(args[0], pydantic.BaseModel):
                cls._config_type = args[0]

    @classmethod
    def get_config_type(cls):
        return cls._config_type

    @classmethod
    def get_schema(cls) -> dict:
        return _resolve_refs(cls.get_config_type().model_json_schema())

    @classmethod
    def parse_name_and_version(cls) -> tuple[str, int]:
        explicit_name = cls.__dict__.get("name")
        explicit_version = cls.__dict__.get("version")
        if explicit_name is not None and explicit_version is not None:
            return explicit_name, explicit_version
        class_name = cls.__name__
        m = re.match(r"^(.+?)V(\d+)$", class_name)
        if m:
            base_name, inferred_version = m.group(1), int(m.group(2))
        else:
            base_name, inferred_version = class_name, 1
        snake = re.sub("([A-Z]+)([A-Z][a-z])", r"\1_\2", base_name)
        snake = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", snake)
        return (
            explicit_name if explicit_name is not None else snake.lower(),
            explicit_version if explicit_version is not None else inferred_version,
        )


def _install_blueprint_stub() -> None:
    mod = types.ModuleType("blueprint")
    mod.BaseModel = pydantic.BaseModel
    mod.Field = pydantic.Field
    mod.ConfigDict = pydantic.ConfigDict
    mod.field_validator = pydantic.field_validator
    mod.model_validator = pydantic.model_validator
    mod.TaskOrGroup = Any
    mod.Blueprint = _Blueprint
    sys.modules["blueprint"] = mod


# --- Envelope: verbatim from blueprint/cli.py _build_version_schema ------------


def _build_version_schema(name: str, version: int, raw_schema: dict) -> dict:
    schema_data = copy.deepcopy(raw_schema)
    schema_data.setdefault("properties", {})
    schema_data["properties"]["blueprint"] = {
        "type": "string",
        "const": name,
        "description": "The blueprint template to use",
    }
    schema_data["properties"]["version"] = {
        "type": "integer",
        "const": version,
        "description": "The blueprint version",
    }
    schema_data["properties"]["depends_on"] = {
        "type": "array",
        "items": {"type": "string"},
        "description": "Steps that must complete before this step runs",
        "default": [],
    }
    schema_data["properties"]["trigger_rule"] = {
        "type": "string",
        "enum": TRIGGER_RULE_VALUES,
        "description": "Trigger rule for this step (default: all_success)",
    }
    schema_data.setdefault("required", [])
    schema_data["required"].insert(0, "blueprint")
    if "version" not in schema_data["required"]:
        schema_data["required"].insert(1, "version")
    schema_data["title"] = name
    schema_data["templateType"] = "blueprint"
    schema_data.pop("$schema", None)
    schema_data["$schema"] = "http://json-schema.org/draft-07/schema#"
    return schema_data


def main() -> None:
    _install_stubs()
    _install_blueprint_stub()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    blueprints: list[tuple[str, int, type]] = []

    for py_file in sorted(TEMPLATES_DIR.glob("*.py")):
        spec = importlib.util.spec_from_file_location(f"templates_{py_file.stem}", py_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        for obj in vars(module).values():
            if (
                isinstance(obj, type)
                and issubclass(obj, _Blueprint)
                and obj is not _Blueprint
                and obj.__module__ == spec.name
            ):
                name, version = obj.parse_name_and_version()
                blueprints.append((name, version, obj))

    count = 0
    for name, version, cls in sorted(blueprints):
        schema = _build_version_schema(name, version, cls.get_schema())
        out = OUT_DIR / f"{name}.schema.json"
        out.write_text(json.dumps(schema, indent=2) + "\n")
        print(f"wrote {out.relative_to(REPO)}  (v{version})")
        count += 1

    print(f"\n{count} schema(s) generated in {OUT_DIR.relative_to(REPO)}")


if __name__ == "__main__":
    main()
