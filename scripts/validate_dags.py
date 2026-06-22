"""Validate every ``*.dag.yaml`` step against its real Blueprint config model.

A lightweight local stand-in for ``blueprint lint`` for environments without
Airflow installed. Reuses the stubbing machinery in ``generate_schemas`` so the
actual Pydantic config models do the validating.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

import generate_schemas as gs

RESERVED = {"blueprint", "depends_on", "version", "trigger_rule"}
REPO = Path(__file__).resolve().parent.parent


def _load_blueprints() -> dict[str, type]:
    gs._install_stubs()
    gs._install_blueprint_stub()
    registry: dict[str, type] = {}
    for py_file in sorted(gs.TEMPLATES_DIR.glob("*.py")):
        spec = importlib.util.spec_from_file_location(f"tpl_{py_file.stem}", py_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        for obj in vars(module).values():
            if (
                isinstance(obj, type)
                and issubclass(obj, gs._Blueprint)
                and obj is not gs._Blueprint
                and obj.__module__ == spec.name
            ):
                name, _ = obj.parse_name_and_version()
                registry[name] = obj.get_config_type()
    return registry


def main() -> int:
    registry = _load_blueprints()
    failures = 0
    for yaml_file in sorted(REPO.glob("dags/*.dag.yaml")):
        data = yaml.safe_load(yaml_file.read_text())
        dag_id = data.get("dag_id", "?")
        steps = data.get("steps", {})
        print(f"\n{yaml_file.relative_to(REPO)}  (dag_id={dag_id}, {len(steps)} steps)")
        step_ids = set(steps)
        for step_id, cfg in steps.items():
            bp = cfg.get("blueprint")
            config_kwargs = {k: v for k, v in cfg.items() if k not in RESERVED}
            # check dependency references resolve
            for dep in cfg.get("depends_on", []):
                if dep not in step_ids:
                    print(f"  FAIL {step_id}: depends_on unknown step '{dep}'")
                    failures += 1
            if bp not in registry:
                print(f"  FAIL {step_id}: unknown blueprint '{bp}'")
                failures += 1
                continue
            try:
                registry[bp](**config_kwargs)
                print(f"  PASS {step_id} -> {bp}")
            except Exception as e:  # noqa: BLE001
                first = str(e).splitlines()[0]
                print(f"  FAIL {step_id} -> {bp}: {first}")
                failures += 1

    print(f"\n{'OK — all steps valid' if not failures else f'{failures} failure(s)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
