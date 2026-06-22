"""Discover every ``*.dag.yaml`` in the dags directory and build the DAGs.

This file is what Airflow's DagBag imports; ``build_all_dags()`` parses each
workflow YAML, validates it against the matching Blueprint config, and registers
the resulting DAG object in this module's namespace.
"""

from blueprint import build_all_dags

build_all_dags()
