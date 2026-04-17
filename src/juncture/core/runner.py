"""Runner: glue between Project, adapter, executor and tests.

``Runner.run()`` is the single entry point used by the CLI and by Keboola
wrapper. It:

1. Loads the project.
2. Picks the connection from ``juncture.yaml`` (or an override).
3. Instantiates the adapter and opens the connection.
4. Compiles the DAG and optionally filters via selectors.
5. Runs models layer-by-layer through :class:`Executor`.
6. Optionally runs data tests afterwards.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from juncture.adapters.base import Adapter
from juncture.adapters.registry import get_adapter
from juncture.core.dag import DAG
from juncture.core.executor import ExecutionResult, Executor
from juncture.core.project import Project, ProjectError
from juncture.core.seeds import load_seeds
from juncture.testing.runner import TestResult, TestRunner

log = logging.getLogger(__name__)


# Connection params whose values are interpreted as filesystem paths. When
# they are relative, the Runner rewrites them to be relative to the project
# root -- otherwise tests and headless runs would have to chdir into the
# project dir, which is clunky.
_PATH_PARAMS = {"path", "duckdb_path", "private_key_path"}


def _resolve_paths(params: dict[str, Any], *, root: Path) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, value in params.items():
        if key in _PATH_PARAMS and isinstance(value, str):
            p = Path(value)
            if not p.is_absolute() and value != ":memory:":
                p = root / p
                p.parent.mkdir(parents=True, exist_ok=True)
                value = str(p)
        resolved[key] = value
    return resolved


@dataclass(kw_only=True)
class RunRequest:
    project_path: Path
    select: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    connection: str | None = None
    threads: int = 4
    full_refresh: bool = False
    run_tests: bool = False
    fail_fast: bool = True
    run_vars: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class RunReport:
    project_name: str
    models: ExecutionResult
    tests: list[TestResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.models.ok and all(t.passed for t in self.tests)


class Runner:
    """Execute a Juncture project end-to-end."""

    def run(self, request: RunRequest) -> RunReport:
        project = Project.load(request.project_path)
        adapter = self._build_adapter(project, request.connection)

        dag = project.dag()
        if request.select or request.exclude:
            dag = self._apply_selectors(dag, request.select, request.exclude)

        schema = project.config.default_schema
        with adapter:
            # Load seeds before the DAG runs so models can ref() them.
            if project.seeds:
                log.info("Loading %d seed(s)", len(project.seeds))
                load_seeds(adapter, project.seeds, schema=schema)

            executor = Executor(
                adapter=adapter,
                schema=schema,
                threads=request.threads,
                fail_fast=request.fail_fast,
                run_vars=request.run_vars,
            )
            models_result = executor.run_with_refs(dag)

            tests: list[TestResult] = []
            if request.run_tests and models_result.ok:
                tests = TestRunner(adapter=adapter, schema=schema).run(project, dag)

        return RunReport(project_name=project.config.name, models=models_result, tests=tests)

    def _build_adapter(self, project: Project, connection: str | None) -> Adapter:
        conn_name = connection or project.config.profile or "default"
        if conn_name not in project.config.connections:
            raise ProjectError(
                f"Connection {conn_name!r} not configured; available: {sorted(project.config.connections)}"
            )
        conn_cfg = project.config.connections[conn_name]
        params = _resolve_paths(conn_cfg.params, root=project.root)
        return get_adapter(conn_cfg.type, **params)

    def _apply_selectors(self, dag: DAG, select: list[str], exclude: list[str]) -> DAG:
        selected = set(dag.nodes) if not select else dag.select(select)
        if exclude:
            selected -= dag.select(exclude)
        if not selected:
            raise ProjectError("Selector produced an empty set of models")
        return dag.subgraph(selected)
