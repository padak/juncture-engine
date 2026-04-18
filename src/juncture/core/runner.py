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

import networkx as nx

from juncture.adapters.base import Adapter
from juncture.adapters.registry import get_adapter
from juncture.core.dag import DAG
from juncture.core.executor import ExecutionResult, Executor
from juncture.core.model import Materialization, Model, ModelKind
from juncture.core.project import Project, ProjectError
from juncture.core.run_history import append_run
from juncture.core.seeds import load_seeds
from juncture.parsers.sqlglot_parser import build_statement_dag
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
    reuse_seeds: bool = False
    parallelism_override: int | None = None
    continue_on_error: bool = False
    #: Append the run's outcome to ``<project>/target/run_history.jsonl``.
    #: Defaults to True so the web render has data to show; set False
    #: in CI where the file would clutter artifact diffs.
    record_history: bool = True
    #: CLI-level ``--disable model_a,model_b`` override — flips
    #: ``Model.disabled`` to True for these names at runtime without
    #: touching ``schema.yml``. Distinct from ``exclude`` because the
    #: disabled model and its downstream are surfaced in the report
    #: (``status=disabled`` / ``skipped+upstream_disabled``) rather
    #: than silently omitted.
    disable_models: list[str] = field(default_factory=list)
    #: ``--enable-only a,b,c`` — inverse of ``--disable``. Everything
    #: not listed becomes ``disabled=True``. Scoped to the DAG the
    #: selectors pruned to.
    enable_only: list[str] | None = None
    #: Active profile (``profiles:`` block in ``juncture.yaml``). When
    #: ``None``, :class:`ProjectConfig` resolves the name from
    #: ``JUNCTURE_PROFILE`` env var or the top-level ``profile:`` field.
    profile: str | None = None


@dataclass(frozen=True, kw_only=True)
class IntraScriptStats:
    """Plan-time introspection of an EXECUTE model's multi-statement body.

    Produced by :meth:`Runner.plan` so users can see what an EXECUTE model
    would do without running it. Mirrors the numbers
    ``scripts/analyze_execute.py`` prints on a standalone file, but scoped
    to the model inside a real project.
    """

    total_statements: int
    layers: int
    widest_layer: int
    layer_sizes: list[int]
    parallelism: int


@dataclass(frozen=True, kw_only=True)
class DryRunNode:
    """One node in the plan: a seed or a model."""

    name: str
    kind: str  # "seed" | "sql" | "python"
    materialization: str | None  # None for seeds
    depends_on: list[str]
    layer: int | None = None  # None for seeds (loaded before the model DAG)
    intra: IntraScriptStats | None = None


@dataclass(kw_only=True)
class DryRunReport:
    project_name: str
    seeds: list[DryRunNode]
    models: list[DryRunNode]
    model_layers: int


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
        project = Project.load(
            request.project_path,
            run_vars=request.run_vars,
            profile=request.profile,
        )
        adapter = self._build_adapter(project, request.connection)

        dag = project.dag()
        if request.select or request.exclude:
            dag = self._apply_selectors(dag, request.select, request.exclude)

        _apply_parallelism_override(project.models, request.parallelism_override)
        _apply_continue_on_error(project.models, request.continue_on_error)
        _apply_disable_overrides(
            project.models,
            disable=request.disable_models,
            enable_only=request.enable_only,
        )

        schema = project.config.default_schema
        with adapter:
            # Load seeds before the DAG runs so models can ref() them.
            if project.seeds:
                log.info("Loading %d seed(s)", len(project.seeds))
                load_seeds(
                    adapter,
                    project.seeds,
                    schema=schema,
                    reuse_existing=request.reuse_seeds,
                )

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

        report = RunReport(project_name=project.config.name, models=models_result, tests=tests)
        if request.record_history:
            try:
                append_run(project.root, report)
            except OSError as exc:
                # A read-only target dir shouldn't sink the whole run;
                # log and move on. The user still gets their RunReport.
                log.warning("Could not append run history: %s", exc)
        return report

    def plan(self, request: RunRequest) -> DryRunReport:
        """Return the execution plan for ``request`` without touching the DB.

        No adapter is opened, no seed data is loaded, no SQL is executed.
        We still load the project, apply selectors, compute DAG layers,
        and — for EXECUTE models — parse the SQL body into its intra-script
        DAG so the returned plan reflects both levels of parallelism the
        real run would use.

        The dialect for intra-script DAG building is fixed to ``duckdb``.
        It only matters for SQLGlot's parse choices, and even then we fall
        back to regex detection for statements the parser rejects.
        """
        project = Project.load(
            request.project_path,
            run_vars=request.run_vars,
            profile=request.profile,
        )

        dag = project.dag()
        if request.select or request.exclude:
            dag = self._apply_selectors(dag, request.select, request.exclude)

        _apply_parallelism_override(project.models, request.parallelism_override)

        seed_nodes = [
            DryRunNode(
                name=seed.name,
                kind="seed",
                materialization=None,
                depends_on=[],
            )
            for seed in project.seeds
        ]

        # Seeds live in project.models with ModelKind.SEED so ref() resolution
        # works, but they're loaded *before* the DAG and shouldn't appear in
        # the model-layer listing. Restrict the DAG to non-seed nodes for
        # layer computation; seeds are already reported via plan.seeds.
        by_name: dict[str, Model] = {m.name: m for m in project.models}
        model_names = {
            name
            for name in dag.nodes
            if (m := by_name.get(name)) is not None and m.kind is not ModelKind.SEED
        }
        model_dag = dag.subgraph(model_names) if model_names else dag

        model_nodes: list[DryRunNode] = []
        layers = list(model_dag.layers()) if model_names else []
        for layer_idx, layer in enumerate(layers):
            for name in layer:
                model = by_name.get(name)
                if model is None:
                    continue
                model_nodes.append(
                    DryRunNode(
                        name=model.name,
                        kind=model.kind.value,
                        materialization=model.materialization.value,
                        # Strip seed names from depends_on for readability —
                        # the seed list is already shown separately.
                        depends_on=sorted(
                            d
                            for d in model.depends_on
                            if (dm := by_name.get(d)) is None or dm.kind is not ModelKind.SEED
                        ),
                        layer=layer_idx + 1,
                        intra=_intra_script_stats(model),
                    )
                )

        return DryRunReport(
            project_name=project.config.name,
            seeds=seed_nodes,
            models=model_nodes,
            model_layers=len(layers),
        )

    def _build_adapter(self, project: Project, connection: str | None) -> Adapter:
        conn_name = self._resolve_connection_name(project, connection)
        conn_cfg = project.config.connections[conn_name]
        params = _resolve_paths(conn_cfg.params, root=project.root)
        return get_adapter(conn_cfg.type, **params)

    @staticmethod
    def _resolve_connection_name(project: Project, connection: str | None) -> str:
        """Pick a connection from :attr:`ProjectConfig.connections`.

        Historically ``profile:`` in ``juncture.yaml`` doubled as the
        name of the connection to use — ``profile: local`` + ``connections:
        {local: {...}}``. With the new ``profiles:`` overlay block the
        same key now means "which overlay to apply", so we can no
        longer read it as a connection name.

        Resolution order:

        1. Explicit ``--connection`` / ``RunRequest.connection`` wins.
        2. Legacy path (no ``profiles:`` block in the project): fall back
           to the old behaviour — top-level ``profile:`` field as
           connection name.
        3. Single-connection shortcut: if only one connection is
           declared, pick it. Common case once profiles take over the
           ``profile:`` field.
        4. Otherwise fail fast so the user must pass ``--connection``.
        """
        connections = project.config.connections
        if connection:
            if connection not in connections:
                raise ProjectError(
                    f"Connection {connection!r} not configured; available: {sorted(connections)}"
                )
            return connection

        if not project.config.available_profiles:
            # Legacy backward-compat: pre-profiles projects used
            # ``profile:`` as a connection name.
            conn_name = project.config.profile or "default"
            if conn_name not in connections:
                raise ProjectError(
                    f"Connection {conn_name!r} not configured; available: {sorted(connections)}"
                )
            return conn_name

        if len(connections) == 1:
            return next(iter(connections))

        raise ProjectError(
            "Project has a 'profiles:' block with multiple connections "
            f"({sorted(connections)}). Pass --connection explicitly, "
            "or leave a single connection in juncture.yaml."
        )

    def _apply_selectors(self, dag: DAG, select: list[str], exclude: list[str]) -> DAG:
        selected = set(dag.nodes) if not select else dag.select(select)
        if exclude:
            selected -= dag.select(exclude)
        if not selected:
            raise ProjectError("Selector produced an empty set of models")
        return dag.subgraph(selected)


def _apply_parallelism_override(models: list[Model], override: int | None) -> None:
    """Apply a CLI ``--parallelism`` override to every EXECUTE model in place.

    The override *replaces* per-model ``config.parallelism`` when set, so
    benchmark runs can sweep ``--parallelism 1 / 2 / 4 / 8`` without
    touching ``schema.yml``. An unset override (``None``) leaves each
    model's own config untouched.
    """
    if override is None:
        return
    if override < 1:
        raise ValueError(f"--parallelism must be >= 1 (got {override})")
    for model in models:
        if model.materialization is Materialization.EXECUTE:
            model.config["parallelism"] = override


def _apply_disable_overrides(
    models: list[Model],
    *,
    disable: list[str],
    enable_only: list[str] | None,
) -> None:
    """Flip ``Model.disabled`` on the listed names in place.

    ``disable`` adds to the set of disabled models (additive on top of
    ``schema.yml`` ``disabled: true`` declarations). ``enable_only``, if
    non-empty, is the inverse: every model *not* in the list becomes
    disabled, regardless of its ``schema.yml`` setting. Combining both
    is supported — ``enable_only`` runs first, then ``disable`` can
    remove more from the survivors.
    """
    by_name = {m.name: m for m in models}
    if enable_only is not None:
        allowed = set(enable_only)
        unknown = allowed - set(by_name)
        if unknown:
            raise ValueError(f"--enable-only names not in project: {sorted(unknown)}")
        for model in models:
            if model.name not in allowed:
                model.disabled = True
    if disable:
        unknown_disable = set(disable) - set(by_name)
        if unknown_disable:
            raise ValueError(f"--disable names not in project: {sorted(unknown_disable)}")
        for name in disable:
            by_name[name].disabled = True


def _apply_continue_on_error(models: list[Model], enabled: bool) -> None:
    """Toggle ``config.continue_on_error`` on every EXECUTE model in place.

    Called from :meth:`Runner.run` when the CLI ``--continue-on-error`` flag
    is set. Intentionally scoped to EXECUTE — the flag exists to turn a
    multi-statement migration body from "fail fast" into "surface every
    primary error in one pass". Non-EXECUTE models are single-statement
    materializations; continue-on-error has no meaning there.
    """
    if not enabled:
        return
    for model in models:
        if model.materialization is Materialization.EXECUTE:
            model.config["continue_on_error"] = True


def _intra_script_stats(model: Model) -> IntraScriptStats | None:
    """Return intra-script stats for an EXECUTE model, else ``None``.

    Non-SQL or non-EXECUTE models get no intra-stats — their execution
    plan is just "one layer, one model".
    """
    if model.kind is not ModelKind.SQL or model.materialization is not Materialization.EXECUTE:
        return None
    if not model.sql:
        return None
    graph = build_statement_dag(model.sql)
    if graph.number_of_nodes() == 0:
        return None
    layers = list(nx.topological_generations(graph))
    parallelism = 1
    try:
        parallelism = max(1, int(model.config.get("parallelism", 1) or 1))
    except (TypeError, ValueError):
        parallelism = 1
    return IntraScriptStats(
        total_statements=graph.number_of_nodes(),
        layers=len(layers),
        widest_layer=max((len(layer) for layer in layers), default=0),
        layer_sizes=[len(layer) for layer in layers],
        parallelism=parallelism,
    )
