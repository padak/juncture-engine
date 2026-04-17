"""Executor: run a DAG layer by layer with a configurable thread pool.

Given a DAG, an adapter, and optional selectors, the executor builds the
subgraph, runs each layer concurrently, and returns per-model results (with
elapsed time, row count, errors). Failures in one model can be configured to
fail fast (default) or continue other branches.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from juncture.adapters.base import MaterializationResult
from juncture.core.context import TransformContext
from juncture.core.dag import DAG
from juncture.core.model import Model, ModelKind
from juncture.parsers.sqlglot_parser import render_refs

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter


log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class ModelRun:
    model: Model
    status: str  # "success" | "failed" | "skipped"
    result: MaterializationResult | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass(kw_only=True)
class ExecutionResult:
    runs: list[ModelRun] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def successes(self) -> int:
        return sum(1 for r in self.runs if r.status == "success")

    @property
    def failures(self) -> int:
        return sum(1 for r in self.runs if r.status == "failed")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.runs if r.status == "skipped")

    @property
    def ok(self) -> bool:
        return self.failures == 0


class Executor:
    """Run a DAG against an adapter with parallelism per layer."""

    def __init__(
        self,
        *,
        adapter: Adapter,
        schema: str,
        threads: int = 4,
        fail_fast: bool = True,
        run_vars: dict[str, object] | None = None,
    ) -> None:
        self.adapter = adapter
        self.schema = schema
        self.threads = max(1, threads)
        self.fail_fast = fail_fast
        self.run_vars = dict(run_vars or {})

    def run(self, dag: DAG) -> ExecutionResult:
        result = ExecutionResult()
        t0 = time.perf_counter()
        failed_ancestors: set[str] = set()

        for layer_index, layer in enumerate(dag.layers()):
            runnable = [name for name in layer if name not in failed_ancestors]
            skipped = [name for name in layer if name in failed_ancestors]

            for name in skipped:
                result.runs.append(
                    ModelRun(
                        model=dag.model(name),
                        status="skipped",
                        error="upstream failure",
                    )
                )

            if not runnable:
                continue

            log.info(
                "Layer %d: running %d model(s) with %d thread(s)", layer_index, len(runnable), self.threads
            )

            with ThreadPoolExecutor(max_workers=self.threads) as pool:
                futures: dict[Future[ModelRun], str] = {
                    pool.submit(self._run_one, dag.model(name)): name for name in runnable
                }
                for future in futures:
                    name = futures[future]
                    try:
                        run = future.result()
                    except Exception as exc:
                        run = ModelRun(
                            model=dag.model(name),
                            status="failed",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    result.runs.append(run)
                    if run.status == "failed":
                        failed_ancestors.update(dag.downstream(name))
                        failed_ancestors.update(_all_descendants(dag, name))
                        if self.fail_fast:
                            # Cancel pending work and stop scheduling further layers.
                            for f in futures:
                                f.cancel()
                            result.elapsed_seconds = time.perf_counter() - t0
                            return result

        result.elapsed_seconds = time.perf_counter() - t0
        return result

    def _run_one(self, model: Model) -> ModelRun:
        t0 = time.perf_counter()
        try:
            if model.kind is ModelKind.SEED:
                # Seeds are already materialized by load_seeds() before the
                # executor starts. We still surface them as a "success" in the
                # run report so users see the complete DAG.
                return ModelRun(
                    model=model,
                    status="success",
                    result=None,
                    elapsed_seconds=time.perf_counter() - t0,
                )
            if model.kind is ModelKind.SQL:
                assert model.sql is not None
                rendered = render_refs(model.sql, self._ref_resolver())
                mat = self.adapter.materialize_sql(model, rendered, schema=self.schema)
            else:
                ctx = TransformContext(
                    model=model,
                    adapter=self.adapter,
                    run_vars=self.run_vars,
                )
                mat = self.adapter.materialize_python(model, ctx, schema=self.schema)
            return ModelRun(
                model=model,
                status="success",
                result=mat,
                elapsed_seconds=time.perf_counter() - t0,
            )
        except Exception as exc:
            return ModelRun(
                model=model,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
                elapsed_seconds=time.perf_counter() - t0,
            )

    def _ref_resolver(self) -> dict[str, str]:
        # Resolver maps ref(model) -> "schema"."model". This is cheap and safe
        # to recompute per run; DAG is small.
        return {}  # placeholder; real resolver is built at run time below

    def run_with_refs(self, dag: DAG) -> ExecutionResult:
        # Build a stable ref resolver for the whole run so SQL sees consistent
        # schemas even if the executor supports overriding per-model later.
        resolver = {name: self.adapter.resolve(name, schema=self.schema) for name in dag.nodes}
        original = self._ref_resolver
        self._ref_resolver = lambda: resolver  # type: ignore[method-assign]
        try:
            return self.run(dag)
        finally:
            self._ref_resolver = original  # type: ignore[method-assign]


def _all_descendants(dag: DAG, node: str) -> set[str]:
    seen: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for child in dag.downstream(current):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen
