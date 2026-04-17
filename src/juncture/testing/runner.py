"""Test runner: executes schema tests declared in ``schema.yml``.

Discovery is driven by ``Project.schemas`` -- each column entry may declare
``tests: [not_null, unique, {relationships: {to: orders, field: id}}]``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from juncture.testing.assertions import (
    assert_accepted_values,
    assert_not_null,
    assert_relationships,
    assert_unique,
)

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter
    from juncture.core.dag import DAG
    from juncture.core.project import Project


@dataclass(frozen=True, kw_only=True)
class TestResult:
    model: str
    column: str | None
    name: str
    passed: bool
    failing_rows: int
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass(kw_only=True)
class TestRunner:
    adapter: Adapter
    schema: str

    def run(self, project: Project, dag: DAG) -> list[TestResult]:
        results: list[TestResult] = []
        for model in dag.models():
            schema_meta = project.schemas.get(model.name, {})
            columns: list[dict[str, Any]] = schema_meta.get("columns", []) or [
                {"name": col.name, "tests": col.tests} for col in model.columns
            ]
            fqn = self.adapter.resolve(model.name, schema=self.schema)
            for col in columns:
                for test in col.get("tests", []):
                    results.append(self._run_one(model.name, fqn, col["name"], test))
        # Custom SQL tests (tests/*.sql): each file is a query returning
        # failing rows. Any non-zero row count fails the test.
        from juncture.parsers.sqlglot_parser import render_refs  # local import to avoid cycle

        resolver = {m.name: self.adapter.resolve(m.name, schema=self.schema) for m in dag.models()}
        for custom in project.custom_tests:
            results.append(self._run_custom(custom.name, render_refs(custom.sql, resolver)))
        return results

    def _run_custom(self, name: str, rendered_sql: str) -> TestResult:
        import time  # local

        t0 = time.perf_counter()
        try:
            arrow = self.adapter.execute_arrow(rendered_sql)
            failing = arrow.num_rows
            return TestResult(
                model="custom",
                column=None,
                name=name,
                passed=failing == 0,
                failing_rows=int(failing),
                elapsed_seconds=time.perf_counter() - t0,
            )
        except Exception as exc:
            return TestResult(
                model="custom",
                column=None,
                name=name,
                passed=False,
                failing_rows=-1,
                error=f"{type(exc).__name__}: {exc}",
                elapsed_seconds=time.perf_counter() - t0,
            )

    def _run_one(self, model: str, fqn: str, column: str, test: Any) -> TestResult:
        t0 = time.perf_counter()
        try:
            name, sql = _compile_test(fqn, column, test)
            failing = self._query_count(sql)
            return TestResult(
                model=model,
                column=column,
                name=name,
                passed=failing == 0,
                failing_rows=failing,
                elapsed_seconds=time.perf_counter() - t0,
            )
        except Exception as exc:
            return TestResult(
                model=model,
                column=column,
                name=str(test),
                passed=False,
                failing_rows=-1,
                error=f"{type(exc).__name__}: {exc}",
                elapsed_seconds=time.perf_counter() - t0,
            )

    def _query_count(self, sql: str) -> int:
        # Every assertion SQL returns a single row with a single "failures" column.
        table = self.adapter.execute_arrow(sql)
        return int(table.to_pylist()[0]["failures"])


def _compile_test(fqn: str, column: str, test: Any) -> tuple[str, str]:
    if isinstance(test, str):
        if test == "not_null":
            return "not_null", assert_not_null(fqn, column)
        if test == "unique":
            return "unique", assert_unique(fqn, column)
        raise ValueError(f"Unknown test {test!r} on {fqn}.{column}")
    if isinstance(test, dict):
        if "relationships" in test:
            rel = test["relationships"]
            return (
                "relationships",
                assert_relationships(fqn, column, to_table=rel["to"], to_column=rel["field"]),
            )
        if "accepted_values" in test:
            values = test["accepted_values"]["values"]
            return "accepted_values", assert_accepted_values(fqn, column, values)
    raise ValueError(f"Unsupported test spec: {test!r}")
