"""MCP server exposing Juncture as callable tools.

This module is intentionally minimal: it builds a server factory that can be
wired to the official MCP Python SDK when available, or used standalone in
tests via the in-memory API.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from juncture.core.project import Project
from juncture.core.runner import Runner, RunRequest
from juncture.parsers.sqlglot_parser import translate_sql


@dataclass(kw_only=True)
class Tool:
    name: str
    description: str
    fn: Callable[..., Any]
    schema: dict[str, Any] = field(default_factory=dict)


def build_server() -> list[Tool]:
    """Return the list of tools Juncture exposes via MCP.

    A real MCP server implementation wires each Tool to the transport layer.
    We keep this plain-Python for now so it is unit-testable without the
    MCP SDK.
    """

    def list_models(project: str) -> list[dict[str, Any]]:
        p = Project.load(Path(project))
        return [
            {
                "name": m.name,
                "kind": m.kind.value,
                "materialization": m.materialization.value,
                "depends_on": sorted(m.depends_on),
                "description": m.description,
                "tags": m.tags,
            }
            for m in p.models
        ]

    def compile_sql(project: str) -> dict[str, Any]:
        p = Project.load(Path(project))
        dag = p.dag()
        return {
            "project": p.config.name,
            "order": dag.topological_order(),
            "edges": [{"from": s, "to": t} for s in dag.nodes for t in dag.downstream(s)],
        }

    def run_subgraph(project: str, select: list[str] | None = None, test: bool = False) -> dict[str, Any]:
        report = Runner().run(
            RunRequest(
                project_path=Path(project),
                select=select or [],
                run_tests=test,
            )
        )
        return {
            "ok": report.ok,
            "project": report.project_name,
            "successes": report.models.successes,
            "failures": report.models.failures,
            "runs": [
                {
                    "name": r.model.name,
                    "status": r.status,
                    "error": r.error,
                    "rows": r.result.row_count if r.result else None,
                }
                for r in report.models.runs
            ],
            "tests": [
                {
                    "model": t.model,
                    "column": t.column,
                    "name": t.name,
                    "passed": t.passed,
                    "failing_rows": t.failing_rows,
                }
                for t in report.tests
            ],
        }

    def translate_sql_tool(sql: str, read: str = "snowflake", to_dialect: str = "duckdb") -> str:
        return translate_sql(sql, read=read, write=to_dialect)

    def explain_model(project: str, name: str) -> dict[str, Any]:
        p = Project.load(Path(project))
        dag = p.dag()
        model = dag.model(name)
        ancestors = _ancestors(dag, name)
        descendants = _descendants(dag, name)
        return {
            "name": model.name,
            "kind": model.kind.value,
            "description": model.description,
            "materialization": model.materialization.value,
            "columns": [
                {"name": c.name, "description": c.description, "tests": c.tests} for c in model.columns
            ],
            "depends_on": sorted(model.depends_on),
            "ancestors": sorted(ancestors),
            "descendants": sorted(descendants),
            "tags": model.tags,
        }

    return [
        Tool(
            name="list_models",
            description="List every model in a Juncture project.",
            fn=list_models,
            schema={"project": "str (project directory path)"},
        ),
        Tool(
            name="compile_sql",
            description="Parse a project and return the DAG (nodes + edges).",
            fn=compile_sql,
            schema={"project": "str"},
        ),
        Tool(
            name="run_subgraph",
            description="Execute a subgraph of a project; optionally run data tests.",
            fn=run_subgraph,
            schema={"project": "str", "select": "list[str] (selectors)", "test": "bool"},
        ),
        Tool(
            name="translate_sql",
            description="Translate SQL between warehouse dialects via SQLGlot.",
            fn=translate_sql_tool,
            schema={"sql": "str", "read": "str (source dialect)", "to_dialect": "str"},
        ),
        Tool(
            name="explain_model",
            description="Return full metadata for one model: columns, tests, ancestors, descendants.",
            fn=explain_model,
            schema={"project": "str", "name": "str"},
        ),
    ]


def _ancestors(dag: Any, name: str) -> set[str]:
    seen: set[str] = set()
    stack = [name]
    while stack:
        current = stack.pop()
        for parent in dag.upstream(current):
            if parent not in seen:
                seen.add(parent)
                stack.append(parent)
    return seen


def _descendants(dag: Any, name: str) -> set[str]:
    seen: set[str] = set()
    stack = [name]
    while stack:
        current = stack.pop()
        for child in dag.downstream(current):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen
