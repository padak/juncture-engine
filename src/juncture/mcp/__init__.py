"""Juncture MCP server: exposes Juncture to any LLM host speaking MCP.

Tools exposed:

* ``list_models(project)`` — names, kinds, materialization, deps.
* ``compile_sql(project)`` — full DAG as JSON.
* ``run_subgraph(project, select, test)`` — execute with selectors.
* ``translate_sql(sql, read, write)`` — SQLGlot dialect translation.
* ``explain_model(project, name)`` — description, columns, tests, ancestors.

This is a **skeleton** (v0.1) — it imports the MCP SDK lazily so Juncture
itself does not take the SDK as a hard dependency. Install with
``pip install 'juncture[mcp]'`` once the extra is added in pyproject.
"""

from juncture.mcp.server import build_server

__all__ = ["build_server"]
