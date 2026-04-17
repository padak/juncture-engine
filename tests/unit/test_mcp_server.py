"""Unit tests for the MCP server skeleton (build_server Tool list)."""

from __future__ import annotations

from pathlib import Path

from juncture.mcp import build_server


def test_build_server_exposes_five_tools() -> None:
    tools = build_server()
    names = {t.name for t in tools}
    assert names == {
        "list_models",
        "compile_sql",
        "run_subgraph",
        "translate_sql",
        "explain_model",
    }


def test_list_models_on_example(tmp_path: Path) -> None:
    project = tmp_path / "mcpex"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(f"""name: mcpex
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/mcpex.duckdb
""")
    (project / "models" / "a.sql").write_text("SELECT 1 AS id")
    (project / "models" / "b.sql").write_text("SELECT * FROM {{ ref('a') }}")

    tools = {t.name: t for t in build_server()}
    models = tools["list_models"].fn(project=str(project))
    names = {m["name"] for m in models}
    assert names == {"a", "b"}
    b = next(m for m in models if m["name"] == "b")
    assert b["depends_on"] == ["a"]


def test_translate_sql_tool() -> None:
    tools = {t.name: t for t in build_server()}
    translated = tools["translate_sql"].fn(sql="SELECT 1", read="duckdb", to_dialect="snowflake")
    assert "SELECT" in translated.upper()
