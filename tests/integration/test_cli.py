"""End-to-end tests via the Typer CliRunner.

We drive ``juncture init`` and ``juncture run`` through the CLI so the
entire stack (argument parsing, project load, runner, adapter, output
rendering) gets exercised together.

Command groups introduced in the CLI v2 restructuring:
  - ``juncture sql translate | sanitize | split``
  - ``juncture migrate keboola | sync-pull``
  - ``juncture debug diagnostics``

Old flat names (e.g. ``juncture translate``) are kept as hidden deprecated
aliases so that existing scripts keep working. Both paths are exercised here.
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from juncture.cli.app import app

runner = CliRunner()


def test_init_scaffolds_usable_project(tmp_path: Path) -> None:
    target = tmp_path / "new_proj"
    result = runner.invoke(app, ["init", str(target), "--name", "new_proj"])
    assert result.exit_code == 0, result.stdout

    assert (target / "juncture.yaml").exists()
    assert (target / "models" / "staging" / "stg_users.sql").exists()
    assert (target / "models" / "marts" / "user_count.sql").exists()
    assert (target / "models" / "schema.yml").exists()


def test_init_then_run_succeeds(tmp_path: Path) -> None:
    target = tmp_path / "e2e"
    runner.invoke(app, ["init", str(target), "--name", "e2e"])
    (target / "data").mkdir(exist_ok=True)
    result = runner.invoke(app, ["run", "--project", str(target), "--test"])
    assert result.exit_code == 0, result.stdout
    assert "success" in result.stdout.lower()


def test_compile_json_emits_dag(tmp_path: Path) -> None:
    target = tmp_path / "jsonproj"
    runner.invoke(app, ["init", str(target), "--name", "jsonproj"])
    result = runner.invoke(app, ["compile", "--project", str(target), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["project"] == "jsonproj"
    assert {"stg_users", "user_count"}.issubset(set(payload["order"]))


def test_docs_writes_manifest(tmp_path: Path) -> None:
    target = tmp_path / "docsproj"
    runner.invoke(app, ["init", str(target), "--name", "docsproj"])
    out = target / "target" / "docs"
    result = runner.invoke(app, ["docs", "--project", str(target), "--output", str(out)])
    assert result.exit_code == 0
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["project"] == "docsproj"
    assert len(manifest["models"]) >= 2


def test_run_fails_with_exit_code_on_bad_ref(tmp_path: Path) -> None:
    target = tmp_path / "bad"
    runner.invoke(app, ["init", str(target), "--name", "bad"])
    # Introduce a typo in a ref to force a DAGError.
    bad_model = target / "models" / "marts" / "user_count.sql"
    bad_model.write_text("SELECT COUNT(*) FROM {{ ref('sgt_users') }}")
    result = runner.invoke(app, ["run", "--project", str(target)])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# New grouped commands (juncture sql / migrate / debug)
# ---------------------------------------------------------------------------


def test_sql_translate_new_path() -> None:
    result = runner.invoke(
        app, ["sql", "translate", "SELECT TO_VARCHAR(42)", "--from", "snowflake", "--to", "duckdb"]
    )
    assert result.exit_code == 0
    assert "SELECT" in result.stdout.upper()


def test_sql_translate_deprecated_alias_still_works() -> None:
    result = runner.invoke(
        app, ["translate", "SELECT TO_VARCHAR(42)", "--from", "snowflake", "--to", "duckdb"]
    )
    assert result.exit_code == 0
    assert "SELECT" in result.stdout.upper()


def test_sql_sanitize_new_path(tmp_path: Path) -> None:
    target = tmp_path / "sanproj"
    runner.invoke(app, ["init", str(target), "--name", "sanproj"])
    result = runner.invoke(
        app,
        [
            "sql",
            "sanitize",
            "--project",
            str(target),
            "--from-dialect",
            "snowflake",
            "--to-dialect",
            "duckdb",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0


def test_sql_split_new_path(tmp_path: Path) -> None:
    sql_file = tmp_path / "task.sql"
    sql_file.write_text(
        "CREATE TABLE a AS SELECT 1 AS id;\nCREATE TABLE b AS SELECT id FROM a;\n"
    )
    out = tmp_path / "split_out"
    result = runner.invoke(
        app,
        ["sql", "split", str(sql_file), "--out", str(out), "--source-dialect", "duckdb", "--dry-run"],
    )
    assert result.exit_code == 0


def test_test_subcommand(tmp_path: Path) -> None:
    target = tmp_path / "testproj"
    runner.invoke(app, ["init", str(target), "--name", "testproj"])
    (target / "data").mkdir(exist_ok=True)
    runner.invoke(app, ["run", "--project", str(target)])
    result = runner.invoke(app, ["test", "--project", str(target)])
    assert result.exit_code == 0
