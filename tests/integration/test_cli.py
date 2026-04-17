"""End-to-end tests via the Typer CliRunner.

We drive ``juncture init`` and ``juncture run`` through the CLI so the
entire stack (argument parsing, project load, runner, adapter, output
rendering) gets exercised together.
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


def test_translate_prints_sql() -> None:
    result = runner.invoke(app, ["translate", "SELECT TO_VARCHAR(42)", "--from", "snowflake", "--to", "duckdb"])
    assert result.exit_code == 0
    # Output is a Rich panel; body contains the translated expression.
    assert "SELECT" in result.stdout.upper()


def test_run_fails_with_exit_code_on_bad_ref(tmp_path: Path) -> None:
    target = tmp_path / "bad"
    runner.invoke(app, ["init", str(target), "--name", "bad"])
    # Introduce a typo in a ref to force a DAGError.
    bad_model = target / "models" / "marts" / "user_count.sql"
    bad_model.write_text("SELECT COUNT(*) FROM {{ ref('sgt_users') }}")
    result = runner.invoke(app, ["run", "--project", str(target)])
    assert result.exit_code != 0
