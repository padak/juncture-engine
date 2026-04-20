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


def test_init_scaffolds_minimal_skeleton(tmp_path: Path) -> None:
    """Default `init` writes juncture.yaml + empty models/ + empty seeds/ +
    README.md. No demo models, no schema.yml, no tests/ directory."""
    target = tmp_path / "new_proj"
    result = runner.invoke(app, ["init", str(target), "--name", "new_proj"])
    assert result.exit_code == 0, result.stdout

    assert (target / "juncture.yaml").exists()
    assert (target / "models").is_dir()
    assert (target / "seeds").is_dir()
    assert (target / "README.md").exists()
    # No demo scaffolding by default.
    assert not (target / "models" / "staging").exists()
    assert not (target / "models" / "marts").exists()
    assert not (target / "models" / "schema.yml").exists()
    assert not (target / "tests").exists()


def test_init_derives_name_from_directory(tmp_path: Path) -> None:
    """`juncture init my_shop` (no --name) must use the dirname for both the
    project name and the DuckDB file path. Covers the tutorial flow where the
    user runs `juncture init my_shop` and then expects `data/my_shop.duckdb`."""
    target = tmp_path / "my_shop"
    result = runner.invoke(app, ["init", str(target)])
    assert result.exit_code == 0, result.stdout

    cfg = (target / "juncture.yaml").read_text()
    assert "name: my_shop" in cfg
    assert "path: data/my_shop.duckdb" in cfg


def test_init_explicit_name_overrides_directory(tmp_path: Path) -> None:
    target = tmp_path / "my_shop"
    result = runner.invoke(app, ["init", str(target), "--name", "shop_v2"])
    assert result.exit_code == 0, result.stdout

    cfg = (target / "juncture.yaml").read_text()
    assert "name: shop_v2" in cfg
    assert "path: data/shop_v2.duckdb" in cfg


def test_init_with_examples_scaffolds_demo(tmp_path: Path) -> None:
    target = tmp_path / "demo_proj"
    result = runner.invoke(app, ["init", str(target), "--name", "demo_proj", "--with-examples"])
    assert result.exit_code == 0, result.stdout

    assert (target / "models" / "staging" / "stg_users.sql").exists()
    assert (target / "models" / "marts" / "user_count.sql").exists()
    assert (target / "models" / "schema.yml").exists()
    assert (target / "tests").is_dir()


def test_init_with_examples_then_run_succeeds(tmp_path: Path) -> None:
    target = tmp_path / "e2e"
    runner.invoke(app, ["init", str(target), "--name", "e2e", "--with-examples"])
    (target / "data").mkdir(exist_ok=True)
    result = runner.invoke(app, ["run", "--project", str(target), "--test"])
    assert result.exit_code == 0, result.stdout
    assert "success" in result.stdout.lower()


def test_compile_json_emits_dag(tmp_path: Path) -> None:
    target = tmp_path / "jsonproj"
    runner.invoke(app, ["init", str(target), "--name", "jsonproj", "--with-examples"])
    result = runner.invoke(app, ["compile", "--project", str(target), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["project"] == "jsonproj"
    assert {"stg_users", "user_count"}.issubset(set(payload["order"]))


def test_docs_writes_manifest(tmp_path: Path) -> None:
    target = tmp_path / "docsproj"
    runner.invoke(app, ["init", str(target), "--name", "docsproj", "--with-examples"])
    out = target / "target" / "docs"
    result = runner.invoke(app, ["docs", "--project", str(target), "--output", str(out)])
    assert result.exit_code == 0
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["project"] == "docsproj"
    assert len(manifest["models"]) >= 2


def test_run_fails_with_exit_code_on_bad_ref(tmp_path: Path) -> None:
    target = tmp_path / "bad"
    runner.invoke(app, ["init", str(target), "--name", "bad", "--with-examples"])
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
    runner.invoke(app, ["init", str(target), "--name", "sanproj", "--with-examples"])
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
    sql_file.write_text("CREATE TABLE a AS SELECT 1 AS id;\nCREATE TABLE b AS SELECT id FROM a;\n")
    out = tmp_path / "split_out"
    result = runner.invoke(
        app,
        ["sql", "split", str(sql_file), "--out", str(out), "--source-dialect", "duckdb", "--dry-run"],
    )
    assert result.exit_code == 0


def test_test_subcommand(tmp_path: Path) -> None:
    target = tmp_path / "testproj"
    runner.invoke(app, ["init", str(target), "--name", "testproj", "--with-examples"])
    (target / "data").mkdir(exist_ok=True)
    runner.invoke(app, ["run", "--project", str(target)])
    result = runner.invoke(app, ["test", "--project", str(target)])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# --profile flag (profiles: block in juncture.yaml)
# ---------------------------------------------------------------------------


def _profiles_project(root: Path) -> None:
    """Scaffold a minimal profiles project with dev + prod env DuckDB paths."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "models").mkdir()
    (root / "data").mkdir()
    (root / "juncture.yaml").write_text(
        """
name: profiles_proj
profile: dev
default_schema: main
connections:
  warehouse:
    type: duckdb
    path: data/base.duckdb
profiles:
  dev:
    connections:
      warehouse:
        path: data/dev.duckdb
  prod:
    connections:
      warehouse:
        path: data/prod.duckdb
""".lstrip()
    )
    (root / "models" / "ping.sql").write_text("SELECT 1 AS ok")


def test_run_uses_default_profile_from_yaml(tmp_path: Path) -> None:
    """Without --profile, the top-level ``profile:`` field picks dev."""
    root = tmp_path / "p1"
    _profiles_project(root)
    result = runner.invoke(app, ["run", "--project", str(root)])
    assert result.exit_code == 0, result.stdout
    assert (root / "data" / "dev.duckdb").exists()
    assert not (root / "data" / "prod.duckdb").exists()


def test_run_with_profile_flag_overrides_yaml(tmp_path: Path) -> None:
    """--profile prod materializes against the prod DuckDB file."""
    root = tmp_path / "p2"
    _profiles_project(root)
    result = runner.invoke(app, ["run", "--project", str(root), "--profile", "prod"])
    assert result.exit_code == 0, result.stdout
    assert (root / "data" / "prod.duckdb").exists()
    assert not (root / "data" / "dev.duckdb").exists()


def test_run_with_unknown_profile_fails(tmp_path: Path) -> None:
    root = tmp_path / "p3"
    _profiles_project(root)
    result = runner.invoke(app, ["run", "--project", str(root), "--profile", "missing"])
    assert result.exit_code != 0
    message = str(result.exception) if result.exception else (result.stdout + (result.stderr or ""))
    assert "profile 'missing' is not declared" in message
