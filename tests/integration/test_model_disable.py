"""Integration: disabled models skip execution without failing the run.

Closes VISION.md #6 ("no conditional execution") for the static case:
authors can mark a model as `disabled: true` in schema.yml, and the
CLI adds `--disable` / `--enable-only` runtime overrides. The key
invariant is that a disabled model does NOT cascade as a failure —
the run stays ok=true and downstream consumers get skipped with
reason=upstream_disabled, visually distinct from upstream_failure.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from juncture.core.runner import Runner, RunRequest


def _make_chain_project(root: Path, *, yaml_extra: str = "") -> Path:
    """Seed -> stg -> mart chain; db file returned for assertions."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "models").mkdir(exist_ok=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: disableproj
profile: local
default_schema: main
default_materialization: table

connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "stg.sql").write_text("SELECT 1 AS id")
    (root / "models" / "mart.sql").write_text("SELECT id FROM {{ ref('stg') }}")
    (root / "models" / "tail.sql").write_text("SELECT id FROM {{ ref('mart') }}")
    (root / "models" / "schema.yml").write_text(
        f"""models:
  - name: stg
    description: staging model
  - name: mart
    description: downstream of stg
{yaml_extra}
"""
    )
    return db_path


def test_disabled_in_schema_yml_skips_downstream_without_failure(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    _make_chain_project(
        project,
        yaml_extra="  - name: tail\n    disabled: true\n",
    )
    report = Runner().run(RunRequest(project_path=project))
    assert report.ok  # disabled is not a failure
    assert report.models.failures == 0
    assert report.models.disabled == 1

    by_name = {r.model.name: r for r in report.models.runs}
    assert by_name["stg"].status == "success"
    assert by_name["mart"].status == "success"
    assert by_name["tail"].status == "disabled"


def test_disabling_upstream_cascades_to_downstream_skipped(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    _make_chain_project(
        project,
        yaml_extra="  - name: mart\n    disabled: true\n",
    )
    report = Runner().run(RunRequest(project_path=project))
    assert report.ok
    assert report.models.disabled == 1

    by_name = {r.model.name: r for r in report.models.runs}
    assert by_name["stg"].status == "success"
    assert by_name["mart"].status == "disabled"
    # tail is downstream of the disabled mart — skipped, not failed.
    assert by_name["tail"].status == "skipped"
    assert by_name["tail"].skipped_reason == "upstream_disabled"


def test_cli_disable_flag_overrides_at_runtime(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    _make_chain_project(project)
    report = Runner().run(RunRequest(project_path=project, disable_models=["tail"]))
    assert report.ok
    by_name = {r.model.name: r for r in report.models.runs}
    assert by_name["tail"].status == "disabled"
    assert by_name["stg"].status == "success"
    assert by_name["mart"].status == "success"


def test_enable_only_disables_everything_else(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    _make_chain_project(project)
    report = Runner().run(RunRequest(project_path=project, enable_only=["stg"]))
    assert report.ok
    by_name = {r.model.name: r for r in report.models.runs}
    assert by_name["stg"].status == "success"
    # mart and tail are not enabled -> disabled, then downstream cascades.
    assert by_name["mart"].status == "disabled"
    # tail is downstream of disabled mart
    assert by_name["tail"].status in ("skipped", "disabled")


def test_disable_unknown_model_name_raises(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    _make_chain_project(project)
    with pytest.raises(ValueError, match="not in project"):
        Runner().run(RunRequest(project_path=project, disable_models=["does_not_exist"]))


def test_compile_json_exposes_disabled_flag(tmp_path: Path) -> None:
    """The manifest `juncture compile --json` emits must include
    `disabled: true` for schema.yml-declared disables, so the web UI
    can render disabled nodes in a dimmed state."""
    project = tmp_path / "proj"
    _make_chain_project(
        project,
        yaml_extra="  - name: tail\n    disabled: true\n",
    )
    import subprocess
    import sys

    venv_juncture = Path(sys.executable).parent / "juncture"
    result = subprocess.run(
        [str(venv_juncture), "compile", "--project", str(project), "--json"],
        check=True,
        capture_output=True,
        text=True,
    )
    manifest = json.loads(result.stdout)
    by_name = {m["name"]: m for m in manifest["models"]}
    assert by_name["tail"]["disabled"] is True
    assert by_name["stg"]["disabled"] is False
