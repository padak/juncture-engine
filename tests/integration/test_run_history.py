"""Integration: Runner persists outcomes to target/run_history.jsonl."""

from __future__ import annotations

import json
from pathlib import Path

from juncture.core.run_history import history_path, read_runs
from juncture.core.runner import Runner, RunRequest


def _project(root: Path) -> Path:
    (root / "models").mkdir(parents=True, exist_ok=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: histproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "stg.sql").write_text("SELECT 1 AS id")
    return db_path


def test_run_appends_to_history(tmp_path: Path) -> None:
    project = tmp_path / "p"
    _project(project)
    Runner().run(RunRequest(project_path=project))

    log = history_path(project)
    assert log.exists()
    raw = log.read_text().strip().splitlines()
    assert len(raw) == 1
    entry = json.loads(raw[0])
    assert entry["project_name"] == "histproj"
    assert entry["ok"] is True
    assert entry["successes"] == 1
    assert len(entry["models"]) == 1


def test_read_runs_returns_newest_first(tmp_path: Path) -> None:
    project = tmp_path / "p"
    _project(project)
    r = Runner()
    r.run(RunRequest(project_path=project))
    r.run(RunRequest(project_path=project))
    r.run(RunRequest(project_path=project))
    runs = read_runs(project)
    assert len(runs) == 3
    # Chronological property: every run has a later or equal started_at
    # than the next one in the returned list.
    timestamps = [run.started_at for run in runs]
    assert timestamps == sorted(timestamps, reverse=True)


def test_record_history_false_skips_append(tmp_path: Path) -> None:
    project = tmp_path / "p"
    _project(project)
    Runner().run(RunRequest(project_path=project, record_history=False))
    assert not history_path(project).exists()


def test_read_runs_respects_limit(tmp_path: Path) -> None:
    project = tmp_path / "p"
    _project(project)
    r = Runner()
    for _ in range(5):
        r.run(RunRequest(project_path=project))
    runs = read_runs(project, limit=2)
    assert len(runs) == 2


def test_read_runs_handles_missing_file(tmp_path: Path) -> None:
    project = tmp_path / "p"
    _project(project)
    assert read_runs(project) == []


def test_run_with_statement_errors_serialises(tmp_path: Path) -> None:
    """When EXECUTE under continue-on-error records errors, the history
    entry must include them in JSON-serialisable form so the web UI
    can render a triage drilldown."""
    project = tmp_path / "p"
    (project / "models").mkdir(parents=True)
    db_path = project / "out.duckdb"
    (project / "juncture.yaml").write_text(
        f"""name: histproj
profile: local
default_schema: main
default_materialization: execute
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (project / "models" / "pipeline.sql").write_text(
        'CREATE OR REPLACE TABLE "main"."a" AS SELECT 1 AS id;\n'
        'CREATE OR REPLACE TABLE "main"."bad" AS SELECT CAST(\'x\' AS INT) AS id;\n'
    )
    Runner().run(RunRequest(project_path=project, continue_on_error=True))

    runs = read_runs(project)
    assert len(runs) == 1
    pipeline = next(m for m in runs[0].models if m["name"] == "pipeline")
    assert pipeline["status"] == "partial"
    assert "statement_errors" in pipeline
    assert len(pipeline["statement_errors"]) == 1
    assert pipeline["statement_errors"][0]["index"] == 1
