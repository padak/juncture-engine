"""Integration: EXECUTE materialization keeps running past failing statements.

Exercises the ``juncture run --continue-on-error`` triage mode:
migrated multi-statement bodies often hold half a dozen primary errors;
the default fail-fast behaviour surfaces one at a time (26 repair rounds
on the pilot Slevomat migration). Continue-on-error is the single knob
that collapses that loop, so the reporter must carry both the successful
side-effects (tables that did get created) and the per-statement errors.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from juncture.core.runner import Runner, RunRequest


def _write_project(root: Path, *, body: str) -> Path:
    """Write a minimal EXECUTE project at ``root``. Returns the DuckDB file path."""
    (root / "models").mkdir(parents=True, exist_ok=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: coeproj
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "pipeline.sql").write_text(body)
    return db_path


def test_continue_on_error_collects_all_primary_errors(tmp_path: Path) -> None:
    project = tmp_path / "coeproj"
    # Three statements, middle one deliberately broken (wrong column type).
    # Without continue-on-error DuckDB aborts on statement #1 and we never
    # see statement #2 as a distinct failure.
    db_path = _write_project(
        project,
        body=(
            'CREATE OR REPLACE TABLE "main"."good_a" AS SELECT 1 AS id;\n'
            'CREATE OR REPLACE TABLE "main"."bad" AS SELECT CAST(\'oops\' AS INT) AS id;\n'
            'CREATE OR REPLACE TABLE "main"."good_b" AS SELECT 2 AS id;\n'
        ),
    )

    report = Runner().run(RunRequest(project_path=project, continue_on_error=True))

    # ok is True because no model "failed" — the EXECUTE model returned
    # partial status with per-statement errors, which is the whole point.
    assert report.ok
    assert report.models.partial == 1
    assert report.models.failures == 0

    runs = [r for r in report.models.runs if r.model.name == "pipeline"]
    assert len(runs) == 1
    run = runs[0]
    assert run.status == "partial"
    assert run.result is not None
    assert len(run.result.statement_errors) == 1
    err = run.result.statement_errors[0]
    assert err.index == 1
    assert "oops" in err.error.lower() or "int" in err.error.lower()

    # The well-formed statements on either side of the break still ran.
    con = duckdb.connect(str(db_path))
    assert con.execute('SELECT id FROM main."good_a"').fetchone() == (1,)
    assert con.execute('SELECT id FROM main."good_b"').fetchone() == (2,)


def test_default_mode_still_fails_fast(tmp_path: Path) -> None:
    project = tmp_path / "failfast"
    db_path = _write_project(
        project,
        body=(
            'CREATE OR REPLACE TABLE "main"."good_a" AS SELECT 1 AS id;\n'
            'CREATE OR REPLACE TABLE "main"."bad" AS SELECT CAST(\'oops\' AS INT) AS id;\n'
            'CREATE OR REPLACE TABLE "main"."good_b" AS SELECT 2 AS id;\n'
        ),
    )

    # Without continue_on_error we must abort on the bad statement, and the
    # trailing good statement must not run.
    report = Runner().run(RunRequest(project_path=project))
    assert not report.ok
    assert report.models.failures == 1

    con = duckdb.connect(str(db_path))
    # good_a ran before the abort; good_b did not.
    assert con.execute('SELECT id FROM main."good_a"').fetchone() == (1,)
    with pytest.raises(duckdb.CatalogException):
        con.execute('SELECT id FROM main."good_b"').fetchone()


def test_continue_on_error_parallel_mode(tmp_path: Path) -> None:
    project = tmp_path / "coeparallel"
    db_path = _write_project(
        project,
        body=(
            # Four layer-0 statements (no deps between them), one broken.
            # Parallel executor must collect the one error and keep running
            # the other three.
            'CREATE OR REPLACE TABLE "main"."a" AS SELECT 1 AS id;\n'
            'CREATE OR REPLACE TABLE "main"."b" AS SELECT 2 AS id;\n'
            'CREATE OR REPLACE TABLE "main"."c" AS SELECT CAST(\'x\' AS INT) AS id;\n'
            'CREATE OR REPLACE TABLE "main"."d" AS SELECT 4 AS id;\n'
        ),
    )
    (project / "models" / "schema.yml").write_text(
        """models:
  - name: pipeline
    config:
      parallelism: 4
"""
    )

    report = Runner().run(RunRequest(project_path=project, continue_on_error=True))
    assert report.ok
    assert report.models.partial == 1

    pipeline_run = next(r for r in report.models.runs if r.model.name == "pipeline")
    assert pipeline_run.result is not None
    assert len(pipeline_run.result.statement_errors) == 1
    assert pipeline_run.result.statement_errors[0].layer == 0

    con = duckdb.connect(str(db_path))
    assert con.execute('SELECT id FROM main."a"').fetchone() == (1,)
    assert con.execute('SELECT id FROM main."b"').fetchone() == (2,)
    assert con.execute('SELECT id FROM main."d"').fetchone() == (4,)
