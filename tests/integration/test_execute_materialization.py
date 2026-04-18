"""Integration: EXECUTE materialization runs multi-statement SQL on DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

from juncture.core.runner import Runner, RunRequest


def test_execute_runs_multi_statement_script(tmp_path: Path) -> None:
    project = tmp_path / "execproj"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: execproj
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {project}/execproj.duckdb
"""
    )
    (project / "models" / "pipeline.sql").write_text(
        "-- Migrated from Snowflake, runs DDL directly.\n"
        'CREATE OR REPLACE TABLE "main"."stg_users" AS SELECT 1 AS id, \'Alice\' AS name;\n'
        'INSERT INTO "main"."stg_users" VALUES (2, \'Bob\');\n'
        'CREATE OR REPLACE TABLE "main"."user_count" AS SELECT COUNT(*) AS total FROM "main"."stg_users";\n'
    )

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok, [r.error for r in report.models.runs if r.error]

    con = duckdb.connect(str(project / "execproj.duckdb"))
    assert con.execute('SELECT total FROM main."user_count"').fetchone() == (2,)
    assert con.execute('SELECT COUNT(*) FROM main."stg_users"').fetchone() == (2,)
