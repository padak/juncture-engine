"""End-to-end: spin up a project in a temp dir, run it, assert DuckDB state."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from juncture.core.project import Project
from juncture.core.runner import Runner, RunRequest


@pytest.fixture()
def mini_project(tmp_path: Path) -> Path:
    project = tmp_path / "mini"
    (project / "models").mkdir(parents=True)

    (project / "juncture.yaml").write_text(
        f"""name: mini
version: 0.1.0
profile: local
default_materialization: table
default_schema: main

connections:
  local:
    type: duckdb
    path: {project}/mini.duckdb
    threads: 2
"""
    )

    (project / "models" / "stg_users.sql").write_text(
        "SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'"
    )
    (project / "models" / "user_count.sql").write_text("SELECT COUNT(*) AS total FROM {{ ref('stg_users') }}")
    (project / "models" / "schema.yml").write_text(
        """models:
  - name: stg_users
    columns:
      - name: id
        tests: [not_null, unique]
      - name: name
        tests: [not_null]
  - name: user_count
    columns:
      - name: total
        tests: [not_null]
"""
    )
    return project


def test_run_materializes_tables(mini_project: Path) -> None:
    report = Runner().run(RunRequest(project_path=mini_project))
    assert report.ok
    assert report.models.successes == 2

    con = duckdb.connect(str(mini_project / "mini.duckdb"))
    rows = con.execute("SELECT total FROM main.user_count").fetchone()
    assert rows == (2,)


def test_run_with_tests_passes(mini_project: Path) -> None:
    report = Runner().run(RunRequest(project_path=mini_project, run_tests=True))
    assert report.ok
    assert len(report.tests) == 4  # stg_users: id(2) + name(1); user_count: total(1)
    assert all(t.passed for t in report.tests)


def test_project_discovers_models(mini_project: Path) -> None:
    project = Project.load(mini_project)
    names = {m.name for m in project.models}
    assert names == {"stg_users", "user_count"}
    user_count = next(m for m in project.models if m.name == "user_count")
    assert user_count.depends_on == {"stg_users"}
