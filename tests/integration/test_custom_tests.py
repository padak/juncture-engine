"""Integration: custom SQL tests under tests/ catch violations."""

from __future__ import annotations

from pathlib import Path

from juncture.core.runner import Runner, RunRequest


def test_custom_sql_test_detects_violations(tmp_path: Path) -> None:
    project = tmp_path / "ctst"
    (project / "models").mkdir(parents=True)
    (project / "tests").mkdir(parents=True)

    (project / "juncture.yaml").write_text(
        f"""name: ctst
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/ctst.duckdb
"""
    )
    (project / "models" / "orders.sql").write_text(
        "SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, -5 UNION ALL SELECT 3, 0 UNION ALL SELECT 4, 50"
    )
    # Fail if any row has amount <= 0
    (project / "tests" / "assert_positive_amounts.sql").write_text(
        "SELECT id, amount FROM {{ ref('orders') }} WHERE amount <= 0"
    )

    report = Runner().run(RunRequest(project_path=project, run_tests=True))
    assert report.models.ok
    failing = [t for t in report.tests if t.name == "assert_positive_amounts"]
    assert len(failing) == 1
    assert not failing[0].passed
    assert failing[0].failing_rows == 2
