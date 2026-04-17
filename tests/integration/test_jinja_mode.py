"""Integration: Jinja rendering in SQL models when ``jinja: true``."""

from __future__ import annotations

from pathlib import Path

from juncture.core.runner import Runner, RunRequest


def test_jinja_ref_and_var(tmp_path: Path) -> None:
    project = tmp_path / "jinj"
    (project / "models").mkdir(parents=True)

    (project / "juncture.yaml").write_text(
        f"""name: jinj
profile: local
default_schema: main
jinja: true
vars:
  min_amount: 100
connections:
  local:
    type: duckdb
    path: {project}/jinj.duckdb
"""
    )

    (project / "models" / "stg_orders.sql").write_text(
        "SELECT 1 AS id, 50 AS amount UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 500"
    )
    # Use Jinja conditional + variable interpolation
    (project / "models" / "big_orders.sql").write_text(
        "SELECT * FROM {{ ref('stg_orders') }} "
        "WHERE amount >= {% if var('min_amount', 0) > 0 %}{{ var('min_amount') }}{% else %}0{% endif %}"
    )

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok

    import duckdb

    con = duckdb.connect(str(project / "jinj.duckdb"))
    count = con.execute("SELECT COUNT(*) FROM main.big_orders").fetchone()[0]
    assert count == 2
