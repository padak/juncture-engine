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


def test_jinja_macros_global_callable(tmp_path: Path) -> None:
    """End-to-end: a macro under macros/ is auto-loaded and usable from any model."""
    project = tmp_path / "macroproj"
    (project / "models").mkdir(parents=True)
    (project / "macros").mkdir(parents=True)

    (project / "juncture.yaml").write_text(
        f"""name: macroproj
profile: local
default_schema: main
jinja: true
vars:
  vip_threshold: 400
connections:
  local:
    type: duckdb
    path: {project}/m.duckdb
"""
    )

    # Two reusable snippets: a date formatter and a VIP predicate that reads var().
    (project / "macros" / "dates.sql").write_text(
        "{% macro day(col) -%}strftime({{ col }}, '%Y-%m-%d'){%- endmacro %}"
    )
    (project / "macros" / "tiers.sql").write_text(
        "{% macro is_vip(col) -%}({{ col }} >= {{ var('vip_threshold', 500) }}){%- endmacro %}"
    )

    (project / "models" / "stg_orders.sql").write_text(
        "SELECT 1 AS id, DATE '2026-01-05' AS ts, 300 AS amount UNION ALL SELECT 2, DATE '2026-01-06', 500"
    )
    # Both macros used in the same model; var() inside the macro should see
    # project vars (400 here) and override the 500 default.
    (project / "models" / "daily_flagged.sql").write_text(
        "SELECT {{ day('ts') }} AS day, id, amount, "
        "{{ is_vip('amount') }} AS is_vip "
        "FROM {{ ref('stg_orders') }}"
    )

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok

    import duckdb

    con = duckdb.connect(str(project / "m.duckdb"))
    rows = con.execute("SELECT id, is_vip FROM main.daily_flagged ORDER BY id").fetchall()
    # Threshold is 400 → id=1 (amount 300) is FALSE, id=2 (amount 500) is TRUE.
    assert rows == [(1, False), (2, True)]


def test_jinja_macros_ignored_without_jinja_mode(tmp_path: Path) -> None:
    """If ``jinja: false`` the macros/ folder is silently skipped."""
    project = tmp_path / "nojinja"
    (project / "models").mkdir(parents=True)
    (project / "macros").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: nojinja
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/n.duckdb
"""
    )
    (project / "macros" / "x.sql").write_text("{% macro fmt(c) -%}UPPER({{ c }}){%- endmacro %}")
    # Model does not reference the macro — it stays as plain SQL.
    (project / "models" / "ok.sql").write_text("SELECT 1 AS id")

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok
