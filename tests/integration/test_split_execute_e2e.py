"""End-to-end: split-execute produces a Juncture project that runs and
yields the same tables a single EXECUTE model would.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from juncture.core.runner import Runner, RunRequest
from juncture.migration.split_execute import split_execute_script

# A small-ish pilot-migration-like script:
# * one external seed (`orders`)
# * three CTAS producers with chain / diamond shape
# * one residual UPDATE that touches a produced table
MIGRATED_SQL = """
CREATE OR REPLACE TABLE stg_orders AS
  SELECT id, status, amount FROM orders;

CREATE OR REPLACE TABLE paid AS
  SELECT id, amount FROM stg_orders WHERE status = 'paid';

CREATE OR REPLACE TABLE cancelled AS
  SELECT id, amount FROM stg_orders WHERE status = 'cancelled';

CREATE OR REPLACE TABLE revenue AS
  SELECT SUM(p.amount) - COALESCE(SUM(c.amount), 0) AS net
  FROM paid p LEFT JOIN cancelled c ON p.id = c.id;
"""


def _write_orders_seed(project: Path) -> None:
    (project / "seeds").mkdir(parents=True, exist_ok=True)
    (project / "seeds" / "orders.csv").write_text(
        "id,status,amount\n1,paid,100\n2,paid,250\n3,cancelled,40\n4,pending,10\n"
    )


def _write_yaml(project: Path, name: str) -> None:
    (project / "juncture.yaml").write_text(
        f"""name: {name}
profile: local
default_schema: main
default_materialization: table

connections:
  local:
    type: duckdb
    path: {project}/{name}.duckdb
"""
    )


def _dump_tables(path: Path, tables: list[str]) -> dict[str, list[tuple]]:
    con = duckdb.connect(str(path))
    return {t: sorted(con.execute(f'SELECT * FROM main."{t}"').fetchall()) for t in tables}


def test_split_execute_equivalent_to_monolith_execute(tmp_path: Path) -> None:
    # --- Baseline: single EXECUTE model.
    baseline = tmp_path / "baseline"
    (baseline / "models").mkdir(parents=True)
    _write_orders_seed(baseline)
    _write_yaml(baseline, "baseline")
    (baseline / "models" / "pipeline.sql").write_text(MIGRATED_SQL)
    (baseline / "models" / "schema.yml").write_text(
        """models:
  - name: pipeline
    materialization: execute
"""
    )
    report = Runner().run(RunRequest(project_path=baseline))
    assert report.ok, [r.error for r in report.models.runs if r.error]

    # --- Target: split into per-table CTAS models + no residual (no INSERT etc.).
    result = split_execute_script(MIGRATED_SQL)
    assert {m.name for m in result.models} == {"stg_orders", "paid", "cancelled", "revenue"}
    assert result.residual is None

    split = tmp_path / "split"
    (split / "models").mkdir(parents=True)
    _write_orders_seed(split)
    _write_yaml(split, "split")
    for m in result.models:
        (split / "models" / f"{m.name}.sql").write_text(m.body.rstrip(";").strip() + "\n")

    report = Runner().run(RunRequest(project_path=split))
    assert report.ok, [r.error for r in report.models.runs if r.error]

    tables = ["stg_orders", "paid", "cancelled", "revenue"]
    baseline_dump = _dump_tables(baseline / "baseline.duckdb", tables)
    split_dump = _dump_tables(split / "split.duckdb", tables)
    assert baseline_dump == split_dump
    # LEFT JOIN of paid onto cancelled finds no matches (disjoint IDs) so
    # COALESCE(SUM(c.amount), 0) = 0 and net = SUM(paid.amount) = 350.
    assert split_dump["revenue"] == [(350,)]


def test_split_execute_with_residual_preserves_semantics(tmp_path: Path) -> None:
    # This script has an UPDATE that depends on a produced CTAS model.
    sql = (
        "CREATE OR REPLACE TABLE things AS SELECT 1 AS id, 10 AS x UNION ALL SELECT 2, 20;"
        "CREATE OR REPLACE TABLE doubled AS SELECT id, x * 2 AS x FROM things;"
        "UPDATE doubled SET x = x + 1 WHERE id = 1;"
    )

    baseline = tmp_path / "baseline"
    (baseline / "models").mkdir(parents=True)
    _write_yaml(baseline, "baseline")
    (baseline / "models" / "pipeline.sql").write_text(sql)
    (baseline / "models" / "schema.yml").write_text(
        """models:
  - name: pipeline
    materialization: execute
"""
    )
    assert Runner().run(RunRequest(project_path=baseline)).ok

    # Split.
    result = split_execute_script(sql)
    assert {m.name for m in result.models} == {"things", "doubled"}
    assert result.residual is not None
    assert "doubled" in result.residual_depends_on

    split = tmp_path / "split"
    (split / "models").mkdir(parents=True)
    _write_yaml(split, "split")
    for m in result.models:
        (split / "models" / f"{m.name}.sql").write_text(m.body.rstrip(";").strip() + "\n")

    # Residual: prepend the LIMIT 0 ref() hints so depends_on is inferred.
    ref_loads = "".join(f"SELECT 1 FROM {{{{ ref('{n}') }}}} LIMIT 0;\n" for n in result.residual_depends_on)
    (split / "models" / "_residual.sql").write_text(ref_loads + "\n" + result.residual)
    (split / "models" / "schema.yml").write_text(
        """models:
  - name: _residual
    materialization: execute
"""
    )
    assert Runner().run(RunRequest(project_path=split)).ok

    baseline_dump = _dump_tables(baseline / "baseline.duckdb", ["things", "doubled"])
    split_dump = _dump_tables(split / "split.duckdb", ["things", "doubled"])
    assert baseline_dump == split_dump
    # Expected after UPDATE: id=1 → x=21, id=2 → x=40.
    assert split_dump["doubled"] == [(1, 21), (2, 40)]
