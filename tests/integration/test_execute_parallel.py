"""Integration tests for parallel EXECUTE materialization.

Opt-in via ``config.parallelism: N`` in ``schema.yml``. Default behaviour
(parallelism <= 1) is covered by ``test_execute_materialization.py``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from juncture.core.runner import Runner, RunRequest


def _scaffold(tmp_path: Path, name: str, sql: str, *, parallelism: int | None) -> Path:
    project = tmp_path / name
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: {name}
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {project}/{name}.duckdb
"""
    )
    (project / "models" / "pipeline.sql").write_text(sql)
    if parallelism is not None:
        (project / "models" / "schema.yml").write_text(
            f"""models:
  - name: pipeline
    materialization: execute
    config:
      parallelism: {parallelism}
"""
        )
    return project


def _run_and_dump(project: Path) -> dict[str, list[tuple]]:
    report = Runner().run(RunRequest(project_path=project))
    assert report.ok, [r.error for r in report.models.runs if r.error]
    name = project.name
    con = duckdb.connect(str(project / f"{name}.duckdb"))
    tables = [
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()
    ]
    return {t: sorted(con.execute(f'SELECT * FROM main."{t}"').fetchall()) for t in tables}


DIAMOND_SQL = """
CREATE OR REPLACE TABLE root AS SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b';
CREATE OR REPLACE TABLE left_branch AS SELECT id, v || '_L' AS v FROM root;
CREATE OR REPLACE TABLE right_branch AS SELECT id, v || '_R' AS v FROM root;
CREATE OR REPLACE TABLE joined AS
  SELECT l.id, l.v AS lv, r.v AS rv
  FROM left_branch l JOIN right_branch r ON l.id = r.id;
"""


def test_parallel_diamond_matches_sequential(tmp_path: Path) -> None:
    seq = _scaffold(tmp_path, "seq", DIAMOND_SQL, parallelism=None)
    par = _scaffold(tmp_path, "par", DIAMOND_SQL, parallelism=4)

    seq_dump = _run_and_dump(seq)
    par_dump = _run_and_dump(par)

    # Parallel execution must be output-equivalent to sequential.
    assert par_dump == seq_dump
    # Four tables built: root, left_branch, right_branch, joined.
    assert set(par_dump) == {"root", "left_branch", "right_branch", "joined"}
    assert par_dump["joined"] == [(1, "a_L", "a_R"), (2, "b_L", "b_R")]


INDEPENDENT_SQL = "\n".join(f"CREATE OR REPLACE TABLE t{i} AS SELECT {i} AS x;" for i in range(8))


def test_parallel_independent_statements_all_succeed(tmp_path: Path) -> None:
    # 8 mutually independent CREATEs — all should end up in layer 0.
    project = _scaffold(tmp_path, "independent", INDEPENDENT_SQL, parallelism=4)
    dump = _run_and_dump(project)
    assert set(dump) == {f"t{i}" for i in range(8)}
    for i in range(8):
        assert dump[f"t{i}"] == [(i,)]


CHAIN_SQL = """
CREATE OR REPLACE TABLE a AS SELECT 1 AS x;
CREATE OR REPLACE TABLE b AS SELECT x * 2 AS x FROM a;
CREATE OR REPLACE TABLE c AS SELECT x * 2 AS x FROM b;
CREATE OR REPLACE TABLE d AS SELECT x * 2 AS x FROM c;
"""


def test_parallel_chain_is_ordered_correctly(tmp_path: Path) -> None:
    # A hard chain cannot benefit from parallelism but must still run in the
    # right order: d depends on c depends on b depends on a.
    project = _scaffold(tmp_path, "chain", CHAIN_SQL, parallelism=4)
    dump = _run_and_dump(project)
    assert dump == {"a": [(1,)], "b": [(2,)], "c": [(4,)], "d": [(8,)]}


def test_parallelism_1_equals_sequential(tmp_path: Path) -> None:
    # parallelism=1 takes the same sequential branch as unset parallelism.
    seq = _scaffold(tmp_path, "seq1", DIAMOND_SQL, parallelism=None)
    par1 = _scaffold(tmp_path, "par1", DIAMOND_SQL, parallelism=1)
    assert _run_and_dump(seq) == _run_and_dump(par1)


def test_invalid_parallelism_fails_fast(tmp_path: Path) -> None:
    project = tmp_path / "bad"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: bad
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {project}/bad.duckdb
"""
    )
    (project / "models" / "pipeline.sql").write_text("CREATE TABLE t AS SELECT 1")
    (project / "models" / "schema.yml").write_text(
        """models:
  - name: pipeline
    materialization: execute
    config:
      parallelism: "four"
"""
    )
    report = Runner().run(RunRequest(project_path=project))
    assert not report.ok
    errors = [r.error for r in report.models.runs if r.error]
    assert any("parallelism" in (e or "").lower() for e in errors), errors


@pytest.mark.parametrize("parallelism", [2, 4, 8])
def test_diamond_stable_across_parallelism_widths(tmp_path: Path, parallelism: int) -> None:
    project = _scaffold(tmp_path, f"w{parallelism}", DIAMOND_SQL, parallelism=parallelism)
    dump = _run_and_dump(project)
    assert dump["joined"] == [(1, "a_L", "a_R"), (2, "b_L", "b_R")]
