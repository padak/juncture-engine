"""Integration: seed CSV files materialize and are ref-able from SQL models."""

from __future__ import annotations

from pathlib import Path

import duckdb

from juncture.core.runner import Runner, RunRequest


def test_seeds_load_and_ref(tmp_path: Path) -> None:
    project = tmp_path / "seedy"
    (project / "models").mkdir(parents=True)
    (project / "seeds").mkdir(parents=True)

    (project / "juncture.yaml").write_text(
        f"""name: seedy
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/seedy.duckdb
"""
    )
    (project / "seeds" / "countries.csv").write_text("code,name\nCZ,Czechia\nUS,United States\nDE,Germany\n")
    (project / "models" / "country_count.sql").write_text("SELECT COUNT(*) AS n FROM {{ ref('countries') }}")

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok

    con = duckdb.connect(str(project / "seedy.duckdb"))
    rows = con.execute("SELECT n FROM main.country_count").fetchone()
    assert rows == (3,)
