"""Integration tests for ``juncture run --dry-run`` (Runner.plan)."""

from __future__ import annotations

from pathlib import Path

from juncture.core.runner import Runner, RunRequest


def _scaffold_execute(tmp_path: Path, *, parallelism: int | None = None) -> Path:
    project = tmp_path / "dryrun_exec"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: dryrun_exec
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {project}/dryrun_exec.duckdb
"""
    )
    (project / "models" / "pipeline.sql").write_text(
        "CREATE OR REPLACE TABLE a AS SELECT 1 AS x;\n"
        "CREATE OR REPLACE TABLE b AS SELECT x FROM a;\n"
        "CREATE OR REPLACE TABLE c AS SELECT x FROM b;\n"
    )
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


def test_dry_run_does_not_open_duckdb(tmp_path: Path) -> None:
    project = _scaffold_execute(tmp_path)
    db_file = project / "dryrun_exec.duckdb"
    assert not db_file.exists()

    plan = Runner().plan(RunRequest(project_path=project))

    # The whole point: plan() must not open the database.
    assert not db_file.exists(), "dry-run created a DuckDB file — Runner.plan should never open the adapter"
    # And the report must still be well-formed.
    assert plan.project_name == "dryrun_exec"
    assert plan.model_layers == 1  # single EXECUTE model, one model-level layer
    assert len(plan.models) == 1
    assert plan.models[0].name == "pipeline"
    assert plan.models[0].materialization == "execute"
    assert plan.models[0].layer == 1


def test_dry_run_exposes_intra_script_layers(tmp_path: Path) -> None:
    project = _scaffold_execute(tmp_path, parallelism=4)
    plan = Runner().plan(RunRequest(project_path=project))

    model = plan.models[0]
    assert model.intra is not None
    # a -> b -> c is a three-statement chain → three intra layers.
    assert model.intra.total_statements == 3
    assert model.intra.layers == 3
    assert model.intra.widest_layer == 1
    assert model.intra.layer_sizes == [1, 1, 1]
    assert model.intra.parallelism == 4


def test_parallelism_cli_override_wins_over_schema(tmp_path: Path) -> None:
    # schema.yml says parallelism=2, but the CLI --parallelism=8 override
    # takes precedence. Plan reflects 8, not 2.
    project = _scaffold_execute(tmp_path, parallelism=2)
    plan = Runner().plan(RunRequest(project_path=project, parallelism_override=8))
    assert plan.models[0].intra is not None
    assert plan.models[0].intra.parallelism == 8


def test_parallelism_override_none_keeps_schema_value(tmp_path: Path) -> None:
    project = _scaffold_execute(tmp_path, parallelism=3)
    plan = Runner().plan(RunRequest(project_path=project, parallelism_override=None))
    assert plan.models[0].intra is not None
    assert plan.models[0].intra.parallelism == 3


def test_dry_run_plain_sql_models_have_no_intra(tmp_path: Path) -> None:
    # Normal `table` materialization: one statement per model, no intra DAG.
    project = tmp_path / "dryrun_table"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: dryrun_table
profile: local
default_schema: main

connections:
  local:
    type: duckdb
    path: {project}/dryrun_table.duckdb
"""
    )
    (project / "models" / "stg.sql").write_text("SELECT 1 AS id")
    (project / "models" / "downstream.sql").write_text("SELECT id * 2 AS doubled FROM {{ ref('stg') }}")
    plan = Runner().plan(RunRequest(project_path=project))

    assert plan.model_layers == 2
    by_name = {n.name: n for n in plan.models}
    assert by_name["stg"].intra is None
    assert by_name["downstream"].intra is None
    assert by_name["downstream"].depends_on == ["stg"]
    assert by_name["stg"].layer == 1
    assert by_name["downstream"].layer == 2


def test_dry_run_separates_seeds_from_models(tmp_path: Path) -> None:
    """Seeds live in project.models as ModelKind.SEED so ref() resolves, but
    dry-run must list them *only* under ``plan.seeds`` — not mixed into the
    model-layer table (which was a real UX regression on the pilot migration:
    208 seeds appeared as "layer 1" rows alongside the actual model).
    """
    project = tmp_path / "seedsep"
    (project / "models").mkdir(parents=True)
    (project / "seeds").mkdir(parents=True)
    # One seed that a model reads, plus one standalone model.
    (project / "seeds" / "orders.csv").write_text("id,amount\n1,100\n2,200\n")
    (project / "juncture.yaml").write_text(
        f"""name: seedsep
profile: local
default_schema: main

connections:
  local:
    type: duckdb
    path: {project}/seedsep.duckdb
"""
    )
    (project / "models" / "summary.sql").write_text("SELECT SUM(amount) AS total FROM {{ ref('orders') }}")

    plan = Runner().plan(RunRequest(project_path=project))

    seed_names = {s.name for s in plan.seeds}
    model_names = {m.name for m in plan.models}

    assert "orders" in seed_names
    assert "orders" not in model_names, (
        "seed leaked into plan.models — dry-run would mix it with actual models"
    )
    assert model_names == {"summary"}
    # Model layer count counts only non-seed nodes.
    assert plan.model_layers == 1


def test_dry_run_respects_selectors(tmp_path: Path) -> None:
    project = tmp_path / "dryrun_select"
    (project / "models").mkdir(parents=True)
    (project / "juncture.yaml").write_text(
        f"""name: dryrun_select
profile: local
default_schema: main

connections:
  local:
    type: duckdb
    path: {project}/dryrun_select.duckdb
"""
    )
    (project / "models" / "a.sql").write_text("SELECT 1 AS x")
    (project / "models" / "b.sql").write_text("SELECT x FROM {{ ref('a') }}")
    (project / "models" / "c.sql").write_text("SELECT x FROM {{ ref('b') }}")

    plan = Runner().plan(
        RunRequest(project_path=project, select=["+b"])  # b + ancestors
    )
    names = {n.name for n in plan.models}
    assert names == {"a", "b"}
