"""End-to-end smoke test for the ``examples/eu_ecommerce`` demo project.

This is the sanity check that every phase-1 plumbing piece survives a real
multi-seed / multi-layer / mixed SQL+Python run:

* `generate_data.py --scale small` is deterministic and produces all seven
  CSVs.
* The compiled DAG exposes at least 15 non-seed models.
* A full ``juncture run --test`` finishes green (all models succeed, all
  data tests pass including custom SQL tests).
* A Python model (``rfm_scores``) actually declares its SQL upstream
  (``int_rfm_inputs``) so cross-language DAG edges are wired up.

The test pulls its seed CSVs into a tmp dir, then uses a fresh
``juncture.yaml`` pointing at a tmp DuckDB file. The original
``examples/eu_ecommerce`` directory is not mutated.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from juncture.core.project import Project
from juncture.core.runner import Runner, RunRequest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_DIR = REPO_ROOT / "examples" / "eu_ecommerce"
GENERATOR = EXAMPLE_DIR / "scripts" / "generate_data.py"


@pytest.fixture(scope="module")
def staged_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Copy the demo project into a tmp dir and generate small-scale seeds.

    Module-scoped: generating the CSVs and running the project is a few
    seconds end-to-end, but re-running it per-test would waste time. Every
    test below operates on the same materialised warehouse.
    """
    tmp = tmp_path_factory.mktemp("eu_ecommerce")
    # Copy the whole example directory — models, schema.yml, tests/, and
    # scripts/. We intentionally do *not* copy seeds/ or data/ because the
    # test wants to verify the generator too.
    for entry in ("models", "scripts", "tests", "juncture.yaml", "README.md"):
        src = EXAMPLE_DIR / entry
        if src.is_dir():
            shutil.copytree(src, tmp / entry)
        else:
            shutil.copy2(src, tmp / entry)
    (tmp / "seeds").mkdir()
    (tmp / "data").mkdir()

    # Generate small-scale CSVs directly into the staged project's seeds dir.
    subprocess.run(
        [
            sys.executable,
            str(GENERATOR),
            "--scale",
            "small",
            "--output-dir",
            str(tmp / "seeds"),
            "--seed",
            "42",
        ],
        check=True,
        capture_output=True,
    )
    # Sanity check the generator output.
    expected_files = {
        "product_categories.csv",
        "products.csv",
        "customers.csv",
        "campaigns.csv",
        "orders.csv",
        "order_items.csv",
        "web_sessions.csv",
    }
    produced = {p.name for p in (tmp / "seeds").glob("*.csv")}
    assert expected_files == produced, f"generator missed files: {expected_files - produced}"

    return tmp


def test_compile_has_at_least_15_models(staged_project: Path) -> None:
    """The non-seed DAG should have 15+ nodes per the Phase-1 demo spec."""
    project = Project.load(staged_project)
    non_seed = [m for m in project.models if m.kind.value != "seed"]
    assert len(non_seed) >= 15, f"expected >= 15 models, got {len(non_seed)}"
    # SQL and Python must both be represented.
    sql_count = sum(1 for m in non_seed if m.kind.value == "sql")
    py_count = sum(1 for m in non_seed if m.kind.value == "python")
    assert sql_count >= 10, sql_count
    assert py_count >= 3, py_count


def test_rfm_scores_depends_on_int_rfm_inputs(staged_project: Path) -> None:
    """Mixed-language DAG edge: Python model -> SQL upstream."""
    project = Project.load(staged_project)
    by_name = {m.name: m for m in project.models}
    assert "rfm_scores" in by_name, "rfm_scores Python model was not discovered"
    assert "int_rfm_inputs" in by_name, "int_rfm_inputs SQL model was not discovered"
    assert "int_rfm_inputs" in by_name["rfm_scores"].depends_on


def test_customer_ltv_crosses_python_sql_boundary_twice(staged_project: Path) -> None:
    """``customer_ltv`` consumes both an SQL model and a Python model."""
    project = Project.load(staged_project)
    by_name = {m.name: m for m in project.models}
    assert "customer_ltv" in by_name
    deps = by_name["customer_ltv"].depends_on
    assert "int_order_facts" in deps  # SQL upstream
    assert "rfm_scores" in deps  # Python upstream


def test_ephemeral_int_active_customer(staged_project: Path) -> None:
    """``int_active_customer`` is flagged ephemeral via schema.yml override."""
    project = Project.load(staged_project)
    by_name = {m.name: m for m in project.models}
    assert by_name["int_active_customer"].materialization.value == "ephemeral"


def test_full_run_is_green_with_tests(staged_project: Path) -> None:
    """``juncture run --test`` exits clean; every model + test passes."""
    report = Runner().run(
        RunRequest(
            project_path=staged_project,
            threads=4,
            run_tests=True,
        )
    )
    failed_models = [r for r in report.models.runs if r.status == "failed"]
    assert not failed_models, [r.error for r in failed_models]

    # Expect at least 15 non-seed models plus the 7 seed nodes; executor
    # reports seeds too.
    assert report.models.successes >= 22, report.models.successes

    failed_tests = [t for t in report.tests if not t.passed]
    assert not failed_tests, textwrap.shorten(
        ", ".join(f"{t.model}.{t.column}:{t.name}={t.failing_rows}" for t in failed_tests),
        width=500,
    )
    # Spot-check: the two custom SQL tests ran and passed.
    custom_names = {t.name for t in report.tests if t.model == "custom"}
    assert "assert_no_negative_revenue" in custom_names
    assert "assert_campaign_dates_sane" in custom_names


def test_segments_and_rfm_produce_all_labels(staged_project: Path) -> None:
    """Segmentation must populate multiple labels (not just one)."""
    import duckdb

    Runner().run(RunRequest(project_path=staged_project, threads=4))
    db = staged_project / "data" / "eu_ecommerce.duckdb"
    assert db.exists(), f"expected DuckDB file at {db}"
    con = duckdb.connect(str(db), read_only=True)
    try:
        segments = {
            row[0] for row in con.execute("SELECT DISTINCT segment FROM main.customer_segments").fetchall()
        }
        # With small-scale data we expect multiple labels observed. The
        # exact mix depends on the seed but at least three of the five
        # labels must be reachable.
        assert len(segments) >= 3, segments
        assert segments.issubset({"vip", "loyal", "regular", "at_risk", "lost"}), segments

        tiers = {row[0] for row in con.execute("SELECT DISTINCT rfm_tier FROM main.rfm_scores").fetchall()}
        assert tiers.issubset({"champion", "loyal", "potential", "at_risk", "hibernating"}), tiers
    finally:
        con.close()
