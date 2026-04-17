"""End-to-end test: Python @transform model composing with SQL upstream."""

from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from juncture.core.runner import Runner, RunRequest  # noqa: E402


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    root = tmp_path / "py_mix"
    (root / "models").mkdir(parents=True)

    (root / "juncture.yaml").write_text(
        f"""name: py_mix
profile: local
default_schema: main

connections:
  local:
    type: duckdb
    path: {root}/py_mix.duckdb
"""
    )

    (root / "models" / "stg_orders.sql").write_text(
        "SELECT 1 AS order_id, 100 AS amount UNION ALL SELECT 2, 250 UNION ALL SELECT 3, 75"
    )

    (root / "models" / "revenue_summary.py").write_text(
        '''
import pandas as pd
from juncture import transform


@transform(name="revenue_summary", depends_on=["stg_orders"])
def revenue_summary(ctx):
    """Total revenue across all staged orders."""
    arrow = ctx.ref("stg_orders")
    df = arrow.to_pandas() if hasattr(arrow, "to_pandas") else pd.DataFrame(arrow)
    return pd.DataFrame({"total_revenue": [int(df["amount"].sum())]})
'''
    )
    return root


def test_python_model_consumes_sql_upstream(project: Path) -> None:
    report = Runner().run(RunRequest(project_path=project))
    assert report.ok, [r.error for r in report.models.runs if r.error]
    totals = {r.model.name: r.result.row_count for r in report.models.runs if r.result}
    assert totals == {"stg_orders": 3, "revenue_summary": 1}
