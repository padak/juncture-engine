"""Unit tests for the Keboola SQL transformation migrator."""

from __future__ import annotations

import json
from pathlib import Path

from juncture.migration import migrate_keboola_sql_transformation


def test_migrates_simple_config(tmp_path: Path) -> None:
    config = {
        "storage": {
            "input": {
                "tables": [
                    {"source": "in.c-raw.orders", "destination": "orders"},
                    {"source": "in.c-raw.customers", "destination": "customers"},
                ]
            },
            "output": {"tables": [{"source": "daily_revenue", "destination": "out.c-marts.daily_revenue"}]},
        },
        "parameters": {
            "blocks": [
                {
                    "name": "staging",
                    "codes": [
                        {
                            "name": "stg_orders",
                            "script": ["SELECT * FROM orders WHERE status = 'completed'"],
                        }
                    ],
                },
                {
                    "name": "marts",
                    "codes": [
                        {
                            "name": "daily_revenue",
                            "script": [
                                "SELECT order_date, SUM(amount) AS revenue "
                                "FROM stg_orders GROUP BY order_date"
                            ],
                        }
                    ],
                },
            ]
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    target = tmp_path / "project"

    result = migrate_keboola_sql_transformation(config_json_path=config_path, target_dir=target)

    assert result.project_path == target
    assert set(result.models) == {"stg_orders", "daily_revenue"}

    stg_sql = (target / "models" / "stg_orders.sql").read_text()
    assert "{{ ref('orders') }}" in stg_sql  # reference rewritten

    revenue_sql = (target / "models" / "daily_revenue.sql").read_text()
    # stg_orders is not in the input map, so it stays bare (it's a model ref
    # that resolves via Juncture's own DAG). This mimics dbt behaviour --
    # ref() is only for declared sources.
    assert "stg_orders" in revenue_sql


def test_deduplicates_same_code_name_in_different_blocks(tmp_path: Path) -> None:
    config = {
        "storage": {"input": {"tables": []}, "output": {"tables": []}},
        "parameters": {
            "blocks": [
                {"name": "a", "codes": [{"name": "clean", "script": "SELECT 1"}]},
                {"name": "b", "codes": [{"name": "clean", "script": "SELECT 2"}]},
            ]
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    result = migrate_keboola_sql_transformation(
        config_json_path=config_path,
        target_dir=tmp_path / "project",
    )
    assert "clean" in result.models
    assert "b__clean" in result.models or any("__clean" in m for m in result.models)
