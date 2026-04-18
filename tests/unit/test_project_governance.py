"""Unit tests: schema.yml governance fields (RFC 0001 §7) survive Project.load."""

from __future__ import annotations

from pathlib import Path

from juncture.core.project import Project


def _write_project(root: Path, schema_yml: str = "", seeds_schema_yml: str = "") -> None:
    (root / "models").mkdir(parents=True)
    (root / "seeds").mkdir(parents=True)
    (root / "juncture.yaml").write_text(
        """name: gov_test
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: out.duckdb
"""
    )
    (root / "models" / "customer_segments.sql").write_text(
        "SELECT customer_id AS id FROM {{ ref('raw_customers') }}"
    )
    if schema_yml:
        (root / "models" / "schema.yml").write_text(schema_yml)
    (root / "seeds" / "raw_customers.csv").write_text("customer_id,name\n1,alice\n")
    if seeds_schema_yml:
        (root / "seeds" / "schema.yml").write_text(seeds_schema_yml)


def test_model_ownership_sla_docs_consumers(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        schema_yml="""version: 2
models:
  - name: customer_segments
    description: Bucket customers into tiers.
    docs: docs/customer_segments.md
    owner: marketing-data@example.com
    team: analytics
    business_unit: Marketing
    criticality: tier-1
    sla:
      freshness_hours: 24
      success_rate_target: 0.99
    consumers:
      - name: Exec dashboard
        url: https://bi.example/execs
      - Retention team
""",
    )
    project = Project.load(tmp_path)
    seg = next(m for m in project.models if m.name == "customer_segments")
    assert seg.owner == "marketing-data@example.com"
    assert seg.team == "analytics"
    assert seg.business_unit == "Marketing"
    assert seg.criticality == "tier-1"
    assert seg.sla_freshness_hours == 24
    assert seg.sla_success_rate_target == 0.99
    assert seg.docs == "docs/customer_segments.md"
    # consumers: first is structured, second is a string promoted to {name: ...}.
    assert seg.consumers == [
        {"name": "Exec dashboard", "url": "https://bi.example/execs"},
        {"name": "Retention team"},
    ]


def test_model_missing_governance_defaults_clean(tmp_path: Path) -> None:
    _write_project(tmp_path)
    project = Project.load(tmp_path)
    seg = next(m for m in project.models if m.name == "customer_segments")
    assert seg.owner is None
    assert seg.criticality is None
    assert seg.sla_freshness_hours is None
    assert seg.consumers == []


def test_seed_pii_retention_source_fields(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        seeds_schema_yml="""version: 2
seeds:
  - name: raw_customers
    source_system: keboola_storage
    source_locator: in.c-main.customers
    pii: true
    retention_days: 365
    owner: data-platform@example.com
""",
    )
    project = Project.load(tmp_path)
    seed = next(s for s in project.seeds if s.name == "raw_customers")
    assert seed.source_system == "keboola_storage"
    assert seed.source_locator == "in.c-main.customers"
    assert seed.pii is True
    assert seed.retention_days == 365
    assert seed.owner == "data-platform@example.com"
