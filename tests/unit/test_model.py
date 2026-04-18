"""Unit tests for the Model dataclass."""

from __future__ import annotations

import pytest

from juncture.core.model import Materialization, Model, ModelKind


def test_sql_model_requires_sql() -> None:
    with pytest.raises(ValueError, match="requires non-empty sql"):
        Model(name="x", kind=ModelKind.SQL)


def test_python_model_requires_callable() -> None:
    with pytest.raises(ValueError, match="requires python_callable"):
        Model(name="x", kind=ModelKind.PYTHON)


def test_defaults_to_table_materialization() -> None:
    model = Model(name="x", kind=ModelKind.SQL, sql="SELECT 1")
    assert model.materialization is Materialization.TABLE


def test_fqn_is_name() -> None:
    model = Model(name="orders", kind=ModelKind.SQL, sql="SELECT 1")
    assert model.fqn == "orders"


def test_governance_fields_default_empty() -> None:
    """M4 ownership / SLA / consumers fields are optional."""
    m = Model(name="x", kind=ModelKind.SQL, sql="SELECT 1")
    assert m.owner is None
    assert m.team is None
    assert m.business_unit is None
    assert m.criticality is None
    assert m.sla_freshness_hours is None
    assert m.sla_success_rate_target is None
    assert m.docs is None
    assert m.consumers == []


def test_governance_fields_carry_through() -> None:
    m = Model(
        name="customer_segments",
        kind=ModelKind.SQL,
        sql="SELECT 1",
        owner="marketing-data@example.com",
        team="analytics",
        business_unit="Marketing",
        criticality="tier-1",
        sla_freshness_hours=24,
        sla_success_rate_target=0.99,
        docs="docs/customer_segments.md",
        consumers=[{"name": "Exec dashboard", "url": "https://bi.example/execs"}],
    )
    assert m.criticality == "tier-1"
    assert m.sla_freshness_hours == 24
    assert m.consumers[0]["url"] == "https://bi.example/execs"
