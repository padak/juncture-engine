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
