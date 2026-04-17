"""Unit tests for incremental state store (against DuckDB in-memory)."""

from __future__ import annotations

from juncture.adapters.duckdb_adapter import DuckDBAdapter
from juncture.core.model import Materialization, Model, ModelKind
from juncture.core.state import StateStore, fingerprint, make_state


def test_state_round_trip() -> None:
    adapter = DuckDBAdapter(path=":memory:")
    adapter.connect()
    try:
        store = StateStore(adapter, schema="main")
        store.ensure()
        model = Model(
            name="orders",
            kind=ModelKind.SQL,
            sql="SELECT 1",
            materialization=Materialization.INCREMENTAL,
            unique_key="id",
        )
        s1 = make_state(model, row_count=42, watermark="2026-04-17")
        store.upsert(s1)
        back = store.get("orders")
        assert back is not None
        assert back.model_name == "orders"
        assert back.row_count == 42
        assert back.watermark == "2026-04-17"
        assert back.fingerprint == fingerprint(model)
    finally:
        adapter.close()


def test_fingerprint_detects_sql_change() -> None:
    m1 = Model(name="x", kind=ModelKind.SQL, sql="SELECT 1")
    m2 = Model(name="x", kind=ModelKind.SQL, sql="SELECT 2")
    assert fingerprint(m1) != fingerprint(m2)


def test_fingerprint_detects_materialization_change() -> None:
    m1 = Model(name="x", kind=ModelKind.SQL, sql="SELECT 1", materialization=Materialization.TABLE)
    m2 = Model(
        name="x",
        kind=ModelKind.SQL,
        sql="SELECT 1",
        materialization=Materialization.INCREMENTAL,
        unique_key="id",
    )
    assert fingerprint(m1) != fingerprint(m2)


def test_list_all_returns_two_entries() -> None:
    adapter = DuckDBAdapter(path=":memory:")
    adapter.connect()
    try:
        store = StateStore(adapter, schema="main")
        store.ensure()
        for name in ["a", "b"]:
            model = Model(name=name, kind=ModelKind.SQL, sql="SELECT 1")
            store.upsert(make_state(model, row_count=1))
        assert len({s.model_name for s in store.list_all()}) == 2
    finally:
        adapter.close()
