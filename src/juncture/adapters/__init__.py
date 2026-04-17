"""Backend adapters: DuckDB (MVP), Snowflake (v0.3), BigQuery + Postgres (later)."""

# Optional backends register themselves on import. They don't force their
# optional dependencies to be installed; the import guards are inside the
# adapter's connect() method.
import contextlib

from juncture.adapters.base import Adapter, AdapterError, MaterializationResult
from juncture.adapters.duckdb_adapter import DuckDBAdapter
from juncture.adapters.registry import get_adapter, register_adapter

with contextlib.suppress(ImportError):  # pragma: no cover
    from juncture.adapters import snowflake_adapter  # noqa: F401

__all__ = [
    "Adapter",
    "AdapterError",
    "DuckDBAdapter",
    "MaterializationResult",
    "get_adapter",
    "register_adapter",
]
