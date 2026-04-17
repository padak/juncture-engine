"""Incremental run state: watermarks and fingerprints per model.

Juncture stores a minimal ``_juncture_state`` table in the target schema with
one row per model:

* ``model_name``       — primary key
* ``last_run_at``      — UTC timestamp of the last successful materialization
* ``watermark``        — user-supplied value (e.g. max event_time seen); the
                         incremental model can use this via ``ctx.vars()``
                         during the next run.
* ``row_count``        — last observed size
* ``fingerprint``      — hash of (sql body / function source, materialization,
                         unique_key) so we can detect when a model was edited.

This module owns the schema creation and accessors. Executor integration is
intentionally opt-in (v0.3) so the MVP stays simple.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter
    from juncture.core.model import Model


STATE_TABLE = "_juncture_state"


@dataclass(frozen=True, kw_only=True)
class ModelState:
    model_name: str
    last_run_at: float
    watermark: str | None
    row_count: int | None
    fingerprint: str


class StateStore:
    """Reads and writes Juncture incremental state in a target schema."""

    def __init__(self, adapter: Adapter, schema: str) -> None:
        self.adapter = adapter
        self.schema = schema

    def _fqn(self) -> str:
        return self.adapter.resolve(STATE_TABLE, schema=self.schema)

    def ensure(self) -> None:
        """Create the state table if it does not exist yet (DuckDB-compatible DDL)."""
        cursor = getattr(self.adapter, "_thread_cursor", lambda: None)()
        if cursor is None:  # pragma: no cover -- adapters without cursor concept
            return
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self._fqn()} ("
            f"  model_name   VARCHAR PRIMARY KEY, "
            f"  last_run_at  DOUBLE, "
            f"  watermark    VARCHAR, "
            f"  row_count    BIGINT, "
            f"  fingerprint  VARCHAR"
            f")"
        )

    def get(self, model_name: str) -> ModelState | None:
        cursor = getattr(self.adapter, "_thread_cursor", lambda: None)()
        if cursor is None:
            return None
        row = cursor.execute(
            f"SELECT model_name, last_run_at, watermark, row_count, fingerprint "
            f"FROM {self._fqn()} WHERE model_name = ?",
            [model_name],
        ).fetchone()
        if not row:
            return None
        return ModelState(
            model_name=row[0],
            last_run_at=float(row[1]),
            watermark=row[2],
            row_count=int(row[3]) if row[3] is not None else None,
            fingerprint=row[4],
        )

    def upsert(self, state: ModelState) -> None:
        cursor = getattr(self.adapter, "_thread_cursor", lambda: None)()
        if cursor is None:
            return
        cursor.execute(
            f"INSERT OR REPLACE INTO {self._fqn()} "
            f"(model_name, last_run_at, watermark, row_count, fingerprint) "
            f"VALUES (?, ?, ?, ?, ?)",
            [state.model_name, state.last_run_at, state.watermark, state.row_count, state.fingerprint],
        )

    def list_all(self) -> list[ModelState]:
        cursor = getattr(self.adapter, "_thread_cursor", lambda: None)()
        if cursor is None:
            return []
        rows = cursor.execute(
            f"SELECT model_name, last_run_at, watermark, row_count, fingerprint FROM {self._fqn()}"
        ).fetchall()
        return [
            ModelState(
                model_name=r[0],
                last_run_at=float(r[1]),
                watermark=r[2],
                row_count=int(r[3]) if r[3] is not None else None,
                fingerprint=r[4],
            )
            for r in rows
        ]


def fingerprint(model: Model) -> str:
    """Hash model identity so we can detect when its body or config changes.

    The hash intentionally includes materialization and unique_key so that
    swapping ``table`` → ``incremental`` triggers a rebuild.
    """
    h = hashlib.sha256()
    h.update(model.name.encode())
    h.update(b"|")
    h.update(model.materialization.value.encode())
    h.update(b"|")
    h.update((model.unique_key or "").encode())
    h.update(b"|")
    if model.sql is not None:
        h.update(model.sql.encode())
    if model.python_callable is not None:
        import inspect

        try:
            src = inspect.getsource(model.python_callable)
        except (OSError, TypeError):
            src = getattr(model.python_callable, "__qualname__", "") or ""
        h.update(src.encode())
    return h.hexdigest()[:16]


def now() -> float:
    return time.time()


def make_state(model: Model, *, row_count: int | None, watermark: str | None = None) -> ModelState:
    return ModelState(
        model_name=model.name,
        last_run_at=now(),
        watermark=watermark,
        row_count=row_count,
        fingerprint=fingerprint(model),
    )
