"""DuckDB adapter: zero-dependency local execution + SQL materialization.

This is the reference adapter. It:

* Opens a DuckDB database file (or in-memory).
* Materializes SQL models as ``CREATE OR REPLACE TABLE`` or ``VIEW``.
* Materializes Python models by calling the function, converting the return
  value to an Arrow Table, and registering it as a DuckDB relation before
  persisting.
* Implements ``fetch_ref`` by selecting from the materialized object as an
  Arrow Table (which pandas and Polars both consume cheaply).
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import networkx as nx

from juncture.adapters.base import Adapter, AdapterError, MaterializationResult
from juncture.adapters.registry import register_adapter
from juncture.core.model import Materialization
from juncture.parsers.sqlglot_parser import (
    build_statement_dag,
    split_statements,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from juncture.core.context import TransformContext
    from juncture.core.model import Model


class DuckDBAdapter(Adapter):
    """Reference adapter using an in-process DuckDB engine."""

    type_name = "duckdb"
    dialect = "duckdb"

    def __init__(
        self,
        *,
        path: str | Path = ":memory:",
        threads: int | None = None,
        extensions: list[str] | None = None,
        memory_limit: str | None = None,
        temp_directory: str | None = None,
        **_: Any,
    ) -> None:
        self.path = str(path) if path != ":memory:" else ":memory:"
        self.threads = threads
        self.extensions = extensions or []
        self.memory_limit = memory_limit
        self.temp_directory = temp_directory
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise AdapterError("DuckDB connection is not open; call connect() first")
        return self._conn

    def connect(self) -> None:
        self._conn = duckdb.connect(database=self.path, read_only=False)
        if self.threads:
            self._conn.execute(f"PRAGMA threads = {int(self.threads)}")
        if self.memory_limit:
            self._conn.execute(f"SET memory_limit = '{self.memory_limit}'")
        if self.temp_directory:
            self._conn.execute(f"SET temp_directory = '{self.temp_directory}'")
        for ext in self.extensions:
            self._conn.execute(f"INSTALL {ext}; LOAD {ext};")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def resolve(self, name: str, *, schema: str) -> str:
        # Quote both schema and name so identifiers can contain dots or
        # hyphens ("in.c-db.carts" from migrated Snowflake projects).
        return f'"{schema}"."{name}"'

    def _ensure_schema(self, schema: str) -> None:
        self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    def _thread_cursor(self) -> duckdb.DuckDBPyConnection:
        """Return a fresh DuckDB cursor.

        DuckDB connections are *not* thread-safe for concurrent writes, but
        cursors obtained via ``connection.cursor()`` share the database and can
        be used from different threads safely.
        """
        return self.conn.cursor()

    def materialize_sql(
        self,
        model: Model,
        rendered_sql: str,
        *,
        schema: str,
    ) -> MaterializationResult:
        if model.sql is None:
            raise AdapterError(f"SQL model {model.name!r} has no SQL body")
        self._ensure_schema(schema)

        # EXECUTE materialization: run the SQL as-is, splitting on semicolons.
        # Used for migrated multi-statement transformations.
        if model.materialization is Materialization.EXECUTE:
            return self._execute_raw(model, rendered_sql, schema=schema)

        fqn = self.resolve(model.name, schema=schema)
        stmt = _build_materialization_statement(
            materialization=model.materialization,
            fqn=fqn,
            select_sql=rendered_sql,
            unique_key=model.unique_key,
        )
        cursor = self._thread_cursor()
        t0 = time.perf_counter()
        cursor.execute(stmt)
        elapsed = time.perf_counter() - t0

        row_count: int | None
        if model.materialization in (Materialization.TABLE, Materialization.INCREMENTAL):
            count_row = cursor.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
            row_count = int(count_row[0]) if count_row else None
        else:
            row_count = None

        return MaterializationResult(
            model_name=model.name,
            materialization=model.materialization,
            fully_qualified=fqn,
            row_count=row_count,
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def _execute_raw(self, model: Model, rendered_sql: str, *, schema: str) -> MaterializationResult:
        """Run ``rendered_sql`` as a sequence of statements separated by ``;``.

        Defaults to sequential execution. When ``model.config["parallelism"]``
        is an integer greater than 1, the body is parsed into an intra-script
        dependency DAG (see :func:`build_statement_dag`) and each topological
        layer is fanned out over a :class:`ThreadPoolExecutor` of that width.

        Row counts would require inspecting each DDL/DML statement; instead
        the adapter reports the number of statements executed as ``row_count``.
        """
        parallelism = _coerce_parallelism(model.config.get("parallelism"))
        if parallelism > 1:
            return self._execute_raw_parallel(
                model, rendered_sql, schema=schema, parallelism=parallelism
            )

        cursor = self._thread_cursor()
        # DuckDB allows `USE schema;` to set the default search path; with
        # quoted schema this keeps bare identifiers from the migrated SQL
        # resolving against the project's target schema.
        cursor.execute(f'USE "{schema}"')

        statements = split_statements(rendered_sql)
        t0 = time.perf_counter()
        for stmt in statements:
            if not stmt.strip():
                continue
            cursor.execute(stmt)
        elapsed = time.perf_counter() - t0

        fqn = self.resolve(model.name, schema=schema)
        return MaterializationResult(
            model_name=model.name,
            materialization=Materialization.EXECUTE,
            fully_qualified=fqn,
            row_count=len(statements),
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def _execute_raw_parallel(
        self,
        model: Model,
        rendered_sql: str,
        *,
        schema: str,
        parallelism: int,
    ) -> MaterializationResult:
        """Parallel variant of :meth:`_execute_raw`.

        Builds the intra-script DAG, then walks
        :func:`networkx.topological_generations` layer by layer. Each layer's
        statements are submitted to a shared ``ThreadPoolExecutor``; the layer
        completes only when every statement in it has finished (or one fails,
        in which case the error is re-raised with layer + statement context).

        Per-layer elapsed time is logged at INFO so users can see where
        parallelism pays off vs. where DuckDB's catalog lock / intra-query
        thread scheduler serialises things.

        Every worker call acquires its own cursor via :meth:`_thread_cursor`
        and issues ``USE "<schema>"`` before the statement — DuckDB cursors
        do not inherit the parent connection's current schema.
        """
        graph = build_statement_dag(rendered_sql, dialect=self.dialect)
        total = graph.number_of_nodes()
        if total == 0:
            return self._empty_execute_result(model, schema=schema)

        layers = list(nx.topological_generations(graph))
        log.info(
            "EXECUTE parallel: %s — %d statements, %d layers, parallelism=%d",
            model.name,
            total,
            len(layers),
            parallelism,
        )

        def _run_one(idx: int) -> None:
            node = graph.nodes[idx]["node"]
            cursor = self._thread_cursor()
            cursor.execute(f'USE "{schema}"')
            cursor.execute(node.sql)

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            for layer_i, layer in enumerate(layers):
                t_layer = time.perf_counter()
                future_to_idx = {pool.submit(_run_one, idx): idx for idx in layer}
                try:
                    for fut in as_completed(future_to_idx):
                        fut.result()
                except Exception as exc:
                    # Cancel any still-queued futures so we don't keep pushing
                    # statements against a database that already errored.
                    for pending in future_to_idx:
                        pending.cancel()
                    failed_idx = future_to_idx[fut]
                    failed_node = graph.nodes[failed_idx]["node"]
                    raise AdapterError(
                        f"EXECUTE parallel failed in layer {layer_i} "
                        f"(statement #{failed_idx}, output={failed_node.output!r}): {exc}"
                    ) from exc
                log.info(
                    "  layer %d/%d: %d statements, %.2fs",
                    layer_i + 1,
                    len(layers),
                    len(layer),
                    time.perf_counter() - t_layer,
                )
        elapsed = time.perf_counter() - t0

        fqn = self.resolve(model.name, schema=schema)
        return MaterializationResult(
            model_name=model.name,
            materialization=Materialization.EXECUTE,
            fully_qualified=fqn,
            row_count=total,
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def _empty_execute_result(self, model: Model, *, schema: str) -> MaterializationResult:
        return MaterializationResult(
            model_name=model.name,
            materialization=Materialization.EXECUTE,
            fully_qualified=self.resolve(model.name, schema=schema),
            row_count=0,
            elapsed_seconds=0.0,
            warnings=[],
        )

    def materialize_python(
        self,
        model: Model,
        context: TransformContext,
        *,
        schema: str,
    ) -> MaterializationResult:
        if model.python_callable is None:
            raise AdapterError(f"Python model {model.name!r} has no callable")

        self._ensure_schema(schema)
        fqn = self.resolve(model.name, schema=schema)

        t0 = time.perf_counter()
        df = model.python_callable(context)
        arrow = _coerce_to_arrow(df)
        elapsed = time.perf_counter() - t0

        cursor = self._thread_cursor()
        temp_name = f"_juncture_tmp_{model.name}"
        cursor.register(temp_name, arrow)
        try:
            stmt = _build_materialization_statement(
                materialization=model.materialization,
                fqn=fqn,
                select_sql=f"SELECT * FROM {temp_name}",
                unique_key=model.unique_key,
            )
            cursor.execute(stmt)
        finally:
            cursor.unregister(temp_name)

        count_row = cursor.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
        row_count = int(count_row[0]) if count_row else 0
        return MaterializationResult(
            model_name=model.name,
            materialization=model.materialization,
            fully_qualified=fqn,
            row_count=row_count,
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def fetch_ref(self, name: str) -> Any:
        return self._thread_cursor().execute(f"SELECT * FROM {name}").to_arrow_table()

    def execute_arrow(self, query: str) -> Any:
        return self._thread_cursor().execute(query).to_arrow_table()


def _coerce_to_arrow(df: Any) -> Any:
    """Accept pandas, polars or Arrow; return an Arrow Table DuckDB can register."""
    if df is None:
        raise AdapterError("Python model returned None; it must return a DataFrame")
    if hasattr(df, "to_arrow"):  # polars.DataFrame
        return df.to_arrow()
    if hasattr(df, "to_pandas") and hasattr(df, "schema"):  # already Arrow Table
        return df
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover
        raise AdapterError(
            "pandas model output requires pyarrow; install with `pip install 'juncture[pandas]'`"
        ) from exc
    return pa.Table.from_pandas(df)


def _build_materialization_statement(
    *,
    materialization: Materialization,
    fqn: str,
    select_sql: str,
    unique_key: str | None,
) -> str:
    stripped = select_sql.rstrip(";").strip()
    if materialization is Materialization.TABLE:
        return f"CREATE OR REPLACE TABLE {fqn} AS ({stripped})"
    if materialization is Materialization.VIEW:
        return f"CREATE OR REPLACE VIEW {fqn} AS ({stripped})"
    if materialization is Materialization.INCREMENTAL:
        if not unique_key:
            raise AdapterError(f"Incremental materialization for {fqn} requires a `unique_key` to merge on")
        # INSERT OR REPLACE INTO preserves existing rows and overwrites by key.
        create = f"CREATE TABLE IF NOT EXISTS {fqn} AS ({stripped} LIMIT 0)"
        upsert = f"INSERT OR REPLACE INTO {fqn} BY NAME SELECT * FROM ({stripped})"
        return f"{create}; {upsert}"
    if materialization is Materialization.EPHEMERAL:
        # Ephemeral models are inlined upstream; we still create a view so that
        # downstream rendering works. Proper inlining happens in the executor.
        return f"CREATE OR REPLACE VIEW {fqn} AS ({stripped})"
    raise AdapterError(f"Unsupported materialization: {materialization}")


def _coerce_parallelism(raw: Any) -> int:
    """Normalise ``config["parallelism"]`` into a positive integer.

    Accepts ``None`` / missing (returns 1, = sequential), an int literal,
    or a string that parses as an int. Anything else raises ``AdapterError``
    so a typo like ``parallelism: "four"`` fails fast rather than silently
    degrading to sequential.
    """
    if raw is None:
        return 1
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise AdapterError(
            f"Invalid parallelism value {raw!r}: expected a positive integer"
        ) from exc
    if value < 1:
        raise AdapterError(f"parallelism must be >= 1 (got {value})")
    return value


register_adapter("duckdb", DuckDBAdapter)
