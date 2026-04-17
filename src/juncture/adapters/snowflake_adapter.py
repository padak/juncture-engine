"""Snowflake adapter (v0.3).

This is an initial implementation that covers the common cases:

* TABLE / VIEW materialization via ``CREATE OR REPLACE``.
* INCREMENTAL via ``MERGE INTO`` on a declared ``unique_key``.
* ``fetch_ref`` streams rows as Arrow through Snowflake's Python connector.
* SQL translation at render time so users can author DuckDB-friendly SQL and
  have it run against Snowflake when the project is deployed.

The optional dependency is gated behind ``pip install 'juncture[snowflake]'``.

Status: **skeleton** — enough structure to register and instantiate, but full
test coverage requires a Snowflake account and therefore only runs in CI
jobs that provide credentials.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from juncture.adapters.base import Adapter, AdapterError, MaterializationResult
from juncture.adapters.registry import register_adapter
from juncture.core.model import Materialization
from juncture.parsers.sqlglot_parser import translate_sql

if TYPE_CHECKING:
    from juncture.core.context import TransformContext
    from juncture.core.model import Model

log = logging.getLogger(__name__)


class SnowflakeAdapter(Adapter):
    """Run models against Snowflake using snowflake-connector-python."""

    type_name = "snowflake"
    dialect = "snowflake"

    def __init__(
        self,
        *,
        account: str,
        user: str,
        password: str | None = None,
        database: str,
        warehouse: str,
        schema: str | None = None,
        role: str | None = None,
        private_key_path: str | None = None,
        **_: Any,
    ) -> None:
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.default_schema = schema
        self.role = role
        self.private_key_path = private_key_path
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import snowflake.connector
        except ImportError as exc:  # pragma: no cover -- optional dep
            raise AdapterError(
                "snowflake-connector-python is not installed. "
                "Install with `pip install 'juncture[snowflake]'`."
            ) from exc

        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "warehouse": self.warehouse,
        }
        if self.password:
            kwargs["password"] = self.password
        if self.role:
            kwargs["role"] = self.role
        if self.default_schema:
            kwargs["schema"] = self.default_schema
        if self.private_key_path:
            kwargs["private_key_file"] = self.private_key_path

        self._conn = snowflake.connector.connect(**kwargs)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def resolve(self, name: str, *, schema: str) -> str:
        return f"{self.database}.{schema}.{name}"

    def _ensure_schema(self, schema: str) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        finally:
            cur.close()

    def materialize_sql(
        self,
        model: Model,
        rendered_sql: str,
        *,
        schema: str,
    ) -> MaterializationResult:
        if model.sql is None:
            raise AdapterError(f"SQL model {model.name!r} has no SQL body")
        # Cross-dialect support: if the user wrote DuckDB-flavoured SQL, try
        # to translate to Snowflake. SQLGlot is best-effort; the user must
        # review output via `juncture compile --json` for production.
        translated = translate_sql(rendered_sql, read="duckdb", write="snowflake")
        self._ensure_schema(schema)

        fqn = self.resolve(model.name, schema=schema)
        stmt = _materialize(model.materialization, fqn, translated, model.unique_key)

        cur = self._conn.cursor()
        t0 = time.perf_counter()
        try:
            cur.execute(stmt)
            elapsed = time.perf_counter() - t0
            if model.materialization in (Materialization.TABLE, Materialization.INCREMENTAL):
                row_count = cur.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
            else:
                row_count = None
        finally:
            cur.close()

        return MaterializationResult(
            model_name=model.name,
            materialization=model.materialization,
            fully_qualified=fqn,
            row_count=int(row_count) if row_count is not None else None,
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def materialize_python(
        self,
        model: Model,
        context: TransformContext,
        *,
        schema: str,
    ) -> MaterializationResult:
        # Python models on Snowflake: call the function, get a DataFrame,
        # write via snowflake.connector.pandas_tools.write_pandas.
        if model.python_callable is None:
            raise AdapterError(f"Python model {model.name!r} has no callable")
        try:
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as exc:  # pragma: no cover
            raise AdapterError(
                "write_pandas requires pyarrow; install `pip install 'juncture[snowflake,pandas]'`"
            ) from exc

        self._ensure_schema(schema)
        fqn = self.resolve(model.name, schema=schema)

        t0 = time.perf_counter()
        df = model.python_callable(context)
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
        success, _, nrows, _ = write_pandas(
            conn=self._conn,
            df=df,
            table_name=model.name,
            database=self.database,
            schema=schema,
            auto_create_table=True,
            overwrite=(model.materialization is Materialization.TABLE),
        )
        elapsed = time.perf_counter() - t0
        if not success:
            raise AdapterError(f"write_pandas failed for model {model.name!r}")

        return MaterializationResult(
            model_name=model.name,
            materialization=model.materialization,
            fully_qualified=fqn,
            row_count=int(nrows),
            elapsed_seconds=elapsed,
            warnings=[],
        )

    def fetch_ref(self, name: str) -> Any:
        cur = self._conn.cursor()
        try:
            cur.execute(f"SELECT * FROM {name}")
            return cur.fetch_arrow_all()
        finally:
            cur.close()

    def execute_arrow(self, query: str) -> Any:
        cur = self._conn.cursor()
        try:
            cur.execute(query)
            return cur.fetch_arrow_all()
        finally:
            cur.close()


def _materialize(
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
            raise AdapterError(f"Incremental materialization on Snowflake for {fqn} requires unique_key")
        return (
            f"MERGE INTO {fqn} AS tgt "
            f"USING ({stripped}) AS src "
            f"ON tgt.{unique_key} = src.{unique_key} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
    if materialization is Materialization.EPHEMERAL:
        return f"CREATE OR REPLACE VIEW {fqn} AS ({stripped})"
    raise AdapterError(f"Unsupported materialization: {materialization}")


register_adapter("snowflake", SnowflakeAdapter)
