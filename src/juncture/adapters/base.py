"""Adapter protocol: everything a backend must implement to run a model.

An adapter is a small interface (~7 methods). The DuckDB adapter is the
reference implementation; Snowflake / BigQuery adapters mirror the same API
and translate SQL via SQLGlot before execution when needed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.core.context import TransformContext
    from juncture.core.model import Materialization, Model


class AdapterError(Exception):
    """Raised when an adapter fails in a way that should abort the run."""


@dataclass(frozen=True, kw_only=True)
class MaterializationResult:
    """Summary of a materialized model run."""

    model_name: str
    materialization: Materialization
    fully_qualified: str
    row_count: int | None
    elapsed_seconds: float
    warnings: list[str]


class Adapter(ABC):
    """Minimum surface every backend must implement."""

    #: Short identifier used in connection configs (e.g. ``duckdb``).
    type_name: str = ""
    #: SQLGlot dialect used by the parser for this backend.
    dialect: str = ""

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def materialize_sql(
        self,
        model: Model,
        rendered_sql: str,
        *,
        schema: str,
    ) -> MaterializationResult:
        """Execute ``rendered_sql`` and persist results per ``model.materialization``."""

    @abstractmethod
    def materialize_python(
        self,
        model: Model,
        context: TransformContext,
        *,
        schema: str,
    ) -> MaterializationResult:
        """Execute the Python transform and persist its DataFrame output."""

    @abstractmethod
    def fetch_ref(self, name: str) -> Any:
        """Return a materialized model's content in a DataFrame-like form."""

    @abstractmethod
    def execute_arrow(self, query: str) -> Any:
        """Execute an ad-hoc query and return an Arrow Table-like object."""

    @abstractmethod
    def resolve(self, name: str, *, schema: str) -> str:
        """Return the fully qualified identifier for a model name."""

    def __enter__(self) -> Adapter:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
