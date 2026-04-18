"""Model: a single transformation (SQL or Python) with metadata."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class ModelKind(StrEnum):
    """Origin of the model code."""

    SQL = "sql"
    PYTHON = "python"
    SEED = "seed"  # CSV loaded from seeds/ before the DAG runs


class Materialization(StrEnum):
    """How the model result is persisted."""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"
    # Raw multi-statement SQL: the adapter runs the body as-is, splitting on
    # semicolons. Used when migrating a Snowflake transformation that already
    # contains its own CREATE OR REPLACE / INSERT DDL and we don't want to
    # rewrite it into a single SELECT (yet).
    EXECUTE = "execute"


@dataclass(frozen=True, kw_only=True)
class ColumnSpec:
    """Declared column with optional description and tests."""

    name: str
    description: str | None = None
    tests: list[str] = field(default_factory=list)
    data_type: str | None = None


@dataclass(kw_only=True)
class Model:
    """One transformation node in the DAG.

    SQL models are .sql files under models/. Python models are functions
    decorated with @transform() and discovered via module import.
    """

    name: str
    kind: ModelKind
    materialization: Materialization = Materialization.TABLE
    path: Path | None = None
    sql: str | None = None
    python_callable: Callable[..., Any] | None = None
    description: str | None = None
    depends_on: set[str] = field(default_factory=set)
    columns: list[ColumnSpec] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)

    unique_key: str | None = None
    partition_by: str | None = None
    cluster_by: list[str] | None = None
    schedule_cron: str | None = None

    #: Author-declared "don't run this model" flag from schema.yml.
    #: Distinct from skipped (upstream failed): disabled is an explicit
    #: opt-out that does not mark the run as failed. Downstream models
    #: receive status=skipped with reason=upstream_disabled so they can
    #: be visually distinguished from cascade-failure skips.
    disabled: bool = False

    # --- Governance surface (RFC 0001 §7, M4) --------------------------
    # Optional fields. A schema.yml with none of these renders the
    # Portfolio tab with empty columns; nothing else changes.
    #
    # Owner / team are freeform (email or handle). criticality is a
    # short string by convention ``tier-1`` / ``tier-2`` / ``tier-3``;
    # we do not validate the value so new tiers can be introduced
    # without a code change. sla.* are numeric targets used by the
    # Portfolio ``sort by SLA breach`` view and the 30-day dashboard.
    # consumers is a list of dicts ``{name, url?, team?}``; a plain
    # string in the YAML is accepted and auto-promoted to ``{name: s}``
    # so early adopters don't have to restructure on day one.
    # docs is the path to a long-form markdown file; if omitted the
    # Portfolio falls back to ``<model>.md`` next to the ``.sql`` file.
    owner: str | None = None
    team: str | None = None
    business_unit: str | None = None
    criticality: str | None = None
    sla_freshness_hours: float | None = None
    sla_success_rate_target: float | None = None
    docs: str | None = None
    consumers: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.kind is ModelKind.SQL and self.sql is None:
            raise ValueError(f"SQL model {self.name!r} requires non-empty sql")
        if self.kind is ModelKind.PYTHON and self.python_callable is None:
            raise ValueError(f"Python model {self.name!r} requires python_callable")
        # Seed models are pre-materialized by load_seeds(); they require no sql
        # or callable. They exist in the DAG purely so downstream models can
        # declare dependencies on them via ref().

    @property
    def fqn(self) -> str:
        """Fully qualified name used for logging and lineage."""
        return self.name
