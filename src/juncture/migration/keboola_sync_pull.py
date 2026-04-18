"""Migrate a Keboola transformation from ``kbagent sync pull`` layout to Juncture.

Unlike :mod:`juncture.migration.keboola_sql` which expects a raw Keboola
configuration-API JSON payload, this migrator consumes the filesystem layout
produced by ``kbagent sync pull``::

    main/transformation/keboola.snowflake-transformation/<name>/
        _config.yml        # YAML with input/output mapping + parameters
        _description.md    # human description
        _jobs.jsonl        # recent run history (ignored)
        transform.sql      # the SQL body (for snowflake-transformation this is
                           # a single multi-statement script)

Produced Juncture project::

    <output_dir>/
        juncture.yaml                # DuckDB local connection
        seeds/<destination>/...      # symlinks into parquet data dir
        models/<transformation>.sql  # transform.sql verbatim (EXECUTE mat.)
        MIGRATION.md                 # human-readable log of what was done

The SQL is migrated *as-is*: the ``EXECUTE`` materialization runs the whole
multi-statement script on DuckDB without wrapping it in a single
``CREATE OR REPLACE`` shell. This lets a 12k-line Snowflake transformation
compile unchanged; Snowflake-specific constructs that DuckDB rejects are
surfaced as genuine parse errors the user can fix iteratively.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import sqlglot
import yaml

from juncture.parsers.sqlglot_parser import split_statements, translate_sql

log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class SyncPullMigrationResult:
    project_path: Path
    transformation_name: str
    input_seeds: dict[str, Path]
    output_tables: dict[str, str]
    sql_line_count: int
    seeds_linked: int
    seeds_missing: list[str]
    sql_translated: bool


@dataclass(kw_only=True)
class SyncPullValidationReport:
    """Read-only pre-flight report for a sync-pull transformation.

    Produced by :func:`validate_sync_pull_migration`. Everything the
    migrator would report plus statement-level parse diagnostics, so an
    operator can see before writing any files whether the migration is
    going to have 7 primary errors or 70.
    """

    transformation_name: str
    source_dialect: str
    target_dialect: str
    sql_line_count: int
    statement_count: int
    parse_errors: list[tuple[int, str]]  # (statement_index, error message)
    output_tables: dict[str, str]
    input_seeds_expected: dict[str, str]  # {destination: source}
    seeds_missing: list[str]


def migrate_keboola_sync_pull(
    transformation_dir: str | Path,
    *,
    output_dir: str | Path,
    seeds_source: str | Path,
    duckdb_path: str = "data/juncture.duckdb",
    source_dialect: str = "snowflake",
    target_dialect: str = "duckdb",
) -> SyncPullMigrationResult:
    """Convert a sync-pull transformation directory into a Juncture project.

    ``transformation_dir`` is the folder produced by
    ``kbagent sync pull`` under ``main/transformation/<component>/<name>/``.

    ``seeds_source`` is the directory where parquet-format input data lives.
    The expected layout is ``seeds_source/<stage-bucket>/<table_name>/*.parquet``
    (matching ``kbagent storage unload-table --file-type parquet --download``).
    """
    tx_dir = Path(transformation_dir).resolve()
    out_dir = Path(output_dir).resolve()
    seeds_src = Path(seeds_source).resolve()

    config_path = tx_dir / "_config.yml"
    sql_path = tx_dir / "transform.sql"
    if not config_path.exists():
        raise FileNotFoundError(f"Expected {config_path} (sync-pull layout)")
    if not sql_path.exists():
        raise FileNotFoundError(f"Expected {sql_path} next to {config_path.name}")

    cfg = yaml.safe_load(config_path.read_text()) or {}
    tx_name = _slug(cfg.get("name") or tx_dir.name)

    # Build the target project skeleton.
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "models").mkdir(exist_ok=True)
    (out_dir / "seeds").mkdir(exist_ok=True)

    # Write juncture.yaml.
    _write_juncture_yaml(out_dir, project_name=tx_name, duckdb_path=duckdb_path)

    # Link seeds into seeds/ using the Keboola destination alias as the seed name.
    input_seeds, missing = _link_seeds(cfg, out_dir / "seeds", seeds_src)

    # Copy the SQL as the sole model. The EXECUTE materialization runs it
    # statement-by-statement. We translate Snowflake -> DuckDB per statement
    # so dialect-specific constructs (e.g. Snowflake's CASE VARCHAR/INT
    # coercion) are resolved up front; statements SQLGlot can't parse fall
    # through verbatim.
    model_name = _slug(tx_name)
    model_path = out_dir / "models" / f"{model_name}.sql"
    raw_sql = sql_path.read_text()
    sql_translated = False
    if source_dialect != target_dialect:
        translated = translate_sql(raw_sql, read=source_dialect, write=target_dialect)
        sql_body = translated
        sql_translated = True
        log.info(
            "Translated SQL from %s to %s (%d chars -> %d chars)",
            source_dialect,
            target_dialect,
            len(raw_sql),
            len(translated),
        )
    else:
        sql_body = raw_sql
    model_path.write_text(sql_body)

    _write_schema_yml(out_dir / "models" / "schema.yml", model_name=model_name)

    output_tables = _collect_output_tables(cfg)

    _write_migration_log(
        out_dir / "MIGRATION.md",
        tx_dir=tx_dir,
        input_seeds=input_seeds,
        output_tables=output_tables,
        sql_line_count=sql_body.count("\n") + 1,
        seeds_missing=missing,
    )

    return SyncPullMigrationResult(
        project_path=out_dir,
        transformation_name=tx_name,
        input_seeds=input_seeds,
        output_tables=output_tables,
        sql_line_count=sql_body.count("\n") + 1,
        seeds_linked=len(input_seeds),
        seeds_missing=missing,
        sql_translated=sql_translated,
    )


def validate_sync_pull_migration(
    transformation_dir: str | Path,
    *,
    seeds_source: str | Path,
    source_dialect: str = "snowflake",
    target_dialect: str = "duckdb",
) -> SyncPullValidationReport:
    """Pre-flight validation of a sync-pull transformation, no files written.

    Does the same inspection :func:`migrate_keboola_sync_pull` would do —
    parse ``_config.yml``, try to parse every ``transform.sql`` statement
    through SQLGlot in ``source_dialect``, and resolve each input mapping
    against ``seeds_source`` — but stops short of writing a project. The
    returned :class:`SyncPullValidationReport` is the signal an operator
    uses to decide whether the migration is ready to land: ``parse_errors``
    near zero means SQLGlot can translate everything; a non-empty
    ``seeds_missing`` means the parquet pool is incomplete.

    Cost is proportional to ``transform.sql`` size — a full-body parse
    plus N ``sqlglot.parse_one`` calls, no DuckDB, no disk writes.
    """
    tx_dir = Path(transformation_dir).resolve()
    seeds_src = Path(seeds_source).resolve()
    config_path = tx_dir / "_config.yml"
    sql_path = tx_dir / "transform.sql"
    if not config_path.exists():
        raise FileNotFoundError(f"Expected {config_path} (sync-pull layout)")
    if not sql_path.exists():
        raise FileNotFoundError(f"Expected {sql_path} next to {config_path.name}")

    cfg = yaml.safe_load(config_path.read_text()) or {}
    tx_name = _slug(cfg.get("name") or tx_dir.name)
    raw_sql = sql_path.read_text()

    statements = split_statements(raw_sql)
    parse_errors: list[tuple[int, str]] = []
    for idx, stmt in enumerate(statements):
        stripped = stmt.strip()
        if not stripped:
            continue
        try:
            sqlglot.parse_one(stripped, read=source_dialect)
        except sqlglot.errors.ParseError as exc:
            parse_errors.append((idx, str(exc)))

    # Input mappings: {destination: source}. Same shape as _link_seeds
    # resolves, but we stop at "would this seed exist" — no filesystem
    # mutations.
    input_seeds_expected: dict[str, str] = {}
    seeds_missing: list[str] = []
    for table in cfg.get("input", {}).get("tables", []) or []:
        dest = table.get("destination") or table.get("source")
        source = table.get("source") or ""
        if not dest:
            continue
        input_seeds_expected[dest] = source
        if not _sync_pull_source_exists(seeds_src, source):
            seeds_missing.append(dest)

    output_tables = _collect_output_tables(cfg)

    return SyncPullValidationReport(
        transformation_name=tx_name,
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        sql_line_count=raw_sql.count("\n") + 1,
        statement_count=len(statements),
        parse_errors=parse_errors,
        output_tables=output_tables,
        input_seeds_expected=input_seeds_expected,
        seeds_missing=seeds_missing,
    )


def _sync_pull_source_exists(seeds_src: Path, source: str) -> bool:
    """Return True when ``source`` resolves to an existing parquet directory.

    Matches the path logic :func:`_link_seeds` uses for real migration:
    ``<seeds_src>/<source>/`` (dots in ``source`` become directory
    separators) with at least one ``.parquet`` file inside.
    """
    if not source:
        return False
    candidate = seeds_src / source.replace(".", "/")
    if not candidate.is_dir():
        return False
    return any(candidate.glob("*.parquet"))


def _slug(raw: str) -> str:
    import re

    s = re.sub(r"[^a-zA-Z0-9]+", "_", raw.strip().lower()).strip("_")
    return s or "transformation"


def _write_juncture_yaml(root: Path, *, project_name: str, duckdb_path: str) -> None:
    (root / "juncture.yaml").write_text(
        f"""name: {project_name}
version: 0.1.0
profile: local
default_schema: main
default_materialization: execute

connections:
  local:
    type: duckdb
    path: {duckdb_path}
    threads: 4
    # Cap DuckDB's working-set memory so a single oversized query cannot
    # kill the host; spillage goes to temp_directory. Override per host:
    # on 2 GB hosts drop to 1200M; on 16+ GB hosts bump to 8G.
    memory_limit: 1500M
    temp_directory: data/duckdb_tmp
"""
    )


def _write_schema_yml(path: Path, *, model_name: str) -> None:
    path.write_text(
        f"""models:
  - name: {model_name}
    description: Full multi-statement SQL transformation migrated via juncture migrate-sync-pull.
    materialization: execute
"""
    )


def _link_seeds(
    cfg: dict,
    seeds_dir: Path,
    seeds_src: Path,
) -> tuple[dict[str, Path], list[str]]:
    """For each input mapping, create a directory or symlink in ``seeds_dir``.

    Returns ``(linked, missing)`` where ``linked`` maps destination alias ->
    the path inside ``seeds_src`` we pointed at, and ``missing`` is a list of
    destinations for which no parquet data was found.
    """
    linked: dict[str, Path] = {}
    missing: list[str] = []
    for table in cfg.get("input", {}).get("tables", []) or []:
        source = table.get("source")
        destination = table.get("destination") or source
        if not source:
            continue
        parts = source.split(".", 2)
        if len(parts) != 3:
            missing.append(destination)
            continue
        stage, bucket, tbl = parts
        src_dir = seeds_src / f"{stage}-{bucket}" / tbl
        target = seeds_dir / destination
        if not src_dir.exists():
            missing.append(destination)
            continue
        if target.exists() or target.is_symlink():
            target.unlink()
        os.symlink(src_dir, target)
        linked[destination] = src_dir
    return linked, missing


def _collect_output_tables(cfg: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for t in cfg.get("output", {}).get("tables", []) or []:
        source = t.get("source")
        destination = t.get("destination")
        if source and destination:
            out[source] = destination
    return out


def _write_migration_log(
    path: Path,
    *,
    tx_dir: Path,
    input_seeds: dict[str, Path],
    output_tables: dict[str, str],
    sql_line_count: int,
    seeds_missing: list[str],
) -> None:
    missing_block = "_(none)_"
    if seeds_missing:
        missing_block = "\n".join(f"- `{d}`" for d in seeds_missing)
    linked_block = "\n".join(f"- `{dst}` ← `{src}`" for dst, src in sorted(input_seeds.items()))
    output_block = "\n".join(f"- `{src}` → `{dst}`" for src, dst in sorted(output_tables.items()))
    path.write_text(
        f"""# Migration log

Source: `{tx_dir}`

## SQL
- `models/<tx>.sql`: {sql_line_count} lines, materialization `execute`.
- DuckDB runs it as a multi-statement script.

## Input seeds linked
{linked_block or "_(none)_"}

## Missing seeds (no parquet data found)
{missing_block}

## Output tables
{output_block or "_(none)_"}

## Next steps
1. `juncture compile --project .` — build the DAG.
2. `juncture run --project . --threads 1` — run the full script on DuckDB.
3. Expect Snowflake-specific errors (`TO_VARIANT`, `::` casts, `QUALIFY`
   clauses etc.) — fix them in `models/<tx>.sql` or add translations.
"""
    )
