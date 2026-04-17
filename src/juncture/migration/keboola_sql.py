"""Migrate a Keboola SQL transformation into a Juncture project.

Input: a Keboola transformation config JSON (as exported via the storage
API or found in the configuration endpoint). The typical shape::

    {
      "storage": {
        "input":  { "tables": [{"source": "in.c-bucket.orders", "destination": "orders"}, ...] },
        "output": { "tables": [{"source": "daily_revenue", "destination": "out.c-marts.daily_revenue"}] }
      },
      "parameters": {
        "blocks": [
          {
            "name": "staging",
            "codes": [
              { "name": "stg_orders", "script": ["SELECT * FROM orders WHERE ..."] }
            ]
          }
        ]
      }
    }

Output: a directory with ``juncture.yaml``, ``models/<block>/<code>.sql``,
and ``schema.yml`` capturing the input→output mapping as descriptions.

Multi-line ``script`` arrays are joined with blank lines.

This is a best-effort migration. We make several opinionated decisions:

* Each ``codes[].name`` becomes a Juncture model name. If the SQL body
  references a table whose name matches an input mapping destination,
  we rewrite it to ``{{ ref('that_name') }}``.
* Output mapping destinations are captured in ``schema.yml`` descriptions
  so a Keboola wrapper can later re-read them to upload the results.
* If the same code name appears in multiple blocks, we prefix with the
  block name (e.g. ``staging__stg_orders``).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class MigrationResult:
    project_path: Path
    models: list[str]
    input_map: dict[str, str]
    output_map: dict[str, str]
    warnings: list[str]


def migrate_keboola_sql_transformation(
    config_json_path: str | Path,
    target_dir: str | Path,
    *,
    backend: str = "duckdb",
    duckdb_path: str = "data/migrated.duckdb",
) -> MigrationResult:
    """Convert a Keboola SQL transformation config to a Juncture project."""
    config_path = Path(config_json_path)
    target = Path(target_dir)
    raw = json.loads(config_path.read_text())

    storage = raw.get("storage", {}) or {}
    params = raw.get("parameters", {}) or {}
    blocks = params.get("blocks", []) or []

    input_map = {t["destination"]: t["source"] for t in storage.get("input", {}).get("tables", [])}
    output_map = {t["source"]: t["destination"] for t in storage.get("output", {}).get("tables", [])}

    target.mkdir(parents=True, exist_ok=True)
    (target / "models").mkdir(exist_ok=True)

    # Write juncture.yaml
    (target / "juncture.yaml").write_text(f"""name: migrated_from_keboola
version: 0.1.0
profile: local
default_schema: main

connections:
  local:
    type: {backend}
    path: {duckdb_path}
    threads: 4
""")

    warnings: list[str] = []
    model_names: list[str] = []
    code_occurrences: dict[str, int] = {}

    for block in blocks:
        block_name = _slug(block.get("name", "block"))
        for code in block.get("codes", []) or []:
            name = _slug(code.get("name", "code"))
            # Deduplicate across blocks.
            if name in code_occurrences:
                name = f"{block_name}__{name}"
            code_occurrences[name] = 1

            script = code.get("script", "")
            sql_body = "\n\n".join(script) if isinstance(script, list) else str(script)
            rewritten, refs_found = _rewrite_refs(sql_body, set(input_map.keys()))
            if refs_found:
                log.info("Model %s: rewrote %d reference(s) to ref()", name, len(refs_found))

            (target / "models" / f"{name}.sql").write_text(rewritten.strip() + "\n")
            model_names.append(name)

    # Write a schema.yml capturing I/O mapping so users (and wrappers)
    # can trace back to Keboola Storage.
    schema_path = target / "models" / "schema.yml"
    schema_payload = {
        "models": [
            {
                "name": name,
                "description": _describe_mapping(name, input_map, output_map),
            }
            for name in model_names
        ]
    }
    schema_path.write_text(_dump_yaml(schema_payload))

    # Write a README recording the migration
    (target / "README.md").write_text(f"""# Migrated Keboola SQL transformation

Source config: `{config_path.name}`

## Input mapping
{_table_md(input_map, "Local name", "Keboola source")}

## Output mapping
{_table_md(output_map, "Local name", "Keboola destination")}

## Next steps
1. Review each model under `models/`. ref() rewrites are best-effort.
2. Run `juncture compile` to confirm the DAG builds.
3. Run `juncture run --test` locally with DuckDB.
4. When ready, deploy via the Keboola Juncture wrapper.
""")

    return MigrationResult(
        project_path=target,
        models=model_names,
        input_map=input_map,
        output_map=output_map,
        warnings=warnings,
    )


def _slug(raw: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", raw.strip().lower())
    return s.strip("_") or "unnamed"


def _rewrite_refs(sql: str, known_tables: set[str]) -> tuple[str, set[str]]:
    """Replace bare references to ``known_tables`` with ``{{ ref('name') }}``.

    This uses a simple word-boundary regex per table name; it does not try to
    be fully SQL-aware. For production migrations we'd use SQLGlot to rewrite
    table tokens only.
    """
    hits: set[str] = set()
    result = sql
    # Sort longest-first so "stg_orders" is replaced before "stg".
    for name in sorted(known_tables, key=len, reverse=True):
        pattern = re.compile(rf"\b{re.escape(name)}\b")
        if pattern.search(result):
            hits.add(name)
            result = pattern.sub(f"{{{{ ref('{name}') }}}}", result)
    return result, hits


def _describe_mapping(name: str, input_map: dict[str, str], output_map: dict[str, str]) -> str:
    parts = []
    if name in input_map:
        parts.append(f"Source: Keboola {input_map[name]}")
    if name in output_map:
        parts.append(f"Output: Keboola {output_map[name]}")
    return "; ".join(parts) or "Migrated from Keboola SQL transformation."


def _table_md(mapping: dict[str, str], left: str, right: str) -> str:
    if not mapping:
        return "_(none)_\n"
    lines = [f"| {left} | {right} |", "|---|---|"]
    for k, v in mapping.items():
        lines.append(f"| `{k}` | `{v}` |")
    return "\n".join(lines) + "\n"


def _dump_yaml(data: Any) -> str:
    import yaml

    return yaml.safe_dump(data, sort_keys=False)
