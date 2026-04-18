"""Run every statement of a Juncture EXECUTE model against an existing DuckDB,
collecting all errors without stopping on the first.

Intended for triaging a migrated Snowflake transformation: we want the full
list of DuckDB rejections in one pass so we can hand them to a code-fix agent.

Usage:
    python scripts/collect_errors.py \
        --db data/juncture.duckdb \
        --sql models/main_task.sql \
        --output errors.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

from juncture.parsers.sqlglot_parser import split_statements


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--sql", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--schema", default="main")
    args = p.parse_args()

    sql_body = Path(args.sql).read_text()
    statements = split_statements(sql_body)
    print(f"Parsed {len(statements)} statements from {args.sql}", file=sys.stderr)

    con = duckdb.connect(args.db)
    con.execute(f'USE "{args.schema}"')

    errors: list[dict] = []
    successes = 0
    for i, stmt in enumerate(statements, start=1):
        if not stmt.strip():
            continue
        try:
            con.execute(stmt)
            successes += 1
        except Exception as exc:
            errors.append(
                {
                    "index": i,
                    "error_class": type(exc).__name__,
                    "error_message": str(exc),
                    "statement_preview": stmt[:400],
                    "statement_length": len(stmt),
                    "full_statement": stmt,
                }
            )
            print(f"[{i:>3}/{len(statements)}] FAILED: {type(exc).__name__}: {str(exc).splitlines()[0][:180]}", file=sys.stderr)

    summary = {
        "total_statements": len(statements),
        "succeeded": successes,
        "failed": len(errors),
        "errors": errors,
    }
    Path(args.output).write_text(json.dumps(summary, indent=2, default=str))
    print(f"\n{successes}/{len(statements)} succeeded, {len(errors)} failed -> {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
