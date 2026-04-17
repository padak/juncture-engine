"""Analyze an EXECUTE-materialized script: intra-script dependency DAG.

Why this exists
---------------
The ``execute`` materialization runs a multi-statement SQL script as a single
black-box node in the Juncture DAG. ``juncture compile`` therefore shows it
with ``Depends on: —`` and no intra-script structure, even though the script
itself has heavy internal dependencies (CREATE TABLE X AS SELECT ... FROM Y).

This script reconstructs that hidden structure:

1. Split the script on top-level semicolons (``split_statements``).
2. For each statement, detect the *output* table (``CREATE [OR REPLACE] TABLE``,
   ``CREATE VIEW``, ``INSERT INTO``) and the *input* tables via SQLGlot
   (``extract_table_references``).
3. Build a ``networkx`` DiGraph where an edge goes from the statement that
   last produced a table to every later statement that reads it.
4. Print topological layers — each layer is a set of statements that could
   in principle run in parallel given only intra-script constraints.

Usage
-----

    .venv/bin/python scripts/analyze_execute.py models/slevomat_main_task.sql

Options:
    --dot OUT.dot       Also write a Graphviz DOT file for visualization.
    --limit N           Truncate SQL headline in fan-out report to N chars
                        (default 80).
    --layers            Print one line per statement per layer (verbose).

Caveats
-------
* ``extract_table_references`` skips fully-qualified names (``schema.table``)
  — reading ``analytics.orders`` will NOT be picked up. For Slevomat this is
  fine because seeds are single-part names.
* Only ``CREATE TABLE/VIEW`` and ``INSERT INTO`` are treated as producers.
  ``UPDATE`` / ``DELETE`` / ``MERGE`` into an existing table are not wired
  as producers — they create an edge on the read side but don't become a
  "new version" node. Good enough for a first-pass estimate of parallelism.
* Edges are statement-index based, so the first occurrence of a table as an
  output is what downstream readers depend on. If the same table is written
  twice, only the latest producer-before-read wires the edge.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import networkx as nx

from juncture.parsers.sqlglot_parser import extract_table_references

try:
    from juncture.parsers.sqlglot_parser import split_statements  # type: ignore[attr-defined]
except ImportError:
    from juncture.adapters.duckdb_adapter import _split_sql_statements as split_statements

CREATE_RE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?(?:TABLE|VIEW)\s+'
    r'(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z0-9_.\-]+)"?',
    re.IGNORECASE,
)
INSERT_RE = re.compile(r'^\s*INSERT\s+INTO\s+"?([A-Za-z0-9_.\-]+)"?', re.IGNORECASE)


def output_of(stmt: str) -> str | None:
    for pat in (CREATE_RE, INSERT_RE):
        m = pat.match(stmt)
        if m:
            return m.group(1)
    return None


def build_graph(sql: str) -> tuple[nx.DiGraph, list[str]]:
    stmts = split_statements(sql)
    g: nx.DiGraph = nx.DiGraph()
    produced_by: dict[str, int] = {}

    for idx, stmt in enumerate(stmts):
        head = stmt.strip().splitlines()[0] if stmt.strip() else ""
        g.add_node(idx, out=output_of(stmt), head=head)
        for inp in extract_table_references(stmt):
            src = produced_by.get(inp)
            if src is not None and src != idx:
                g.add_edge(src, idx, via=inp)
        out = output_of(stmt)
        if out:
            produced_by[out] = idx

    return g, stmts


def write_dot(g: nx.DiGraph, out_path: Path) -> None:
    lines = ["digraph execute_dag {", '  rankdir=LR;', '  node [shape=box, fontsize=10];']
    for n, data in g.nodes(data=True):
        label = f"#{n}"
        if data.get("out"):
            label += f"\\n{data['out']}"
        lines.append(f'  {n} [label="{label}"];')
    for u, v, data in g.edges(data=True):
        lines.append(f'  {u} -> {v} [label="{data.get("via", "")}"];')
    lines.append("}")
    out_path.write_text("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("sql_path", type=Path, help="Path to the EXECUTE model SQL file")
    ap.add_argument("--dot", type=Path, help="Write Graphviz DOT file for visualization")
    ap.add_argument("--limit", type=int, default=80, help="Truncate SQL head in fan-out report")
    ap.add_argument("--layers", action="store_true", help="Print every statement per layer")
    args = ap.parse_args()

    sql = args.sql_path.read_text()
    g, stmts = build_graph(sql)

    layers = list(nx.topological_generations(g))
    with_out = sum(1 for _, d in g.nodes(data=True) if d["out"])
    print(
        f"{len(stmts)} statements | {with_out} with detected output table | "
        f"{g.number_of_edges()} intra-script edges | {len(layers)} layers"
    )
    widest = max((len(layer) for layer in layers), default=0)
    print(f"Widest layer: {widest} statements (= upper bound on useful parallelism)")

    print("\nLayer histogram:")
    for i, layer in enumerate(layers):
        bar = "#" * min(len(layer), 60)
        print(f"  layer {i:>3}: {len(layer):>4} {bar}")

    if args.layers:
        print("\nStatements per layer:")
        for i, layer in enumerate(layers):
            print(f"  layer {i}:")
            for idx in sorted(layer):
                data = g.nodes[idx]
                print(f"    #{idx:>4} out={data['out']!s:<40} {data['head'][: args.limit]}")

    print("\nTop-10 producers by fan-out (candidates for pinning / caching):")
    ranked = sorted(((n, g.out_degree(n)) for n in g.nodes), key=lambda x: -x[1])[:10]
    for idx, fo in ranked:
        data = g.nodes[idx]
        print(
            f"  #{idx:>4} fan-out={fo:>3} out={data['out']!s:<40} "
            f"{data['head'][: args.limit]}"
        )

    if args.dot:
        write_dot(g, args.dot)
        print(f"\nWrote Graphviz DOT to {args.dot}")
        print(f"Render with: dot -Tsvg {args.dot} > {args.dot.with_suffix('.svg')}")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
