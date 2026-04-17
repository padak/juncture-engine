"""Analyze an EXECUTE-materialized script: intra-script dependency DAG.

Why this exists
---------------
The ``execute`` materialization runs a multi-statement SQL script as a single
black-box node in the Juncture DAG. ``juncture compile`` therefore shows it
with ``Depends on: —`` and no intra-script structure, even though the script
itself has heavy internal dependencies (CREATE TABLE X AS SELECT ... FROM Y).

This script is a thin CLI wrapper over
:func:`juncture.parsers.sqlglot_parser.build_statement_dag`, printing the
hidden DAG in a human-friendly shape: layer histogram, widest-layer
parallelism ceiling, top-10 fan-out producers, optional Graphviz DOT.

Usage
-----

    .venv/bin/python scripts/analyze_execute.py models/<model>.sql

Options:
    --dot OUT.dot       Write Graphviz DOT file for visualization.
    --limit N           Truncate SQL headline in reports to N chars (default 80).
    --layers            Print one line per statement per layer (verbose).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import networkx as nx

from juncture.parsers.sqlglot_parser import StatementNode, build_statement_dag


def write_dot(g: nx.DiGraph, out_path: Path) -> None:
    lines = ["digraph execute_dag {", '  rankdir=LR;', '  node [shape=box, fontsize=10];']
    for n, data in g.nodes(data=True):
        node: StatementNode = data["node"]
        label = f"#{n}"
        if node.output:
            label += f"\\n{node.output}"
        lines.append(f'  {n} [label="{label}"];')
    for u, v, data in g.edges(data=True):
        lines.append(f'  {u} -> {v} [label="{data.get("via", "")}"];')
    lines.append("}")
    out_path.write_text("\n".join(lines))


def _head(node: StatementNode, limit: int) -> str:
    first_line = node.sql.strip().splitlines()[0] if node.sql.strip() else ""
    return first_line[:limit]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    ap.add_argument("sql_path", type=Path, help="Path to the EXECUTE model SQL file")
    ap.add_argument("--dot", type=Path, help="Write Graphviz DOT file for visualization")
    ap.add_argument("--limit", type=int, default=80, help="Truncate SQL head in reports")
    ap.add_argument("--layers", action="store_true", help="Print every statement per layer")
    args = ap.parse_args()

    sql = args.sql_path.read_text()
    graph = build_statement_dag(sql)

    total = graph.number_of_nodes()
    with_out = sum(1 for _, data in graph.nodes(data=True) if data["node"].output)
    layers = list(nx.topological_generations(graph))
    widest = max((len(layer) for layer in layers), default=0)

    print(
        f"{total} statements | {with_out} with detected output table | "
        f"{graph.number_of_edges()} intra-script edges | {len(layers)} layers"
    )
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
                node: StatementNode = graph.nodes[idx]["node"]
                print(f"    #{idx:>4} out={node.output!s:<40} {_head(node, args.limit)}")

    print("\nTop-10 producers by fan-out (candidates for pinning / caching):")
    ranked = sorted(((n, graph.out_degree(n)) for n in graph.nodes), key=lambda x: -x[1])[:10]
    for idx, fo in ranked:
        node: StatementNode = graph.nodes[idx]["node"]
        print(
            f"  #{idx:>4} fan-out={fo:>3} out={node.output!s:<40} "
            f"{_head(node, args.limit)}"
        )

    if args.dot:
        write_dot(graph, args.dot)
        print(f"\nWrote Graphviz DOT to {args.dot}")
        print(f"Render with: dot -Tsvg {args.dot} > {args.dot.with_suffix('.svg')}")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
