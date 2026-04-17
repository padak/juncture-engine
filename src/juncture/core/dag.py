"""Directed acyclic graph of models with topological ordering and layering.

The DAG is backed by networkx but exposed via a small surface so we can swap
implementations if needed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

import networkx as nx

from juncture.core.model import Model


class DAGError(Exception):
    """Raised when the DAG has structural problems (cycle, missing ref)."""


class DAG:
    """Thin wrapper around ``networkx.DiGraph`` carrying ``Model`` payloads.

    Nodes are model names (str); the full :class:`Model` object is stored as
    the ``model`` attribute on the node.
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()

    @classmethod
    def from_models(cls, models: Iterable[Model]) -> DAG:
        dag = cls()
        by_name: dict[str, Model] = {}
        for model in models:
            if model.name in by_name:
                raise DAGError(f"Duplicate model name: {model.name!r}")
            by_name[model.name] = model
            dag._graph.add_node(model.name, model=model)

        for model in by_name.values():
            for upstream in model.depends_on:
                if upstream not in by_name:
                    raise DAGError(
                        f"Model {model.name!r} depends on unknown model {upstream!r}. "
                        f"Did you misspell ref('{upstream}')?"
                    )
                dag._graph.add_edge(upstream, model.name)

        dag._check_acyclic()
        return dag

    def _check_acyclic(self) -> None:
        try:
            cycle = nx.find_cycle(self._graph)
        except nx.NetworkXNoCycle:
            return
        path = " -> ".join(n for n, _ in cycle) + f" -> {cycle[-1][1]}"
        raise DAGError(f"Cycle detected: {path}")

    @property
    def nodes(self) -> list[str]:
        return list(self._graph.nodes)

    def model(self, name: str) -> Model:
        if name not in self._graph:
            raise KeyError(f"No model named {name!r}")
        return self._graph.nodes[name]["model"]

    def models(self) -> list[Model]:
        return [self._graph.nodes[n]["model"] for n in self._graph.nodes]

    def upstream(self, name: str) -> set[str]:
        return set(self._graph.predecessors(name))

    def downstream(self, name: str) -> set[str]:
        return set(self._graph.successors(name))

    def topological_order(self) -> list[str]:
        return list(nx.topological_sort(self._graph))

    def layers(self) -> Iterator[list[str]]:
        """Yield successive layers of nodes that can run in parallel.

        Layer N contains every node whose longest predecessor path has length N.
        Within a layer, all nodes are independent and may run concurrently.
        """
        in_degree = {n: self._graph.in_degree(n) for n in self._graph.nodes}
        remaining = set(self._graph.nodes)
        while remaining:
            current = sorted(n for n in remaining if in_degree[n] == 0)
            if not current:
                raise DAGError("DAG has cycle; topological layering failed")
            yield current
            remaining.difference_update(current)
            for node in current:
                for succ in self._graph.successors(node):
                    in_degree[succ] -= 1

    def select(self, patterns: list[str]) -> set[str]:
        """Select models by name, ``+prefix`` (with ancestors) or ``suffix+``.

        Examples:
            ``["orders"]``          -> just "orders"
            ``["+orders"]``         -> "orders" and all upstream
            ``["orders+"]``         -> "orders" and all downstream
            ``["+orders+"]``        -> "orders" + all ancestors + descendants
            ``["tag:marts"]``       -> all models tagged "marts"
        """
        result: set[str] = set()
        for pattern in patterns:
            expand_up = pattern.startswith("+")
            expand_down = pattern.endswith("+")
            stripped = pattern.strip("+")

            if stripped.startswith("tag:"):
                tag = stripped.removeprefix("tag:")
                matched = {n for n in self._graph.nodes if tag in self.model(n).tags}
            else:
                if stripped not in self._graph:
                    raise DAGError(f"Unknown model in selector: {pattern!r}")
                matched = {stripped}

            result.update(matched)
            if expand_up:
                for m in matched:
                    result.update(nx.ancestors(self._graph, m))
            if expand_down:
                for m in matched:
                    result.update(nx.descendants(self._graph, m))
        return result

    def subgraph(self, names: set[str]) -> DAG:
        sub = DAG()
        sub._graph = self._graph.subgraph(names).copy()
        return sub

    def __len__(self) -> int:
        return len(self._graph)

    def __contains__(self, name: object) -> bool:
        return name in self._graph
