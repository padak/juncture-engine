"""Unit tests for the DAG: topology, cycles, layering, selectors."""

from __future__ import annotations

import pytest

from juncture.core.dag import DAG, DAGError
from juncture.core.model import Model, ModelKind


def _sql(name: str, deps: set[str] | None = None, tags: list[str] | None = None) -> Model:
    return Model(
        name=name,
        kind=ModelKind.SQL,
        sql="SELECT 1",
        depends_on=deps or set(),
        tags=tags or [],
    )


def test_linear_topological_order() -> None:
    dag = DAG.from_models(
        [
            _sql("a"),
            _sql("b", {"a"}),
            _sql("c", {"b"}),
        ]
    )
    assert dag.topological_order() == ["a", "b", "c"]


def test_layers_parallelizable_siblings() -> None:
    dag = DAG.from_models(
        [
            _sql("source"),
            _sql("left", {"source"}),
            _sql("right", {"source"}),
            _sql("merged", {"left", "right"}),
        ]
    )
    layers = list(dag.layers())
    assert layers == [["source"], ["left", "right"], ["merged"]]


def test_cycle_detection() -> None:
    with pytest.raises(DAGError, match="Cycle detected"):
        DAG.from_models(
            [
                _sql("a", {"b"}),
                _sql("b", {"a"}),
            ]
        )


def test_missing_ref_raises() -> None:
    with pytest.raises(DAGError, match="depends on unknown model"):
        DAG.from_models([_sql("b", {"nonexistent"})])


def test_duplicate_name_raises() -> None:
    with pytest.raises(DAGError, match="Duplicate model name"):
        DAG.from_models([_sql("x"), _sql("x")])


def test_selector_single_model() -> None:
    dag = DAG.from_models([_sql("a"), _sql("b", {"a"})])
    assert dag.select(["a"]) == {"a"}


def test_selector_upstream_and_downstream() -> None:
    dag = DAG.from_models(
        [
            _sql("root"),
            _sql("mid", {"root"}),
            _sql("leaf", {"mid"}),
        ]
    )
    assert dag.select(["+mid"]) == {"root", "mid"}
    assert dag.select(["mid+"]) == {"mid", "leaf"}
    assert dag.select(["+mid+"]) == {"root", "mid", "leaf"}


def test_selector_by_tag() -> None:
    dag = DAG.from_models(
        [
            _sql("raw_a", tags=["raw"]),
            _sql("mart_b", tags=["mart"]),
            _sql("mart_c", tags=["mart"]),
        ]
    )
    assert dag.select(["tag:mart"]) == {"mart_b", "mart_c"}


def test_subgraph_preserves_edges() -> None:
    dag = DAG.from_models(
        [
            _sql("a"),
            _sql("b", {"a"}),
            _sql("c", {"b"}),
        ]
    )
    sub = dag.subgraph({"a", "b"})
    assert sub.nodes == ["a", "b"]
    assert sub.upstream("b") == {"a"}
