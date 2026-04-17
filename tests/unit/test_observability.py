"""Unit tests for OpenLineage emitter (mostly no-op/log-only paths)."""

from __future__ import annotations

from juncture.core.model import Model, ModelKind
from juncture.observability import NullLineageEmitter, OpenLineageEmitter


def test_null_emitter_is_a_true_noop() -> None:
    e = NullLineageEmitter()
    model = Model(name="x", kind=ModelKind.SQL, sql="SELECT 1")
    # Must never raise
    e.start(model, "r1", [], [])
    e.complete(model, "r1", 5)
    e.fail(model, "r1", "boom")


def test_openlineage_log_only_when_sdk_missing(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    # Force ImportError path so we go to "log-only" mode without touching the SDK.
    import sys

    for name in list(sys.modules):
        if name.startswith("openlineage"):
            monkeypatch.setitem(sys.modules, name, None)

    e = OpenLineageEmitter(namespace="test", url=None)
    model = Model(name="m", kind=ModelKind.SQL, sql="SELECT 1")
    # Running shouldn't raise even when openlineage is absent.
    e.start(model, "r1", [], [])
    e.complete(model, "r1", 42)
    e.fail(model, "r1", "oops")
