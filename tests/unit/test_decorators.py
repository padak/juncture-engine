"""Unit tests for @transform decorator."""

from __future__ import annotations

from juncture.core.decorators import get_metadata, is_transform, transform


def test_transform_marks_function() -> None:
    @transform(name="my_model")
    def _fn(ctx):
        return None

    assert is_transform(_fn)
    meta = get_metadata(_fn)
    assert meta["name"] == "my_model"


def test_transform_default_name_is_function_name() -> None:
    @transform()
    def churn_score(ctx):
        return None

    assert get_metadata(churn_score)["name"] == "churn_score"


def test_transform_depends_on_normalized_to_set() -> None:
    @transform(depends_on=["a", "b", "a"])
    def _fn(ctx):
        return None

    assert get_metadata(_fn)["depends_on"] == {"a", "b"}


def test_transform_passes_through_docstring() -> None:
    @transform()
    def _fn(ctx):
        """Compute churn risk for each customer."""
        return None

    assert get_metadata(_fn)["description"] == "Compute churn risk for each customer."
