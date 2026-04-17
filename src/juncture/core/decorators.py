"""@transform decorator for Python models.

Registers a Python function as a Juncture model. Discovery is handled by
importing the module; the decorator attaches metadata as attributes on the
function so Project.discover_models() can pick it up.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from juncture.core.model import Materialization

_TRANSFORM_MARK = "__juncture_transform__"


def transform(
    name: str | None = None,
    *,
    materialization: str | Materialization = Materialization.TABLE,
    depends_on: list[str] | None = None,
    description: str | None = None,
    columns: list[dict[str, Any]] | None = None,
    tags: list[str] | None = None,
    unique_key: str | None = None,
    schedule_cron: str | None = None,
    **config: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a function as a Juncture Python model.

    The decorated function receives a :class:`TransformContext` and returns a
    DataFrame (pandas, Polars, or PyArrow Table). Dependencies may be declared
    explicitly via ``depends_on`` or inferred from ``ctx.ref("other")`` calls
    at runtime.
    """
    materialization_value = (
        materialization if isinstance(materialization, Materialization) else Materialization(materialization)
    )

    def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        metadata = {
            "name": name or fn.__name__,
            "materialization": materialization_value,
            "depends_on": set(depends_on or []),
            "description": description or (fn.__doc__.strip() if fn.__doc__ else None),
            "columns": columns or [],
            "tags": tags or [],
            "unique_key": unique_key,
            "schedule_cron": schedule_cron,
            "config": config,
        }
        setattr(_wrapped, _TRANSFORM_MARK, metadata)
        return _wrapped

    return _decorator


def is_transform(obj: Any) -> bool:
    """True when ``obj`` was produced by :func:`transform`."""
    return hasattr(obj, _TRANSFORM_MARK)


def get_metadata(obj: Any) -> dict[str, Any]:
    """Return metadata dict attached by :func:`transform`."""
    if not is_transform(obj):
        raise TypeError(f"{obj!r} is not a @transform-decorated function")
    return dict(getattr(obj, _TRANSFORM_MARK))
