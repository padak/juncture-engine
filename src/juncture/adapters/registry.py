"""Adapter registry: name-based lookup for backend implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter

_REGISTRY: dict[str, type[Adapter]] = {}


def register_adapter(name: str, cls: type[Adapter]) -> None:
    _REGISTRY[name] = cls


def get_adapter(name: str, **params: Any) -> Adapter:
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"No adapter registered as {name!r}. Available: {available}")
    return _REGISTRY[name](**params)
