"""TransformContext: runtime object passed to @transform-decorated functions.

The context exposes:

* ``ctx.ref(name)`` -- returns the upstream model as a DataFrame-like object
  (Polars or pandas, depending on what the adapter provides).
* ``ctx.config(key, default)`` -- access project/model config.
* ``ctx.vars(key, default)`` -- access runtime variables.
* ``ctx.logger`` -- structured logger.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter
    from juncture.core.model import Model


@dataclass(kw_only=True)
class TransformContext:
    """Runtime context passed to Python models."""

    model: Model
    adapter: Adapter
    run_vars: dict[str, Any] = field(default_factory=dict)
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("juncture.transform"))

    def ref(self, name: str) -> Any:
        """Return upstream model's materialized content as a DataFrame.

        The concrete return type depends on the adapter; DuckDB returns an
        Arrow Table that works with pandas, Polars and DuckDB's relation API.
        """
        return self.adapter.fetch_ref(name)

    def config(self, key: str, default: Any = None) -> Any:
        return self.model.config.get(key, default)

    def vars(self, key: str, default: Any = None) -> Any:
        return self.run_vars.get(key, default)

    def sql(self, query: str) -> Any:
        """Run a SQL query directly against the adapter's current connection."""
        return self.adapter.execute_arrow(query)
