"""Juncture: multi-backend SQL + Python transformation engine.

Local-first, DuckDB-native, Keboola-compatible.
"""

from juncture._version import __version__
from juncture.core.decorators import transform
from juncture.core.model import Model, ModelKind
from juncture.core.project import Project

__all__ = [
    "Model",
    "ModelKind",
    "Project",
    "__version__",
    "transform",
]
