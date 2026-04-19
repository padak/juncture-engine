"""Expose ``juncture.__version__`` read from installed package metadata.

Single source of truth for the version string is ``pyproject.toml``; this
module resolves it at import time via ``importlib.metadata`` so the value
never drifts between the build system and the runtime.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("juncture")
except PackageNotFoundError:
    # Running from a source checkout without `pip install -e` — return a
    # sentinel rather than crashing. Normal dev / CI flows go through
    # `make install`, so this path is rare.
    __version__ = "0+unknown"
