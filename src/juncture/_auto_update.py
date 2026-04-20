"""Auto-update flow for the Juncture CLI.

Pattern cloned from kbagent (``keboola_agent_cli.auto_update``) and
Claude Code: on startup, check GitHub for a newer release, upgrade via
``uv tool install --upgrade`` (fall back to ``pip install --upgrade``),
and ``os.execvpe`` the same command in the upgraded binary. The
re-exec'd process prints "What's new" from :mod:`juncture._changelog`.

All user-visible output goes to ``sys.stderr.write()`` — stdout is kept
clean for JSON consumers and shell pipes. The entire flow is wrapped in
a blanket ``try / except Exception`` so auto-update can never crash the
CLI.

Guardrails (see :func:`_should_skip`):

* ``JUNCTURE_SKIP_UPDATE=1`` is set by the re-exec guard to break loops.
* ``JUNCTURE_AUTO_UPDATE`` in ``{false, 0, no}`` is the user opt-out.
* Editable installs (``pip install -e .``) are detected through PEP 660
  ``direct_url.json`` and skipped — we never clobber a checkout.
* Subcommands ``update``, ``changelog``, ``version``, ``web`` are
  skipped (self-reference or long-running daemon).
* ``--json`` anywhere in ``sys.argv`` disables auto-update so scripts
  parsing machine output don't get stderr contamination.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

import httpx
import platformdirs
from packaging.version import InvalidVersion, Version

from juncture._changelog import ENV_UPDATED_FROM, format_whats_new
from juncture._version import __version__

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants (kept here to avoid a one-off ``constants`` module)
# ---------------------------------------------------------------------------

GITHUB_REPO: str = "padak/juncture-engine"
INSTALL_SOURCE: str = "git+https://github.com/padak/juncture-engine.git"
ENV_AUTO_UPDATE: str = "JUNCTURE_AUTO_UPDATE"
ENV_SKIP_UPDATE: str = "JUNCTURE_SKIP_UPDATE"
AUTO_UPDATE_CHECK_INTERVAL: int = 3600  # 1 hour TTL for the version cache
VERSION_CHECK_TIMEOUT: float = 4.0  # seconds for the GitHub API call
VERSION_CACHE_FILENAME: str = "version_cache.json"

# Subcommands that MUST NOT trigger auto-update. ``update`` / ``changelog`` /
# ``version`` are self-referential; ``web`` starts a long-lived HTTP server
# that a mid-request re-exec would break.
SKIP_COMMANDS: frozenset[str] = frozenset({"update", "changelog", "version", "web"})


# ---------------------------------------------------------------------------
# Version cache (~/.config/juncture/version_cache.json on Linux/macOS)
# ---------------------------------------------------------------------------


def _get_cache_path() -> Path:
    config_dir = Path(platformdirs.user_config_dir("juncture"))
    return config_dir / VERSION_CACHE_FILENAME


def _read_cache() -> dict | None:
    cache_path = _get_cache_path()
    try:
        if not cache_path.is_file():
            return None
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "last_check" in data and "latest_version" in data:
            return data
        return None
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _write_cache(latest_version: str) -> None:
    cache_path = _get_cache_path()
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_check": time.time(),
            "latest_version": latest_version,
        }
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        # Non-critical; next run will re-fetch.
        pass


def _is_cache_fresh(cache: dict, ttl: int) -> bool:
    try:
        return (time.time() - float(cache["last_check"])) < ttl
    except (KeyError, TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Install-mode detection + skip logic
# ---------------------------------------------------------------------------


def _is_dev_install() -> bool:
    """True when Juncture is installed from a source checkout (PEP 660).

    Auto-updating over an editable install would overwrite the user's
    working copy, which is never what they want.
    """
    try:
        dist = distribution("juncture")
    except PackageNotFoundError:
        # Running from a source tree without ``pip install -e``.
        return True

    try:
        direct_url = dist.read_text("direct_url.json")
        if not direct_url:
            return False
        data = json.loads(direct_url)
        return bool(data.get("dir_info", {}).get("editable", False))
    except (OSError, json.JSONDecodeError, ValueError):
        return False


def _is_machine_output() -> bool:
    """Heuristic: ``--json`` anywhere in argv means a script is parsing
    output; don't pollute stderr with "Updating..." lines the caller might
    be capturing."""
    return "--json" in sys.argv


def _should_skip() -> bool:
    # Re-exec guard — set by _re_exec() below.
    if os.environ.get(ENV_SKIP_UPDATE) == "1":
        return True
    # Explicit user opt-out.
    auto_update_val = os.environ.get(ENV_AUTO_UPDATE, "").lower().strip()
    if auto_update_val in ("false", "0", "no"):
        return True
    if _is_dev_install():
        return True
    if _is_machine_output():
        return True
    argv = sys.argv
    if len(argv) >= 2:
        cmd = argv[1].lower()
        if cmd in SKIP_COMMANDS:
            return True
    return False


# ---------------------------------------------------------------------------
# GitHub lookup + semver comparison
# ---------------------------------------------------------------------------


def _fetch_latest_version(timeout: float = VERSION_CHECK_TIMEOUT) -> str | None:
    """Look up the latest Juncture release tag via the GitHub API.

    Returns a version string like ``"0.41.0"`` (leading ``v`` stripped),
    or ``None`` on any network / parsing error.
    """
    try:
        response = httpx.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            timeout=timeout,
            follow_redirects=True,
            headers={"Accept": "application/vnd.github.v3+json"},
        )
        response.raise_for_status()
        tag = response.json().get("tag_name", "")
        version = tag.lstrip("v")
        if re.match(r"^\d+\.\d+\.\d+", version):
            return version
        return None
    except (httpx.HTTPError, KeyError, ValueError):
        logger.debug("Failed to fetch latest juncture version", exc_info=True)
        return None


def _is_up_to_date(local: str | None, latest: str | None) -> bool | None:
    """Three-valued result: True (no update), False (update available),
    None (can't tell — treat as no update)."""
    if local is None or latest is None:
        return None
    try:
        return Version(local) >= Version(latest)
    except InvalidVersion:
        return None


# ---------------------------------------------------------------------------
# Perform update + re-exec
# ---------------------------------------------------------------------------


def _perform_update(latest_version: str) -> bool:
    """Invoke ``uv tool install --upgrade`` (fallback: ``pip install --upgrade``)."""
    uv_path = shutil.which("uv")
    if uv_path:
        cmd = [uv_path, "tool", "install", "--upgrade", INSTALL_SOURCE]
    else:
        pip_path = shutil.which("pip")
        if pip_path is None:
            return False
        cmd = [pip_path, "install", "--upgrade", INSTALL_SOURCE]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except OSError:
        return False


def _re_exec() -> None:
    """Replace the current process with the freshly installed binary.

    Sets ``JUNCTURE_SKIP_UPDATE=1`` to break the loop; falls back to
    ``python -m juncture`` if the ``juncture`` entry point is missing.
    """
    env = os.environ.copy()
    env[ENV_SKIP_UPDATE] = "1"
    juncture_path = shutil.which("juncture")
    if juncture_path:
        os.execvpe("juncture", sys.argv, env)
    else:
        new_argv = [sys.executable, "-m", "juncture", *sys.argv[1:]]
        os.execvpe(sys.executable, new_argv, env)


def show_post_update_changelog() -> None:
    """Print "What's new" after a successful re-exec.

    Called from the CLI root. Reads (and pops) ``JUNCTURE_UPDATED_FROM``
    so the message fires exactly once per update.
    """
    try:
        old_version = os.environ.pop(ENV_UPDATED_FROM, "")
        if not old_version:
            return
        msg = format_whats_new(old_version, __version__)
        if msg:
            sys.stderr.write(msg)
    except Exception:
        pass  # Never crash


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def maybe_auto_update() -> None:
    """Main auto-update orchestration.

    Called at the very top of the CLI root callback. Never raises —
    any exception is caught, logged at DEBUG, and the CLI continues with
    the current version.
    """
    try:
        if _should_skip():
            return

        cache = _read_cache()
        latest_version: str | None = None

        if cache and _is_cache_fresh(cache, AUTO_UPDATE_CHECK_INTERVAL):
            latest_version = cache.get("latest_version")
        else:
            latest_version = _fetch_latest_version(timeout=VERSION_CHECK_TIMEOUT)
            if latest_version:
                _write_cache(latest_version)

        if latest_version is None:
            return

        up_to_date = _is_up_to_date(__version__, latest_version)
        if up_to_date is True or up_to_date is None:
            return

        sys.stderr.write(f"Updating juncture v{__version__} -> v{latest_version}...\n")

        if not _perform_update(latest_version):
            sys.stderr.write("Auto-update failed; continuing with current version.\n")
            return

        sys.stderr.write(f"Updated to v{latest_version}. Re-launching...\n")
        os.environ[ENV_UPDATED_FROM] = __version__
        _re_exec()
    except Exception:
        logger.debug("Auto-update check failed", exc_info=True)
