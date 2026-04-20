"""Unit tests for the auto-update flow.

Network is never touched: ``_fetch_latest_version`` is the only function
that would call out, and we don't cover it here — it's a thin httpx
wrapper. Everything else is pure logic around caches, env vars, argv
and the PEP 660 editable-install marker.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

from juncture import _auto_update

# ---------------------------------------------------------------------------
# _is_up_to_date
# ---------------------------------------------------------------------------


def test_is_up_to_date_equal() -> None:
    assert _auto_update._is_up_to_date("0.40.2", "0.40.2") is True


def test_is_up_to_date_older() -> None:
    assert _auto_update._is_up_to_date("0.40.2", "0.41.0") is False


def test_is_up_to_date_newer() -> None:
    # Local > latest can happen mid-release while GitHub hasn't published yet.
    assert _auto_update._is_up_to_date("0.41.0", "0.40.2") is True


def test_is_up_to_date_handles_missing() -> None:
    assert _auto_update._is_up_to_date(None, "0.40.2") is None
    assert _auto_update._is_up_to_date("0.40.2", None) is None


def test_is_up_to_date_handles_garbage() -> None:
    assert _auto_update._is_up_to_date("not-a-version", "0.40.2") is None


# ---------------------------------------------------------------------------
# Version cache round-trip
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the platformdirs config dir to a tmp_path for the test."""
    monkeypatch.setattr(
        "juncture._auto_update.platformdirs.user_config_dir",
        lambda _name: str(tmp_path),
    )
    return tmp_path


def test_cache_empty_by_default(isolated_cache: Path) -> None:
    assert _auto_update._read_cache() is None


def test_cache_roundtrip(isolated_cache: Path) -> None:
    _auto_update._write_cache("0.41.0")
    cache = _auto_update._read_cache()
    assert cache is not None
    assert cache["latest_version"] == "0.41.0"
    assert "last_check" in cache


def test_cache_ignores_malformed_file(isolated_cache: Path) -> None:
    (isolated_cache / _auto_update.VERSION_CACHE_FILENAME).write_text("{not json")
    assert _auto_update._read_cache() is None


def test_cache_ignores_wrong_shape(isolated_cache: Path) -> None:
    (isolated_cache / _auto_update.VERSION_CACHE_FILENAME).write_text('{"foo": "bar"}')
    assert _auto_update._read_cache() is None


def test_is_cache_fresh_when_recent() -> None:
    cache = {"last_check": time.time(), "latest_version": "0.41.0"}
    assert _auto_update._is_cache_fresh(cache, ttl=3600) is True


def test_is_cache_fresh_when_old() -> None:
    cache = {"last_check": time.time() - 7200, "latest_version": "0.41.0"}
    assert _auto_update._is_cache_fresh(cache, ttl=3600) is False


def test_is_cache_fresh_when_missing_timestamp() -> None:
    assert _auto_update._is_cache_fresh({}, ttl=3600) is False


# ---------------------------------------------------------------------------
# _should_skip — every guard has its own test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_env_and_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test to: regular install, ``juncture run`` invocation,
    no env vars set. Individual tests override the part they're testing."""
    monkeypatch.delenv(_auto_update.ENV_SKIP_UPDATE, raising=False)
    monkeypatch.delenv(_auto_update.ENV_AUTO_UPDATE, raising=False)
    monkeypatch.setattr(sys, "argv", ["juncture", "run"])
    monkeypatch.setattr(_auto_update, "_is_dev_install", lambda: False)


def test_should_not_skip_default() -> None:
    assert _auto_update._should_skip() is False


def test_should_skip_on_re_exec_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_auto_update.ENV_SKIP_UPDATE, "1")
    assert _auto_update._should_skip() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "FALSE", "No"])
def test_should_skip_on_opt_out(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(_auto_update.ENV_AUTO_UPDATE, value)
    assert _auto_update._should_skip() is True


def test_should_skip_on_editable_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_auto_update, "_is_dev_install", lambda: True)
    assert _auto_update._should_skip() is True


@pytest.mark.parametrize("cmd", list(_auto_update.SKIP_COMMANDS))
def test_should_skip_on_self_reference_subcommand(monkeypatch: pytest.MonkeyPatch, cmd: str) -> None:
    monkeypatch.setattr(sys, "argv", ["juncture", cmd])
    assert _auto_update._should_skip() is True


def test_should_skip_on_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["juncture", "compile", "--json"])
    assert _auto_update._should_skip() is True
