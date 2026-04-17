"""Unit tests for env var interpolation in juncture.yaml."""

from __future__ import annotations

from pathlib import Path

import pytest

from juncture.core.project import ProjectConfig, ProjectError


def _write_yaml(tmp: Path, content: str) -> Path:
    path = tmp / "juncture.yaml"
    path.write_text(content)
    return path


def test_interpolates_simple_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "/tmp/my.duckdb")
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: local
connections:
  local:
    type: duckdb
    path: ${DB_PATH}
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.connections["local"].params["path"] == "/tmp/my.duckdb"


def test_default_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DB_PATH", raising=False)
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: local
connections:
  local:
    type: duckdb
    path: ${DB_PATH:-data/fallback.duckdb}
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.connections["local"].params["path"] == "data/fallback.duckdb"


def test_missing_var_without_default_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REQUIRED_VAR", raising=False)
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: local
connections:
  local:
    type: duckdb
    path: ${REQUIRED_VAR}
""",
    )
    with pytest.raises(ProjectError, match=r"REQUIRED_VAR.*not set"):
        ProjectConfig.from_file(path)


def test_interpolates_nested_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SF_USER", "alice")
    monkeypatch.setenv("SF_PASS", "s3cret")
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: prod
connections:
  prod:
    type: snowflake
    user: ${SF_USER}
    password: ${SF_PASS}
""",
    )
    cfg = ProjectConfig.from_file(path)
    params = cfg.connections["prod"].params
    assert params["user"] == "alice"
    assert params["password"] == "s3cret"
