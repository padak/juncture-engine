"""Unit tests for the ``profiles:`` block in juncture.yaml.

A profile is a named overlay that selectively overrides top-level keys
(``vars``, ``connections.<name>``, ``default_schema``, ...). The rules
matter: ``vars`` is a shallow dict merge (profile wins per-key),
``connections.<name>`` is a per-key merge (so a profile can override
just ``path`` without repeating ``type``), and scalars replace wholesale.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from juncture.core.project import ProjectConfig, ProjectError


def _write_yaml(tmp: Path, content: str) -> Path:
    path = tmp / "juncture.yaml"
    path.write_text(content)
    return path


def test_no_profiles_block_is_backward_compatible(tmp_path: Path) -> None:
    """Legacy project without profiles: continues to work."""
    path = _write_yaml(
        tmp_path,
        """
name: legacy
profile: default
connections:
  local:
    type: duckdb
    path: data/legacy.duckdb
vars:
  lookback_days: 90
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.active_profile is None
    assert cfg.available_profiles == []
    assert cfg.vars == {"lookback_days": 90}
    assert cfg.connections["local"].params["path"] == "data/legacy.duckdb"


def test_profile_overlay_merges_vars_shallowly(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
vars:
  as_of: "2026-03-31"
  lookback_days: 90
  region: eu
profiles:
  dev:
    vars:
      lookback_days: 7
""",
    )
    cfg = ProjectConfig.from_file(path, profile="dev")
    assert cfg.active_profile == "dev"
    assert cfg.vars == {"as_of": "2026-03-31", "lookback_days": 7, "region": "eu"}


def test_profile_overlay_merges_connections_per_key(tmp_path: Path) -> None:
    """Profile can override just ``path`` while inheriting ``type``."""
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  warehouse:
    type: duckdb
    path: data/base.duckdb
    memory_limit: 4GB
profiles:
  dev:
    connections:
      warehouse:
        path: data/dev.duckdb
""",
    )
    cfg = ProjectConfig.from_file(path, profile="dev")
    params = cfg.connections["warehouse"].params
    assert cfg.connections["warehouse"].type == "duckdb"
    assert params["path"] == "data/dev.duckdb"
    assert params["memory_limit"] == "4GB"


def test_profile_can_override_connection_type(tmp_path: Path) -> None:
    """Profile can switch a connection from DuckDB (dev) to Snowflake (prod)."""
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  warehouse:
    type: duckdb
    path: data/dev.duckdb
profiles:
  prod:
    connections:
      warehouse:
        type: snowflake
        account: myacct
        database: ANALYTICS
""",
    )
    cfg = ProjectConfig.from_file(path, profile="prod")
    assert cfg.connections["warehouse"].type == "snowflake"
    assert cfg.connections["warehouse"].params["account"] == "myacct"
    assert cfg.connections["warehouse"].params["database"] == "ANALYTICS"


def test_profile_overrides_default_schema(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
default_schema: main
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev:
    default_schema: dev_petr
""",
    )
    cfg = ProjectConfig.from_file(path, profile="dev")
    assert cfg.default_schema == "dev_petr"


def test_unknown_profile_fails_fast(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev: {}
""",
    )
    with pytest.raises(ProjectError, match=r"profile 'prod' is not declared"):
        ProjectConfig.from_file(path, profile="prod")


def test_explicit_arg_wins_over_env_and_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUNCTURE_PROFILE", "staging")
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: dev
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev:
    vars: {marker: "from-dev"}
  staging:
    vars: {marker: "from-staging"}
  prod:
    vars: {marker: "from-prod"}
""",
    )
    cfg = ProjectConfig.from_file(path, profile="prod")
    assert cfg.active_profile == "prod"
    assert cfg.vars["marker"] == "from-prod"


def test_env_var_wins_over_yaml_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUNCTURE_PROFILE", "staging")
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: dev
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev:
    vars: {marker: "from-dev"}
  staging:
    vars: {marker: "from-staging"}
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.active_profile == "staging"
    assert cfg.vars["marker"] == "from-staging"


def test_yaml_profile_field_used_when_no_explicit_or_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("JUNCTURE_PROFILE", raising=False)
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: dev
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev:
    vars: {marker: "from-dev"}
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.active_profile == "dev"
    assert cfg.vars["marker"] == "from-dev"


def test_profile_with_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Env vars should be interpolated inside the profile overlay too."""
    monkeypatch.setenv("KEBOOLA_BRANCH_ID", "1234")
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  warehouse:
    type: duckdb
    path: data/base.duckdb
profiles:
  branch:
    default_schema: "dev_${KEBOOLA_BRANCH_ID}"
""",
    )
    cfg = ProjectConfig.from_file(path, profile="branch")
    assert cfg.default_schema == "dev_1234"


def test_available_profiles_lists_all_declared(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev: {}
  staging: {}
  prod: {}
""",
    )
    cfg = ProjectConfig.from_file(path)
    assert cfg.available_profiles == ["dev", "prod", "staging"]


def test_empty_profiles_block_treated_as_absent(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
profile: default
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles: {}
""",
    )
    cfg = ProjectConfig.from_file(path, profile=None)
    assert cfg.active_profile is None
    assert cfg.available_profiles == []


def test_profile_overlay_not_a_mapping_fails(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
name: proj
connections:
  local:
    type: duckdb
    path: ":memory:"
profiles:
  dev: "not a mapping"
""",
    )
    with pytest.raises(ProjectError, match=r"expected a mapping"):
        ProjectConfig.from_file(path, profile="dev")
