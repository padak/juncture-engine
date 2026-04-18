"""Unit tests for the Jinja macro loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from juncture.core.project import ProjectError, _load_jinja_macros


def test_missing_macros_dir_returns_empty(tmp_path: Path) -> None:
    # A project without a macros/ directory yields no macros.
    assert _load_jinja_macros(tmp_path / "macros", {}) == {}


def test_empty_macros_dir_returns_empty(tmp_path: Path) -> None:
    (tmp_path / "macros").mkdir()
    assert _load_jinja_macros(tmp_path / "macros", {}) == {}


def test_single_macro_renders(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "my_date.sql").write_text(
        "{% macro my_date(col) -%}strftime({{ col }}, '%Y-%m-%d'){%- endmacro %}"
    )
    loaded = _load_jinja_macros(macros, {})
    assert "my_date" in loaded
    assert loaded["my_date"]("order_ts") == "strftime(order_ts, '%Y-%m-%d')"


def test_nested_macros_dir_is_discovered(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    (macros / "dates").mkdir(parents=True)
    (macros / "dates" / "my_date.sql").write_text(
        "{% macro my_date(col) -%}strftime({{ col }}, '%Y-%m-%d'){%- endmacro %}"
    )
    loaded = _load_jinja_macros(macros, {})
    assert "my_date" in loaded


def test_multiple_macros_across_files(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "a.sql").write_text("{% macro fmt_date(col) -%}DATE({{ col }}){%- endmacro %}")
    (macros / "b.sql").write_text("{% macro fmt_money(col) -%}ROUND({{ col }}, 2){%- endmacro %}")
    loaded = _load_jinja_macros(macros, {})
    assert set(loaded) >= {"fmt_date", "fmt_money"}
    assert loaded["fmt_money"]("amount") == "ROUND(amount, 2)"


def test_macro_uses_var_helper(tmp_path: Path) -> None:
    """Macros can read project vars via the injected var() stub."""
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "tier.sql").write_text(
        "{% macro is_vip(col) -%}({{ col }} >= {{ var('vip_threshold', 500) }}){%- endmacro %}"
    )
    loaded = _load_jinja_macros(macros, {"vip_threshold": 1000})
    assert loaded["is_vip"]("ltv") == "(ltv >= 1000)"


def test_macro_referencing_ref_passes_through(tmp_path: Path) -> None:
    """ref() inside a macro body stays as a literal ref() macro so the
    downstream regex picks up the dependency; it is never resolved here."""
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "join.sql").write_text("{% macro orders_src() -%}{{ ref('stg_orders') }}{%- endmacro %}")
    loaded = _load_jinja_macros(macros, {})
    assert "ref('stg_orders')" in loaded["orders_src"]()


def test_macro_syntax_error_raises(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "broken.sql").write_text("{% macro bad(col -%}oops{%- endmacro %}")
    with pytest.raises(ProjectError, match="macros"):
        _load_jinja_macros(macros, {})
