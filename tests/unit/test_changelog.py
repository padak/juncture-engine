"""Tests for the manually-maintained CHANGELOG dict + formatters."""

from __future__ import annotations

from packaging.version import Version

from juncture._changelog import (
    CHANGELOG,
    format_whats_new,
    get_changelog,
    get_version_notes,
)


def test_changelog_not_empty() -> None:
    assert len(CHANGELOG) >= 1


def test_changelog_is_ordered_newest_first() -> None:
    """The dict iteration order is load-bearing: `juncture changelog` and
    `format_whats_new` both rely on it. Assert descending semver order."""
    versions = [Version(v) for v in CHANGELOG]
    assert versions == sorted(versions, reverse=True), (
        f"CHANGELOG is not strictly newest-first: {list(CHANGELOG)}"
    )


def test_get_changelog_respects_limit() -> None:
    assert len(get_changelog(limit=2)) <= 2
    # limit greater than available entries -> return all
    assert len(get_changelog(limit=100)) == len(CHANGELOG)


def test_get_version_notes_found() -> None:
    first = next(iter(CHANGELOG))
    assert get_version_notes(first) == CHANGELOG[first]


def test_get_version_notes_missing() -> None:
    assert get_version_notes("99.99.99") is None


def test_format_whats_new_includes_all_notes() -> None:
    version = next(iter(CHANGELOG))
    msg = format_whats_new("0.0.0", version)
    assert f"v{version}" in msg
    for note in CHANGELOG[version]:
        assert note in msg


def test_format_whats_new_mentions_old_version() -> None:
    version = next(iter(CHANGELOG))
    msg = format_whats_new("0.1.2", version)
    assert "0.1.2" in msg


def test_format_whats_new_unknown_version_returns_empty() -> None:
    assert format_whats_new("0.0.0", "99.99.99") == ""
