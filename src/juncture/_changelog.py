"""Changelog data + formatters for Juncture releases.

Maintained manually: one-line summaries per version, newest-first. Each
release commit that bumps ``pyproject.toml`` adds a new entry at the top
of :data:`CHANGELOG`. The ``juncture changelog`` CLI command and the
auto-update "What's new" message both read from this dict.

Why not fetch from GitHub? The data ships *with* the installed package so
``juncture changelog`` works offline and the post-update "What's new"
message is correct even if the network is down. Release notes on GitHub
can be longer; this dict carries the scannable summary.
"""

from __future__ import annotations

# Ordered newest-first. Each value is a list of brief one-line descriptions.
CHANGELOG: dict[str, list[str]] = {
    "0.41.3": [
        "New: `juncture version` subcommand -- live GitHub fetch, prints "
        "a rich panel with current version + update hint "
        "(`v0.41.3  ->  v0.41.4 available (run: juncture update)`). "
        "Port of the kbagent `version` UX: explicit check, no auto-install",
        "Revert: `juncture -V` / `--version` is back to a lightweight "
        "local-only print. 0.41.2 briefly made it trigger `maybe_auto_update()` "
        "(auto-install + re-exec), which was the wrong semantics for a flag "
        "scripts call just to learn the installed version. If you want a "
        "live update check, use the new `juncture version` subcommand",
        "Chore: restore the version-cache TTL to 1 hour (was 5 min in "
        "0.41.2). With `juncture version` covering the explicit-check lane, "
        "the cached auto-update path can go back to the gentler cadence "
        "without starving users of fresh-release information",
    ],
    "0.41.2": [
        "Fix: `juncture -V` / `--version` now triggers the auto-update flow. "
        "Typer's `is_eager=True` on the option made the version callback "
        "fire *before* the `_root` body, so `maybe_auto_update()` never ran; "
        "the callback now calls it explicitly before printing the version",
        "Fix: reduce the version-cache TTL from 1 hour to 5 minutes. With "
        "the old TTL, a `juncture run` right after a release was "
        "served from a stale cache (which still said 'you're on latest') "
        "until the next hour rolled over. 5 minutes keeps network load "
        "bounded while making new releases visible within minutes. "
        "Manual `juncture update` still bypasses the cache entirely",
    ],
    "0.41.1": [
        "UX: `juncture init` now creates an empty `macros/` directory and "
        "ships `juncture.yaml` with an explicit `jinja: false` placeholder "
        "(plus a one-line comment explaining when to flip it). The "
        "tutorial's Level 3 'flip on Jinja mode' step is now a visible "
        "find-and-replace instead of a blind insert",
        "Docs: TUTORIAL.md L1 scaffold tree includes `macros/`; L3 shows "
        "the full before/after of the `jinja` flag with surrounding yaml "
        "context (fixes the 'wait, where does `jinja: true` go?' trap)",
    ],
    "0.41.0": [
        "New: auto-update on startup -- checks GitHub for a newer release, "
        "upgrades via `uv tool install --upgrade`, re-execs the same command "
        "(pattern cloned from kbagent / Claude Code)",
        "New: `juncture update` -- manual upgrade command that bypasses the cache TTL",
        "New: `juncture changelog` -- one-line summaries per version",
        "Guardrails: editable installs auto-detected (PEP 660 "
        "`direct_url.json`) and skipped; `juncture web`, `--json` output, "
        "and the update/changelog/version subcommands do not trigger "
        "auto-update; opt out globally with `JUNCTURE_AUTO_UPDATE=false`",
        "Fix: `juncture init <dir>` (without `--name`) now derives the "
        "project name from the target directory (`init my_shop` creates a "
        "config named `my_shop`, not `my_juncture_project`)",
    ],
    "0.40.2": [
        "Breaking: `juncture init` is now minimal by default "
        "(juncture.yaml, empty `models/`, empty `seeds/`, README.md); the "
        "old demo scaffold is opt-in via `--with-examples`",
        "Fix: tutorial_shop data generator default `--output-dir` now "
        "resolves to `./seeds` in CWD (not the repo path where the script "
        "lives)",
        "Docs: TUTORIAL.md L1 aligned with both fixes",
    ],
    "0.40.1": [
        "Docs: tutorial refreshed -- install via `uv tool install git+...`, "
        "new L1 'Under the hood: DuckDB' section (CSV=TABLE vs "
        "parquet=VIEW), deterministic seed generator, 'With Claude Code' "
        "box per level",
        "Refactor: version lives only in `pyproject.toml`; runtime reads it via `importlib.metadata`",
    ],
    "0.40.0": [
        "Phase 1 gate closed: DuckDB adapter runs real workloads end-to-end "
        "(208 parquet seeds x 374 SQL statements), web UI, migrate-keboola "
        "(raw-JSON and sync-pull), parallel EXECUTE, observability/lineage "
        "optional dep",
    ],
}

# Number of versions shown by default in ``juncture changelog``.
DEFAULT_CHANGELOG_LIMIT = 5

# Environment variable set by auto-update before re-exec so the next
# process can show "What's new in vX.Y.Z".
ENV_UPDATED_FROM = "JUNCTURE_UPDATED_FROM"


def get_changelog(limit: int = DEFAULT_CHANGELOG_LIMIT) -> dict[str, list[str]]:
    """Return the *limit* most recent changelog entries (newest first)."""
    items = list(CHANGELOG.items())[:limit]
    return dict(items)


def get_version_notes(version: str) -> list[str] | None:
    """Return changelog entries for a specific version, or None if unknown."""
    return CHANGELOG.get(version)


def format_whats_new(old_version: str, new_version: str) -> str:
    """Format a brief 'What's new' message for display after auto-update.

    Intentionally shows only the new version's entries (not every
    intermediate tag) — keeps the post-update stderr footprint small.
    Returns an empty string if the new version is not in the dict, which
    can happen during a release before the changelog entry has been
    committed.
    """
    notes = get_version_notes(new_version)
    if not notes:
        return ""
    lines = [f"  What's new in v{new_version} (was v{old_version}):"]
    for note in notes:
        lines.append(f"    - {note}")
    return "\n".join(lines) + "\n"
