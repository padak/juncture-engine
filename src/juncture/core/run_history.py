"""Append-only JSONL log of ``juncture run`` outcomes.

Each Runner.run() appends a summary record to ``<project>/target/run_history.jsonl``.
The web render (`juncture web`) reads these records to show a run-history
view. Structured as JSONL so parsing is trivial, rotation is a simple
file split, and concurrent runs serialise over POSIX append semantics.

Intentionally minimal: no SQLite, no rotation logic, no compression.
If a project runs 10k times the file is still < 50 MB. Operators who
want long-term archival can rename the file periodically.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.core.runner import RunReport

log = logging.getLogger(__name__)

_HISTORY_RELATIVE = Path("target") / "run_history.jsonl"


@dataclass(frozen=True, kw_only=True)
class RunHistoryEntry:
    """One persisted run outcome. JSON-serialised to a single line."""

    run_id: str
    project_name: str
    started_at: str  # ISO-8601 UTC
    elapsed_seconds: float
    ok: bool
    successes: int
    failures: int
    skipped: int
    partial: int
    disabled: int
    models: list[dict[str, Any]] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)


def history_path(project_root: Path) -> Path:
    """Canonical location of the JSONL log for ``project_root``."""
    return project_root / _HISTORY_RELATIVE


def append_run(
    project_root: Path,
    report: RunReport,
    *,
    run_id: str | None = None,
    started_at: datetime | None = None,
) -> RunHistoryEntry:
    """Append one run outcome to the project's run history.

    Always creates the ``target/`` directory if missing. The write is a
    single ``open(..., "a") + json.dumps + flush``; POSIX append is
    atomic for lines under the filesystem's page-size limit so
    concurrent runs don't interleave records.
    """
    entry = _entry_from_report(
        report,
        run_id=run_id or uuid.uuid4().hex[:16],
        started_at=started_at or datetime.now(UTC),
    )
    target = history_path(project_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry), default=_json_default) + "\n")
        fh.flush()
    log.debug("Recorded run %s for %s -> %s", entry.run_id, entry.project_name, target)
    return entry


def read_runs(project_root: Path, *, limit: int | None = None) -> list[RunHistoryEntry]:
    """Return the most recent runs (newest first) from the history log.

    Returns an empty list when no history exists. ``limit`` caps the
    number of entries returned (None = unlimited). Malformed lines are
    dropped with a debug-level warning so a corrupt file doesn't break
    the UI; real operators should investigate via the raw file.
    """
    target = history_path(project_root)
    if not target.exists():
        return []
    entries: list[RunHistoryEntry] = []
    with target.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                entries.append(RunHistoryEntry(**payload))
            except (ValueError, TypeError) as exc:
                log.debug("Skipping malformed run-history line: %s", exc)
                continue
    entries.reverse()  # newest first
    if limit is not None:
        entries = entries[:limit]
    return entries


def _entry_from_report(
    report: RunReport,
    *,
    run_id: str,
    started_at: datetime,
) -> RunHistoryEntry:
    """Translate a RunReport into the JSONL-friendly entry shape."""
    models_json: list[dict[str, Any]] = []
    for r in report.models.runs:
        entry: dict[str, Any] = {
            "name": r.model.name,
            "kind": r.model.kind.value,
            "materialization": r.model.materialization.value,
            "status": r.status,
            "elapsed_seconds": round(r.elapsed_seconds, 4),
            "error": r.error,
            "row_count": r.result.row_count if r.result else None,
            "skipped_reason": r.skipped_reason,
        }
        if r.result and r.result.statement_errors:
            entry["statement_errors"] = [
                {
                    "index": se.index,
                    "error": se.error,
                    "layer": se.layer,
                }
                for se in r.result.statement_errors
            ]
        models_json.append(entry)

    tests_json = [
        {
            "model": t.model,
            "column": t.column,
            "name": t.name,
            "passed": t.passed,
            "failing_rows": t.failing_rows,
        }
        for t in report.tests
    ]

    models_result = report.models
    return RunHistoryEntry(
        run_id=run_id,
        project_name=report.project_name,
        started_at=started_at.isoformat(),
        elapsed_seconds=round(models_result.elapsed_seconds, 4),
        ok=report.ok,
        successes=models_result.successes,
        failures=models_result.failures,
        skipped=models_result.skipped,
        partial=models_result.partial,
        disabled=models_result.disabled,
        models=models_json,
        tests=tests_json,
    )


def _json_default(obj: Any) -> Any:
    """Fallback JSON encoder for time / dataclass fields."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    # Fall through — json.dumps raises TypeError with a clearer message.
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ``time.time`` imported so tests can monkeypatch it if they need
# deterministic timestamps; not used directly here.
_ = time
