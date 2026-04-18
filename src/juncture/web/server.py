"""stdlib HTTP server for the Juncture web render.

Design constraints (from CLAUDE.md + Phase 1 gate):

- No extras dependency. ``http.server`` + ``json`` + ``pathlib`` only.
- Local-first. Binds to ``127.0.0.1`` by default; no auth, no HTTPS.
- Read-only in Phase 1. POST endpoints would want FastAPI; we skip
  them until a trigger-run feature is actually requested.

Routes:

- ``GET /`` → ``static/index.html``
- ``GET /assets/<path>`` → files under ``static/``
- ``GET /api/manifest`` → DAG + per-model metadata (disabled, tags, path)
- ``GET /api/manifest/openlineage`` → manifest in OpenLineage RunEvent shape
- ``GET /api/models/<name>`` → per-model detail (source + columns + tests)
- ``GET /api/models/<name>/history`` → last-N run outcomes for reliability
- ``GET /api/runs`` → ``?limit=N`` run history summary
- ``GET /api/runs/<run_id>`` → the full entry for that run
- ``GET /api/runs/<run_id>/diagnostics`` → classified statement errors
- ``GET /api/seeds`` → per-seed metadata (format, inferred types, sentinels)
- ``GET /api/portfolio`` → model x governance x SLA x last-run aggregation
- ``GET /api/models/<name>/contract`` → columns + tests + downstream blast radius
- ``GET /api/models/<name>/docs`` → long-form markdown (docs: field)
- ``GET /api/reliability`` → 7/30-day SLA attainment + top slow / top failing
- ``GET /api/llm-knowledge`` → single-shot LLM-friendly snapshot of the project
- ``GET /api/project`` → project name + config snapshot
- ``GET /api/project/config`` → full ``juncture.yaml`` parsed shape
- ``GET /api/project/readme`` → raw README markdown (if present)
- ``GET /api/project/git`` → last commit + branch (best-effort)

Everything returns JSON with ``Content-Type: application/json;
charset=utf-8``; static assets set their MIME type from the file
extension.
"""

from __future__ import annotations

import contextlib
import json
import logging
import mimetypes
import subprocess
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import yaml

from juncture.core.model import Model, ModelKind
from juncture.core.project import Project
from juncture.core.run_history import read_runs
from juncture.diagnostics import classify_statement_errors
from juncture.observability.lineage import manifest_to_openlineage_events
from juncture.parsers.sqlglot_parser import render_refs

log = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def build_app(project_path: Path, *, host: str, port: int, profile: str | None = None) -> ThreadingHTTPServer:
    """Factory wiring ``project_path`` into a threading HTTP server.

    The project is reloaded on every request so schema.yml / model
    edits surface without restarting — the cost is a few ms per
    request on a 300-model project, which we happily eat for DX.

    ``profile`` pins the active profile from ``juncture.yaml``'s
    ``profiles:`` block for the lifetime of the web session; all
    payload builders pass it to :meth:`Project.load`.
    """
    handler_cls = _make_handler(project_path, profile=profile)
    return ThreadingHTTPServer((host, port), handler_cls)


def serve(
    project_path: Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    profile: str | None = None,
) -> None:
    """Blocking ``serve_forever`` loop; used by ``juncture web``."""
    server = build_app(project_path.resolve(), host=host, port=port, profile=profile)
    log.info("juncture web: serving %s on http://%s:%s", project_path, host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down (SIGINT)")
    finally:
        server.server_close()


def _make_handler(project_path: Path, *, profile: str | None = None) -> type[BaseHTTPRequestHandler]:
    """Return a handler class closed over ``project_path`` (+ optional profile).

    ``http.server`` constructs handlers per-request, so we close the
    path into the class body rather than passing it through the
    constructor.
    """
    active_profile = profile

    class JunctureHandler(BaseHTTPRequestHandler):
        # Override the noisy default access log; route it to our logger.
        def log_message(self, fmt: str, *args: Any) -> None:
            log.info("%s - %s", self.address_string(), fmt % args)

        # --- HTTP routing -------------------------------------------------
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/" or path == "/index.html":
                    self._send_static("index.html")
                elif path.startswith("/assets/"):
                    self._send_static(path[len("/assets/") :])
                elif path == "/api/project":
                    self._send_json(self._project_payload())
                elif path == "/api/project/config":
                    self._send_json(self._project_config_payload())
                elif path == "/api/project/readme":
                    self._send_json(self._project_readme_payload())
                elif path == "/api/project/git":
                    self._send_json(self._project_git_payload())
                elif path == "/api/manifest":
                    self._send_json(self._manifest_payload())
                elif path == "/api/manifest/openlineage":
                    self._send_json(self._manifest_openlineage_payload())
                elif path == "/api/seeds":
                    self._send_json(self._seeds_payload())
                elif path == "/api/portfolio":
                    self._send_json(self._portfolio_payload())
                elif path == "/api/reliability":
                    self._send_json(self._reliability_payload())
                elif path == "/api/llm-knowledge":
                    self._send_json(self._llm_knowledge_payload())
                elif path.startswith("/api/models/") and path.endswith("/contract"):
                    name = unquote(path[len("/api/models/") : -len("/contract")])
                    self._send_json(self._model_contract_payload(name))
                elif path.startswith("/api/models/") and path.endswith("/docs"):
                    name = unquote(path[len("/api/models/") : -len("/docs")])
                    self._send_json(self._model_docs_payload(name))
                elif path.startswith("/api/models/") and path.endswith("/history"):
                    name = unquote(path[len("/api/models/") : -len("/history")])
                    query = parse_qs(parsed.query or "")
                    limit = int(query.get("limit", ["20"])[0])
                    self._send_json(self._model_history_payload(name, limit=limit))
                elif path.startswith("/api/models/"):
                    name = unquote(path[len("/api/models/") :])
                    if not name:
                        self._send_error(404, "Not found")
                        return
                    self._send_json(self._model_detail_payload(name))
                elif path == "/api/runs":
                    query = parse_qs(parsed.query or "")
                    limit = int(query.get("limit", ["50"])[0])
                    self._send_json(self._runs_payload(limit=limit))
                elif path.startswith("/api/runs/") and path.endswith("/diagnostics"):
                    run_id = path[len("/api/runs/") : -len("/diagnostics")]
                    self._send_json(self._run_diagnostics_payload(run_id))
                elif path.startswith("/api/runs/"):
                    run_id = path[len("/api/runs/") :]
                    self._send_json(self._run_detail_payload(run_id))
                else:
                    self._send_error(404, "Not found")
            except FileNotFoundError:
                self._send_error(404, "Not found")
            except Exception as exc:  # pragma: no cover
                log.exception("Handler error")
                self._send_error(500, f"Server error: {exc}")

        # --- Payload builders ---------------------------------------------
        def _project_payload(self) -> dict[str, Any]:
            project = Project.load(project_path, profile=active_profile)
            return {
                "name": project.config.name,
                "version": project.config.version,
                "profile": project.config.profile,
                "path": str(project.root),
                "default_materialization": project.config.default_materialization.value,
                "default_schema": project.config.default_schema,
            }

        def _manifest_payload(self) -> dict[str, Any]:
            project = Project.load(project_path, profile=active_profile)
            dag = project.dag()
            # Precompute PII set so the UI can propagate a ring from seed to descendants.
            pii_seeds = {s.name for s in project.seeds if s.pii}
            pii_transitive: set[str] = set(pii_seeds)
            for name in dag.topological_order():
                parents = dag.upstream(name)
                if pii_transitive & set(parents):
                    pii_transitive.add(name)
            return {
                "project": project.config.name,
                "models": [
                    {
                        "name": m.name,
                        "kind": m.kind.value,
                        "materialization": m.materialization.value,
                        "depends_on": sorted(m.depends_on),
                        "tags": m.tags,
                        "disabled": m.disabled,
                        "description": m.description,
                        "schedule_cron": m.schedule_cron,
                        "path": _relative_path(m, project.root),
                        "governance": _governance_payload(m),
                        "pii": m.name in pii_transitive,
                    }
                    for m in dag.models()
                ],
                "seeds": [
                    {
                        "name": s.name,
                        "pii": s.pii,
                        "retention_days": s.retention_days,
                        "source_system": s.source_system,
                        "source_locator": s.source_locator,
                        "owner": s.owner,
                    }
                    for s in project.seeds
                ],
                "order": dag.topological_order(),
                "edges": [{"from": src, "to": tgt} for src in dag.nodes for tgt in dag.downstream(src)],
            }

        def _manifest_openlineage_payload(self) -> dict[str, Any]:
            # Reuses the manifest builder so the two exports stay in sync.
            manifest = self._manifest_payload()
            return {
                "project": manifest["project"],
                "events": manifest_to_openlineage_events(manifest),
            }

        def _project_config_payload(self) -> dict[str, Any]:
            """Return the parsed ``juncture.yaml`` as pure JSON (no env interpolation).

            We read the raw YAML text so the user sees exactly what they
            have on disk, including any ``${VAR}`` placeholders (the UI
            should show these verbatim — resolving them would leak env-var
            values into the browser, which we never want).
            """
            project = Project.load(project_path, profile=active_profile)
            yaml_path = project.root / "juncture.yaml"
            try:
                raw_text = yaml_path.read_text()
            except OSError:
                raw_text = ""
            try:
                parsed = yaml.safe_load(raw_text) or {}
            except yaml.YAMLError as exc:
                parsed = {"_parse_error": str(exc)}
            return {
                "path": str(yaml_path.relative_to(project.root)) if yaml_path.exists() else None,
                "raw": raw_text,
                "parsed": parsed,
            }

        def _project_readme_payload(self) -> dict[str, Any]:
            """Return README.md markdown if present, else ``markdown: null``.

            Case-insensitive filename match (README.md / readme.md) so the
            endpoint works across case-preserving filesystems.
            """
            project = Project.load(project_path, profile=active_profile)
            for candidate in ("README.md", "readme.md", "Readme.md"):
                target = project.root / candidate
                if target.is_file():
                    return {"filename": candidate, "markdown": target.read_text()}
            return {"filename": None, "markdown": None}

        def _project_git_payload(self) -> dict[str, Any]:
            """Return last-commit info + branch via ``git``; best-effort.

            Falls back to ``available=False`` when the project is not a
            git checkout, git is missing, or the shell-out fails.
            Important for the Keboola Docker wrapper (RFC §12 q.2),
            where ``git`` is intentionally absent.
            """
            project = Project.load(project_path, profile=active_profile)
            try:
                commit = subprocess.run(
                    ["git", "-C", str(project.root), "log", "-1", "--pretty=%H%n%s%n%an%n%ae%n%aI"],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=3,
                )
                branch = subprocess.run(
                    ["git", "-C", str(project.root), "rev-parse", "--abbrev-ref", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=3,
                )
            except (OSError, subprocess.SubprocessError):
                return {"available": False}
            parts = commit.stdout.rstrip("\n").split("\n")
            if len(parts) < 5:
                return {"available": False}
            return {
                "available": True,
                "sha": parts[0],
                "subject": parts[1],
                "author": parts[2],
                "email": parts[3],
                "date": parts[4],
                "branch": branch.stdout.strip(),
            }

        def _model_detail_payload(self, name: str) -> dict[str, Any]:
            project = Project.load(project_path, profile=active_profile)
            model = next((m for m in project.models if m.name == name), None)
            if model is None:
                raise FileNotFoundError(name)

            sql_body: str | None = None
            sql_rendered: str | None = None
            python_source: str | None = None
            if model.kind is ModelKind.SQL and model.sql is not None:
                sql_body = model.sql
                resolver = {m.name: f"{project.config.default_schema}.{m.name}" for m in project.models}
                sql_rendered = render_refs(model.sql, resolver)
            elif model.kind is ModelKind.PYTHON and model.path is not None:
                try:
                    python_source = model.path.read_text()
                except OSError:
                    python_source = None

            columns = [
                {
                    "name": c.name,
                    "description": c.description,
                    "tests": list(c.tests),
                    "data_type": c.data_type,
                }
                for c in model.columns
            ]
            # Column-level tests become a flat list so the frontend can
            # render them as a single table without reassembling.
            # ColumnSpec.tests is annotated list[str], but schema.yml lets
            # the element be a dict too (``- relationships: {...}``);
            # the Any cast reflects that runtime flexibility.
            tests: list[dict[str, Any]] = []
            for c in model.columns:
                for raw in c.tests:
                    t: Any = raw
                    if isinstance(t, str):
                        tests.append({"column": c.name, "name": t, "config": {}})
                        continue
                    if isinstance(t, dict) and t:
                        # schema.yml dict form: ``- relationships: {to: ref('x'), field: id}``
                        test_name, cfg = next(iter(t.items()))
                        tests.append({"column": c.name, "name": test_name, "config": cfg or {}})

            return {
                "name": model.name,
                "kind": model.kind.value,
                "materialization": model.materialization.value,
                "path": _relative_path(model, project.root),
                "description": model.description,
                "depends_on": sorted(model.depends_on),
                "tags": list(model.tags),
                "disabled": model.disabled,
                "schedule_cron": model.schedule_cron,
                "config": dict(model.config),
                "columns": columns,
                "tests": tests,
                "sql": sql_body,
                "sql_rendered": sql_rendered,
                "python_source": python_source,
                "governance": _governance_payload(model),
            }

        def _runs_payload(self, *, limit: int) -> dict[str, Any]:
            entries = read_runs(project_path, limit=limit)
            # Return summary fields only to keep the list endpoint cheap;
            # full model drilldowns live on /api/runs/<id>.
            summaries = [
                {
                    "run_id": e.run_id,
                    "started_at": e.started_at,
                    "elapsed_seconds": e.elapsed_seconds,
                    "ok": e.ok,
                    "successes": e.successes,
                    "failures": e.failures,
                    "skipped": e.skipped,
                    "partial": e.partial,
                    "disabled": e.disabled,
                }
                for e in entries
            ]
            return {"runs": summaries}

        def _run_detail_payload(self, run_id: str) -> dict[str, Any]:
            entries = read_runs(project_path)
            for e in entries:
                if e.run_id == run_id:
                    return asdict(e)
            raise FileNotFoundError(run_id)

        def _run_diagnostics_payload(self, run_id: str) -> dict[str, Any]:
            """Aggregate statement errors in a run, classified by bucket.

            Returns bucket counts at the top and per-model breakdown so the
            UI can render "type_mismatch: 7, sentinel: 3" and offer a
            click-through filter (RFC §5.2 P1.4).
            """
            entries = read_runs(project_path)
            run = next((e for e in entries if e.run_id == run_id), None)
            if run is None:
                raise FileNotFoundError(run_id)

            buckets: dict[str, int] = {}
            subcategories: dict[str, int] = {}
            per_model: dict[str, list[dict[str, Any]]] = {}

            for m in run.models:
                raw_errors = m.get("statement_errors") or []
                if not raw_errors:
                    continue
                classifications = classify_statement_errors([se.get("error", "") for se in raw_errors])
                model_entries: list[dict[str, Any]] = []
                for se, cls in zip(raw_errors, classifications, strict=False):
                    buckets[cls.bucket.value] = buckets.get(cls.bucket.value, 0) + 1
                    subcategories[cls.subcategory] = subcategories.get(cls.subcategory, 0) + 1
                    model_entries.append(
                        {
                            "index": se.get("index"),
                            "layer": se.get("layer"),
                            "error": se.get("error"),
                            "bucket": cls.bucket.value,
                            "subcategory": cls.subcategory,
                            "fix_hint": cls.fix_hint,
                            "operands": cls.operands,
                        }
                    )
                per_model[m["name"]] = model_entries
            return {
                "run_id": run.run_id,
                "buckets": buckets,
                "subcategories": subcategories,
                "per_model": per_model,
            }

        def _model_history_payload(self, name: str, *, limit: int) -> dict[str, Any]:
            """Per-model last-N run outcomes for the reliability micro-chart.

            Computes p50 / p95 elapsed and the 30-day success rate from
            whatever run_history.jsonl already has; no new persistence.
            """
            entries = read_runs(project_path)
            runs: list[dict[str, Any]] = []
            elapsed_samples: list[float] = []
            for e in entries[:limit]:
                for m in e.models:
                    if m.get("name") == name:
                        runs.append(
                            {
                                "run_id": e.run_id,
                                "started_at": e.started_at,
                                "status": m.get("status"),
                                "elapsed_seconds": m.get("elapsed_seconds"),
                                "row_count": m.get("row_count"),
                            }
                        )
                        el = m.get("elapsed_seconds")
                        if isinstance(el, int | float):
                            elapsed_samples.append(float(el))
                        break
            recent_30d = _runs_in_last_days(entries, 30, name)
            return {
                "model": name,
                "runs": runs,
                "p50_elapsed_seconds": _percentile(elapsed_samples, 0.50),
                "p95_elapsed_seconds": _percentile(elapsed_samples, 0.95),
                "success_rate_30d": _success_rate(recent_30d),
                "sample_size_30d": len(recent_30d),
            }

        def _portfolio_payload(self) -> dict[str, Any]:
            """Model x governance x last-run x 30-day SLA attainment.

            Used by the CDO Portfolio tab. Pure aggregation over
            ``run_history.jsonl`` + ``schema.yml`` governance — no DB
            hits, so a 300-model project renders in one manifest load.
            """
            from datetime import UTC, datetime

            project = Project.load(project_path, profile=active_profile)
            entries = read_runs(project_path)
            now = datetime.now(UTC)
            latest = entries[0] if entries else None
            latest_by_model: dict[str, dict[str, Any]] = {}
            if latest:
                for m in latest.models:
                    latest_by_model[m["name"]] = m

            rows: list[dict[str, Any]] = []
            for model in project.models:
                last = latest_by_model.get(model.name)
                thirty = _runs_in_last_days(entries, 30, model.name)
                success_30d = _success_rate(thirty)
                # Freshness breach: hours since last successful run > target.
                last_success_age_hours: float | None = None
                for e in entries:
                    for mm in e.models:
                        if mm.get("name") == model.name and mm.get("status") == "success":
                            try:
                                last_success_age_hours = (
                                    now - datetime.fromisoformat(e.started_at)
                                ).total_seconds() / 3600.0
                            except ValueError:
                                last_success_age_hours = None
                            break
                    if last_success_age_hours is not None:
                        break
                freshness_breach = (
                    model.sla_freshness_hours is not None
                    and last_success_age_hours is not None
                    and last_success_age_hours > model.sla_freshness_hours
                )
                success_breach = (
                    model.sla_success_rate_target is not None
                    and success_30d is not None
                    and success_30d < model.sla_success_rate_target
                )
                rows.append(
                    {
                        "name": model.name,
                        "kind": model.kind.value,
                        "materialization": model.materialization.value,
                        "governance": _governance_payload(model),
                        "last_status": last["status"] if last else None,
                        "last_elapsed_seconds": last["elapsed_seconds"] if last else None,
                        "last_started_at": latest.started_at if latest and last else None,
                        "last_success_age_hours": last_success_age_hours,
                        "success_rate_30d": success_30d,
                        "sample_30d": len(thirty),
                        "freshness_breach": bool(freshness_breach),
                        "success_breach": bool(success_breach),
                    }
                )
            return {"models": rows, "total": len(rows)}

        def _reliability_payload(self) -> dict[str, Any]:
            """Portfolio-wide reliability dashboard (RFC §5.3 P2.6)."""
            from datetime import UTC, datetime, timedelta

            project = Project.load(project_path, profile=active_profile)
            entries = read_runs(project_path)
            now = datetime.now(UTC)
            windows = {"7d": 7, "30d": 30}
            tiers: dict[str, dict[str, dict[str, int]]] = {w: {} for w in windows}
            slowest: list[dict[str, Any]] = []
            failure_buckets: dict[str, int] = {}

            for model in project.models:
                tier = model.criticality or "untagged"
                per_model = _runs_in_last_days(entries, 30, model.name)
                p95 = _percentile([m["elapsed_seconds"] for m in per_model if m.get("elapsed_seconds")], 0.95)
                if p95 is not None:
                    slowest.append({"name": model.name, "p95_elapsed_seconds": p95, "tier": tier})
                for window, days in windows.items():
                    bucket = tiers[window].setdefault(tier, {"ok": 0, "total": 0})
                    for m in per_model:
                        try:
                            started = datetime.fromisoformat(entries[0].started_at)
                        except (ValueError, IndexError):
                            continue
                        if started < now - timedelta(days=days):
                            continue
                        bucket["total"] += 1
                        if m.get("status") == "success":
                            bucket["ok"] += 1

            for e in entries[:30]:
                for m in e.models:
                    for se in m.get("statement_errors") or []:
                        cls = classify_statement_errors([se.get("error", "")])[0]
                        failure_buckets[cls.bucket.value] = failure_buckets.get(cls.bucket.value, 0) + 1

            slowest.sort(key=lambda x: x["p95_elapsed_seconds"] or 0, reverse=True)
            return {
                "tiers": {
                    window: {
                        tier: {
                            "attainment": (data["ok"] / data["total"]) if data["total"] else None,
                            "sample": data["total"],
                        }
                        for tier, data in per_tier.items()
                    }
                    for window, per_tier in tiers.items()
                },
                "slowest": slowest[:10],
                "failure_buckets": failure_buckets,
            }

        def _model_contract_payload(self, name: str) -> dict[str, Any]:
            """Per-model contract view: columns x tests + downstream blast radius."""
            project = Project.load(project_path, profile=active_profile)
            model = next((m for m in project.models if m.name == name), None)
            if model is None:
                raise FileNotFoundError(name)
            dag = project.dag()
            downstream = sorted(dag.downstream(name))
            return {
                "name": model.name,
                "columns": [
                    {
                        "name": c.name,
                        "description": c.description,
                        "data_type": c.data_type,
                        "tests": list(c.tests),
                    }
                    for c in model.columns
                ],
                "downstream": downstream,  # models that ref() this one
                "would_break": downstream,  # blast radius == direct+transitive below
            }

        def _model_docs_payload(self, name: str) -> dict[str, Any]:
            """Long-form markdown for the Metadata tab.

            Source of truth (first hit wins): ``docs:`` in schema.yml →
            ``<model_name>.md`` next to the SQL file → no docs.
            """
            project = Project.load(project_path, profile=active_profile)
            model = next((m for m in project.models if m.name == name), None)
            if model is None:
                raise FileNotFoundError(name)
            if model.docs:
                doc_path = project.root / model.docs
                if doc_path.is_file():
                    return {
                        "source": str(doc_path.relative_to(project.root)),
                        "markdown": doc_path.read_text(),
                    }
            if model.path:
                sibling = model.path.with_suffix(".md")
                if sibling.is_file():
                    return {"source": str(sibling.relative_to(project.root)), "markdown": sibling.read_text()}
            return {"source": None, "markdown": None}

        def _llm_knowledge_payload(self) -> dict[str, Any]:
            """Single-shot project snapshot shaped for LLM ingestion.

            Packs everything a model needs to answer "what does this
            project do, what are the dependencies, what's the source of
            each transformation" without chasing N endpoints. Resolves:

            - Project metadata + parsed ``juncture.yaml`` + README
            - Git head (when available)
            - Full DAG: every model with path, kind, materialization,
              dependencies, description, columns, tests, and source
              (SQL body + rendered FQN variant, or Python source)
            - Seeds with inferred types + sentinel cache
            - The most recent run outcome (not the full history; we want
              the file to stay under a few MB even on 300-model projects)

            The output is one JSON document so copy-paste into a
            ChatGPT / Claude context window or a RAG index is frictionless.
            """
            project = Project.load(project_path, profile=active_profile)
            dag = project.dag()
            manifest = self._manifest_payload()

            # Per-model deep detail reuses the single-model endpoint so the
            # format stays 1:1 with what the UI's Source tab already shows.
            models_detail: list[dict[str, Any]] = []
            for m in dag.models():
                try:
                    models_detail.append(self._model_detail_payload(m.name))
                except FileNotFoundError:
                    continue

            seeds_payload = self._seeds_payload()
            cfg_payload = self._project_config_payload()
            readme_payload = self._project_readme_payload()
            git_payload = self._project_git_payload()
            latest = read_runs(project_path, limit=1)
            latest_run = asdict(latest[0]) if latest else None

            return {
                "format_version": "1",
                "project": {
                    "name": project.config.name,
                    "version": project.config.version,
                    "profile": project.config.profile,
                    "root": str(project.root),
                    "default_materialization": project.config.default_materialization.value,
                    "default_schema": project.config.default_schema,
                },
                "config": cfg_payload,
                "readme": readme_payload,
                "git": git_payload,
                "dag": {
                    "order": manifest["order"],
                    "edges": manifest["edges"],
                },
                "models": models_detail,
                "seeds": seeds_payload["seeds"],
                "latest_run": latest_run,
            }

        def _seeds_payload(self) -> dict[str, Any]:
            """Per-seed metadata: format, path, inferred types, sentinels.

            Inferred types come from ``Project.seed_schemas()`` (cached in
            ``.juncture/seed_schemas.json``). Sentinels live in a sibling
            ``.juncture/seed_sentinels.json`` populated by the seed loader
            when run with the sentinel-detection flag; if the file is
            missing the sentinels field is an empty dict.
            """
            project = Project.load(project_path, profile=active_profile)
            schemas = project.seed_schemas()
            sentinels_path = project.root / ".juncture" / "seed_sentinels.json"
            sentinels: dict[str, dict[str, Any]] = {}
            if sentinels_path.is_file():
                try:
                    sentinels = json.loads(sentinels_path.read_text()) or {}
                except (OSError, ValueError):
                    sentinels = {}

            # Row counts: pull from the most recent run's models[] if the
            # seed appears there, else unknown.
            run_rows: dict[str, int | None] = {}
            latest = read_runs(project_path, limit=1)
            if latest:
                for m in latest[0].models:
                    if m.get("kind") == "seed":
                        run_rows[m["name"]] = m.get("row_count")

            seeds: list[dict[str, Any]] = []
            for s in project.seeds:
                parquet_files = 0
                if s.format == "parquet" and s.path.is_dir():
                    parquet_files = sum(1 for _ in s.path.glob("*.parquet"))
                seeds.append(
                    {
                        "name": s.name,
                        "format": s.format,
                        "path": _relative_path_str(s.path, project.root),
                        "parquet_files": parquet_files,
                        "inferred_types": schemas.get(s.name, {}),
                        "sentinels": sentinels.get(s.name, {}),
                        "row_count": run_rows.get(s.name),
                    }
                )
            return {"seeds": seeds}

        # --- Response helpers ---------------------------------------------
        def _send_json(self, payload: Any, *, status: int = 200) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_static(self, relative: str) -> None:
            target = (_STATIC_DIR / relative).resolve()
            static_root = _STATIC_DIR.resolve()
            # Prevent ``../`` from escaping the static dir.
            if (
                static_root not in target.parents
                and target != static_root
                and not str(target).startswith(str(static_root))
            ):
                self._send_error(403, "Forbidden")
                return
            if not target.is_file():
                raise FileNotFoundError(relative)
            mime, _ = mimetypes.guess_type(target.name)
            body = target.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mime or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, code: int, message: str) -> None:
            body = json.dumps({"error": message}).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            with contextlib.suppress(BrokenPipeError):
                self.wfile.write(body)

    return JunctureHandler


def _governance_payload(model: Model) -> dict[str, Any]:
    """Serialise the optional M4 governance fields of a Model."""
    return {
        "owner": model.owner,
        "team": model.team,
        "business_unit": model.business_unit,
        "criticality": model.criticality,
        "sla_freshness_hours": model.sla_freshness_hours,
        "sla_success_rate_target": model.sla_success_rate_target,
        "docs": model.docs,
        "consumers": list(model.consumers),
    }


def _relative_path(model: Model, root: Path) -> str | None:
    """Return the model's file path relative to the project root.

    Seeds may live outside the models directory (parquet dirs, symlinked
    seed pools); we still surface a path so the frontend can hint where
    the source lives, falling back to the absolute path when ``relative_to``
    raises ``ValueError``.
    """
    if model.path is None:
        return None
    return _relative_path_str(model.path, root)


def _relative_path_str(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _percentile(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile; ``None`` on empty input.

    Deliberately stdlib-only so the web server picks up no new deps.
    ``q`` is a fraction in ``[0, 1]``.
    """
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, round(q * (len(ordered) - 1))))
    return ordered[k]


def _runs_in_last_days(entries: list[Any], days: int, model_name: str) -> list[dict[str, Any]]:
    """Return per-model entries whose run start is within ``days`` of now."""
    from datetime import UTC, datetime, timedelta

    cutoff = datetime.now(UTC) - timedelta(days=days)
    picked: list[dict[str, Any]] = []
    for e in entries:
        try:
            started = datetime.fromisoformat(e.started_at)
        except ValueError:
            continue
        if started < cutoff:
            continue
        for m in e.models:
            if m.get("name") == model_name:
                picked.append(m)
                break
    return picked


def _success_rate(models: list[dict[str, Any]]) -> float | None:
    if not models:
        return None
    ok = sum(1 for m in models if m.get("status") == "success")
    return ok / len(models)
