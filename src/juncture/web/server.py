"""stdlib HTTP server for the Juncture web render.

Design constraints (from CLAUDE.md + Phase 1 gate):

- No extras dependency. ``http.server`` + ``json`` + ``pathlib`` only.
- Local-first. Binds to ``127.0.0.1`` by default; no auth, no HTTPS.
- Read-only in Phase 1. POST endpoints would want FastAPI; we skip
  them until a trigger-run feature is actually requested.

Routes:

- ``GET /`` → ``static/index.html``
- ``GET /assets/<path>`` → files under ``static/``
- ``GET /api/manifest`` → DAG + per-model metadata (disabled, tags)
- ``GET /api/runs`` → ``?limit=N`` run history summary
- ``GET /api/runs/<run_id>`` → the full entry for that run
- ``GET /api/project`` → project name + config snapshot

Everything returns JSON with ``Content-Type: application/json;
charset=utf-8``; static assets set their MIME type from the file
extension.
"""

from __future__ import annotations

import contextlib
import json
import logging
import mimetypes
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from juncture.core.project import Project
from juncture.core.run_history import read_runs

log = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def build_app(project_path: Path, *, host: str, port: int) -> ThreadingHTTPServer:
    """Factory wiring ``project_path`` into a threading HTTP server.

    The project is reloaded on every request so schema.yml / model
    edits surface without restarting — the cost is a few ms per
    request on a 300-model project, which we happily eat for DX.
    """
    handler_cls = _make_handler(project_path)
    return ThreadingHTTPServer((host, port), handler_cls)


def serve(project_path: Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    """Blocking ``serve_forever`` loop; used by ``juncture web``."""
    server = build_app(project_path.resolve(), host=host, port=port)
    log.info("juncture web: serving %s on http://%s:%s", project_path, host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down (SIGINT)")
    finally:
        server.server_close()


def _make_handler(project_path: Path) -> type[BaseHTTPRequestHandler]:
    """Return a handler class closed over ``project_path``.

    ``http.server`` constructs handlers per-request, so we close the
    path into the class body rather than passing it through the
    constructor.
    """

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
                elif path == "/api/manifest":
                    self._send_json(self._manifest_payload())
                elif path == "/api/runs":
                    query = parse_qs(parsed.query or "")
                    limit = int(query.get("limit", ["50"])[0])
                    self._send_json(self._runs_payload(limit=limit))
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
            project = Project.load(project_path)
            return {
                "name": project.config.name,
                "version": project.config.version,
                "profile": project.config.profile,
                "path": str(project.root),
                "default_materialization": project.config.default_materialization.value,
                "default_schema": project.config.default_schema,
            }

        def _manifest_payload(self) -> dict[str, Any]:
            project = Project.load(project_path)
            dag = project.dag()
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
                    }
                    for m in dag.models()
                ],
                "order": dag.topological_order(),
                "edges": [{"from": src, "to": tgt} for src in dag.nodes for tgt in dag.downstream(src)],
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
