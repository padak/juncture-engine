"""Local web render for a Juncture project.

Stdlib-only HTTP server + vanilla-JS single-page UI: DAG view, run
history, per-model drilldown. No build step, no npm, no extras dep —
runs against any Juncture project on a developer laptop or CI box.

Exposed entry points:

- :func:`juncture.web.server.build_app` — wires a project path into a
  :class:`http.server.ThreadingHTTPServer` with the correct static
  assets directory.
- :func:`juncture.web.server.serve` — blocking ``serve_forever`` loop
  intended for the ``juncture web`` CLI command.

See ``docs/DESIGN.md`` §3.16 for the architecture and the JSON shape
the frontend consumes.
"""

from juncture.web.server import build_app, serve

__all__ = ["build_app", "serve"]
