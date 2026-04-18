"""Integration: ThreadingHTTPServer serves DAG + runs JSON + static assets."""

from __future__ import annotations

import json
import threading
from http.client import HTTPConnection
from pathlib import Path

import pytest

from juncture.core.runner import Runner, RunRequest
from juncture.web.server import build_app


@pytest.fixture
def project_with_history(tmp_path: Path) -> Path:
    root = tmp_path / "webproj"
    (root / "models").mkdir(parents=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: webproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "stg.sql").write_text("SELECT 1 AS id")
    (root / "models" / "mart.sql").write_text("SELECT id FROM {{ ref('stg') }}")
    Runner().run(RunRequest(project_path=root))
    return root


def _serve_in_thread(project: Path):
    """Start the web server on an ephemeral port; return (server, host, port)."""
    server = build_app(project, host="127.0.0.1", port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, "127.0.0.1", port


def _get(host: str, port: int, path: str) -> tuple[int, dict, str]:
    """Minimal JSON/text GET helper."""
    conn = HTTPConnection(host, port, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        return resp.status, dict(resp.getheaders()), body
    finally:
        conn.close()


def test_index_html_served_at_root(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/")
        assert status == 200
        assert "<title>Juncture" in body
    finally:
        server.shutdown()
        server.server_close()


def test_cytoscape_js_vendored_and_served(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, headers, body = _get(host, port, "/assets/cytoscape.min.js")
        assert status == 200
        assert "javascript" in headers.get("Content-Type", "").lower()
        # sanity: the real cytoscape file is hundreds of KB, not an empty stub
        assert len(body) > 100_000
    finally:
        server.shutdown()
        server.server_close()


def test_api_manifest_returns_dag(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/manifest")
        assert status == 200
        payload = json.loads(body)
        assert payload["project"] == "webproj"
        names = {m["name"] for m in payload["models"]}
        assert {"stg", "mart"} <= names
        # disabled flag is present (False for both here)
        assert all("disabled" in m for m in payload["models"])
        # P0.2: relative path per model so the frontend can hint source location.
        paths = {m["name"]: m["path"] for m in payload["models"]}
        assert paths["stg"] == "models/stg.sql"
        assert paths["mart"] == "models/mart.sql"
    finally:
        server.shutdown()
        server.server_close()


def test_api_model_detail_sql_returns_source_and_rendered(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/models/mart")
        assert status == 200
        detail = json.loads(body)
        assert detail["name"] == "mart"
        assert detail["kind"] == "sql"
        assert detail["path"] == "models/mart.sql"
        # Verbatim body preserves the ref() macro for the raw view.
        assert "{{ ref('stg') }}" in detail["sql"]
        # Rendered body resolves refs to schema.name for the "executed SQL" tab.
        assert "{{ ref('stg') }}" not in detail["sql_rendered"]
        assert "main.stg" in detail["sql_rendered"]
        assert detail["depends_on"] == ["stg"]
        assert detail["python_source"] is None
    finally:
        server.shutdown()
        server.server_close()


def test_api_model_detail_unknown_returns_404(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/models/nope")
        assert status == 404
        assert "error" in json.loads(body)
    finally:
        server.shutdown()
        server.server_close()


def test_api_model_detail_python_exposes_source(tmp_path: Path) -> None:
    root = tmp_path / "pyproj"
    (root / "models").mkdir(parents=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: pyproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "seed.sql").write_text("SELECT 1 AS id")
    (root / "models" / "scored.py").write_text(
        '''"""A Python model that depends on a SQL staging model."""
from juncture import transform


@transform(depends_on=["seed"])
def scored(ctx):
    return ctx.ref("seed")
'''
    )
    Runner().run(RunRequest(project_path=root))

    server, host, port = _serve_in_thread(root)
    try:
        status, _, body = _get(host, port, "/api/models/scored")
        assert status == 200
        detail = json.loads(body)
        assert detail["kind"] == "python"
        assert detail["sql"] is None
        assert "@transform" in detail["python_source"]
        assert "def scored" in detail["python_source"]
        assert detail["depends_on"] == ["seed"]
    finally:
        server.shutdown()
        server.server_close()


def test_api_model_detail_exposes_columns_and_tests(tmp_path: Path) -> None:
    root = tmp_path / "schemaproj"
    (root / "models").mkdir(parents=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: schemaproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "stg.sql").write_text("SELECT 1 AS id, 'a' AS name")
    (root / "models" / "schema.yml").write_text(
        """version: 2
models:
  - name: stg
    description: Staging table.
    columns:
      - name: id
        description: Primary key.
        tests: [not_null, unique]
      - name: name
        tests:
          - accepted_values:
              values: [a, b, c]
"""
    )
    Runner().run(RunRequest(project_path=root))

    server, host, port = _serve_in_thread(root)
    try:
        status, _, body = _get(host, port, "/api/models/stg")
        assert status == 200
        detail = json.loads(body)
        cols = {c["name"] for c in detail["columns"]}
        assert cols == {"id", "name"}
        tests_by_col = {(t["column"], t["name"]) for t in detail["tests"]}
        assert ("id", "not_null") in tests_by_col
        assert ("id", "unique") in tests_by_col
        assert ("name", "accepted_values") in tests_by_col
        accepted = next(t for t in detail["tests"] if t["name"] == "accepted_values")
        assert accepted["config"]["values"] == ["a", "b", "c"]
    finally:
        server.shutdown()
        server.server_close()


def test_api_runs_returns_history_summary(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/runs")
        assert status == 200
        payload = json.loads(body)
        assert len(payload["runs"]) == 1
        run = payload["runs"][0]
        assert run["ok"] is True
        assert run["successes"] >= 2
    finally:
        server.shutdown()
        server.server_close()


def test_api_run_detail_returns_model_list(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        _, _, runs_body = _get(host, port, "/api/runs")
        run_id = json.loads(runs_body)["runs"][0]["run_id"]
        status, _, body = _get(host, port, f"/api/runs/{run_id}")
        assert status == 200
        detail = json.loads(body)
        assert detail["run_id"] == run_id
        assert len(detail["models"]) >= 2
    finally:
        server.shutdown()
        server.server_close()


def test_api_unknown_run_id_returns_404(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/runs/does-not-exist")
        assert status == 404
        assert "error" in json.loads(body)
    finally:
        server.shutdown()
        server.server_close()


def test_static_directory_traversal_blocked(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, _ = _get(host, port, "/assets/../../../../etc/passwd")
        # Either 403 or 404 is acceptable — what matters is we don't leak.
        assert status in (403, 404)
    finally:
        server.shutdown()
        server.server_close()
