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


def test_api_project_config_returns_raw_and_parsed(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/project/config")
        assert status == 200
        cfg = json.loads(body)
        assert cfg["path"] == "juncture.yaml"
        assert "name: webproj" in cfg["raw"]
        assert cfg["parsed"]["name"] == "webproj"
        assert cfg["parsed"]["profile"] == "local"
    finally:
        server.shutdown()
        server.server_close()


def test_api_project_readme_missing_returns_null(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/project/readme")
        assert status == 200
        payload = json.loads(body)
        assert payload["markdown"] is None
    finally:
        server.shutdown()
        server.server_close()


def test_api_project_readme_present_returns_markdown(project_with_history: Path) -> None:
    (project_with_history / "README.md").write_text("# webproj\n\nToy test project.\n")
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/project/readme")
        assert status == 200
        payload = json.loads(body)
        assert payload["filename"] == "README.md"
        assert "# webproj" in payload["markdown"]
    finally:
        server.shutdown()
        server.server_close()


def test_api_project_git_missing_returns_unavailable(project_with_history: Path) -> None:
    # tmp_path is not a git checkout — endpoint must degrade gracefully.
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/project/git")
        assert status == 200
        payload = json.loads(body)
        assert payload["available"] is False
    finally:
        server.shutdown()
        server.server_close()


def test_api_manifest_openlineage_shape(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/manifest/openlineage")
        assert status == 200
        payload = json.loads(body)
        assert payload["project"] == "webproj"
        # One RunEvent per model.
        names = {e["job"]["name"] for e in payload["events"]}
        assert {"stg", "mart"} <= names
        mart_event = next(e for e in payload["events"] if e["job"]["name"] == "mart")
        # mart depends on stg → stg appears in inputs as a Dataset.
        assert any(i["name"] == "stg" for i in mart_event["inputs"])
        # Outputs carry the kind/materialization facet we emit.
        facet = mart_event["outputs"][0]["facets"]["junctureModel"]
        assert facet["kind"] == "sql"
        assert facet["materialization"] == "table"
    finally:
        server.shutdown()
        server.server_close()


def test_api_seeds_empty_when_no_seeds(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/seeds")
        assert status == 200
        assert json.loads(body) == {"seeds": []}
    finally:
        server.shutdown()
        server.server_close()


def test_api_seeds_exposes_csv_seed(tmp_path: Path) -> None:
    root = tmp_path / "seedproj"
    (root / "models").mkdir(parents=True)
    (root / "seeds").mkdir(parents=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: seedproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "seeds" / "raw_users.csv").write_text("id,name\n1,alice\n2,bob\n")
    (root / "models" / "mart.sql").write_text("SELECT id FROM {{ ref('raw_users') }}")
    Runner().run(RunRequest(project_path=root))

    server, host, port = _serve_in_thread(root)
    try:
        status, _, body = _get(host, port, "/api/seeds")
        assert status == 200
        payload = json.loads(body)
        assert [s["name"] for s in payload["seeds"]] == ["raw_users"]
        seed = payload["seeds"][0]
        assert seed["format"] == "csv"
        assert seed["path"].endswith("raw_users.csv")
        # CSV seeds currently record row_count=None in the run history; the
        # seeds endpoint surfaces whatever the history has (unchanged here).
        assert "row_count" in seed
        # No cached seed_sentinels.json in a fresh project — sentinels is empty.
        assert seed["sentinels"] == {}
    finally:
        server.shutdown()
        server.server_close()


def test_api_model_history_returns_sparkline_data(project_with_history: Path) -> None:
    # Add a second run to give the history more than one data point.
    Runner().run(RunRequest(project_path=project_with_history))
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/models/mart/history?limit=20")
        assert status == 200
        payload = json.loads(body)
        assert payload["model"] == "mart"
        assert len(payload["runs"]) == 2
        assert all(r["status"] == "success" for r in payload["runs"])
        assert payload["p50_elapsed_seconds"] is not None
        assert payload["success_rate_30d"] == 1.0
    finally:
        server.shutdown()
        server.server_close()


def test_api_run_diagnostics_bucketizes_errors(tmp_path: Path) -> None:
    """Feed a handcrafted run_history entry with mixed error shapes."""
    from juncture.core.run_history import history_path

    root = tmp_path / "diagproj"
    (root / "models").mkdir(parents=True)
    db_path = root / "out.duckdb"
    (root / "juncture.yaml").write_text(
        f"""name: diagproj
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {db_path}
"""
    )
    (root / "models" / "stg.sql").write_text("SELECT 1 AS id")
    Runner().run(RunRequest(project_path=root))

    # Append a synthetic failing run with three statement errors the
    # classifier recognises as conversion + missing_object.
    fake = {
        "run_id": "fakefakefake0001",
        "project_name": "diagproj",
        "started_at": "2026-04-18T12:00:00+00:00",
        "elapsed_seconds": 0.1,
        "ok": False,
        "successes": 0,
        "failures": 1,
        "skipped": 0,
        "partial": 0,
        "disabled": 0,
        "models": [
            {
                "name": "bad",
                "kind": "sql",
                "materialization": "execute",
                "status": "failed",
                "elapsed_seconds": 0.01,
                "error": "boom",
                "row_count": None,
                "skipped_reason": None,
                "statement_errors": [
                    {
                        "index": 0,
                        "layer": 0,
                        "error": "Conversion Error: Could not convert string '' to INT64",
                    },
                    {
                        "index": 1,
                        "layer": 0,
                        "error": "Conversion Error: Could not convert string 'n/a' to BIGINT",
                    },
                    {
                        "index": 2,
                        "layer": 1,
                        "error": "Catalog Error: Table with name whatever does not exist",
                    },
                ],
            }
        ],
        "tests": [],
    }
    hp = history_path(root)
    hp.parent.mkdir(parents=True, exist_ok=True)
    with hp.open("a") as fh:
        fh.write(json.dumps(fake) + "\n")

    server, host, port = _serve_in_thread(root)
    try:
        status, _, body = _get(host, port, "/api/runs/fakefakefake0001/diagnostics")
        assert status == 200
        payload = json.loads(body)
        assert payload["buckets"]["conversion"] == 2
        assert payload["buckets"]["missing_object"] == 1
        assert "bad" in payload["per_model"]
        entries = payload["per_model"]["bad"]
        assert entries[0]["bucket"] == "conversion"
        assert entries[2]["bucket"] == "missing_object"
    finally:
        server.shutdown()
        server.server_close()


def test_api_llm_knowledge_bundles_everything(project_with_history: Path) -> None:
    server, host, port = _serve_in_thread(project_with_history)
    try:
        status, _, body = _get(host, port, "/api/llm-knowledge")
        assert status == 200
        kb = json.loads(body)
        assert kb["format_version"] == "1"
        assert kb["project"]["name"] == "webproj"
        model_names = {m["name"] for m in kb["models"]}
        assert {"stg", "mart"} <= model_names
        mart = next(m for m in kb["models"] if m["name"] == "mart")
        assert mart["sql"] and "{{ ref('stg') }}" in mart["sql"]
        assert mart["sql_rendered"] and "main.stg" in mart["sql_rendered"]
        assert kb["dag"]["order"] == ["stg", "mart"]
        assert kb["latest_run"]["ok"] is True
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
