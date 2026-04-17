"""Keboola runner: translate a Keboola component config into a Juncture run.

Usage inside the Keboola component Docker image::

    from juncture.keboola import KeboolaRunner

    KeboolaRunner.from_keboola_config_json("/data/config.json").run()

The Keboola config.json looks roughly like::

    {
      "storage": {
        "input":  { "tables": [...] },
        "output": { "tables": [...] }
      },
      "parameters": {
        "project_path": "/code",
        "connection": "from-keboola",
        "select": ["+fct_orders"],
        "threads": 4,
        "vars": {"run_date": "2026-04-17"}
      },
      "image_parameters": {
        "backend": "duckdb",
        "duckdb_path": "/data/workspace.duckdb"
      },
      "authorization": { ... backend-specific credentials ... }
    }

The wrapper does three things:

1. Builds a ``juncture.yaml`` on the fly based on the image parameters
   (so that users don't have to author connections themselves).
2. Copies Keboola input tables into DuckDB (or registers them in the
   warehouse connection), making them available to ``ref()``.
3. After the run, uploads outputs declared in ``storage.output.tables``
   back to Keboola Storage (via SAPI).

**Status**: skeleton. MVP only wires the Juncture engine; steps 2 and 3 are
stubs that log what they would do. Full SAPI upload lands in v0.4.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from juncture.core.runner import Runner, RunRequest

log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class KeboolaConfig:
    """Simplified view of Keboola's ``/data/config.json`` file."""

    project_path: Path
    connection: str = "from-keboola"
    select: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    threads: int = 4
    run_tests: bool = True
    vars: dict[str, Any] = field(default_factory=dict)
    backend: str = "duckdb"
    duckdb_path: Path | None = None
    input_tables: list[dict[str, Any]] = field(default_factory=list)
    output_tables: list[dict[str, Any]] = field(default_factory=list)
    authorization: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> KeboolaConfig:
        params = raw.get("parameters", {}) or {}
        image = raw.get("image_parameters", {}) or {}
        storage = raw.get("storage", {}) or {}
        duckdb_path = image.get("duckdb_path")
        return cls(
            project_path=Path(params.get("project_path", "/code")),
            connection=params.get("connection", "from-keboola"),
            select=params.get("select", []),
            exclude=params.get("exclude", []),
            threads=int(params.get("threads", 4)),
            run_tests=bool(params.get("run_tests", True)),
            vars=params.get("vars", {}),
            backend=image.get("backend", "duckdb"),
            duckdb_path=Path(duckdb_path) if duckdb_path else None,
            input_tables=storage.get("input", {}).get("tables", []),
            output_tables=storage.get("output", {}).get("tables", []),
            authorization=raw.get("authorization", {}) or {},
        )


@dataclass(kw_only=True)
class KeboolaRunner:
    """Runs a Juncture project inside a Keboola component container."""

    config: KeboolaConfig

    @classmethod
    def from_keboola_config_json(cls, path: str | Path) -> KeboolaRunner:
        return cls(config=KeboolaConfig.from_json(json.loads(Path(path).read_text())))

    def run(self) -> bool:
        """Execute the Juncture project and return True if everything passed."""
        self._ensure_project_yaml()
        self._stage_input_tables()

        report = Runner().run(
            RunRequest(
                project_path=self.config.project_path,
                select=self.config.select,
                exclude=self.config.exclude,
                connection=self.config.connection,
                threads=self.config.threads,
                run_tests=self.config.run_tests,
                run_vars=self.config.vars,
            )
        )

        self._upload_outputs(report)
        return report.ok

    def _ensure_project_yaml(self) -> None:
        """Generate a juncture.yaml for the Keboola runtime if the user didn't ship one."""
        juncture_yaml = self.config.project_path / "juncture.yaml"
        if juncture_yaml.exists():
            return
        if self.config.backend == "duckdb":
            path = self.config.duckdb_path or (self.config.project_path / "data" / "juncture.duckdb")
            juncture_yaml.write_text(
                f"""name: keboola_job
profile: {self.config.connection}
default_schema: main

connections:
  {self.config.connection}:
    type: duckdb
    path: {path}
    threads: {self.config.threads}
"""
            )
        else:  # pragma: no cover -- v0.4 will cover Snowflake/BigQuery here
            raise NotImplementedError(f"Keboola backend {self.config.backend!r} not yet wired up")

    def _stage_input_tables(self) -> None:
        """Register Keboola input tables as seeds or external tables.

        MVP: log the list; the adapter-level implementation lands in v0.4
        (SAPI download + DuckDB register for DuckDB backend, or external
        tables for Snowflake/BigQuery).
        """
        if not self.config.input_tables:
            return
        log.info("[keboola] %d input tables would be staged:", len(self.config.input_tables))
        for t in self.config.input_tables:
            log.info("  - %s -> %s", t.get("source"), t.get("destination"))

    def _upload_outputs(self, report: Any) -> None:
        """Upload Juncture materialized tables back to Keboola Storage.

        MVP: log planned uploads. Real SAPI integration lands in v0.4.
        """
        if not self.config.output_tables:
            return
        log.info("[keboola] %d output tables would be uploaded:", len(self.config.output_tables))
        for t in self.config.output_tables:
            log.info("  - %s -> %s", t.get("source"), t.get("destination"))
