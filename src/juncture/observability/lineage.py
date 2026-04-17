"""OpenLineage event emission.

Protocol: for every model run, we emit three events:

* ``START`` (when the job begins)
* ``COMPLETE`` (success)
* ``FAIL`` (exception)

Each event has ``inputs`` (upstream models) and ``outputs`` (this model).
Column-level facets are planned for v1; for MVP we emit table-level lineage.

If ``openlineage-python`` is not installed, the emitter is a no-op. To
enable: ``pip install 'juncture[lineage]'`` and set
``JUNCTURE_OPENLINEAGE_URL`` to the Marquez/Atlan/Datahub OL endpoint.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.core.model import Model

log = logging.getLogger(__name__)


class LineageEmitter(ABC):
    """Interface any emitter must implement."""

    @abstractmethod
    def start(self, model: Model, run_id: str, upstream: list[str], downstream: list[str]) -> None: ...
    @abstractmethod
    def complete(self, model: Model, run_id: str, row_count: int | None) -> None: ...
    @abstractmethod
    def fail(self, model: Model, run_id: str, error: str) -> None: ...


class NullLineageEmitter(LineageEmitter):
    """Default no-op emitter. Keeps the runner code branch-free."""

    def start(self, *a: Any, **kw: Any) -> None:
        pass

    def complete(self, *a: Any, **kw: Any) -> None:
        pass

    def fail(self, *a: Any, **kw: Any) -> None:
        pass


@dataclass(kw_only=True)
class OpenLineageEmitter(LineageEmitter):
    """Emits events compliant with the OpenLineage spec.

    We import the SDK lazily so Juncture's core stays dependency-free.
    """

    namespace: str = "juncture"
    job_namespace: str = "juncture"
    url: str | None = field(default_factory=lambda: os.environ.get("JUNCTURE_OPENLINEAGE_URL"))
    _client: Any = field(default=None, init=False)

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        try:
            from openlineage.client import OpenLineageClient
            from openlineage.client.transport.http import HttpConfig, HttpTransport
        except ImportError:  # pragma: no cover
            log.warning("openlineage-python not installed; lineage events will be logged only")
            self._client = "log-only"
            return
        if self.url:
            transport = HttpTransport(HttpConfig(url=self.url))
            self._client = OpenLineageClient(transport=transport)
        else:
            self._client = OpenLineageClient()

    def _emit(self, event_type: str, run_id: str, job_name: str, **fields: Any) -> None:
        self._ensure_client()
        payload = {
            "eventType": event_type,
            "eventTime": _iso_now(),
            "run": {"runId": run_id},
            "job": {"namespace": self.job_namespace, "name": job_name},
            **fields,
        }
        if self._client == "log-only":
            log.info("[openlineage] %s", payload)
            return
        try:
            from openlineage.client.facet import Facet  # noqa: F401
            from openlineage.client.run import Dataset, Job, Run, RunEvent

            event = RunEvent(
                eventType=event_type,
                eventTime=payload["eventTime"],
                run=Run(runId=run_id),
                job=Job(namespace=self.job_namespace, name=job_name),
                inputs=[Dataset(namespace=self.namespace, name=i) for i in fields.get("inputs", [])],
                outputs=[Dataset(namespace=self.namespace, name=o) for o in fields.get("outputs", [])],
                producer="juncture",
            )
            self._client.emit(event)
        except Exception as exc:  # pragma: no cover -- don't let lineage fail a run
            log.warning("[openlineage] emit failed: %s", exc)

    def start(self, model: Model, run_id: str, upstream: list[str], downstream: list[str]) -> None:
        self._emit(
            "START",
            run_id,
            model.name,
            inputs=upstream,
            outputs=[model.name],
        )

    def complete(self, model: Model, run_id: str, row_count: int | None) -> None:
        self._emit(
            "COMPLETE",
            run_id,
            model.name,
            outputs=[model.name],
            rowCount=row_count,
        )

    def fail(self, model: Model, run_id: str, error: str) -> None:
        self._emit(
            "FAIL",
            run_id,
            model.name,
            outputs=[model.name],
            error=error,
        )


def _iso_now() -> str:
    from datetime import datetime

    return datetime.now(UTC).isoformat()
