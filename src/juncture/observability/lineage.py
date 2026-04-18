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


# --- Static manifest → OpenLineage shape (used by the web export) -------
# RFC 0001 §5.2 P1.5: the UI lets a user download manifest.openlineage.json
# as a one-shot static export. This produces one RunEvent per model plus
# the upstream Dataset references, so an OpenLineage-aware catalog can
# bootstrap its Job + Dataset registry off a Juncture project without
# running anything. The shape follows OpenLineage spec 1.0.5.

_OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json"
_OPENLINEAGE_PRODUCER = "https://github.com/padak/juncture-engine"


def manifest_to_openlineage_events(
    manifest: dict[str, Any],
    *,
    namespace: str | None = None,
    event_time: str | None = None,
) -> list[dict[str, Any]]:
    """Convert a ``/api/manifest`` payload into OpenLineage RunEvent dicts.

    One ``RunEvent`` (eventType=COMPLETE) is emitted per model. Inputs
    list every declared dependency as a Dataset; outputs list the model
    itself. Seeds appear both as independent events and as inputs for
    downstream models.

    The function returns plain ``dict`` objects (not the openlineage-python
    SDK classes) so the endpoint works without the optional
    ``openlineage-python`` install. ``namespace`` defaults to
    ``juncture:<project>``. ``event_time`` is an ISO-8601 string; the
    default is the current UTC clock, per RFC §12 question 4.
    """
    ns = namespace or f"juncture:{manifest.get('project', 'unknown')}"
    ts = event_time or _iso_now()
    events: list[dict[str, Any]] = []
    for model in manifest.get("models", []):
        inputs = [{"namespace": ns, "name": dep} for dep in model.get("depends_on", [])]
        outputs = [
            {
                "namespace": ns,
                "name": model["name"],
                "facets": {
                    "dataSource": {
                        "_producer": _OPENLINEAGE_PRODUCER,
                        "_schemaURL": f"{_OPENLINEAGE_SCHEMA_URL}#/$defs/DataSourceDatasetFacet",
                        "name": ns,
                    },
                    "junctureModel": {
                        "_producer": _OPENLINEAGE_PRODUCER,
                        "_schemaURL": f"{_OPENLINEAGE_SCHEMA_URL}#/$defs/DatasetFacet",
                        "kind": model.get("kind"),
                        "materialization": model.get("materialization"),
                        "disabled": bool(model.get("disabled", False)),
                        "tags": list(model.get("tags") or []),
                    },
                },
            }
        ]
        events.append(
            {
                "eventType": "COMPLETE",
                "eventTime": ts,
                "producer": _OPENLINEAGE_PRODUCER,
                "schemaURL": f"{_OPENLINEAGE_SCHEMA_URL}#/$defs/RunEvent",
                "run": {"runId": model["name"]},  # 1 synthetic run per model
                "job": {"namespace": ns, "name": model["name"]},
                "inputs": inputs,
                "outputs": outputs,
            }
        )
    return events
