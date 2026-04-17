"""Keboola component integration.

This module is imported by the Keboola wrapper Docker image. It bridges the
standalone Juncture engine to the Keboola runtime conventions:

* reads ``/data/config.json`` at runtime,
* builds an in-memory ``juncture.yaml`` that targets the Keboola workspace,
* runs the DAG and reports results back to Keboola storage.

See ``docker/keboola/`` for the Dockerfile and entrypoint script.
"""

from juncture.keboola.runner import KeboolaConfig, KeboolaRunner

__all__ = ["KeboolaConfig", "KeboolaRunner"]
