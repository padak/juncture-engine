# Keboola component image for Juncture.
#
# The Keboola runtime mounts:
#   /code  -- user project (juncture.yaml, models/, schema.yml ...)
#   /data  -- Keboola component data dir (config.json, in/, out/, artifacts/)
#
# The entrypoint reads /data/config.json, generates juncture.yaml if needed,
# stages Keboola input tables, runs the Juncture DAG, then uploads outputs.

FROM python:3.12-slim

WORKDIR /app

# System deps: DuckDB wheel is self-contained; we only need build basics.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Juncture. In production this would pin a tagged release.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[pandas]" \
    && pip uninstall -y sqlglotrs || true

COPY docker/keboola/entrypoint.py /app/entrypoint.py

ENV JUNCTURE_KEBOOLA_CONFIG=/data/config.json
ENV JUNCTURE_KEBOOLA_PROJECT=/code

ENTRYPOINT ["python", "/app/entrypoint.py"]
