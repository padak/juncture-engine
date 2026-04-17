# Keboola component image

Builds a Keboola-compatible container that wraps the Juncture engine. A
Keboola transformation using this component receives the standard
`/data/config.json` and mounts the user's project at `/code`.

## Build locally

```bash
docker build -t juncture-keboola -f docker/keboola/Dockerfile .
```

## Smoke test

Create a minimal mock Keboola config + project and run the image against it.

```bash
mkdir -p /tmp/kb_smoke/code /tmp/kb_smoke/data
cat > /tmp/kb_smoke/data/config.json <<'EOF'
{
  "parameters": {
    "project_path": "/code",
    "connection": "from-keboola",
    "threads": 2,
    "run_tests": true
  },
  "image_parameters": { "backend": "duckdb", "duckdb_path": "/data/workspace.duckdb" },
  "storage": { "input": { "tables": [] }, "output": { "tables": [] } }
}
EOF
cp -R examples/simple/* /tmp/kb_smoke/code/
rm -f /tmp/kb_smoke/code/juncture.yaml   # let the wrapper generate it

docker run --rm \
  -v /tmp/kb_smoke/code:/code \
  -v /tmp/kb_smoke/data:/data \
  juncture-keboola
```

You should see the standard Juncture run table followed by the test table.
The DuckDB file ends up at `/tmp/kb_smoke/data/workspace.duckdb`.

## Status

This is a **skeleton**. Input table staging and output upload via SAPI are
stubs that log intended actions today; full wiring lands in v0.4 per
[`docs/ROADMAP.md`](../../docs/ROADMAP.md).
