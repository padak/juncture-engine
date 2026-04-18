# Troubleshooting + debugging

Concrete recipes for common failure modes. Read top-to-bottom on the
first failure of a session; jump to the matching section thereafter.

## Table of contents

1. [Triage workflow](#1-triage-workflow)
2. [Project & DAG errors](#2-project--dag-errors)
3. [Adapter / connection errors](#3-adapter--connection-errors)
4. [Seed errors](#4-seed-errors)
5. [Profile errors](#5-profile-errors)
6. [Test failures](#6-test-failures)
7. [Migration errors (EXECUTE)](#7-migration-errors-execute)
8. [Reading `run_history.jsonl`](#8-reading-run_historyjsonl)
9. [Reading the manifest](#9-reading-the-manifest)
10. [Dialect translation issues](#10-dialect-translation-issues)

---

## 1. Triage workflow

When something fails, run these in order:

```bash
# 1. Is the project structure sane?
juncture compile --project PATH --json | jq '.models | length'

# 2. What did the runner actually do?
juncture run --project PATH --select +failing_model --dry-run

# 3. Run with verbose error capture (for EXECUTE bodies)
juncture run --project PATH --continue-on-error

# 4. Bucket the errors
juncture debug diagnostics --project PATH

# 5. Inspect the materialised data (DuckDB CLI)
duckdb data/<project>.duckdb -c '.tables'
duckdb data/<project>.duckdb -c 'SELECT * FROM main.<failing_model> LIMIT 5'
```

Each step rules out a layer: 1 = config/discovery, 2 = DAG plan, 3 =
execution surface, 4 = error classification, 5 = persisted state.

---

## 2. Project & DAG errors

| Error | Cause | Fix |
|---|---|---|
| `ProjectError: No juncture.yaml in <path>` | wrong `--project` path or missing file | `cd` into the project, or pass `--project /abs/path` |
| `ProjectError: 'name' is required` | minimal `juncture.yaml` missing the `name:` key | add `name: <project>` at top |
| `ProjectError: Environment variable ${X} ... not set` | env var referenced in YAML not exported | export it, or use `${X:-fallback}` |
| `DAGError: Model 'x' depends on unknown model 'y'` | typo in `ref()` or filename | `juncture compile --json` to list models; check filename = ref name |
| `DAGError: Cycle detected: a → b → a` | mutual `ref()` | break the cycle; use `materialization: ephemeral` for an inlined CTE |
| `ProjectError: duplicate model name 'x'` | `models/a/x.sql` and `models/b/x.sql` exist | rename one (filename stem must be unique) |

Run `juncture compile --project . --json` first whenever a new error
mentions `Project` or `DAG`. It exits before opening any DB and produces
a manifest of every model it discovered — fastest way to spot a typo.

---

## 3. Adapter / connection errors

| Error | Cause | Fix |
|---|---|---|
| `ProjectError: Connection 'x' not configured` | `--connection x` (or `profile: x`) but no `connections.x:` block | check `juncture.yaml`; with profiles, use one connection + `--profile NAME` |
| `AdapterError: Could not establish connection` (Snowflake) | bad credentials / network | re-export `${SNOW_*}` vars; test with `snowsql` / `snowflake-connector-python` first |
| `IO Error: Failed to open <path>` (DuckDB) | parent dir doesn't exist | the path resolver creates parents under `data/` automatically; otherwise `mkdir -p` |
| `OutOfMemoryException` (DuckDB) | parquet seed bigger than `memory_limit` | bump `memory_limit: 8GB` and add `temp_directory: /tmp/duckdb` to spill to disk |

---

## 4. Seed errors

| Error | Cause | Fix |
|---|---|---|
| Seed not found at runtime | symlink missing or pointing nowhere (sync-pull layouts) | `ls -la seeds/<name>` — relink with `kbagent storage unload-table --file-type parquet --download` |
| Type inference picks VARCHAR for everything | Keboola parquet exports with sentinels (`""`, `"NULL"`) | declare per-column types in `seeds/schema.yml` |
| `Conversion Error: Could not convert string '' to DOUBLE` | sentinel value in source | wrap reads with `TRY_CAST(col AS DOUBLE)`; long-term, scrub at ingest |
| Seed name with dots (`in.c-db.x`) breaks ref | quoting issue | the engine handles this — don't sanitise; check it's referenced as `{{ ref('in.c-db.x') }}` literally |
| Re-run is slow on every iteration | re-loading parquet each time | `juncture run --reuse-seeds` skips re-load + re-inference |

---

## 5. Profile errors

| Error | Cause | Fix |
|---|---|---|
| `ProjectError: profile 'X' is not declared` | `--profile X` or `JUNCTURE_PROFILE=X` but no `profiles.X:` overlay | check spelling; available names listed in error message |
| `ProjectError: Connection X not configured; available: ['warehouse']` | profile resolved to a name that doesn't match the legacy `profile: <connection>` shape | with `profiles:` block, keep one connection and let profiles override its params; pass `--connection` explicitly if multiple |
| Profile not applied even with `JUNCTURE_PROFILE` set | YAML doesn't actually have a `profiles:` block | env var only kicks in when `profiles:` exists (backward compat) |
| `default_schema` from profile ignored | typo in scalar key (e.g. `default_schemas` plural) | scalar keys replace wholesale; only listed keys merge |

---

## 6. Test failures

Tests are reported in the run report with `failing_rows`, `model`,
`column`, `name`. They do **not** abort the run; the exit code reflects
the failure for CI.

```bash
# See test results in detail
juncture run --project . --test 2>&1 | tail -40

# Reproduce a single test by hand
duckdb data/<project>.duckdb -c \
  "SELECT * FROM main.stg_orders WHERE order_id IS NULL LIMIT 5"
```

Common causes:

- **`not_null` failing on a Keboola export**: source had empty strings;
  parquet ingest converted them to NULLs. Either accept and `COALESCE`
  in staging, or fix at source.
- **`unique` failing**: duplicate rows from join in upstream — check the
  CTE that produced this column.
- **`relationships` failing**: foreign key column has values not in the
  parent — usually orphaned records; document as a known data-quality
  issue or filter them out.
- **`accepted_values` failing**: source produced a new enum value;
  decide whether to extend the list or fail loudly.

---

## 7. Migration errors (EXECUTE)

When `materialization: execute` is the model kind (typical for migrations
from `kbagent sync pull`), errors come from individual statements inside
the multi-statement body.

```bash
# Collect every failing statement; don't abort on first
juncture run --project ./migrated --continue-on-error

# Bucket them
juncture debug diagnostics --project ./migrated
```

Output:

```
Error buckets
┌──────────────────────┬───────┐
│ Bucket               │ Count │
├──────────────────────┼───────┤
│ type_mismatch        │    15 │
│ conversion           │     4 │
│ missing_object       │     2 │
└──────────────────────┴───────┘

Representative error per subcategory:

type_mismatch/case_branches
  error: Mismatch Type ERROR: Cannot mix INTEGER and VARCHAR in CASE expression
  fix:   Run `juncture sql sanitize` — applies harmonize_case_types AST pass
```

Buckets and their typical fixes are listed in
[`migration.md` §4](migration.md#4-repair-loop).

For deep dives into individual buckets, the source of the regex rules
is at `src/juncture/diagnostics/classifier.py`.

---

## 8. Reading `run_history.jsonl`

Every `juncture run` appends one JSON line to
`<project>/target/run_history.jsonl`. Used by the web UI and reliability
dashboard. Useful when you don't have the web UI open.

```bash
# Last 5 runs, summary
tail -5 target/run_history.jsonl | jq '{run_id, started_at, ok, failures: (.models | map(select(.status=="failed")) | length)}'

# Drill into the latest failure
tail -1 target/run_history.jsonl | jq '.models | map(select(.status=="failed"))'

# Per-model timing distribution
jq -r '.models[] | "\(.elapsed_seconds)\t\(.model.name)"' target/run_history.jsonl | sort -rn | head
```

Schema (per line):

```json
{
  "run_id": "20260418-1234",
  "started_at": "...",
  "finished_at": "...",
  "project_name": "my_shop",
  "ok": false,
  "models": [
    {
      "model": {"name": "...", "kind": "sql", "materialization": "table"},
      "status": "success | failed | skipped | partial | disabled",
      "elapsed_seconds": 0.42,
      "error": null,
      "result": {
        "row_count": 1234,
        "statement_errors": []
      }
    }
  ],
  "tests": [...]
}
```

---

## 9. Reading the manifest

```bash
juncture compile --project . --json > /tmp/manifest.json
jq '.models[] | select(.disabled)' /tmp/manifest.json     # disabled models
jq '.models[] | {name, kind, depends_on}' /tmp/manifest.json
jq '.order' /tmp/manifest.json                              # topological order
```

A static, no-DB view of the project. Always run this first when joining
an unfamiliar codebase.

---

## 10. Dialect translation issues

Three flavours of "translation went wrong":

### (a) The translated SQL doesn't parse against the target

```bash
juncture sql translate '<offending statement>' --from snowflake --to duckdb
```

If the output looks wrong, file an issue with the SQLGlot project — the
parse tree is theirs. Workaround: rewrite the statement in target-dialect
SQL by hand and store it that way.

### (b) The SQL parses but produces wrong results

Usually a function whose semantics differ across dialects (`LEAST`/
`GREATEST` NULL handling, integer-division behavior, timestamp precision
truncation). Add a unit test against expected output.

### (c) The SQL parses and produces "right" results but slow

DuckDB and Snowflake have different optimal join orders, hash vs.
broadcast joins, etc. Profile with `EXPLAIN ANALYZE` against both.
Generally not a Juncture concern unless query performance is in the SLA.
