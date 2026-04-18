# Migration

Two migration entry points: **`juncture migrate keboola`** for raw Keboola
config JSON, **`juncture migrate sync-pull`** for the filesystem layout
that `kbagent sync pull` produces (the preferred path for production
transformations). Plus the **repair loop** that turns a 26-iteration agent
session into 2-3 iterations.

## Table of contents

1. [Migrate from a Keboola raw config JSON](#1-migrate-from-a-keboola-raw-config-json)
2. [Migrate from `kbagent sync pull` layout](#2-migrate-from-kbagent-sync-pull-layout)
3. [Pre-flight check (`--validate`)](#3-pre-flight-check---validate)
4. [Repair loop](#4-repair-loop)
5. [Cross-dialect translation tips](#5-cross-dialect-translation-tips)
6. [Migrate from dbt](#6-migrate-from-dbt)
7. [Migrate from Keboola Python transformation](#7-migrate-from-keboola-python-transformation)

---

## 1. Migrate from a Keboola raw config JSON

```bash
juncture migrate keboola path/to/config.json --output ./migrated --backend duckdb
```

What it does:

- Reads each code block in the Keboola transformation config
- Splits scripts into one `.sql` model per code block
- Generates `juncture.yaml` with a single DuckDB connection
- Maps input mapping → seeds, output mapping → model materializations

Use this when you have the JSON exported from the Keboola UI (e.g. via
the Storage API).

---

## 2. Migrate from `kbagent sync pull` layout

The preferred path. `kbagent sync pull` materialises a Keboola
transformation as a filesystem directory; Juncture ingests it whole.

### Step 1 — pull the transformation locally

```bash
kbagent sync pull --project MY_PROJECT
# Produces: main/transformation/keboola.snowflake-transformation/<name>/
```

### Step 2 — pull the parquet seeds

```bash
kbagent storage unload-table --file-type parquet --download <table_id> ...
# Produces: parquet directories of input tables
```

### Step 3 — migrate

```bash
juncture migrate sync-pull \
  main/transformation/keboola.snowflake-transformation/<name>/ \
  --seeds <parquet_dir> \
  --output ./migrated \
  --source-dialect snowflake \
  --target-dialect duckdb
```

What it does:

- Symlinks every parquet seed dir into `migrated/seeds/` (no copy)
- Concatenates the transformation's SQL blocks into one big script
- Translates Snowflake → DuckDB statement-by-statement via SQLGlot
- Writes the script as **one model with `materialization: execute`**
  (multi-statement, runs as-is — no `CREATE OR REPLACE` wrapping)
- Generates `juncture.yaml` with a DuckDB connection at
  `data/juncture.duckdb`
- Prints a coverage report: SQL lines, seeds linked, output tables

Use `--source-dialect duckdb` to skip translation when the SQL is
already DuckDB-native.

---

## 3. Pre-flight check (`--validate`)

Before writing project files, sanity-check the inputs:

```bash
juncture migrate sync-pull <transform_dir> \
  --seeds <parquet_dir> \
  --validate
```

Output (exit code 1 if any issue):

```
Validation — my_transformation
  SQL lines              4,213
  Statements             374
  Parse errors           0          (red if > 0)
  Input seeds expected   208
  Missing seeds          0          (red if > 0)
  Output tables          12
```

Catches: parse failures (statements SQLGlot can't read), missing parquet
directories, dotted seed names that don't resolve. Run this first; only
proceed when both columns are green.

---

## 4. Repair loop

Migrating a real production transformation typically produces failures
on first run because of cross-dialect type coercion edge cases. The
repair loop collapses the next migration from ~26 agent rounds to 2-3.

### Step 1 — run with `--continue-on-error`

```bash
juncture run --project ./migrated --continue-on-error
```

Default behavior aborts on the first failing statement. With this flag,
EXECUTE materializations keep running, collect every failing statement,
and emit a `RunReport` with `statement_errors` per model.

### Step 2 — bucket the errors

```bash
juncture debug diagnostics --project ./migrated
```

Reads the run report, classifies every error via regex → bucket lookup,
prints a representative error and a `fix_hint` per subcategory.

Buckets (from `juncture.diagnostics.classifier.ErrorBucket`):

| Bucket | What it means | Common fix |
|---|---|---|
| `type_mismatch` | implicit coercion across dialects (CASE branches with VARCHAR + INT, ...) | `juncture sql sanitize` (`harmonize_case_types`) |
| `function_signature` | function args differ across dialects (`DATE_DIFF`, …) | `harmonize_function_args` AST pass |
| `conversion` | `TRY_CAST` / `CAST` failures (sentinels in source data) | declare per-column type in `seeds/schema.yml` or wrap with `TRY_CAST` |
| `missing_object` | table or column not found | check seed name + dotted identifier survival |
| `idempotence` | second run breaks because previous tables exist | wrap in `CREATE OR REPLACE` (rare in EXECUTE bodies) |
| `parser` | SQLGlot couldn't parse → fell back to regex | inspect statement; sometimes a comment/edge-case syntax |
| `other` | regex didn't match a known pattern | manual triage |

### Step 3 — apply AST passes

```bash
juncture sql sanitize --project ./migrated
```

Re-translates every `models/*.sql` through these AST passes:

- **`harmonize_case_types`** — fixes Snowflake CASE branches that mix
  VARCHAR and INT; DuckDB rejects these.
- **`harmonize_binary_ops`** — coerces operands of binary ops to a
  compatible type.
- **`harmonize_function_args`** — fixes function signature differences
  (e.g. `DATE_DIFF`, `LISTAGG` → `STRING_AGG`).
- **`fix_timestamp_arithmetic`** — rewrites `timestamp - timestamp`
  expressions DuckDB can't natively eval.

Schema-aware: pass `schema=Project.seed_schemas()` so SQLGlot's
`annotate_types` knows column types from parquet metadata + overrides.

### Step 4 — rerun and iterate

```bash
juncture run --project ./migrated --continue-on-error
juncture debug diagnostics --project ./migrated
```

Each iteration should drop the count significantly. When buckets are
down to "manual fix needed" (often <10 statements), drop
`--continue-on-error` and treat them individually.

---

## 5. Cross-dialect translation tips

Standalone translation of one statement:

```bash
juncture sql translate 'SELECT TO_VARCHAR(42)' --from snowflake --to duckdb
```

Edge cases to flag when translating Snowflake → DuckDB:

| Pattern | Issue | Workaround |
|---|---|---|
| `VARIANT` columns | no DuckDB equivalent | cast to VARCHAR or use `STRUCT` |
| Nanosecond timestamps | DuckDB tops out at microseconds | acceptable for analytics; document the truncation |
| `LISTAGG(col, sep) WITHIN GROUP (ORDER BY x)` | DuckDB has `STRING_AGG(col, sep ORDER BY x)` | `harmonize_function_args` handles it |
| Identifier case | Snowflake uppercases unquoted names; DuckDB lowercases them | quote when case matters |
| `LEAST` / `GREATEST` with NULL | Snowflake/BigQuery ignore NULLs; DuckDB/Postgres return NULL | wrap each arg with `COALESCE` |
| `DATE_DIFF(unit, end, start)` arg order | varies across dialects | SQLGlot translates; verify with a unit test |

Always verify by running the transformation end-to-end against the
target. Translation is best-effort; trust the tests, not the parser.

For deep field notes (failure taxonomy of 20+ patterns from real
migrations), see `docs/MIGRATION_TIPS.md` in the repo root.

---

## 6. Migrate from dbt

Juncture's `ref()` macro and `models/` layout map 1:1. There is no
`migrate-dbt` subcommand yet (planned, see `docs/RESEARCH.md`); meanwhile
do it by hand:

| dbt | Juncture | Notes |
|---|---|---|
| `models/**/*.sql` | `models/**/*.sql` | identical layout; copy as-is |
| `{{ ref('x') }}` | `{{ ref('x') }}` | identical; works without `jinja: true` too |
| `{{ source('s', 't') }}` | `{{ ref('t') }}` | put source data in `seeds/` |
| `dbt_project.yml` | `juncture.yaml` | write by hand; `connections:` block from `profiles.yml` |
| `profiles.yml` | `juncture.yaml profiles:` block | per-environment connection overrides |
| `schema.yml` tests block | `schema.yml columns: tests:` | identical built-ins (`not_null`, `unique`, `relationships`, `accepted_values`) |
| `dbt run --select +x` | `juncture run --select +x` | identical selector grammar |
| `{{ config(materialized='incremental', unique_key='id') }}` | `schema.yml: materialization: incremental` + `config: {unique_key: id}` | move config to schema.yml |
| Jinja macros (dbt-utils, dbt-expectations) | not auto-imported | rewrite as plain `{% macro %}` files in `macros/` (when `jinja: true`) |
| Python models (`@asset`-style) | `@transform(depends_on=[...])` | one decorator per file |

Full Jinja + dbt packages compatibility is **not** a goal (see
`docs/RESEARCH.md` §1 for the rationale).

---

## 7. Migrate from Keboola Python transformation

1. Export each `.py` block.
2. Wrap each block as `@transform(depends_on=["input_table_1", ...])`.
3. Replace `pandas.read_csv(in_table)` with `ctx.ref("input_table").to_pandas()`.
4. Replace `df.to_csv(out_table)` with `return df` — Juncture persists
   the returned DataFrame per the model's materialization.
5. Replace `print()` with `ctx.logger.info()`.
6. Move secrets from inline strings to `${ENV_VAR}` in `juncture.yaml`,
   then `.env` file (gitignored).
