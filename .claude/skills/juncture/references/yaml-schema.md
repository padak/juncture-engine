# YAML schema reference

Complete reference for `juncture.yaml` (project config) and `schema.yml`
(per-model metadata + tests). Includes profiles, env interpolation, and
governance fields.

## Table of contents

1. [`juncture.yaml` top-level keys](#1-junctureyaml-top-level-keys)
2. [Connections](#2-connections)
3. [Vars](#3-vars)
4. [Env var interpolation + `.env`](#4-env-var-interpolation--env)
5. [Profiles](#5-profiles)
6. [Jinja mode](#6-jinja-mode)
7. [`schema.yml` model spec](#7-schemayml-model-spec)
8. [`schema.yml` governance fields](#8-schemayml-governance-fields)
9. [`seeds/schema.yml`](#9-seedsschemayml)

---

## 1. `juncture.yaml` top-level keys

```yaml
name: my_project              # required — project identifier
version: "0.1.0"              # optional — informational, default "0.1.0"
profile: dev                  # default profile name (see §5)
default_schema: main          # default target schema (DuckDB: main)
default_materialization: table  # table | view | incremental | ephemeral | execute
jinja: false                  # when true, all SQL goes through Jinja (StrictUndefined)
models_path: models           # override only if non-default layout
tests_path:  tests
macros_path: macros
seeds_path:  seeds

connections: {...}            # required (see §2)
vars: {...}                   # optional (see §3)
profiles: {...}               # optional (see §5)
model_defaults: {}            # optional — defaults applied to every model
```

Unknown top-level keys are silently ignored. Required field is `name` only
(plus at least one connection).

---

## 2. Connections

`connections:` is a mapping `{name: ConnectionConfig}`. Each entry must
have `type`; remaining keys become adapter parameters.

### DuckDB (`type: duckdb`)

```yaml
connections:
  local:
    type: duckdb
    path: data/local.duckdb       # ":memory:" allowed; relative resolved against project root
    threads: 4                    # ThreadPool size for parallel seed loading + intra-statement
    memory_limit: 4GB             # PRAGMA memory_limit
    temp_directory: /tmp/duckdb   # PRAGMA temp_directory (matters for big parquet)
    extensions: [httpfs, postgres]  # auto-installed + loaded on connect
```

### Snowflake (`type: snowflake`, stub in MVP)

```yaml
connections:
  prod:
    type: snowflake
    account:   "${SNOWFLAKE_ACCOUNT}"
    user:      "${SNOWFLAKE_USER}"
    password:  "${SNOWFLAKE_PASSWORD}"
    database:  ANALYTICS
    schema:    PUBLIC
    warehouse: COMPUTE_WH
    role:      ANALYST
    private_key_path: /secrets/sf.p8   # alternative to password
```

### Path resolution

Connection params named `path`, `duckdb_path`, or `private_key_path`
that hold a relative string are rewritten to be relative to the project
root. So `path: data/x.duckdb` works whether you run `juncture run`
from inside the project or from a parent directory.

### Connection selection at run time

| Situation | Resolution |
|---|---|
| `--connection NAME` passed | use NAME (must exist) |
| Project has **no** `profiles:` block | legacy: top-level `profile:` field is interpreted as connection name |
| Project has `profiles:` block, **one** connection defined | use that single connection |
| Project has `profiles:` block, **multiple** connections | must pass `--connection` explicitly |

**Common pattern**: keep one connection (`warehouse`) and let profiles
swap its `path` / `account` / `database`.

---

## 3. Vars

```yaml
vars:
  as_of: "2026-03-31"
  lookback_days: 90
  vip_threshold_eur: 500
```

Read from SQL (`jinja: true` required) and Python identically:

```sql
WHERE order_ts >= CAST('{{ var("as_of") }}' AS DATE)
                  - INTERVAL '{{ var("lookback_days") }} days'
```

```python
as_of    = pd.to_datetime(ctx.vars("as_of"))
lookback = int(ctx.vars("lookback_days", 90))
```

### Override priority

1. CLI: `--var key=value` (one per flag, repeatable)
2. Profile: `profiles.<name>.vars` (merged shallow over top-level)
3. `juncture.yaml vars:` block

If a key is missing in all three AND no Jinja default is given,
`StrictUndefined` raises at render time — no silent defaults.

---

## 4. Env var interpolation + `.env`

```yaml
# ${VAR}              — required; ProjectError at startup if unset
# ${VAR:-fallback}    — optional; uses fallback when unset (empty string is allowed)
connections:
  local:
    type: duckdb
    path: ${JUNCTURE_DB_PATH:-data/local.duckdb}
```

- Interpolation runs **before** the profile overlay, so `${VAR}` inside a
  profile works the same as at top level.
- `.env` next to `juncture.yaml` is auto-loaded via `python-dotenv`. Don't
  commit it.
- **Secrets MUST come from env vars or `.env`.** Never hard-code a token,
  password, or private key in `juncture.yaml`. Examples for placeholders
  in docs / templates: `your-token`, `xxx`, `change-me`.

---

## 5. Profiles

Named overlay over the top-level keys. One project file describes many
environments.

```yaml
name: my_shop
profile: dev                         # default profile when --profile not given

connections:
  warehouse:
    type: duckdb
    path: data/base.duckdb

vars:
  lookback_days: 90

profiles:
  dev:
    default_schema: dev_petr
    connections:
      warehouse:
        path: data/dev.duckdb        # overrides path only; type stays
    vars:
      lookback_days: 7               # merges over top-level vars

  staging:
    default_schema: "dev_${KEBOOLA_BRANCH_ID}"
    connections:
      warehouse:
        type: snowflake
        account: "${SNOW_ACCOUNT}"
        database: STAGING_DB

  prod:
    default_schema: analytics
    connections:
      warehouse:
        type: snowflake
        account: "${SNOW_ACCOUNT}"
        database: PROD_DB
```

### Merge rules

| Key in profile overlay | Merge behavior |
|---|---|
| `vars:` | shallow dict merge (profile keys override top-level keys) |
| `connections.<name>:` | per-connection shallow merge (per-key) |
| `default_schema`, `default_materialization`, `jinja`, `*_path`, `model_defaults` | wholesale replace |
| any other key | wholesale replace |

### Profile selection

Resolved priority (high → low):

1. CLI flag: `juncture run --profile prod`
2. Env var: `JUNCTURE_PROFILE=prod`
3. Top-level `profile:` field in `juncture.yaml`
4. None (no overlay applied)

A profile name resolved but not declared under `profiles:` aborts with
`ProjectError` — no silent fallback. Available profile names are
`ProjectConfig.available_profiles`; the active one is
`ProjectConfig.active_profile`.

### Backward compat

Projects **without** a `profiles:` block keep the legacy meaning of the
`profile:` field (= name of the connection to use). This is why the
classic `profile: local` + `connections: {local: {...}}` shape from
`juncture init` still works.

---

## 6. Jinja mode

Set `jinja: true` to enable Jinja2 rendering on **every** SQL model.

```yaml
jinja: true
```

What it gives you:

- `{{ ref('x') }}` works (this works without jinja too via brace parsing).
- `{{ var('key', default) }}` reads merged vars.
- `{% if %}` / `{% for %}` / inline expressions.
- Auto-loaded macros from `macros/**/*.sql`. Drop a `{% macro %}` file in
  there; call by name from any model — no `{% import %}` needed.
- `StrictUndefined`: missing variables fail loudly at render time.

```sql
-- macros/dates.sql
{% macro my_date(col) -%}
  strftime({{ col }}, '%Y-%m-%d')
{%- endmacro %}

-- models/daily_revenue.sql
SELECT {{ my_date('order_ts') }} AS day, ... FROM {{ ref('orders') }}
```

When `jinja: false`, only the brace-parsing of `{{ ref(...) }}` and
`$ref(...)` runs — no macros, no `var()`, no control flow. Models are
treated as raw SQL.

---

## 7. `schema.yml` model spec

`schema.yml` files live next to model files (any nesting depth). Each
file describes models found in the same directory tree below it.

```yaml
version: 2
models:
  - name: stg_orders
    description: Raw orders staged for downstream use.
    materialization: incremental                  # overrides project default
    tags: [staging, hourly]
    config:
      unique_key: order_id                        # required for incremental
      lookback_days: 3                            # incremental window
      parallelism: 4                              # EXECUTE only
      continue_on_error: false                    # EXECUTE only (CLI flag overrides)
    columns:
      - name: order_id
        description: Surrogate key.
        data_type: BIGINT
        tests: [not_null, unique]
      - name: customer_id
        tests:
          - not_null
          - relationships: { to: stg_customers, field: id }
      - name: status
        tests:
          - accepted_values:
              values: [completed, refunded, pending]
    disabled: false                               # see §8 governance / disable toggle
```

### Built-in tests

| Test | YAML form | What it checks |
|---|---|---|
| `not_null` | `tests: [not_null]` | column has no NULLs |
| `unique` | `tests: [unique]` | column values are unique |
| `relationships` | `relationships: {to: ref_model, field: col}` | every value exists in `ref_model.col` |
| `accepted_values` | `accepted_values: {values: [a, b, c]}` | column values ⊆ list |

Each compiles to one `SELECT COUNT(*)` query. A test with `count > 0`
fails; the run report shows the failing-row count.

### `disabled` toggle

```yaml
- name: legacy_mart
  disabled: true        # always skipped
```

CLI overrides: `juncture run --disable a,b` or `--enable-only x,y` flips
`disabled` at runtime without editing the file. Disabled models report
`status=disabled`; their downstream gets `status=skipped` with
`skipped_reason=upstream_disabled`. The run does **not** fail.

---

## 8. `schema.yml` governance fields

(Web UI Portfolio + Reliability + PII propagation use these.)

```yaml
- name: customer_summary
  owner:        petr@keboola.com
  team:         analytics
  criticality:  critical                # low | medium | high | critical
  sla:          60                      # minutes; reliability dashboard scores against this
  docs: |
    Long-form markdown rendered in the web UI Models tab.
  columns:
    - name: email
      pii: true                         # propagates a PII ring colour through the DAG
    - name: created_at
      retention_days: 365               # informational; surfaced in Portfolio
```

Seeds get the same governance surface in their `seeds/schema.yml`:
`source_system`, `source_locator`, `pii`, `retention_days`, `owner`.

---

## 9. `seeds/schema.yml`

```yaml
seeds:
  - name: orders
    source_system: keboola              # informational (web UI)
    source_locator: in.c-orders.events  # informational
    pii: false
    retention_days: 730
    owner: data-platform@keboola.com
    columns:                            # type overrides
      - {name: amount_eur, type: DECIMAL(18,4)}
      - {name: order_ts,   type: TIMESTAMP}
```

Only `name:` is mandatory. Type overrides win over inferred types and
flow into `Project.seed_schemas()` — used by schema-aware SQL translation
during migration.
