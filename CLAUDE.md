# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Doc orientation — read these first

The four canonical docs split cleanly by question:

- **What & why** → [`docs/VISION.md`](docs/VISION.md) — problem
  statement, target characteristics, non-goals, success criteria.
  Stable; rarely moves.
- **How we deliver it, in phases** → [`docs/STRATEGY.md`](docs/STRATEGY.md)
  — four phases with Goal / Done-done / Deliverables / Out-of-scope.
  **Web render is the binding gate between Phase 1 and Phase 2.**
- **Where we are right now** → [`docs/STATUS.md`](docs/STATUS.md) —
  current phase, current sprint, engine capabilities shipped, open
  risks. Updates weekly; psáno česky pro Petra.
- **What to actually work on** → [`docs/ROADMAP.md`](docs/ROADMAP.md)
  — detailed task list grouped by the phases from STRATEGY.md.

Supporting docs:
- [`docs/DESIGN.md`](docs/DESIGN.md) — architecture (Project, DAG,
  Adapter, Executor, Testing, Seeds, Migration).
- [`docs/RESEARCH.md`](docs/RESEARCH.md) — competitive landscape and
  why current tools (dbt, SQLMesh, Dagster, …) fall short.
- [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — `juncture.yaml`,
  env vars, `schema.yml`, seed type overrides, parallel EXECUTE config.
- [`docs/MIGRATION_TIPS.md`](docs/MIGRATION_TIPS.md) — Snowflake→DuckDB
  cross-dialect migration field notes; taxonomy of 20+ type-coercion
  failure patterns plus a repair-loop blueprint. Source of the Phase 1
  post-pilot hardening sprints (A/B).
- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — performance numbers
  (≤ 10 % overhead target).

Before touching any area, skim the relevant doc; don't duplicate its
content in code comments or commit messages.

## Project

Juncture (`src/juncture`, package name `juncture`, version `0.1.0a0`): a
multi-backend SQL + Python transformation engine. Local-first, DuckDB-native,
Keboola-compatible. Python 3.11+, Apache 2.0. CLI entrypoint:
`juncture = "juncture.cli:app"` (Typer).

Unifies four Keboola transformation components (`snowflake-`, `python-`,
`duckdb-`, `dbt-transformation`) into one engine so SQL and Python models
live in the same DAG with parallelism, multi-backend SQL translation
(SQLGlot), and first-class data tests.

## Common commands

All work goes through the project `.venv` and the `Makefile`:

```bash
make install      # creates .venv and installs -e '.[dev,pandas]'
make fmt          # ruff format src tests
make lint         # ruff check src tests
make typecheck    # mypy src   (strict mode is enforced)
make test         # pytest -v --cov=juncture --cov-report=term-missing
make test-fast    # pytest -x --ff
make examples     # run examples/simple and examples/ecommerce with --test
make clean        # wipe build artifacts, caches, *.duckdb files, target/ dirs
```

Single-test runs:

```bash
.venv/bin/pytest tests/unit/test_dag.py -v
.venv/bin/pytest tests/integration/test_duckdb_end_to_end.py::test_name -v
```

CLI on an example project:

```bash
.venv/bin/juncture compile --project examples/simple --json
.venv/bin/juncture run     --project examples/simple --test --threads 4
.venv/bin/juncture translate 'SELECT TO_VARCHAR(42)' --from snowflake --to duckdb
```

Before submitting changes: `make fmt && make lint && make typecheck && make test`.
CI (`.github/workflows/ci.yml`) runs lint + test on 3.11/3.12 × Ubuntu/macOS
plus example smoke tests.

## Architecture (big picture)

A **Juncture project** is a directory (`juncture.yaml` + `models/` +
optional `seeds/`, `tests/`, `schema.yml`). A model is either a `.sql`
file or a Python function decorated with `@transform`. Models declare
deps via `{{ ref('other') }}` / `$ref(other)` (SQL) or `depends_on=[...]`
(Python). The pipeline:

```
juncture.yaml + models/ + schema.yml
  → Project (juncture.core.project)       discover + validate
  → list[Model] (juncture.core.model)     SQL | PYTHON | SEED, with materialization
  → DAG (juncture.core.dag)               networkx DiGraph, .layers() for parallelism
  → Executor (juncture.core.executor)     ThreadPoolExecutor, layer by layer
  → Adapter (juncture.adapters.*)         materialize_sql / materialize_python
  → TestRunner (juncture.testing)         compiles schema.yml tests to COUNT(*) queries
```

The `Runner` (`juncture.core.runner`) is the single high-level entry
point used by both the CLI and the Keboola wrapper — takes a
`RunRequest`, returns a `RunReport`.

### Key modules

- `juncture.core` — engine. `project`, `model`, `dag`, `executor`,
  `runner`, `seeds`, `state` (incremental `_juncture_state` table),
  `type_inference` (hybrid full-scan/sample for parquet seeds),
  `decorators` (`@transform`), `context` (the `ctx` passed to Python
  models: `ctx.ref(name)`, `ctx.vars()`).
- `juncture.adapters` — `base.Adapter` ABC + registry. Implementations:
  `duckdb_adapter` (MVP), `snowflake_adapter` (stub with MERGE
  incrementals and `write_pandas`). New backends must implement
  `connect`, `close`, `materialize_sql`, `materialize_python`,
  `fetch_ref`, `execute_arrow`, `resolve`.
- `juncture.parsers.sqlglot_parser` — `extract_refs`, `render_refs`
  (swap macros for FQNs at runtime), `parse_sql`, `translate_sql`,
  `extract_table_references` (best-effort lineage for raw/legacy SQL).
- `juncture.testing` — `assertions` (`not_null`, `unique`,
  `relationships`, `accepted_values`) and `runner`. Every builtin
  compiles to a single-row `SELECT COUNT(*)`.
- `juncture.cli.app` — Typer commands: `init | compile | run | test |
  docs | translate | migrate-keboola`. `--json` mode is the stable
  contract for agents.
- `juncture.migration` — `keboola_sql` (raw config-JSON input) and
  `keboola_sync_pull` (filesystem layout from `kbagent sync pull`;
  produces DuckDB project with symlinked parquet seeds and `EXECUTE`
  materialization for multi-statement scripts).
- `juncture.keboola.runner` + `docker/keboola/` — thin Keboola
  component wrapper that reads `/data/config.json` and shells out to
  `juncture run`.
- `juncture.observability.lineage` — optional OpenLineage START /
  COMPLETE / FAIL emitter (SDK is an extras dep).
- `juncture.mcp.server` — MCP tool skeleton (`list_models`,
  `compile_sql`, `run_subgraph`, `translate_sql`, `explain_model`);
  plain Python functions ready for MCP SDK wiring.

### Non-obvious conventions to preserve

- **Model name = file stem, ignoring subdirectory.** `models/a/x.sql`
  and `models/b/x.sql` is a hard error. Subdirs are organizational only.
- **Two ref macro forms are both supported**: `{{ ref('x') }}` (dbt
  style) and `$ref(x)` (brace-free, survives shell escaping). Don't
  accidentally drop support for either.
- **Per-thread DuckDB cursors are mandatory.** Sharing one DuckDB
  connection across threads was the first bug hit; each model run must
  get its own `cursor()` (see `DuckDBAdapter._thread_cursor`). Seeds
  use the same mechanism for parallel loading, capped by the
  connection's `threads` setting.
- **Parquet seeds are materialized as VIEW, not TABLE** (see commit
  `44d2f6a`). DuckDB `memory_limit` / `temp_directory` on the
  connection matters for large parquet dirs.
- **Seed names may contain dots** (e.g. `in.c-db.carts`) so migrated
  Snowflake identifiers survive verbatim. Don't sanitize them.
- **`_discover_seeds` follows symlinked directories** on purpose —
  `keboola_sync_pull` migrations symlink parquet dirs from
  `kbagent storage unload-table` into `seeds/`.
- **Type inference for parquet seeds is hybrid**: full-scan under 1M
  rows, sampled above. Both paths must stay correct.
- **`EXECUTE` materialization** exists for multi-statement Snowflake
  SQL migrated as-is — it does not wrap in `CREATE OR REPLACE`. Don't
  "fix" this to behave like `table`.
- **Failures cascade.** A failed model marks descendants as `skipped`.
  With `fail_fast=true` (default) the executor cancels pending futures.
- **Never silently default required config.** `juncture.yaml` without
  `connections`, a connection missing required params, or an unset
  `${VAR}` must fail fast at startup. The env-var interpolator
  supports `${VAR}` (required) and `${VAR:-fallback}` (optional) only.

### Configuration layers

1. `juncture.yaml` — project metadata, `connections`, `profile`,
   `default_materialization`, `default_schema`, `vars`, optional
   `jinja: true` for full Jinja mode (StrictUndefined, `ref()` +
   `var()` helpers).
2. Environment — interpolated into `juncture.yaml`; `.env` auto-loaded
   via `python-dotenv`.
3. `schema.yml` per model directory — descriptions, column specs,
   data tests, materialization overrides.

See `docs/DESIGN.md`, `docs/CONFIGURATION.md`, `docs/ROADMAP.md`,
`docs/RESEARCH.md`.

## Testing conventions

- `tests/unit/` — pure-Python, no DB. Parser, DAG, model, decorator,
  migration, state, type inference, MCP server, observability,
  sql_split, project env-vars.
- `tests/integration/` — full runner against DuckDB: `test_cli.py`,
  `test_duckdb_end_to_end.py`, `test_python_model.py`, `test_seeds.py`,
  `test_parquet_seeds.py`, `test_jinja_mode.py`, `test_custom_tests.py`,
  `test_execute_materialization.py`.
- `pytest.ini_options` sets `filterwarnings = ["error", ...]` — warnings
  are failures except for the explicit DuckDB/SQLGlot allow-list. Don't
  silence new warnings; fix them.
- `mypy` runs in **strict mode** (`warn_unused_ignores`,
  `warn_unreachable`). `duckdb`, `sqlglot`, `networkx` are the only
  modules with `ignore_missing_imports`.
- Ruff line-length is 110; select set is `E,W,F,I,B,C4,UP,RUF,SIM`;
  `E501` and `B008` are ignored globally; `S101` ignored under `tests/`.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`,
`fix:`, `docs:`, `refactor:`, `test:`, `chore:`. No AI attribution
footers.

## Docs maintenance (IMPORTANT)

The project uses a small, deliberate set of files under `docs/`. Keep
them in sync as code changes — stale docs here are worse than missing
docs, because both humans and agents rely on them to plan.

**File map and who owns what:**

| File | Scope | Update trigger |
|---|---|---|
| `docs/STATUS.md` | Living "where we are right now" snapshot (**Czech**, per user preference). Phases done / in flight / blocked. | Any time a phase starts or finishes, a blocker is hit/resolved, or a commit lands that shifts the story. Bump the `Last updated` line + branch + commit hash at the top. |
| `docs/ROADMAP.md` | Phased plan `v0.1 → v2`. Uses `[x]` / `[ ]` checkboxes per deliverable. | When a feature lands: flip the box to `[x]` in the same commit. When scope changes: move items between versions, never silently delete. |
| `docs/DESIGN.md` | Source of truth for architecture (components, adapter contract, materialization strategies, error model). | When a new component, materialization, or cross-cutting concern is added. Don't let `DESIGN.md` lag behind the code — if a reader of `DESIGN.md` would build a wrong mental model, it's broken. |
| `docs/CONFIGURATION.md` | User-facing reference for `juncture.yaml`, `.env`, `schema.yml`, selectors, seeds layout. | When a new config key, connection field, seed layout, or selector syntax is added. |
| `docs/BENCHMARKS.md` | Performance numbers + how they were measured. | When a benchmark script changes or a new measured number is available. Don't delete old numbers — add a dated row. |
| `docs/RESEARCH.md` | Competitive landscape (dbt, SQLMesh, Dagster, …). Time-neutral. | Only when a referenced tool changes materially or we adopt/reject an idea that wasn't there before. |
| `docs/src.md` | Four Keboola transformation component repo URLs. | Only if those repos move/rename. |
| `docs/priv/` | Private, gitignored (Oldies PDFs, Confluence proposal). | Do not commit. |

**Rules of thumb:**

- **Checkboxes in `ROADMAP.md` are load-bearing.** If you ship a feature,
  flip `[ ]` to `[x]` *in the same PR*. If you discover a v0.2 item is
  actually hard and slipping, move it to v0.3 with a short reason — don't
  leave it ambiguous.
- **`STATUS.md` is Czech on purpose.** It's the hand-off document the
  user reads first. Keep it scannable: "point", "kde jsme", "co zbývá
  dodělat". Update the header line (date / branch / commit) every time
  you touch it.
- **Don't duplicate.** `DESIGN.md` is the "how it's built" doc;
  `CONFIGURATION.md` is the "how to use it" doc; `ROADMAP.md` is "what's
  next"; `STATUS.md` is "where we are right now". If new text could go
  into two of these, pick one and cross-link.
- **Don't add new top-level `docs/` files without a reason.** We
  already trimmed `MORNING_BRIEF.md` once it went stale; don't recreate
  the same problem under a new name. If you need a one-off note, put it
  under `docs/priv/` (gitignored) or in a PR description.
- **No AI attribution, no emoji in docs text.** Matches the repo's
  commit policy.
