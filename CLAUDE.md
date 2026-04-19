# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Doc orientation — read these first

Public docs under `docs/` (committed to the repo):

- **What & why** → [`docs/VISION.md`](docs/VISION.md) — problem
  statement, target characteristics, non-goals, success criteria.
  Stable; rarely moves.
- **What to actually work on** → [`docs/ROADMAP.md`](docs/ROADMAP.md)
  — phased task list, one checkbox per deliverable.
- **How to use it** → [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md)
  — `juncture.yaml`, env vars, `schema.yml`, seed type overrides,
  parallel EXECUTE config, Jinja macros, profiles.
- **How to onboard a new user** → [`docs/TUTORIAL.md`](docs/TUTORIAL.md)
  — four-level walkthrough (L1 zero to first SELECT, L2 Python in the
  DAG, L3 macros + ephemeral, L4 external `--var` parameters).
  Companion project: [`examples/tutorial_shop/`](examples/tutorial_shop/).
- **How it's built** → [`docs/DESIGN.md`](docs/DESIGN.md) —
  architecture (Project, DAG, Adapter, Executor, Testing, Seeds,
  Migration).

Private dev track lives under `docs/priv/` (gitignored):

- `STATUS.md` — weekly snapshot / working notebook (Czech).
- `STRATEGY.md` — phased delivery plan with done-done criteria.
- `RESEARCH.md` — competitive landscape; opinionated takes on dbt /
  SQLMesh / Dagster.
- `MIGRATION_TIPS.md` — Snowflake → DuckDB migration field notes,
  failure taxonomy, repair-loop blueprint.
- `BENCHMARKS.md` — performance numbers.
- `rfcs/` — design proposals (e.g. `0001-web-ui-v2.md`).
- `src.md` — four legacy Keboola transformation repo URLs.

Before touching any area, skim the relevant public doc; don't duplicate
its content in code comments or commit messages. When updating a
private doc, keep it local — it is not pushed.

## Project

Juncture (`src/juncture`, package name `juncture`; version lives in
`pyproject.toml`, exposed at runtime via `juncture.__version__`): a
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

See `docs/DESIGN.md`, `docs/CONFIGURATION.md`, `docs/ROADMAP.md`.

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

**MANDATORY pre-commit checklist — run every time before `git commit`:**

```bash
make fmt && make lint && make typecheck && make test
```

`make fmt` (ruff format) is separate from `make lint` (ruff check) — CI
runs both and will fail if format is dirty. Never skip `make fmt`.

## Docs maintenance (IMPORTANT)

The project uses a small, deliberate set of files under `docs/`. Keep
them in sync as code changes — stale docs here are worse than missing
docs, because both humans and agents rely on them to plan.

**Public file map (committed):**

| File | Scope | Update trigger |
|---|---|---|
| `docs/VISION.md` | What + why. Stable reference; rarely moves. | Only when the vision itself moves (not when sprints / architecture / landscape move). |
| `docs/ROADMAP.md` | Phased task list with `[x]` / `[ ]` checkboxes per deliverable. | When a feature lands, flip the box to `[x]` in the same PR. When scope changes, move items between phases with a short reason — never silently delete. |
| `docs/TUTORIAL.md` | Four-level onboarding narrative for a new user (zero → first SELECT → Python-in-DAG → shared macros/ephemeral → CLI `--var` params). Mirrors `examples/tutorial_shop/`. | When a new Level N feature (new idiom worth teaching) lands, or when scaffold / `ref()` / `@transform` ergonomics change. Don't add new levels for marginal features. |
| `docs/CONFIGURATION.md` | User-facing reference for `juncture.yaml`, `.env`, `schema.yml`, selectors, seeds, macros, profiles. | When a new config key, connection field, seed layout, or selector syntax is added. |
| `docs/DESIGN.md` | Source of truth for architecture (components, adapter contract, materialization strategies, error model). | When a new component, materialization, or cross-cutting concern is added. If a reader of `DESIGN.md` would build a wrong mental model, it's broken. |

**Private dev track (`docs/priv/`, gitignored):**

| File | Scope | Update trigger |
|---|---|---|
| `priv/STATUS.md` | Weekly "where we are right now" snapshot (**Czech**). Phases done / in flight / blocked. | Any time a phase starts or finishes, a blocker is hit/resolved, or a commit lands that shifts the story. Bump the `Last updated` line at the top. |
| `priv/STRATEGY.md` | Phased delivery plan with Goal / Done-done / Deliverables / Out-of-scope per phase. | When a phase's done-done criterion or out-of-scope boundary shifts. |
| `priv/RESEARCH.md` | Competitive landscape (dbt, SQLMesh, Dagster, …). Opinionated. | Only when a referenced tool changes materially or we adopt/reject an idea that wasn't there before. |
| `priv/MIGRATION_TIPS.md` | Snowflake → DuckDB migration field notes; taxonomy of type-coercion failure patterns plus a repair-loop blueprint. | After each migration round, capture new failure patterns. |
| `priv/BENCHMARKS.md` | Performance numbers + how they were measured. | When a benchmark script changes or a new measured number is available. Don't delete old numbers — add a dated row. |
| `priv/rfcs/` | Design proposals (e.g. `0001-web-ui-v2.md`) that cross multiple files or change public surface. | New RFC per non-trivial proposal; mark status at top (`proposed` / `accepted` / `implemented`). |
| `priv/src.md` | Four legacy Keboola transformation repo URLs. | Only if those repos move/rename. |

**Rules of thumb:**

- **Checkboxes in `ROADMAP.md` are load-bearing.** If you ship a feature,
  flip `[ ]` to `[x]` *in the same PR*. If you discover an item is
  slipping, move it to a later phase with a short reason — don't leave
  it ambiguous.
- **`STATUS.md` (private) is Czech on purpose.** It's the hand-off
  document the user reads first. Keep it scannable: "point", "kde jsme",
  "co zbývá dodělat". Update the header line (date / branch / commit)
  every time you touch it.
- **Don't duplicate.** `DESIGN.md` is "how it's built"; `CONFIGURATION.md`
  is "how to use it"; `ROADMAP.md` is "what's next"; private `STATUS.md`
  is "where we are right now". If new text could go into two of these,
  pick one and cross-link.
- **Don't add new top-level public `docs/` files without a reason.**
  We trimmed the public set deliberately. If you need a one-off note,
  put it under `docs/priv/` (gitignored) or in a PR description.
- **Public docs must not link to private docs.** GitHub renders broken
  links; readers without the priv tree get 404s. Inline the relevant
  fact or drop the sentence.
- **No AI attribution, no emoji in docs text.** Matches the repo's
  commit policy.
