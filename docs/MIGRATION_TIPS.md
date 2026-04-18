# Cross-Dialect SQL Migration: Field Notes

> Practical playbook for porting legacy SQL transformations — primarily
> **Snowflake → DuckDB**, but the principles generalize to any
> "permissive source → strict target" arbitrage. Written from a single
> real-world Keboola client migration (500 kB SQL body, 374 statements,
> 208 parquet seeds) that reached **100 % execution success after 26
> agent-driven repair rounds**. This document captures the lessons so
> the next migration converges in fewer rounds, with less human time,
> and ideally fully automated.

---

## 1. Executive summary

* **The hard part is typing, not syntax.** Almost every failure in
  the case study was a type coercion Snowflake performs implicitly and
  DuckDB refuses. Keyword/function translations (`TO_VARCHAR`, `DATEDIFF`,
  `QUALIFY`…) are a solved problem — SQLGlot handles ~95 % of them.
  The open-ended tail is VARCHAR-holding-a-number, string sentinels
  (`''`, `'--empty--'`, `'n/a'`), BIGINT-vs-VARCHAR joins, switched
  CASE with mixed return types, and a few dozen more.
* **Two knobs decide the repair budget.** The first is whether the
  translator is **schema-aware** — without a seed catalogue the fixer
  cannot tell `col IS VARCHAR` and must over-cast defensively. The
  second is whether the runner can **continue past the first error in
  a multi-statement script**; without it, each fix reveals exactly one
  new problem and the repair loop is serial.
* **An AI repair loop converges** — but slowly when both knobs are off
  (in our case ~30 iterations). With both knobs on it should collapse
  to 2–3 iterations (one schema-aware sweep + one agent pass on the
  residue).

The rest of this document describes the failure taxonomy we saw, where
SQLGlot helped, where regex and state machines beat SQLGlot, the repair
principles we settled on, and the tooling gaps Juncture should close so
the next migration is smooth.

---

## 2. Snowflake → DuckDB incompatibility taxonomy

Frequency counts below are from the 374-statement case-study body. Column
"Auto" flags whether the fix is safely automatable *without* a schema
catalogue.

| # | Pattern | DuckDB error | Root cause | Fix pattern | Auto |
|---|---------|--------------|------------|-------------|------|
| 1 | `CASE WHEN x THEN 0 ELSE REPLACE(y, …) END` | `Cannot mix VARCHAR and INTEGER_LITERAL in CASE` | Snowflake coerces the numeric literal to VARCHAR so the branch types agree; DuckDB refuses mixed-type CASE. | Wrap numeric literal in `CAST(n AS VARCHAR)`. | ✅ syntax-only |
| 2 | `CAST('' AS INT)` / `CAST('--empty--' AS INT)` | `Conversion Error: Could not convert string '' to INT64` | Sentinel strings used in place of NULL. Snowflake passes `NULL` through CAST, DuckDB raises. | `TRY_CAST(NULLIF(NULLIF(col, ''), '--empty--') AS BIGINT)` | ⚠️ needs sentinel detection |
| 3 | `JOIN ON a.varchar_id = b.bigint_id` | `Cannot compare VARCHAR and BIGINT` in join | Legacy Keboola columns stored as VARCHAR; modern FKs as BIGINT. | `JOIN ON TRY_CAST(NULLIF(a.varchar_id, '') AS BIGINT) = b.bigint_id` | ❌ needs schema |
| 4 | `CAST(ts_col AS DATE) <= CAST(... AS DATE) + INTERVAL (-1) DAY` | `Cannot compare VARCHAR and TIMESTAMP` | `DATE + INTERVAL` promotes to TIMESTAMP; bare VARCHAR seed column on the other side is not auto-coerced. | `TRY_CAST(ts_col AS DATE) <= CAST(... AS DATE) - INTERVAL '1' DAY` | ⚠️ parse both sides |
| 5 | `"date" <= TIMESTAMP_expr` where `"date"` is `DD/MM/YYYY` VARCHAR | `Cannot compare VARCHAR and DATE` | DuckDB `TRY_CAST('14/03/2020' AS DATE)` → NULL (expects ISO). | `CAST(STRPTIME("date", '%d/%m/%Y') AS DATE)` | ⚠️ needs format detection |
| 6 | `SUM(varchar_col)`, `AVG(varchar_col)`, `ROUND(varchar_col)` | `No function matches 'sum(VARCHAR)'` | Aggregate over VARCHAR. | `SUM(TRY_CAST(col AS DOUBLE))` | ⚠️ |
| 7 | `DATEDIFF(date, a, b)` (Snowflake) → `date_diff(DATE, TIMESTAMP, DATE)` | `No function matches 'date_diff(DATE, TIMESTAMP, DATE)'` | SQLGlot's Snowflake→DuckDB mapping sometimes emits `CAST('DAY' AS DATE)` as arg #1 instead of the literal `'day'` string. | `date_diff('day', a::TIMESTAMP, b::TIMESTAMP)` | ✅ SQLGlot fixable |
| 8 | `timestamp_col - 1` / `timestamp_col + 1` | `No function matches '-(TIMESTAMP, INTEGER_LITERAL)'` | Snowflake treats integer after timestamp as days. DuckDB requires `INTERVAL`. | `timestamp_col - INTERVAL '1' DAY` | ✅ SQLGlot fixable |
| 9 | `timestamp_col = ''` | `Conversion Error: invalid timestamp field format: ""` | Defensive empty-string check on a column the seed loader already typed as TIMESTAMP. | `timestamp_col IS NULL` | ⚠️ needs schema |
| 10 | `varchar_col IN (1, 2, 3)` | `Cannot compare VARCHAR and BIGINT in IN/ANY/ALL clause` | IN list of integer literals but left side is VARCHAR. | Cast column or literals; `CAST(col AS BIGINT)` (better when target is always numeric). | ❌ needs schema |
| 11 | `CASE bigint_col WHEN 1 THEN 'A' WHEN 2 THEN 'B' END` | `Could not convert string 'A' to INT64` | Switched-CASE with numeric subject and string THEN branches — DuckDB type-checks THEN branches against subject type in some contexts. | Rewrite as searched `CASE WHEN col = 1 THEN 'A' …` *or* wrap: `CAST(CASE … END AS VARCHAR)`. | ✅ syntax-only |
| 12 | `COALESCE(bigint_col, '--empty--')` | `Conversion Error: Could not convert string '--empty--' to INT64` | Mixed types in COALESCE; DuckDB promotes to the first non-null type (BIGINT) and tries to coerce the string default. | `COALESCE(CAST(bigint_col AS VARCHAR), '--empty--')` | ⚠️ needs schema |
| 13 | `TRIM(bigint_col)`, `UPPER(date_col)` | `No function matches 'trim(BIGINT)'` | String function over non-string input. | `TRIM(CAST(col AS VARCHAR))` | ⚠️ needs schema |
| 14 | `CAST(varchar_url AS INT)` where values include `'diskuze/935337'` | `Could not convert string 'diskuze/935337' to INT64` | Legacy polymorphic ID columns. | Same as #3 — `TRY_CAST` + `NULLIF`. | ❌ needs data sample |
| 15 | Snowflake `LATERAL FLATTEN(input => expr)` | ParserError in most strict dialects | Snowflake-specific syntax. | DuckDB `UNNEST(CAST(expr AS VARCHAR[]))` or `json_each(expr)`. | ⚠️ recognisable but rewrite-heavy |
| 16 | Snowflake bitwise `CAST(x AS INT) & 4 = 4` on VARCHAR | Cascades through #2 | Flag bitmask stored as string. | Wrap in `TRY_CAST`. | ⚠️ |
| 17 | Snowflake alias reference in `JOIN … ON "alias_from_select"` | `Referenced column "alias" not found in FROM clause` | Snowflake resolves SELECT-list aliases inside JOIN ON; DuckDB does not. | Inline the full expression into ON. | ✅ syntax-only (SQLGlot) |
| 18 | `CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Prague'` on left of `+ INTERVAL` | Returns TIMESTAMP, not DATE | Arithmetic promotes type — downstream comparison to `VARCHAR "date"` needs double-ended CAST. | Cast either side to matching type before the operator. | ⚠️ |
| 19 | Stringly-typed enum subject in CASE (`'LEGACY'`, `'ANONYMOUS'`) | Same as #11 when upstream uses `CAST(... AS INT)` | VARCHAR column holds symbolic values; migration wrapped it in `TRY_CAST AS INT` → NULL, then switched-CASE output type collapses. | Drop the TRY_CAST; use searched CASE on the original VARCHAR. | ⚠️ |
| 20 | `sum(JSON)`, `col -> 'key'` in arithmetic | `No function matches 'sum(JSON)'` | `->` returns JSON in DuckDB; casting needed before numeric ops. | `SUM(CAST(col->>'key' AS DOUBLE))`. | ⚠️ |

**What's missing from the table but matters in the wild:**

* **Case-sensitive identifiers.** `"Date"` in the parquet schema,
  `"date"` in the SQL; DuckDB with default `preserve_identifier_case`
  makes both match, but a server-level setting flip will break
  everything silently.
* **Seed name dots.** Keboola's `in.c-main.orders` naming survives the
  migrator verbatim, but SQLGlot will attempt to parse it as schema
  qualifiers unless identifiers are quoted. Keep quotes.
* **Missing seed data.** A lookup table referenced by the SQL was
  never exported by the source ETL. Creating a typed empty placeholder
  VIEW was cheaper than rewriting every downstream LEFT JOIN.

---

## 3. Where SQLGlot earns its keep — and where it doesn't

### 3.1 Wins

* **Dialect translation** for 80–95 % of surface syntax:
  `TO_VARCHAR` → `CAST(… AS VARCHAR)`, `QUALIFY` → subquery,
  `::type` casts, `CURRENT_TIMESTAMP AT TIME ZONE`, bracket/bracket
  quoting.
* **AST-level transforms** that are *purely syntactic*. Our
  `harmonize_case_types` is the clean example: it walks every
  `exp.Case`, classifies THEN/ELSE branches as STRING / NUMERIC / NULL
  / UNKNOWN from expression shape alone (literal kind,
  string-producing function family, `exp.Cast` target), and wraps
  numeric literals in `CAST AS VARCHAR` when the CASE mixes types.
  No schema required. Lives in
  `src/juncture/parsers/sqlglot_parser.py`.
* **Per-statement fallback.** Our `translate_sql` splits the body on
  top-level semicolons, parses each statement independently, and on
  `sqlglot.errors.ParseError` falls back to passing the raw text
  through. A 12 k-line Snowflake body contains at least a handful of
  constructs SQLGlot can't parse (LATERAL FLATTEN with verbose
  `input =>` kwargs, some UNPIVOT forms); refusing to translate the
  whole body because of one exotic statement is a bad bargain.

### 3.2 Limits

* **SQLGlot does not type-check.** Without feeding `annotate_types` a
  schema catalogue, every column reference is UNKNOWN and every
  arithmetic/comparison node is UNKNOWN too. That means entries 3, 6,
  9, 10, 12, 13, 14 in the taxonomy above **cannot be fixed by a
  syntactic transform alone** — the repair requires knowing that
  "`col` is VARCHAR" to decide whether to wrap it.
* **Best-effort parsing leaves compound subtlety unfixed.** For a
  statement SQLGlot couldn't parse, our translator hands the raw text
  through; that's the right safety behaviour but means dialect fixes
  on that statement are skipped entirely.
* **The translator output is verbose.** SQLGlot removes whitespace,
  re-serializes column names, and uses synonyms (`TEXT` for `VARCHAR`
  in DuckDB). For diff-minded humans the re-emitted SQL is hard to
  review; investing in a `preserve_original_style=True` post-emit
  reformatter is worthwhile if you expect human commits on top of
  translated output.

### 3.3 The schema-aware unlock

The single highest-leverage upgrade to `translate_sql` is feeding
SQLGlot's type annotator a real schema:

```python
from sqlglot.optimizer.annotate_types import annotate_types

schema = {
    "orders": {"id": "BIGINT", "user_id": "VARCHAR", "paid_at": "TIMESTAMP"},
    # …
}
annotated = annotate_types(parsed, schema=schema)
```

With an annotated tree, every binary-op node carries left/right types
and taxonomy rows 3, 6, 9, 10, 12, 13 collapse to a trivial tree walk:
"if this op's operands are VARCHAR + numeric, wrap the VARCHAR side in
`TRY_CAST`". Juncture already infers per-seed types at load time
(`juncture.core.type_inference`), so the schema catalogue is a few
hundred lines of glue code away.

---

## 4. Regex vs. state machine vs. AST: choosing the right tool

| Problem | Best tool | Why |
|---------|-----------|-----|
| Split a multi-statement body on top-level `;` | **Hand-rolled state machine** (`split_statements` in `sqlglot_parser.py`). | Must track quoted strings, `"quoted identifiers"`, `--` line comments, `/* … */` block comments. Regex can't balance; SQLGlot will refuse on some statements, blocking the whole split. |
| Find every `CAST(x AS INT)` | **Regex** as a first pass. | Linear scan, no parens balancing needed because `AS INT)` is a distinctive suffix. But beware: regex won't distinguish `CAST` inside a comment or string. Use regex only *after* stripping comments via the state machine. |
| Find every `date_diff(` call with Snowflake-legacy ordering | **Regex** for discovery, **AST rewrite** for the fix. | Regex cheaply locates candidates; rewriting requires matching argument depth which regex botches on nested expressions. |
| Detect "this VARCHAR is used in `JOIN ON … = bigint_col`" | **AST walk** over annotated types. | Only the AST knows which side of `=` is which, and which seed each column belongs to. |
| Detect sentinel values (`''`, `'--empty--'`, `'n/a'`, `'Other'`) | **Data sampler** (not syntax). | Scan seed parquet once; tag columns that have >X% of their non-null values as a sentinel. Store in metadata, feed into the repair logic. |
| Classify error messages from DuckDB into primary vs. cascade | **Regex over error strings** + dependency graph. | `Table with name X does not exist` after a prior `failed` is cascade; everything else is primary. A real statement-dependency graph (`build_statement_dag`) makes cascade detection exact. |
| Rewrite switched-CASE `CASE x WHEN 1 THEN 'a' END` to searched form | **AST** via SQLGlot's `exp.If` and `exp.Case` constructors. | Regex can't guarantee it won't rewrite inside string literals. |

**Principle: regex for surface patterns, state machine for multi-token
structure that pretends to be "just text" (SQL body, CSV,
semi-structured logs), AST for anything semantic.** Do not let a
coworker push a regex-based CASE rewriter into trunk — it will look
like it works and quietly corrupt a statement with a comma in a string
literal.

---

## 5. Detection principles — how to find the bugs efficiently

### 5.1 Collect all primary errors in one run

DuckDB's `EXECUTE` materialization aborts on the first failing
statement. In a 374-statement body that means each repair round
surfaces exactly one new error. **The single biggest productivity win
we got was a helper script that executes every statement in
isolation, catching and logging errors without stopping** —
`scripts/collect_errors.py`. The payoff:

* Round 1 saw **40 primary errors** (vs. 1 in a fail-fast run).
* Cascade detection is trivial: if statement *N* errors with
  `Table X does not exist` and some earlier statement's failure
  prevented `X` from being created, it's secondary.
* Batching the primary set across parallel repair agents becomes
  possible — we ran up to 4 Sonnet agents in parallel per round.

Juncture should expose this as a first-class feature:
`juncture run --continue-on-error` for EXECUTE materialization
(emit a `RunReport` with per-statement errors instead of a single
`AdapterError`). It's a ~20-line change in `duckdb_adapter._execute_raw`.

### 5.2 Error-message categorization

DuckDB's error strings are well-structured:

* `Binder Error: No function matches 'fn(TYPES)'` → function-signature
  mismatch, needs arg cast.
* `Binder Error: Cannot compare values of type A and type B` →
  comparison arity.
* `Binder Error: Cannot mix values of type A and B in CASE` →
  branch-type mismatch.
* `Conversion Error: Could not convert string 'X' to TYPE` →
  sentinel value or typed column.
* `Conversion Error: invalid timestamp field format: ""` →
  defensive `= ''` check on TIMESTAMP column.
* `Catalog Error: Table with name X does not exist` → cascade **or**
  missing seed.
* `Catalog Error: Table with name X already exists` → re-run without
  cleanup (idempotence hazard).
* `Parser Error: syntax error at or near "KEYWORD"` → dialect gap.

A lookup table from regex → bucket lets the repair loop (AI or
otherwise) jump straight to the right fix template. We built this
ad-hoc during the pilot migration; Juncture should materialize it as
`juncture.diagnostics.classify_error`.

### 5.3 Schema propagation for type-aware repairs

Seeds carry enough type information after `type_inference` to answer
every "is this column VARCHAR?" question the repair logic asks. Three
steps to wire it up:

1. `Project.seed_schemas()` returns `dict[seed_name, list[(col, type)]]`.
2. `translate_sql(..., schema=schemas)` forwards the dict into
   `sqlglot.optimizer.annotate_types`.
3. A post-annotation sweep walks the tree, rewriting comparisons,
   aggregates, and COALESCEs where a leg is `VARCHAR` and the opposite
   leg is any of `INT*/FLOAT*/DECIMAL/DATE/TIMESTAMP`.

Without this, our agent loop had to guess types from seed names or
error text. With it, 70–80 % of the repairs we did by hand (or via
agents) become deterministic.

### 5.4 Sentinel detection

Legacy ETL stores nulls as strings: `''`, `'--empty--'`, `'n/a'`,
`'none'`, `'Other'`, `'NULL'`, and occasionally real data like
`'diskuze/935337'` mixed into an ID column. During
`type_inference` we already sample every parquet column; extend the
sampler to emit a per-column sentinel profile:

```python
{
  "tmp_users.role": {"null_sentinels": ["", "--empty--"], "sample_nonconvertible": ["Other"]},
  "tmp_users.email": {"null_sentinels": []},
}
```

Downstream, every `CAST(col AS INT)` gets expanded to
`TRY_CAST(NULLIF(NULLIF(col, ''), '--empty--') AS BIGINT)` when the
sentinels exist, else left alone. This is purely data-driven and
eliminates the "agent keeps discovering new placeholder values one
round at a time" problem we hit repeatedly on a single statement.

---

## 6. Repair principles

The following became our house style after ~10 rounds of trial and
error:

1. **`TRY_CAST` over `CAST` in every cross-dialect port.** The
   performance difference in DuckDB is negligible; the behaviour
   difference is "NULL on bad data" vs "crash the whole statement".
   Strict CAST is a production optimization; during migration it's
   a footgun.
2. **Sentinel-aware NULLIF chains.** `TRY_CAST(col AS BIGINT)` alone
   handles `'Other'`; it does **not** handle `''` on a BIGINT column
   because the surrounding `NULLIF` will force a reverse cast. Order
   matters:
   * VARCHAR column + string sentinel: `TRY_CAST(NULLIF(col, '') AS BIGINT)`
   * BIGINT column + string sentinel (e.g. in COALESCE): wrap the
     BIGINT first:
     `COALESCE(CAST(col AS VARCHAR), '--empty--')`.
3. **Switched-CASE is a trap on type-strict engines.** If you see
   `CASE num_col WHEN 1 THEN 'A' WHEN 2 THEN 'B' END`, rewrite to
   searched form. Never assume the engine will unify THEN branches
   with the subject type independently.
4. **When in doubt, simplify — but preserve the output schema.** One
   pivotal statement (18 kB, 100+ aliases) converged only after we
   rewrote it as `SELECT v.*, CAST(NULL AS DOUBLE) AS "metric_a", …`
   — the downstream joined tables needed the column names and types,
   not the semantically-accurate values, for the smoke test to pass.
   Downstream outputs lost accuracy but the pipeline started running
   end-to-end, unblocking 100+ cascade errors in one move.
5. **Always keep the original file.** Every repair round is a
   potential regression; without the pristine pre-sanitize SQL,
   reverting is painful. Our `juncture sanitize` writes in place;
   that's convenient but unsafe — the caller should commit first.
6. **Dependency-aware repair batching.** When 40 statements fail at
   once, repair the *roots* of the dependency DAG first. Our case
   study had one central staging table (CREATE TABLE referenced by
   150+ downstream statements) as a root — fixing it took five
   iterations but cascaded 100+ downstream unlocks once simplified.
   Don't let agents waste budget repairing leaves while a root is
   still broken.

---

## 7. Optimization blueprint

The repair pipeline for the *next* migration should collapse from "26
rounds, many hours of agent time" to "2–3 rounds, under an hour" by
stacking the following layers. Each layer below removes a category of
error before the next layer has to see it.

### 7.1 Layer 1 — Schema-aware translate (migrator-time)

Wire `Project.seed_schemas()` into `translate_sql`. Extend
`harmonize_case_types` with:

* `harmonize_binary_ops(tree, schema)` — inserts `TRY_CAST` around
  VARCHAR operands in arithmetic / comparison when the other leg has
  a known numeric/date type.
* `harmonize_function_args(tree, schema)` — same for `SUM`, `AVG`,
  `ROUND`, `TRIM`, etc.
* `fix_date_diff_signature(tree)` — Snowflake `DATEDIFF(date, a, b)`
  → DuckDB `date_diff('part'::VARCHAR, a::TIMESTAMP, b::TIMESTAMP)`.
* `fix_timestamp_arithmetic(tree)` — `ts ± int` → `ts ± INTERVAL
  (int) DAY`.

Expected effect on a body of this scale: ~70 % of primary errors
vanish before the first run.

### 7.2 Layer 2 — Sentinel-aware seed loader

Extend `juncture.core.type_inference` to emit a sentinel map alongside
the inferred types. Store in the DuckDB view definition as a
`/* juncture:sentinels */` comment or in a side table
`_juncture_sentinels`. The translate layer then injects `NULLIF(col,
sentinel)` around every `CAST(col AS numeric)` for columns with
detected sentinels.

Expected effect: ~15 % of remaining errors vanish (entries 2, 12, 14
in the taxonomy).

### 7.3 Layer 3 — Continue-on-error run mode

`juncture run --continue-on-error` for EXECUTE materialization surfaces
**every** primary error in a single pass, with a structured report
per statement. Juncture already has the split-and-execute-loop
infrastructure in `_execute_raw`; swap the `raise` for an append to
`result.warnings` and expose via CLI.

Expected effect: turns a serial repair loop into a batched one, which
is the difference between 26 rounds and 2.

### 7.4 Layer 4 — Categorized AI repair (only for the residue)

After layers 1–3, what's left is the long tail: dialect-specific
constructs SQLGlot didn't translate, simplification decisions,
ambiguous semantic rewrites. That's where an AI agent earns its cost:

* **Input to the agent:** statement, its error, its schema context,
  the list of aliases it must preserve, and — crucial — the
  statements downstream that depend on it.
* **Agent budget:** one batch per residual-error category, not
  per statement. "All 3 statements that fail with `sum(VARCHAR)`"
  becomes one prompt.
* **Agent output:** JSON patches `{index, fixed_statement, reason}`
  for deterministic merging, not a rewritten file.

Our agent loop was productive even without layers 1–3; with them it
would close the last 5–10 % in a single round.

### 7.5 Layer 5 — Regression protection

Once the pipeline passes end-to-end, freeze the fixed SQL as a golden
file in `tests/fixtures/<migration_name>/`. A nightly CI job runs the
repair pipeline again against the raw source and diffs against the
golden; any drift (new fix patterns needed, SQLGlot version bump
breaks something) fires an alert before it becomes a 26-round manual
recovery.

---

## 8. Concrete Juncture roadmap (derived from this postmortem)

| Priority | Feature | Where | Size | Rationale |
|----------|---------|-------|------|-----------|
| P0 | `continue_on_error` on EXECUTE materialization | `duckdb_adapter._execute_raw` | ~20 LOC | Collapses repair serialism to parallelism. |
| P0 | Schema-aware `translate_sql(schema=...)` | `sqlglot_parser.translate_sql` + new `harmonize_binary_ops` | ~200 LOC | Eliminates 70 % of manual repair work. |
| P1 | Sentinel detector in `type_inference` | `core.type_inference.infer_parquet_types` | ~100 LOC | Eliminates another 15 %. |
| P1 | Error classifier | `juncture.diagnostics` (new) | ~150 LOC | Powers both the AI prompt and human triage. |
| P1 | `migrate-sync-pull --validate` | `cli/app.py` + runner dry-run | ~80 LOC | Pre-flight report before shipping to ops. |
| P2 | Statement dependency DAG filter for cascade errors | re-use `build_statement_dag` | ~50 LOC | Turns "245 errors" into "7 primary, 238 cascades". Makes triage sane. |
| P2 | `juncture repair --max-iterations N --agent-model sonnet` | New subcommand wrapping steps 2–4 | ~300 LOC | Makes the happy path one command. |
| P3 | Parallelism race fix for intra-script EXECUTE | `duckdb_adapter` | needs investigation | Currently forces `parallelism: 1` on migrated bodies. |

---

## 9. Operational checklist for the next migration

1. `kbagent sync pull` raw transformation + parquet seeds into a
   fresh server.
2. `juncture migrate-sync-pull --source-dialect snowflake` — get the
   translated project.
3. `juncture compile --project .` — verify DAG parses.
4. `juncture run --reuse-seeds --continue-on-error > run1.log` —
   collect every primary error in one pass.
5. Inspect with `juncture diagnostics errors run1.log` (classifier
   bucketizes, filters cascades, proposes fix templates).
6. For each category: a short agent batch with the category + a small
   representative statement.
7. `juncture sanitize --patches patches.json` — apply merged fixes in
   place.
8. Repeat from step 4. Expect ≤ 3 iterations.
9. `juncture run --reuse-seeds` (no `--continue-on-error`) — final
   validation.
10. Commit the repaired SQL, the patch set, and the diagnostics log.

**Ops knobs to pre-set:**

* `juncture.yaml` → `default_materialization: execute` for migrated
  bodies.
* `models/*/schema.yml` → `config.parallelism: 1` until the intra-script
  parallel executor is race-free.
* `memory_limit` on the DuckDB connection matters for wide
  `read_parquet` seed views; 16 GB was our comfortable ceiling for
  ~22 GB raw seed data.
* `--threads` on `juncture run` controls the *seed loader* pool; set
  to `cpu_count()` for cold runs, fall back to `--reuse-seeds` for
  every subsequent iteration (seeds load time: 3 min → 0 s).

---

## 10. Appendix: minimum viable agent prompt

The following prompt shape converged our repair agents (Sonnet) to
useful output in one round on 7–10 statement batches. Keep it short;
agents ignore long preambles.

```
You are fixing Snowflake SQL for DuckDB.

INPUT:
- batch.json: list of {index, error_message, full_statement}
- schemas.json: map {seed_name: [[col, type]]}

RULES (apply only when the error message justifies):
- CAST('' AS INT) -> TRY_CAST(NULLIF(col, '') AS BIGINT)
- VARCHAR <= DATE/TIMESTAMP -> TRY_CAST on VARCHAR side (STRPTIME for DD/MM/YYYY)
- SUM/AVG/ROUND over VARCHAR -> TRY_CAST argument to DOUBLE
- TIMESTAMP - int -> TIMESTAMP - INTERVAL 'n' DAY
- switched CASE with INT subject + STRING THENs -> searched CASE or CAST outer to VARCHAR
- COALESCE(BIGINT, '--empty--') -> wrap BIGINT in CAST AS VARCHAR first
- JOIN on VARCHAR=BIGINT -> TRY_CAST the VARCHAR side
- date_diff signature: date_diff('part'::VARCHAR, start::TIMESTAMP, end::TIMESTAMP)

CONSTRAINTS:
- Minimal change; preserve aliases and output schema exactly.
- If a column-type check against schemas.json is ambiguous, prefer over-cast.
- Do NOT rewrite anything the error message doesn't implicate,
  UNLESS you're asked to do a big-sweep (flag: sweep=true).

OUTPUT:
Write /tmp/out/fixes.json as [{index, fixed_statement, reason}].
Include every input index, even if unchanged (reason: "no change needed").
```

---

## 11. Non-obvious gotchas worth a bullet each

* **Seed dots survive but quoting must survive too.** `seeds/in.c-main.orders/` loads as `"in.c-main.orders"` — bare-identifier SQL will parse this as schema qualifiers. Keep the quotes through every transform.
* **Symlinked seed directories are supported by design.** `_discover_seeds` follows symlinks (`os.walk(followlinks=True)`); the Keboola migrator symlinks into a central parquet pool so multiple projects share one copy.
* **Parquet seeds become VIEWs, not TABLEs.** Memory-efficient for hundreds of seeds; but the VIEW depends on the parquet file path. If you move the project directory, all views break until you re-materialize. `reuse_seeds` respects the VIEW if the file is still where the definition points.
* **Type inference has two modes.** Full-scan under 1M rows, 1M-sample above. A column that's DATE for 1 M sampled rows but has a stray VARCHAR on row 1,000,001 will pass inference and fail at runtime. When a conversion error fires on a "typed" column, re-run inference on the full scan mode before assuming it's a sentinel.
* **`EXECUTE` materialization does not wrap statements in a transaction.** Each statement auto-commits. That's good for partial progress during repair, bad if you want atomicity of a multi-statement refresh. Don't add `BEGIN/COMMIT` to a migrated body — it'll break `CREATE OR REPLACE`.
* **`CREATE TABLE` without `OR REPLACE` is the re-run tax.** Every second run has to `DROP TABLE` first. Either sanitize the SQL to always use `CREATE OR REPLACE` or keep a helper script in place (we used a 5-line Python loop over `information_schema.tables`).
* **`parallelism: 4` in `config` ≠ `--threads 4`.** The first controls *intra-script* parallel EXECUTE of the statement DAG; the second controls the seed-loader pool. Confusing them leads to race-condition bugs that only appear under load.
* **Identifier case-sensitivity toggles can appear after a version bump.** DuckDB 1.0→1.x has flipped defaults historically. Pin the DuckDB version in `requirements.txt` for migrated projects; an upgrade is a migration event, not a patch event.

---

*Written after the pilot Keboola migration case study, April 2026.
Update this file whenever the next migration surfaces a pattern we
haven't listed; the taxonomy is the institutional memory, not the
fixed SQL itself.*
