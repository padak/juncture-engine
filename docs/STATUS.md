# Status — aktuální bod práce

> Živý dokument. Aktualizuj po každé dokončené fázi / významném commitu.
> Psáno česky pro Petra. Kód, API, commity a ostatní docs zůstávají v angličtině.
>
> **Last updated:** 2026-04-18 · branch `main` · **Phase 1 gate closed + Phase 1.6 ergonomics wave + onboarding track shipped.** Mezi gate a Phase 2 doběhly: web UI v2 end-to-end (M1–M4 + sidebar polish, PR #8–#13), Jinja macros (#15), profiles (#18), tutorial + `examples/tutorial_shop/` (#16), CLI sub-app grouping (#14), README rewrite + screenshoty (#17 + follow-up docs commity). Další krok: **Phase 2 adapters** (Snowflake / BigQuery / JDBC) + real SAPI upload v Keboola wrapperu.

## Point: co a proč děláme

Juncture je multi-backend SQL + Python transformační engine, který nahrazuje čtyři
legacy Keboola komponenty jedním enginem (SQL + Python v jednom DAGu, lokálně přes
DuckDB, v produkci přes SQLGlot translaci na Snowflake/BigQuery/JDBC). Motivaci
a dlouhodobý cíl viz [`VISION.md`](VISION.md); sekvenci fází, jak to dodáváme,
viz [`STRATEGY.md`](STRATEGY.md).

## Kde jsme ve fázích

**Fáze 1 — DuckDB-first + web render + E2E proof** (viz
[`STRATEGY.md`](STRATEGY.md)). **Jsme uvnitř Phase 1.** Done-done kritérium: nový
uživatel naklonuje repo, načte reálnou Keboola transformaci přes
`juncture migrate-sync-pull`, spustí ji lokálně proti DuckDB, a v prohlížeči na
`localhost:N` vidí DAG + run history projektu. **Engine část je zavřená, pilotní
migrace E2E prošla; chybí `continue-on-error` + schema-aware translate + web
render.** Web render je závazná brána do Phase 2.

## Engine capabilities shipped

Commity na `phase-3-slevomat-migration` nad MVP basline `04eaac5`:

| Commit | Věc |
|---|---|
| `239d5c3` | parquet seed loader + quoted identifiers v DuckDB adapteru |
| `75d7134` | `EXECUTE` materializace (multi-statement SQL as-is) + `sync-pull` migrator |
| `6f18b0f` | `_discover_seeds` následuje symlinkované adresáře (`os.walk(followlinks=True)`) |
| `44d2f6a` | parquet seedy jako VIEW (ne TABLE) + DuckDB `memory_limit` / `temp_directory` |
| `cfbc5ee` | hybridní type inference (full-scan < 1 M řádků, sample nad 1 M) |
| `0e76ff7` | paralelní seed loading přes `ThreadPoolExecutor` |
| `5dc2485` | `scripts/analyze_execute.py` — intra-script dependency DAG nástroj |
| `4fc601d` | `harmonize_case_types` AST pass + `juncture sanitize` CLI (odblokuje VARCHAR/INT v CASE) |
| `a300e37` | `StatementNode` + `build_statement_dag` — intra-script DAG API v parseru |
| `840fd1e` | **Parallel EXECUTE** — `config.parallelism: N` iteruje vrstvy přes `ThreadPoolExecutor` |
| `8637137` | **Dry-run** — `juncture run --dry-run` ukáže plán bez otevření DB |
| `017b920` | `--reuse-seeds` přeskočí re-inference a re-materializaci seedů |
| `2256c3f` | `juncture split-execute` rozloží EXECUTE monolit na mini-modely s auto-inferovanými `ref()` |
| `e7ced30` | lepší YAMLError hint v `schema.yml` loaderu |
| `499ab5d` | `--dry-run` už nemíchá seedy do vrstev modelů |
| `540194d` | `--parallelism / -P` override na CLI pro benchmark runy |
| `b186606` | `juncture compile --dot <file>` exportuje DAG jako Graphviz DOT |

Architekturu viz [`DESIGN.md`](DESIGN.md); detailní task list viz
[`ROADMAP.md`](ROADMAP.md).

## Pilot migration — real-world test

Produkční Keboola transformace jednoho reálného klienta jako forcing function pro
parquet seedy, EXECUTE materializaci a type inference.

- **Vstup:** `kbagent sync pull` layout, **374 Snowflake SQL statementů**,
  **208 parquet seedů** (~22 GB).
- **Výsledek: 100 % úspěch** — všech 374 statementů běží E2E proti DuckDB.
  K tomu bodu vedlo **26 iterací agent-driven repair**; playbook, taxonomie chyb
  a plán, jak to příště zkrátit na 2–3 iterace, je v
  [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md).
- **Artefakty na test serveru** (DO droplet 4 vCPU / 32 GB, volume
  `/mnt/volume_nyc1_juncture/juncture-data/`): původní migrovaný projekt
  s jedním `EXECUTE` monolitem a paralelně **311-modelový `split-execute`
  derivát** určený pro DAG-level paralelní benchmark. (Dva adresáře vedle sebe
  si drží historické jméno z interního trackingu — cesty jsou v ops poznámkách,
  ne tady.)
- **Benchmark scénáře S1–S5** (sekvenční baseline, parallel EXECUTE {2, 4, 8},
  split-execute DAG s `--threads N`) jsou rozepsané v
  [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §9 operational checklist; čísla
  poputují do [`BENCHMARKS.md`](BENCHMARKS.md).

## Sprint A+B — Post-pilot hardening *(P0+P1 hotovo, historické)*

Z pilotu vypadlo devět konkrétních Juncture featur ve dvou sprintech (A a B).
Priority jsou z [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §8:

| Priorita | Feature | Kde | Status |
|---|---|---|---|
| **P0** | `juncture run --continue-on-error` na EXECUTE | `duckdb_adapter._execute_raw` | **done** (`bc572f6`) |
| **P0** | Schema-aware `translate_sql(schema=...)` | `sqlglot_parser.translate_sql` | **done** (`edb07ab`) |
| **P1** | Sentinel detector v `type_inference` | `core.type_inference.detect_sentinels` | **done** (`c31809d`) |
| **P1** | Error classifier `juncture diagnostics` | `juncture.diagnostics` (nový modul) | **done** (`7f6a4be`) |
| **P1** | `migrate-sync-pull --validate` | `migration.keboola_sync_pull` + CLI | **done** (`72abadd`) |
| **P2** | Statement dependency DAG filter na cascade errory | re-use `build_statement_dag` | pending |
| **P2** | `juncture repair --max-iterations N` orchestrátor | nový subcommand | pending |
| **P3** | Fix race condition intra-script paralelního EXECUTE | `duckdb_adapter` | pending |

**Výsledný efekt:** plán z [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §7
("~70 % primárních chyb vymizí před prvním runem, dalších ~15 %
sentinelů vymizí sampling pass, continue-on-error zkolapsuje repair
loop ze seriálního na dávkový") je teď v kódu. Zbývá ho ověřit další
migrací.

## Phase 1 gate — CLOSED

Z [`STRATEGY.md`](STRATEGY.md) Phase 1 máme ve stavu **hotovo** vše
kromě P3 race fixu (neblokuje gate):

- [x] **Continue-on-error + diagnostics** (Sprint A z
      [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §8 — P0/P1 výše).
- [x] **Schema-aware `translate_sql`** — napojeno na `Project.seed_schemas()`
      + SQLGlot `annotate_types`; `harmonize_binary_ops` vloží `TRY_CAST` kolem
      VARCHAR operandů (Sprint B).
- [x] **Sentinel detector** v `type_inference` — per-column sentinel profily
      (downstream injection do `CAST`/`TRY_CAST` wrapperů je follow-up).
- [ ] **Intra-script parallel EXECUTE race fix** (P3 — pending, neblokuje
      gate, nutí `parallelism: 1` na migrovaných bodies).
- [x] **Web render** — `juncture web --project <p>` startuje stdlib
      `http.server` + cytoscape.js DAG + run-history z
      `target/run_history.jsonl`. Vendored JS, žádný build, žádné extras.
- [x] Pilot-migration benchmark čísla zaznamenaná v
      [`BENCHMARKS.md`](BENCHMARKS.md) (7 scénářů: monolith cold/warm,
      parallel EXECUTE, split DAG cold + threads 1/4/8).

## Phase 1.6 ergonomics & onboarding — shipped

Po zavření gate dopadly featury, které Phase 1 nedefinovala, ale dávají
Juncture ruku pro reálné použití — mix engine polishe, UX knobu a
onboardingu:

- **EU e-commerce demo projekt** (`examples/eu_ecommerce/`): 16 modelů
  (13 SQL + 3 Python), 57 data tests, deterministický data generator
  ve třech škálách. Primární E2E showcase; mapuje 1:1 na
  [`VISION.md`](VISION.md) 10 problémů.
- **Model disable toggle** — `disabled: true` v `schema.yml` + CLI
  `--disable` / `--enable-only`. `status=disabled` + downstream
  `skipped_reason=upstream_disabled`, run se nefailuje.
- **Jinja macros** (`macros/**/*.sql`, PR #15) — dbt-style global
  loader. Když `jinja: true`, každý `{% macro %}` je automaticky
  Jinja global ve všech modelech bez `{% import %}`. Odemyká
  "define a rule once, use everywhere" (VISION §Problem 2).
- **Profiles (`profiles:` block)** (PR #18) — pojmenované overlays nad
  `juncture.yaml` pro dev/staging/prod split. `--profile` /
  `JUNCTURE_PROFILE` / top-level `profile:` precedence. Per-key merge
  na `vars` a `connections.<name>`, wholesale replace na skalárech.
  Odemyká kbagent-branch per-schema mapování.
- **CLI subcommand grouping** (PR #14) — `sql` / `migrate` / `debug`
  sub-apps nad core `init / compile / run / test / docs / web`.
  Stabilnější top-level surface; `juncture translate` → `juncture sql
  translate`, `juncture diagnostics` → `juncture debug diagnostics` atd.
- **Onboarding tutorial** (PR #16) — [`docs/TUTORIAL.md`](TUTORIAL.md)
  čtyřúrovňový walkthrough (L1 zero → L2 Python v DAGu → L3 macros +
  ephemeral → L4 external `--var`) + funkční L4 projekt v
  `examples/tutorial_shop/`. `make examples` ho pouští s `--var`
  overridem, tedy nárok L4 je continuously verified.
- **Bug fix:** `--var` CLI flag ovlivňoval jen Python `ctx.vars()`;
  SQL Jinja se renderovalo s juncture.yaml defaults. `Project.load`
  teď přijímá `run_vars` → konzistentní precedence napříč SQL + Python
  (chycené při psaní tutorial L4).
- **README rewrite** (PR #17 + docs-only commity) z pohledu nového
  usera: hero + Runs screenshots, honest "What ships today" catalog,
  "Development plan" sjednocený Phase 2 + Phase 4 seznam, etymologie
  názvu jako mnemotechnika pitch bodů.

**Můžeme začít Phase 2 (Snowflake/BigQuery/JDBC adaptéry + real SAPI
upload v Keboola wrapperu).**

## Paralelní track: Web UI v2

Po prvním použití Phase 1 web render (PR #5) vypadlo pět konkrétních
vad, které shrnuje RFC [`docs/rfcs/0001-web-ui-v2.md`](rfcs/0001-web-ui-v2.md).
Čtyři milníky M1–M4 (viz [`ROADMAP.md`](ROADMAP.md) §1.8) přidají
source viewer, diagnostics v prohlížeči, seeds tab a CDO portfolio
vrstvu. Běží paralelně s Phase 2 adaptéry — žádný track neblokuje
druhý.

| Milník | Branch | Stav |
|---|---|---|
| M1 — P0 "readable" | `feat/web-v2-readable` | **merged (#8)** — source viewer + kind/status split + clickable Runs drawer + tests panel |
| M2 — P1 overview + search + export | `feat/web-v2-overview` | **merged (#9)** — Project tab (juncture.yaml + README + git) + DAG search + manifest/OpenLineage download |
| M3 — P1 diagnostics + seeds + reliability | `feat/web-v2-diagnostics` | **merged (#10)** — Seeds tab + Diagnostics bucket panel + reliability sparkline + LLM knowledge download |
| M4 — P2 governance (PR1/2: schema) | `feat/web-v2-governance` | **merged (#11)** — `schema.yml` + seeds gain owner/SLA/docs/consumers + pii/retention/source_system |
| M4 — P2 governance (PR2/2: UI) | `feat/web-v2-compliance` | **merged (#12)** — Portfolio + Reliability tabs, PII ring propagation v DAGu, contract endpoint, long-form docs |
| Follow-up — sidebar polish | `feat/web-v2-sidebar-polish` | **merged (#13)** — card-based Metadata layout, collapsible Legend, sticky Detail tabs, export popover |

## Risks / open questions

- **Split-execute vs monolith EXECUTE performance tradeoff — vyřešeno.**
  Split DAG (3:06 warm s threads=4) je o ~13 % rychlejší než monolith
  EXECUTE (3:34 warm); threads > 4 už nic nepřidá (widest layer 293 sytí
  4 CPU). Detail: [`BENCHMARKS.md`](BENCHMARKS.md) §Pilot-migration.
- **Intra-script parallel EXECUTE race condition (P3).** Dnes nuceně
  `parallelism: 1` na migrovaných bodies — blokuje jeden z benchmark scénářů.
  **Neblokuje Phase 1 gate**, ale je to první rozumný Phase 2 side quest.
- **kbagent OOM bug** reported proti
  <https://github.com/padak/keboola_agent_cli>. Nevlastní Juncture, ale limituje
  migrace dostatečně velkých projektů.
- **Ephemeral materializace dnes vyrobí VIEW, ne pravé CTE inlinování.**
  `duckdb_adapter._build_materialization_statement` pro `EPHEMERAL` spouští
  `CREATE OR REPLACE VIEW`, ale [`DESIGN.md`](DESIGN.md) §3.5 říká "inlined
  upstream at render time". Funkčně OK pro DuckDB (downstream SELECT trefí
  view bez kopie), ale na Snowflake/BigQuery by se choval jinak než dbt.
  Dořešit v Phase 2 spolu s adaptéry.
- **`{{ var() }}` v SQL vyžaduje `jinja: true`.** Dnešní mini-makro parser
  zná jen `ref()`. Bez `jinja: true` flagu projde `{{ var('x') }}` SQLGlotu
  verbatim a rozbije parse. Dopsat do [`CONFIGURATION.md`](CONFIGURATION.md).

## Co NENÍ teď priorita

Záměrně odložené až za Phase 1 gate (mirroring [`STRATEGY.md`](STRATEGY.md)
Phase 2 a dál):

- Snowflake / BigQuery / JDBC adaptéry (Phase 2 — až po web renderu).
- Produkce Keboola component wrapperu — real SAPI upload, branch mapping,
  OpenLineage napojení (Phase 2).
- MCP server jako shipping produkt (Phase 3) — skeleton už existuje
  v `juncture.mcp.server`, ale dokud nebude API freeze, nešpekujeme.
- v2.0 differentiators — virtual data environments, semantic layer, AI dialect
  arbitrage, agentic authoring (Phase 4).
