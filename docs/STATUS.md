# Status — aktuální bod práce

> Živý dokument. Aktualizuj po každé dokončené fázi / významném commitu.
> Psáno česky pro Petra. Kód, API, commity a ostatní docs zůstávají v angličtině.
>
> **Last updated:** 2026-04-18 · branch `feat/phase-1-disable` · disable toggle landed.

## Point: co a proč děláme

Juncture je multi-backend SQL + Python transformační engine, který nahrazuje čtyři
legacy Keboola komponenty jedním enginem (SQL + Python v jednom DAGu, lokálně přes
DuckDB, v produkci přes SQLGlot translaci na Snowflake/BigQuery/Postgres). Motivaci
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

## Current sprint: Post-pilot hardening **(P0+P1 hotovo)**

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

## Phase 1 gate — almost closed

Z [`STRATEGY.md`](STRATEGY.md) Phase 1 zbývají tyto unchecked deliverables,
než půjdeme na Phase 2:

- [x] **Continue-on-error + diagnostics** (Sprint A z
      [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §8 — P0/P1 výše).
- [x] **Schema-aware `translate_sql`** — napojeno na `Project.seed_schemas()`
      + SQLGlot `annotate_types`; `harmonize_binary_ops` vloží `TRY_CAST` kolem
      VARCHAR operandů (Sprint B).
- [x] **Sentinel detector** v `type_inference` — per-column sentinel profily
      (downstream injection do `CAST`/`TRY_CAST` wrapperů je follow-up).
- [ ] **Intra-script parallel EXECUTE race fix** (P3 výše).
- [ ] **Web render** — malý Python HTTP server (`juncture docs --serve` nebo
      ekvivalent) renderuje compiled DAG, per-model schema a run history
      z manifestu. **Závazná brána Phase 1 → Phase 2.** V kódu zatím není.
- [x] Pilot-migration benchmark čísla zaznamenaná v
      [`BENCHMARKS.md`](BENCHMARKS.md) (7 scénářů: monolith cold/warm,
      parallel EXECUTE, split DAG cold + threads 1/4/8).

Navíc příjde **Balík 0: EU e-commerce demo projekt** (`examples/eu_ecommerce/`)
— 16 modelů (13 SQL + 3 Python), 57 data tests, deterministický data
generator ve třech škálách. Nahrazuje Slevomat jako primární E2E showcase
a mapuje 1:1 na [`VISION.md`](VISION.md) 10 problémů (ephemeral macro
ekvivalent, parametrizované segmenty, mix SQL + Python v jednom DAGu).
Hotové na branchi `feat/phase-1-demo-ecommerce`.

## Risks / open questions

- **Split-execute vs monolith EXECUTE performance tradeoff — vyřešeno.**
  Split DAG (3:06 warm s threads=4) je o ~13 % rychlejší než monolith
  EXECUTE (3:34 warm); threads > 4 už nic nepřidá (widest layer 293 sytí
  4 CPU). Detail: [`BENCHMARKS.md`](BENCHMARKS.md) §Pilot-migration.
- **Intra-script parallel EXECUTE race condition (P3).** Dnes nuceně
  `parallelism: 1` na migrovaných bodies — blokuje jeden z benchmark scénářů.
- **kbagent OOM bug** reported proti
  <https://github.com/padak/keboola_agent_cli>. Nevlastní Juncture, ale limituje
  migrace dostatečně velkých projektů.
- **Web render not started.** Binding gate Phase 1 → Phase 2 leží na zelené
  louce; dokud neproběhne, nezačínáme na Snowflake/BigQuery/Postgres adaptérech.

## Co NENÍ teď priorita

Záměrně odložené až za Phase 1 gate (mirroring [`STRATEGY.md`](STRATEGY.md)
Phase 2 a dál):

- Snowflake / BigQuery / Postgres adaptéry (Phase 2 — až po web renderu).
- Produkce Keboola component wrapperu — real SAPI upload, branch mapping,
  OpenLineage napojení (Phase 2).
- MCP server jako shipping produkt (Phase 3) — skeleton už existuje
  v `juncture.mcp.server`, ale dokud nebude API freeze, nešpekujeme.
- v2.0 differentiators — virtual data environments, semantic layer, AI dialect
  arbitrage, agentic authoring (Phase 4).
