# Status — aktuální bod práce

> Živý dokument. Aktualizuj po každé dokončené fázi / významném commitu.
> Psáno česky pro Petra. Kód, API, commity a ostatní docs zůstávají v angličtině.
>
> **Last updated:** 2026-04-18 · branch `phase-3-slevomat-migration` · commit `HEAD` (po Phase 3c — parallel EXECUTE).

## Point: co a proč děláme

Nahrazujeme čtyři Keboola transformační komponenty (`snowflake-`, `python-`,
`duckdb-`, `dbt-transformation`) jedním enginem — **Juncture**. Hlavní důvod:
dnes nemluví komponenty mezi sebou, kód nežije v gitu, mix SQL + Python je
nemožný, paralelismus je ruční. Juncture sjednocuje všechno do jednoho DAGu
(SQL + Python), běží lokálně proti DuckDB, je agent-friendly a v produkci
cílí na Snowflake / BigQuery / Postgres přes SQLGlot translaci.

Motivaci a historii viz [`RESEARCH.md`](RESEARCH.md), architekturu
[`DESIGN.md`](DESIGN.md), fázovaný plán [`ROADMAP.md`](ROADMAP.md).

## Kde jsme

**Fáze 1 — MVP (v0.1):** ✓ hotová. Initial commit `04eaac5`.
**Fáze 2 — ergonomie (v0.2):** ✓ ve velkém hotová (seedy, Jinja, env vars,
custom SQL testy, incremental state store, env var interpolation, migration
helper). Pár položek z `ROADMAP.md` ještě čeká — viz sekce v0.2 tam.
**Fáze 3 — Slevomat E2E migrace:** rozběhnutá, **ještě nedoběhla end-to-end**.

### Co přibylo na Phase 3 branchi (commity nad `04eaac5`)

| Commit | Věc |
|---|---|
| `239d5c3` | parquet seed loader + quoted identifiers v DuckDB adapteru |
| `75d7134` | `EXECUTE` materializace (multi-statement SQL as-is) + sync-pull migrator |
| `6f18b0f` | `_discover_seeds` následuje symlinkované adresáře (`os.walk(followlinks=True)`) |
| `44d2f6a` | parquet seedy jako VIEW (ne TABLE) + DuckDB `memory_limit` / `temp_directory` |
| `cfbc5ee` | hybridní type inference (full-scan < 1 M řádků, sample nad 1 M) |
| `0e76ff7` | paralelní seed loading přes `ThreadPoolExecutor` |
| `4fc601d` | CASE type harmonization Snowflake→DuckDB + `juncture sanitize` CLI (řeší VARCHAR/INT blocker) |
| `a300e37` | `StatementNode` + `build_statement_dag` — intra-script DAG API v parseru |
| _HEAD_ | **Parallel EXECUTE** — `config.parallelism: N` iteruje vrstvy přes `ThreadPoolExecutor` |

### Infrastruktura

- DigitalOcean droplet 4 vCPU / 32 GB RAM, IP `159.65.220.209`, volume
  `/mnt/volume_nyc1_juncture/juncture-data/slevomat-project/`.
- Nainstalováno `kbagent` + `juncture` (editable install).
- Read-only Storage API token pro Slevomat, ~22 GB parquet dat,
  208 tabulek jako symlinkované seedy.

## Reálný test case: Slevomat migrace

- Vstup: `main/transformation/keboola.snowflake-transformation/<name>/`
  layout z `kbagent sync pull`.
- Migrovaný script: **`slevomat_main_task.sql`**, 673 řádků, 374 statementů.
- Sync-pull migrator vygeneroval `juncture.yaml`, `seeds/` (symlinks na
  parquet) a `models/<name>.sql` s `EXECUTE` materializací.
- **SQLGlot:** 374/374 statementů Snowflake → DuckDB přeloženo bez chyb.
- **Seedy:** 208 parquet adresářů loadnuto (paralelně, ~45 min → zrychleno
  na ~cca 10 min po `ThreadPoolExecutor`).
- **Executor blocker:** VARCHAR vs `INTEGER_LITERAL` v `CASE` výrazech.
  Slevomat drží vše v Storage jako VARCHAR, takže DuckDB CAST je nutný.
  Commit `cfbc5ee` zavádí hybridní type inference — **ověření na
  Slevomatu ještě neproběhlo** (předchozí session končila na API 529 /
  prompt-too-long).

## Co zbývá dodělat

### Bezprostředně (dokončit Phase 3)

- [ ] Dotáhnout Slevomat E2E — typové chyby v původním SQL řešíme
      manuálně přes Sonet agenty (není to problém Juncture).
- [ ] Změřit **sekvenční baseline** vs **parallel EXECUTE** na Slevomatu
      — různé hodnoty `parallelism` (1, 2, 4, 8) → `BENCHMARKS.md`.
- [ ] Rozhodnout, jestli potřebujeme **Cestu B (split monolitu na
      mini-modely)** na základě reálného zisku z Cesty C.
- [ ] Nahlásit / opravit OOM v kbagent (z Phase 3 kick-off).

### Phase 2 / v0.2 — zbývající položky

- [ ] **Model selectors** rozšířit o `path:marts/`, `state:modified+`.
- [ ] **Unit testy modelů** (input → expected output v YAML).
- [ ] **`juncture docs --serve`** — statický HTML s DAG + sloupci (minimální
      React site v roadmapě).
- [ ] **Structured logging** (JSON mode pro ingestion).
- [ ] **pre-commit hooks** — ruff, mypy, schema.yml lint.

### Phase 4 — "real backends" (v0.3)

- [ ] Snowflake adapter — MERGE incrementals, Arrow fetch, CLUSTER BY.
- [ ] BigQuery adapter — partitioning, clustering, external tables.
- [ ] Postgres adapter — ON CONFLICT pro incrementals.
- [ ] Dialect guard: detekce inkompatibilních funkcí při compile.
- [ ] Connection-agnostic testy (DuckDB lokálně = Snowflake v produkci).

### Phase 5 — "Keboola integration" (v0.4)

- [ ] Keboola component Docker image s reálným SAPI uploadem (teď jen stub).
- [ ] Auto-generate `juncture.yaml` z Keboola config na straně wrappera.
- [ ] Dev/prod branch support — Keboola branches → separate schemas.
- [ ] OpenLineage eventy napojené na Keboola Lineage.
- [ ] Job artifacts — každý run uploaduje `manifest.json` + logy.

### Web UI / render

- Zvažovaný "webový render CLI" (malý Python server pro vizualizaci DAGu /
  cfg) zmíněný jako Phase 1/2 cíl — v commitech zatím **nenalezen**.
  Rozhodnout: potřebujeme to, nebo to spadne do `juncture docs --serve`?

## Poznámka k demo scénáři

Cílový flow pro prezentaci:
1. `juncture migrate-keboola-sync-pull ...` na Slevomat repozitáři.
2. `juncture run --project /tmp/slevomat` (DuckDB, lokálně).
3. `juncture test`.
4. Přehodit `connection: snowflake` v `juncture.yaml` a spustit totéž
   v Keboole přes Docker wrapper.

Kde je nejbližší blocker: bod 2 (VARCHAR/INT CASE). Jakmile spadne, flow
prochází end-to-end.
