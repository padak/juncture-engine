# RFC 0001 — Web UI v2

*Status: proposed · 2026-04-18 · supersedes the minimum viable web
render shipped in PR #5 (commit `6d80db9`).*

## 1. Motivation

The Phase 1 gate web render (`juncture web`, PR #5) closed the
strategic binding gate between Phase 1 and Phase 2 — a user can
clone the repo, run a project, and inspect the DAG in a browser.
First user feedback (2026-04-18, against `examples/eu_ecommerce` at
commit `88368f1`) identified five concrete gaps that block the UI
from being usable for anyone who did not build the project:

1. Node type (seed / SQL / Python) is visually dominated by the
   "last-run status" colour — a user cannot tell SQL from Python at
   a glance.
2. No way to read the SQL body / Python source of a model from the
   UI; you have to `cat` the file in another terminal.
3. No project-level overview: no README, no config snapshot, no
   authorship / metadata surface.
4. "What does this model actually do?" — `description` is a single
   line; there is no long-form doc surface.
5. In the Runs view the per-model table is not clickable — no
   drilldown into per-model errors, statement-level diagnostics,
   or test results.

This RFC proposes a prioritised follow-up that turns the current
"glance only" UI into a tool usable by both a **data engineer** (DE)
debugging a failing run and a **Chief Data Officer** (CDO)
auditing the portfolio. The scope is large enough that it is not
one PR; the RFC splits it into three sequenced deliveries (P0 → P2).

## 2. What v1 (PR #5) already ships

Baseline, so later sections can point at what changes:

- `juncture web --project <p>` (host / port flags) via stdlib
  `ThreadingHTTPServer`; no extras dependency.
- Five JSON endpoints: `/api/project`, `/api/manifest`,
  `/api/runs?limit=N`, `/api/runs/<id>`, and an asset route under
  `/assets/` serving the vendored `cytoscape.min.js`, `app.css`,
  `app.js`.
- Two tabs in a single-page app: **DAG** (cytoscape breadth-first
  layout with a right sidebar) and **Runs** (history table on the
  left, per-model result table on the right when a run is selected).
- Legend covering kind (`seed` / `sql` / `python`), disabled state,
  and last-run status (`success` / `failed` / `partial`).
- Run persistence via `juncture.core.run_history.append_run` to
  `<project>/target/run_history.jsonl`.

## 3. Observed gaps (evidence)

Captured from live inspection of the running server plus payload
comparison against the `eu_ecommerce` demo:

### 3.1 Kind vs. status rendering collision

Dnešní CSS:

```css
.kind-seed    { border-color: #e0b96a; }
.status-success { background-color: rgba(46,160,67,.10); }
```

Border is 2 px; fill is 12 % opacity. On a 140×34 px node the fill
dominates perceptually; the kind colour registers as "slightly
different outline" rather than as a type cue. Node labels carry no
kind glyph / icon either. **Result:** once any run has been recorded,
you cannot tell a Python model from a SQL model without clicking it.

### 3.2 API has data the UI hides

`/api/runs/<id>` already returns `tests` (per-column test pass/fail
from `TestRunner`) and per-model `statement_errors` (from
`--continue-on-error`). The frontend does not render either:
`app.js` drops `detail.tests` and shows `statement_errors` only as
a single truncated line of flow text.

### 3.3 Source code surface missing on the server

`Model.sql` (string body), `Model.path` (filesystem path), and for
Python models `model.python_callable.__code__.co_filename` all live
in `juncture.core.model`. `/api/manifest` emits eight fields per
model; `sql`, `path`, and `columns` are not among them. There is no
per-model source endpoint. **Consequence:** a UI "read the code"
feature is server-side work first, frontend second.

### 3.4 No project-level overview

Top bar shows only project name + filesystem path. There is no
surface for:

- `juncture.yaml` (connections, vars, default_materialization,
  threads / memory_limit).
- Project-level `README.md` (if present).
- Git metadata (last commit, branch, authors).
- Environment: which `.env` keys were interpolated, which vars
  took defaults.

### 3.5 Runs view has a dead interaction

The per-model table in the Runs panel shows `name | status |
elapsed | rows | error`. Rows are not `cursor: pointer`, not
clickable, and there is no drilldown target. For a migration-triage
run (200 models, 40 errors) the triage loop is: read error snippet
→ open terminal → grep `juncture diagnostics` → back to browser.
That breaks the whole point of a UI.

## 4. Personas and what they need

The UI serves two distinct roles that overlap only partially.

### 4.1 Data engineer (DE)

Building, debugging, or handing off a Juncture project.

**Must-have:**
- See the DAG with kind + status + lineage cleanly.
- Read the SQL / Python body of any model (one click).
- See column schema + test results.
- Drill into a failed run: which statement, what error, what
  diagnostics bucket, what downstream cascaded.
- See seed metadata: format, row count, inferred types, detected
  sentinels.

**Nice-to-have:**
- Search / filter on a 200-model project.
- Per-model reliability timeline (last N runs).
- Git blame / last-change-date per model.
- Ability to kick a run from the UI (out of scope in read-only v2;
  tracked for v3).

### 4.2 Chief data officer (CDO) / data governance

Auditing a portfolio, not debugging one model.

**Must-have:**
- Ownership per model (team / owner email).
- Criticality tier + SLA (freshness target, success-rate target).
- Source-system lineage up to the seed (Keboola bucket / S3 path
  / SaaS connector — not just the local parquet file).
- Long-form documentation ("business purpose", "consumer
  dashboards") surfaced alongside the technical description.

**Nice-to-have:**
- PII / retention / compliance badges that propagate through the
  DAG (seed marked PII → downstream models inherit a ring colour).
- Reliability dashboard: success rate 30 d, median elapsed, SLA
  breach counter.
- Export of the manifest for the org catalog (OpenLineage →
  DataHub / Collibra).

## 5. Proposed scope

Three priority tiers. Each tier is a self-contained PR; lower tiers
do not depend on higher ones.

### 5.1 P0 — "readable" (blocks further UI iteration)

Fixes the five observed gaps so the UI is usable for anyone who did
not build the project. **Target: one PR, ~1 engineer-day.**

| # | Deliverable | Files touched |
|---|---|---|
| P0.1 | `/api/models/<name>` endpoint returning `{path, kind, materialization, sql?, python_source?, columns, tests, config, depends_on, disabled, description}`. SQL body is returned verbatim + a `rendered` variant with `ref()` macros resolved to `schema.name`. Python source is read from `model.path` (file stem `.py`). | `web/server.py` + `core/model.py` helper |
| P0.2 | `/api/manifest` adds `path` (relative to project root) per model so the frontend can hint "click to see source". | `web/server.py` |
| P0.3 | Frontend: right sidebar gains **Source / Schema / Tests / Metadata** tabs. Source tab renders the SQL or Python via a vendored syntax highlighter (prism.js, ~50 kB, under `static/prism.min.js`). No build step; plain `<link>` + `<script>`. | `static/index.html`, `static/app.js`, `static/app.css`, `static/prism.min.*` |
| P0.4 | DAG rendering: **shape ≠ status**. Kind encoded as node shape + fill colour; last-run status encoded as border thickness + ring colour. SQL = round-rectangle blue fill, Python = round-rectangle green fill, seed = parallelogram amber fill. Border `#6fa8dc` 2 px = never-run, `#2ea043` 3 px = success, `#d1242f` 4 px = failed, dashed `#b3b5b9` = disabled. | `static/app.js`, `static/app.css` |
| P0.5 | Runs view: per-model table rows become clickable. Click expands an inline drawer under the row with (a) every `statement_errors` entry with `{index, layer, error, sql_snippet}`, (b) rendered SQL with the failing statement highlighted, (c) `tests` filtered to this model with `{column, name, passed, failing_rows}`. | `static/app.js`, `static/app.css` |
| P0.6 | Frontend renders `detail.tests` on run drilldown as a second table under the per-model one. | `static/app.js` |

**Acceptance criteria:**

- Given a Python model, I can see its `def` + body in the UI
  without leaving the browser.
- Given a failed `--continue-on-error` run with 10 statement
  errors, I can click the model row in Runs and see all 10 errors
  with their statement SQL rendered.
- On the DAG, I can distinguish SQL / Python / seed at a glance
  even when every node has a "success" last-run status.
- Test results from the last run appear in the Runs tab (they are
  already in the API payload and were being dropped).

### 5.2 P1 — "productive" (a real engineer can debug without the terminal)

Adds project-level overview, search, seed inspection, and
diagnostics integration. **Target: one PR, ~2 engineer-days.**

| # | Deliverable | Notes |
|---|---|---|
| P1.1 | Third tab in the top bar: **Project**. Renders `juncture.yaml` as a pretty table (connections, vars, default_materialization, threads / memory_limit / temp_directory). Also renders `README.md` (if present) via vendored `markdown-it` (~60 kB). Git metadata block: last commit hash + subject + author + date + branch from `git log -1` (swallow errors when not a git repo). | New `/api/project/config`, `/api/project/readme`, `/api/project/git` endpoints |
| P1.2 | Fourth tab: **Seeds**. Table of every seed with `{name, format, path, row_count (last run), inferred_types, sentinels, parquet_files_count}`. The sentinel profile reuses `juncture.core.type_inference.detect_sentinels` output from the type cache. Clicking a seed row expands to show the full `columns` → `type` table. | New `/api/seeds` endpoint |
| P1.3 | DAG search box (top-bar input): fulltext over model name, tags, description. Highlights matching nodes in cytoscape; non-matching nodes fade to 0.2 opacity. Clears on Esc. | Pure frontend |
| P1.4 | **Diagnostics panel** in Runs view: aggregates `statement_errors` across all models in the run, buckets them via `/api/runs/<id>/diagnostics` (new endpoint that calls `juncture.diagnostics.classify_statement_errors`), shows a Counter summary at the top of the run detail: `type_mismatch: 7, sentinel: 3, missing_object: 12`. Click a bucket → filters the model drawers open below. | Server: new endpoint, ~20 LOC. Frontend: ~80 LOC. |
| P1.5 | **Download manifest** button (DAG tab). Exports the current `/api/manifest` payload as `manifest.json` + a derived `manifest.openlineage.json` with the OpenLineage DatasetEvent shape (reuses `juncture.observability.lineage`). | Frontend only for the JSON download; OpenLineage shape via existing helper |
| P1.6 | Per-model reliability micro-chart in the DAG side panel: last-20-runs status bar (each run = one coloured tick), p50 / p95 elapsed, 30-day success rate. Data derived from `run_history.jsonl` via new `/api/models/<name>/history` endpoint. | Server: 30 LOC. Frontend: sparkline built with SVG, no extra lib. |

**Acceptance criteria:**

- I can land on the page, click Project, and see the full
  `juncture.yaml` + a rendered README without reading the
  filesystem.
- On a project with 300 seeds, I can filter the seeds tab to show
  only those with detected sentinels.
- On a failed run, the Diagnostics panel shows 5 buckets with counts
  and I can click "sentinel" to see only those errors.
- Searching "customer" in the top bar fades every node whose name,
  tag, or description does not contain "customer".

### 5.3 P2 — "governable" (CDO view)

Adds ownership, SLA, business-purpose, portfolio reliability, and
the compliance propagation. **Target: two PRs, ~3 engineer-days.**

This tier requires `schema.yml` schema changes — see §7 for the
exact grammar.

| # | Deliverable | Notes |
|---|---|---|
| P2.1 | Extend `schema.yml` with `owner`, `team`, `business_unit`, `criticality` (tier-1/2/3), `sla.freshness_hours`, `sla.success_rate_target`, `docs` (path to a markdown file next to the model), `consumers` (list of downstream dashboards / teams). Model dataclass gains these optional fields; `Project._load_sql_model` / `_load_python_models` populate them. | `core/model.py`, `core/project.py` |
| P2.2 | Extend seed entries in `seeds/schema.yml` with `source_system`, `source_locator`, `pii`, `retention_days`, `owner`. | `core/project.py` |
| P2.3 | **Portfolio** tab: a grid / table of every model × (owner, team, criticality, last-run status, last-run age, 30-day SLA attainment). Sortable columns; filters by team / criticality. Uses `/api/portfolio` which joins manifest metadata with `run_history.jsonl` aggregates. | Server: 80 LOC. Frontend: 150 LOC. |
| P2.4 | **Data contracts** view: per-model panel showing `columns` × `type` × `tests` × `description`, plus the "would break" list (downstream models that reference these columns, computed from SQLGlot `extract_table_references` on the rendered SQL). | Server: 60 LOC. Frontend: 80 LOC. |
| P2.5 | **Compliance badges**: a PII seed propagates a `pii: true` ring colour to every downstream model via DAG descendants. Seed retention shows as a badge on the seed node. Hover → "retention: 365 d (157 d remaining)". | Frontend cytoscape styling + small backend helper |
| P2.6 | **Reliability dashboard** (landing page "today" widget on Project tab): per-tier SLA attainment for the last 7 / 30 days, slowest N models by p95 elapsed, top failure buckets. Uses `run_history.jsonl` only; no new persistence needed. | Server: 70 LOC. Frontend: 100 LOC. |
| P2.7 | Long-form docs rendering: if a model has `docs: docs/<model>.md` set in `schema.yml`, render it in the Metadata tab (uses the same `markdown-it` already vendored for P1.1). Autodiscover fallback: if `<model_name>.md` sits next to the `.sql` file, use it. | Server: 20 LOC. Frontend: ~30 LOC. |

**Acceptance criteria:**

- A model with `criticality: tier-1` and `sla.freshness_hours: 24`
  whose last run was 26 h ago renders with a warning ring and is
  sortable to the top of the Portfolio tab.
- A seed marked `pii: true` propagates a purple ring to all
  downstream models; clicking the seed shows its `source_system`,
  `retention_days`, and an estimated "days until expiry".
- The Reliability dashboard shows "tier-1 SLA attainment, 30 d:
  94 %" and drills into the three breaches.

## 6. Out of scope

Explicitly not included in this RFC; tracked in `ROADMAP.md` or
later RFCs:

- **Trigger-a-run from the UI** (POST /api/run). Requires
  authentication + CSRF thinking that we do not yet want in a
  local-first tool. Revisit as a separate RFC (0002) when someone
  asks for it.
- **Column-level lineage** (which column of mart X comes from which
  column of seed Y). Scheduled for Phase 3.1 in `ROADMAP.md`.
- **Deployment as a hosted service** — the UI stays local-first;
  multi-tenancy, auth, RBAC, and persistent storage backend are
  out of scope.
- **Dashboarding / metrics consumption** — the UI shows Juncture
  state, not business KPIs. Semantic layer (Phase 4) will own that.
- **Live-reload of runs** (SSE / WebSocket push). Not needed;
  a browser refresh hitting the JSONL file is fine for local dev.

## 7. `schema.yml` grammar changes (P2)

For the P2 ownership / SLA / docs surface:

```yaml
# models/marts/schema.yml
models:
  - name: customer_segments
    description: Bucket customers into vip/loyal/regular/at_risk/lost.
    docs: docs/customer_segments.md       # optional long-form markdown
    owner: marketing-data@example.com     # email or team handle
    team: analytics
    business_unit: Marketing
    criticality: tier-1                   # tier-1/2/3 (freeform allowed;
                                          # tier-1 is the highest priority)
    sla:
      freshness_hours: 24                 # how stale before we alert
      success_rate_target: 0.99           # 30-day rolling minimum
    consumers:                            # free text, used for governance
      - name: "Exec dashboard"
        url: https://bi.example/execs
      - name: "Retention team"
    columns: [ ... ]                      # existing shape unchanged
    tests: [ ... ]                        # existing shape unchanged

# seeds/schema.yml
seeds:
  - name: customers
    source_system: keboola_storage        # freeform
    source_locator: "in.c-main.customers" # freeform; for Keboola it's the
                                          # bucket+table identifier
    pii: true
    retention_days: 365
    owner: data-platform@example.com
    columns: [ ... ]                      # existing shape unchanged
```

All new fields are **optional**. A project that defines none renders
exactly as today (minus an empty Portfolio column).

## 8. New / changed API surface summary

| Endpoint | Status | Added in | Purpose |
|---|---|---|---|
| `GET /api/models/<name>` | NEW | P0.1 | Per-model metadata + SQL / Python source + columns + tests. |
| `GET /api/manifest` | CHANGED | P0.2 | Adds `path` per model. |
| `GET /api/project/config` | NEW | P1.1 | `juncture.yaml` snapshot. |
| `GET /api/project/readme` | NEW | P1.1 | Raw markdown of project README. |
| `GET /api/project/git` | NEW | P1.1 | Last commit + branch via `git log`. |
| `GET /api/seeds` | NEW | P1.2 | Seed list with inferred types + sentinels. |
| `GET /api/runs/<id>/diagnostics` | NEW | P1.4 | Classified statement errors aggregated. |
| `GET /api/models/<name>/history` | NEW | P1.6 | Per-model last-N run stats. |
| `GET /api/portfolio` | NEW | P2.3 | Model × owner × SLA × last-run aggregation. |
| `GET /api/models/<name>/contract` | NEW | P2.4 | Columns + tests + downstream "would break". |

All still GET, still no auth (local-first). Content type unchanged
(`application/json; charset=utf-8`). Error shape unchanged
(`{"error": "..."}` on 4xx/5xx).

## 9. Front-end dependencies

All vendored into `src/juncture/web/static/`, **no npm**, **no
build step**. Current v1 has one (`cytoscape.min.js`, 400 kB).

- **v2 adds:** `prism.min.js` (~50 kB, MIT, SQL + Python grammars,
  one theme).
- **v2 adds:** `markdown-it.min.js` (~90 kB, MIT). Pinned version
  committed to git.

Total static payload stays under 600 kB — still smaller than a
typical React bundle for the same functionality.

## 10. Non-goals on the implementation

- Do **not** switch to FastAPI / Flask / aiohttp. stdlib
  `http.server` continues to be adequate for read-only local use.
- Do **not** introduce a frontend framework (React / Vue /
  Svelte). Vanilla JS + tiny helpers is the floor; crossing it has
  to be justified in a later RFC.
- Do **not** add a session / cookie layer. Every request is
  stateless, every endpoint is anonymous.
- Do **not** persist derived state beyond `run_history.jsonl`. The
  portfolio / reliability views compute on every request; caching
  is deferred until a 10 k-run history actually slows the UI down.

## 11. Milestones and sequence

| Milestone | Branches | Merges into `main` | Cumulative value |
|---|---|---|---|
| M1 — P0 "readable" | `feat/web-v2-readable` | 1 squash merge | UI is self-contained; no terminal needed to read code / debug runs. |
| M2 — P1.1 + P1.3 + P1.5 "project overview + search + export" | `feat/web-v2-overview` | 1 squash merge | Any engineer lands on the page and understands the project; 300-model DAG is navigable. |
| M3 — P1.2 + P1.4 + P1.6 "diagnostics + seeds + reliability" | `feat/web-v2-diagnostics` | 1 squash merge | Migration-triage loop is fully in-browser. |
| M4 — P2 "governance" | `feat/web-v2-governance` + `feat/web-v2-compliance` | 2 squash merges (schema changes first, UI second) | CDO / governance lens ships; ownership + SLA + PII propagate through the DAG. |

Each milestone updates `docs/STATUS.md` + flips the relevant
`ROADMAP.md` checkboxes in the same PR, per the repo's docs
maintenance rule.

## 12. Open questions

1. **`schema.yml` `consumers` field shape** — should it be free-form
   string list, or structured `{name, url, team}` objects? Proposal:
   structured, because the Portfolio view will want to render links.
   If users dislike the verbosity we can accept both shapes.

2. **Git metadata on CI / Keboola wrapper** — `git log` is not
   guaranteed to work inside the Keboola Docker wrapper. Fallback:
   embed the commit SHA at build time via `pyproject.toml` build
   hook. Not required for the local-first use case; track separately.

3. **Markdown sanitisation** — README and per-model docs are author
   content, but `juncture web` is often pointed at a directory
   checked out from someone else's repo. We should use
   `markdown-it` with its default sanitiser on (no raw HTML), not
   full pass-through. Confirm before P1.1 lands.

4. **SLA clock source** — "last run was X hours ago" needs a
   definition for "now". Propose: server process wall clock at the
   time of the API call; no NTP / time-zone sanity layer. Flag if
   anyone runs `juncture web` on a machine with a skewed clock.

5. **Portfolio export** — do we need CSV / Excel download for the
   CDO tab, or is "click to copy JSON" enough? Leave as a P2.3
   follow-up; do not block the view on the export format.

## 13. Decision

This RFC is **proposed**. Once accepted, it becomes the backlog for
M1 through M4. Acceptance requires: one pair of engineer eyes on
this document + one CDO / governance stakeholder sign-off on §5.3.

Tracking: `ROADMAP.md` §1.8 (new subsection) cross-links to this
file; `STATUS.md` will record which milestone is in flight.
