/*
 * Juncture web — vanilla JS single-page app.
 *
 * Three views surface inside the DAG side panel (Metadata / Source /
 * Schema / Tests) via the /api/models/<name> endpoint; a page refresh
 * is always authoritative since the server reloads the project per
 * request.
 */

(() => {
  const $ = (sel, root = document) => root.querySelector(sel);

  // --- API ------------------------------------------------------------
  const api = {
    project:       () => fetch("/api/project").then(r => r.json()),
    projectCfg:    () => fetch("/api/project/config").then(r => r.json()),
    projectReadme: () => fetch("/api/project/readme").then(r => r.json()),
    projectGit:    () => fetch("/api/project/git").then(r => r.json()),
    manifest:      () => fetch("/api/manifest").then(r => r.json()),
    manifestOL:    () => fetch("/api/manifest/openlineage").then(r => r.json()),
    llmKb:         () => fetch("/api/llm-knowledge").then(r => r.json()),
    model:         (name) => fetch(`/api/models/${encodeURIComponent(name)}`).then(r => r.json()),
    modelHistory:  (name, limit = 20) => fetch(`/api/models/${encodeURIComponent(name)}/history?limit=${limit}`).then(r => r.json()),
    modelContract: (name) => fetch(`/api/models/${encodeURIComponent(name)}/contract`).then(r => r.json()),
    modelDocs:     (name) => fetch(`/api/models/${encodeURIComponent(name)}/docs`).then(r => r.json()),
    portfolio:     () => fetch("/api/portfolio").then(r => r.json()),
    reliability:   () => fetch("/api/reliability").then(r => r.json()),
    seeds:         () => fetch("/api/seeds").then(r => r.json()),
    runs:          (limit = 50) => fetch(`/api/runs?limit=${limit}`).then(r => r.json()),
    run:           (id) => fetch(`/api/runs/${encodeURIComponent(id)}`).then(r => r.json()),
    runDiag:       (id) => fetch(`/api/runs/${encodeURIComponent(id)}/diagnostics`).then(r => r.json()),
  };

  // --- Tab switching (top bar) ----------------------------------------
  document.querySelectorAll(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.view;
      document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach(v => v.classList.toggle("active", v.id === `view-${target}`));
      if (target === "project" && !projectTabLoaded) {
        renderProjectTab();
        projectTabLoaded = true;
      }
      if (target === "seeds" && !seedsTabLoaded) {
        renderSeedsTab();
        seedsTabLoaded = true;
      }
      if (target === "portfolio" && !portfolioTabLoaded) {
        renderPortfolioTab();
        portfolioTabLoaded = true;
      }
      if (target === "reliability" && !reliabilityTabLoaded) {
        renderReliabilityTab();
        reliabilityTabLoaded = true;
      }
      // Cytoscape needs an explicit resize + layout kick after its container
      // goes from display:none back to visible, otherwise the canvas is blank.
      if (target === "dag" && cyInstance) {
        cyInstance.resize();
        cyInstance.fit(undefined, 24);
      }
    });
  });

  // --- State ----------------------------------------------------------
  let latestRunByModel = {};   // model_name -> last-seen status
  let latestTestsByModel = {}; // model_name -> [{column,name,passed,failing_rows}]
  let manifestCache = null;
  let currentModelName = null;
  let currentModelDetail = null;
  let currentDetailTab = "metadata";
  let sourceView = "rendered";  // "rendered" | "raw"
  let cyInstance = null;       // cytoscape handle — kept so search can fade nodes
  let projectTabLoaded = false;
  let seedsTabLoaded = false;
  let portfolioTabLoaded = false;
  let reliabilityTabLoaded = false;

  // --- DAG render -----------------------------------------------------
  async function renderDag() {
    const manifest = await api.manifest();
    manifestCache = manifest;

    const elements = [];
    manifest.models.forEach(m => {
      const lastStatus = latestRunByModel[m.name];
      const classes = [`kind-${m.kind}`];
      if (m.disabled) classes.push("status-disabled");
      else if (lastStatus) classes.push(`status-${lastStatus}`);
      else classes.push("status-never");
      if (m.pii) classes.push("pii-inherit");
      elements.push({
        data: { id: m.name, label: m.name, kind: m.kind, disabled: m.disabled, pii: !!m.pii },
        classes: classes.join(" ")
      });
    });
    manifest.edges.forEach(e => {
      elements.push({ data: { id: `${e.from}->${e.to}`, source: e.from, target: e.to } });
    });

    const cy = cytoscape({
      container: document.getElementById("cy"),
      elements,
      layout: { name: "breadthfirst", directed: true, spacingFactor: 1.15, padding: 24 },
      wheelSensitivity: 0.2,
      style: [
        { selector: "node", style: {
          "background-color": "#ffffff",
          "border-width": 2, "border-color": "#9aa0a6",
          "label": "data(label)", "font-size": 11, "color": "#1e2024",
          "text-valign": "center", "text-halign": "center",
          "text-wrap": "ellipsis", "text-max-width": "130px",
          "width": 140, "height": 34,
          "shape": "round-rectangle",
          "font-family": "-apple-system, BlinkMacSystemFont, sans-serif",
        }},
        /* --- Kind encoded as shape + fill (not status) --------------- */
        { selector: ".kind-seed", style: {
          "shape": "round-rhomboid",
          "background-color": "rgba(224,185,106,.35)", "border-color": "#b38a38",
        }},
        { selector: ".kind-sql", style: {
          "shape": "round-rectangle",
          "background-color": "rgba(111,168,220,.22)", "border-color": "#3674b5",
        }},
        { selector: ".kind-python", style: {
          "shape": "round-rectangle",
          "background-color": "rgba(147,196,125,.22)", "border-color": "#4f893a",
        }},
        /* --- Status encoded as border thickness + ring colour -------- */
        { selector: ".status-never",   style: { "border-width": 2 } },
        { selector: ".status-success", style: { "border-width": 3, "border-color": "#2ea043" } },
        { selector: ".status-partial", style: { "border-width": 3, "border-color": "#b08800" } },
        { selector: ".status-failed",  style: { "border-width": 4, "border-color": "#d1242f" } },
        { selector: ".status-skipped", style: { "border-width": 2, "border-color": "#b08800",
                                                "border-style": "dashed" } },
        { selector: ".status-disabled", style: {
          "opacity": 0.5, "border-style": "dashed", "border-color": "#9aa0a6",
        }},
        { selector: "node:selected", style: { "border-width": 5 } },
        { selector: ".pii-inherit", style: {
          // PII overlay: pink outer ring + slightly pink background so the
          // inherited-PII propagation is visible without overpowering kind/status.
          "underlay-color": "#c026d3",
          "underlay-padding": 4,
          "underlay-opacity": 0.35,
          "underlay-shape": "round-rectangle",
        }},
        { selector: ".faded", style: { "opacity": 0.2 } },
        { selector: "edge", style: {
          "width": 1.2, "line-color": "#c6c8cd", "target-arrow-color": "#c6c8cd",
          "target-arrow-shape": "triangle", "curve-style": "bezier",
        }},
      ],
    });

    cy.on("tap", "node", (evt) => {
      const name = evt.target.id();
      selectModel(name);
    });
    cyInstance = cy;
  }

  // --- DAG search ------------------------------------------------------
  function applySearch(query) {
    if (!cyInstance) return;
    const q = (query || "").trim().toLowerCase();
    if (!q) {
      cyInstance.nodes().removeClass("faded");
      cyInstance.edges().removeClass("faded");
      return;
    }
    const matchers = manifestCache ? manifestCache.models.filter(m => {
      const tags = (m.tags || []).join(" ").toLowerCase();
      const desc = (m.description || "").toLowerCase();
      return m.name.toLowerCase().includes(q) || tags.includes(q) || desc.includes(q);
    }).map(m => m.name) : [];
    const matchSet = new Set(matchers);
    cyInstance.nodes().forEach(n => n.toggleClass("faded", !matchSet.has(n.id())));
    cyInstance.edges().forEach(e => {
      const keep = matchSet.has(e.source().id()) && matchSet.has(e.target().id());
      e.toggleClass("faded", !keep);
    });
  }

  // Debounced listener — small projects don't need it, but 300-model DAGs do.
  let searchTimer = null;
  const searchInput = document.getElementById("dag-search");
  if (searchInput) {
    searchInput.addEventListener("input", () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => applySearch(searchInput.value), 90);
    });
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Escape") { searchInput.value = ""; applySearch(""); }
    });
  }

  // --- Manifest / OpenLineage downloads --------------------------------
  function downloadJson(filename, payload) {
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  }
  // Export popover toggling.
  const exportToggle = document.getElementById("dag-export-toggle");
  const exportPop = document.getElementById("dag-export-pop");
  if (exportToggle && exportPop) {
    exportToggle.addEventListener("click", (e) => {
      e.stopPropagation();
      exportPop.hidden = !exportPop.hidden;
    });
    document.addEventListener("click", (e) => {
      if (!exportPop.hidden && !exportPop.contains(e.target) && e.target !== exportToggle) {
        exportPop.hidden = true;
      }
    });
  }
  const dlManifest = document.getElementById("dl-manifest");
  const dlOpenLineage = document.getElementById("dl-openlineage");
  const dlLlmKb = document.getElementById("dl-llm-kb");
  if (dlManifest) dlManifest.addEventListener("click", async () => {
    downloadJson("manifest.json", await api.manifest());
  });
  if (dlOpenLineage) dlOpenLineage.addEventListener("click", async () => {
    downloadJson("manifest.openlineage.json", await api.manifestOL());
  });
  if (dlLlmKb) dlLlmKb.addEventListener("click", async () => {
    dlLlmKb.disabled = true; dlLlmKb.textContent = "…";
    try {
      const kb = await api.llmKb();
      downloadJson("llm-knowledge.json", kb);
    } finally {
      dlLlmKb.disabled = false; dlLlmKb.textContent = "LLM kb";
    }
  });

  // --- Model detail (four-tab sidebar) ---------------------------------
  async function selectModel(name) {
    currentModelName = name;
    try {
      currentModelDetail = await api.model(name);
    } catch (e) {
      $("#model-detail").innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
      return;
    }
    // Default source view: if SQL model has refs, show rendered; else raw.
    if (currentModelDetail.kind === "sql" && currentModelDetail.depends_on.length === 0) {
      sourceView = "raw";
    } else {
      sourceView = "rendered";
    }
    renderDetailTab();
  }

  function renderDetailTab() {
    const panel = $("#model-detail");
    panel.classList.remove("detail-empty");
    const m = currentModelDetail;
    if (!m) return;
    if (currentDetailTab === "metadata") panel.innerHTML = renderMetadataTab(m);
    else if (currentDetailTab === "source") panel.innerHTML = renderSourceTab(m);
    else if (currentDetailTab === "schema") panel.innerHTML = renderSchemaTab(m);
    else if (currentDetailTab === "tests") panel.innerHTML = renderTestsTab(m);

    if (currentDetailTab === "source") wireSourceToolbar();
    if (currentDetailTab === "source") highlightActiveSource();
  }

  function renderMetadataTab(m) {
    const lastStatus = latestRunByModel[m.name] || "never run";
    const pill = (label, cls = "") =>
      `<span class="chip ${cls}">${escape(label)}</span>`;
    const deps = m.depends_on.length
      ? m.depends_on.map(d => `<code class="dep-chip">${escape(d)}</code>`).join(" ")
      : "<em>none</em>";
    const tags = (m.tags || []).map(t => pill(t, "chip-tag")).join(" ");
    // Fire-and-forget history + docs; their containers fill in async so the
    // main metadata block is not blocked on two round-trips.
    loadReliability(m.name);
    loadDocs(m.name);

    const g = m.governance || {};
    const hasGov = g.owner || g.team || g.business_unit || g.criticality
                || g.sla_freshness_hours != null || g.sla_success_rate_target != null
                || (g.consumers || []).length;

    const consumersHtml = (g.consumers || []).map(c => {
      if (c.url) return `<a href="${escape(c.url)}" target="_blank" rel="noopener">${escape(c.name)}</a>`;
      return escape(c.name);
    }).join(" · ");

    // Identity: model name as an h3, with chips for kind/materialization and
    // the last-run status so a reader instantly sees "what + how did it go".
    const statusCls = lastStatus.replace(/\s+/g, "-");
    const identityHead = `
      <div class="md-identity">
        <h3 class="md-title">${escape(m.name)}</h3>
        <div class="md-chips">
          ${pill(m.kind, `chip-kind chip-kind-${m.kind}`)}
          ${pill(m.materialization, "chip-materialization")}
          <span class="status-pill ${escape(statusCls)}">${escape(lastStatus)}</span>
          ${m.disabled ? pill("disabled", "chip-disabled") : ""}
        </div>
        ${m.description ? `<p class="md-desc">${escape(m.description)}</p>` : ""}
      </div>`;

    // Core info: path + depends_on + tags in a 2-column key-value grid so
    // the vertical space is spent on content, not labels.
    const coreGrid = `
      <dl class="md-grid">
        ${m.path ? `<dt>Source</dt><dd><code>${escape(m.path)}</code></dd>` : ""}
        <dt>Depends on</dt><dd>${deps}</dd>
        ${m.schedule_cron ? `<dt>Schedule</dt><dd><code>${escape(m.schedule_cron)}</code></dd>` : ""}
        ${tags ? `<dt>Tags</dt><dd>${tags}</dd>` : ""}
      </dl>`;

    const govCard = hasGov ? `
      <section class="md-card">
        <h4 class="md-card-title">Governance</h4>
        <dl class="md-grid">
          ${g.owner ? `<dt>Owner</dt><dd>${escape(g.owner)}</dd>` : ""}
          ${g.team ? `<dt>Team</dt><dd><code>${escape(g.team)}</code></dd>` : ""}
          ${g.business_unit ? `<dt>BU</dt><dd>${escape(g.business_unit)}</dd>` : ""}
          ${g.criticality ? `<dt>Tier</dt><dd>${pill(g.criticality, `chip-tier chip-${g.criticality}`)}</dd>` : ""}
          ${g.sla_freshness_hours != null ? `<dt>SLA freshness</dt><dd>${g.sla_freshness_hours}h</dd>` : ""}
          ${g.sla_success_rate_target != null ? `<dt>SLA success</dt><dd>&ge; ${(g.sla_success_rate_target * 100).toFixed(0)}%</dd>` : ""}
          ${consumersHtml ? `<dt>Consumers</dt><dd>${consumersHtml}</dd>` : ""}
        </dl>
      </section>` : "";

    // Reliability and Docs sit in their own cards so the async fill-in does
    // not shift the rest of the metadata block; also gives them a clean
    // "loading" label without polluting the grid.
    const reliabilityCard = `
      <section class="md-card" id="reliability-card">
        <h4 class="md-card-title">Reliability</h4>
        <div id="reliability-block" class="md-reliability"><em>loading&hellip;</em></div>
      </section>`;
    const docsCard = `
      <section class="md-card" id="docs-card">
        <h4 class="md-card-title">Long-form docs</h4>
        <div id="docs-block"><em>loading&hellip;</em></div>
      </section>`;

    return `
      <div class="md-root">
        ${identityHead}
        ${coreGrid}
        ${govCard}
        ${reliabilityCard}
        ${docsCard}
      </div>`;
  }

  async function loadDocs(name) {
    let d;
    try { d = await api.modelDocs(name); } catch (_) { return; }
    const block = document.getElementById("docs-block");
    if (!block) return;
    if (!d.markdown) {
      block.innerHTML = '<em class="md-muted">No long-form docs. Add <code>docs:</code> in schema.yml or drop a sibling <code>.md</code>.</em>';
      return;
    }
    const html = window.markdownit
      ? window.markdownit({ html: false, linkify: true }).render(d.markdown)
      : `<pre>${escape(d.markdown)}</pre>`;
    block.innerHTML = `<div class="proj-readme md-docs">${html}</div>
      <div class="md-source">source: <code>${escape(d.source)}</code></div>`;
  }

  async function loadReliability(name) {
    let h;
    try { h = await api.modelHistory(name, 20); } catch (_) { return; }
    const block = document.getElementById("reliability-block");
    if (!block) return;
    if (!h.runs.length) {
      block.innerHTML = '<em class="md-muted">No run history yet.</em>';
      return;
    }
    const p50 = h.p50_elapsed_seconds != null ? `${h.p50_elapsed_seconds.toFixed(2)}s` : "—";
    const p95 = h.p95_elapsed_seconds != null ? `${h.p95_elapsed_seconds.toFixed(2)}s` : "—";
    const sr = h.success_rate_30d != null ? `${(h.success_rate_30d * 100).toFixed(0)}%` : "—";
    // Inline layout: sparkline on the left, compact stats on the right,
    // one horizontal row so the Reliability card stays dense.
    block.innerHTML = `
      <div class="md-rel-row">
        ${renderSparkline(h.runs)}
        <dl class="md-rel-stats">
          <div><dt>p50</dt><dd>${p50}</dd></div>
          <div><dt>p95</dt><dd>${p95}</dd></div>
          <div><dt>30d</dt><dd>${sr} <span class="md-muted">(${h.sample_size_30d})</span></dd></div>
        </dl>
      </div>`;
  }

  function renderSourceTab(m) {
    if (m.kind === "seed") {
      return `<div class="detail-block">
        <p><em>Seeds have no source body &mdash; inspect the file at
        <code>${escape(m.path || "")}</code> directly.</em></p></div>`;
    }
    if (m.kind === "python") {
      return `
        <div class="detail-block">
          <div class="source-toolbar"><button class="active" disabled>python</button></div>
          <pre class="source-block"><code class="language-python">${
            escape(m.python_source || "(source file not readable)")
          }</code></pre>
        </div>`;
    }
    const rawHas = !!m.sql, rendHas = !!m.sql_rendered;
    const body = sourceView === "raw" ? (m.sql || "") : (m.sql_rendered || m.sql || "");
    return `
      <div class="detail-block">
        <div class="source-toolbar" id="source-toolbar">
          ${rendHas ? `<button data-src="rendered" class="${sourceView === "rendered" ? "active" : ""}">rendered</button>` : ""}
          ${rawHas ? `<button data-src="raw" class="${sourceView === "raw" ? "active" : ""}">raw (with ref())</button>` : ""}
        </div>
        <pre class="source-block"><code class="language-sql">${escape(body)}</code></pre>
      </div>`;
  }

  function renderSchemaTab(m) {
    if (!m.columns.length) {
      return `<div class="detail-block">
        <p><em>No columns declared in schema.yml for this model.</em></p></div>`;
    }
    const rows = m.columns.map(c => `
      <tr>
        <td><code>${escape(c.name)}</code></td>
        <td>${c.data_type ? `<code>${escape(c.data_type)}</code>` : "&mdash;"}</td>
        <td>${c.description ? escape(c.description) : "<em>&mdash;</em>"}</td>
        <td>${(c.tests || []).map(t => {
          const key = typeof t === "string" ? t : Object.keys(t)[0];
          return `<code>${escape(key)}</code>`;
        }).join(" ") || "&mdash;"}</td>
      </tr>
    `).join("");
    return `
      <div class="detail-block">
        <table class="schema-table">
          <thead><tr><th>Column</th><th>Type</th><th>Description</th><th>Tests</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  }

  function renderTestsTab(m) {
    const lastTests = (latestTestsByModel[m.name] || []).reduce((acc, t) => {
      acc[`${t.column}:${t.name}`] = t;
      return acc;
    }, {});
    if (!m.tests.length) {
      return `<div class="detail-block">
        <p><em>No tests declared for this model.</em></p></div>`;
    }
    const rows = m.tests.map(t => {
      const last = lastTests[`${t.column}:${t.name}`];
      const pill = last
        ? `<span class="status-pill ${last.passed ? "passed" : "failed"}">${last.passed ? "passed" : "failed"}</span>`
        : "<em>not yet run</em>";
      const fail = last && !last.passed ? ` <span style="color:var(--text-dim)">(${last.failing_rows} rows)</span>` : "";
      return `
        <tr>
          <td><code>${escape(t.column)}</code></td>
          <td class="col-test">${escape(t.name)}</td>
          <td>${pill}${fail}</td>
        </tr>`;
    }).join("");
    return `
      <div class="detail-block">
        <table class="tests-table">
          <thead><tr><th>Column</th><th>Test</th><th>Last run</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  }

  function wireSourceToolbar() {
    const tb = document.getElementById("source-toolbar");
    if (!tb) return;
    tb.querySelectorAll("button[data-src]").forEach(btn => {
      btn.addEventListener("click", () => {
        sourceView = btn.dataset.src;
        renderDetailTab();
      });
    });
  }

  function highlightActiveSource() {
    if (window.Prism) {
      document.querySelectorAll("pre.source-block code").forEach(c => Prism.highlightElement(c));
    }
  }

  // Detail-tab switching
  document.querySelectorAll(".detail-tab").forEach(btn => {
    btn.addEventListener("click", () => {
      currentDetailTab = btn.dataset.detail;
      document.querySelectorAll(".detail-tab").forEach(b => b.classList.toggle("active", b === btn));
      if (currentModelDetail) renderDetailTab();
    });
  });

  // --- Runs view ------------------------------------------------------
  async function renderRuns() {
    const { runs } = await api.runs();
    const body = $("#runs-body");
    if (runs.length === 0) {
      body.innerHTML = '<tr><td colspan="8" style="color:var(--text-dim); padding: 12px 0;">No runs recorded yet &mdash; try <code>juncture run --project .</code>.</td></tr>';
      $("#status-line").textContent = "No runs yet.";
      return;
    }
    body.innerHTML = "";
    runs.forEach((run, i) => {
      const tr = document.createElement("tr");
      tr.dataset.runId = run.run_id;
      if (i === 0) tr.classList.add("selected");
      tr.innerHTML = `
        <td><code>${escape(run.run_id.substring(0, 7))}</code></td>
        <td>${escape(formatTime(run.started_at))}</td>
        <td class="${run.ok ? "ok-yes" : "ok-no"}">${run.ok ? "ok" : "fail"}</td>
        <td>${run.elapsed_seconds.toFixed(2)}s</td>
        <td>${run.successes}</td>
        <td>${run.failures}</td>
        <td>${run.partial}</td>
        <td>${run.disabled}</td>`;
      tr.addEventListener("click", () => selectRun(run.run_id));
      body.appendChild(tr);
    });
    // Prime the DAG colouring + the tests tab from the most recent run.
    if (runs.length) {
      const detail = await api.run(runs[0].run_id);
      latestRunByModel = {};
      detail.models.forEach(m => { latestRunByModel[m.name] = m.status; });
      latestTestsByModel = {};
      (detail.tests || []).forEach(t => {
        (latestTestsByModel[t.model] = latestTestsByModel[t.model] || []).push(t);
      });
      selectRun(runs[0].run_id);
    }
    $("#status-line").textContent = `${runs.length} run(s) on record`;
  }

  async function selectRun(runId) {
    document.querySelectorAll("#runs-body tr").forEach(tr => tr.classList.toggle("selected", tr.dataset.runId === runId));
    const [detail, diag] = await Promise.all([api.run(runId), api.runDiag(runId).catch(() => null)]);

    const panel = $("#run-detail");
    panel.classList.remove("detail-empty");
    const testsByModel = (detail.tests || []).reduce((acc, t) => {
      (acc[t.model] = acc[t.model] || []).push(t);
      return acc;
    }, {});

    // Diagnostics bucket summary (only when the run has statement_errors).
    const bucketEntries = diag ? Object.entries(diag.buckets || {}) : [];
    const diagBlock = bucketEntries.length ? `
      <div class="diag-panel">
        <h3>Diagnostics</h3>
        <div class="diag-buckets">
          ${bucketEntries.map(([b, n]) =>
            `<button class="diag-bucket" data-bucket="${escape(b)}"><span class="b-name">${escape(b)}</span> <span class="b-count">${n}</span></button>`
          ).join("")}
          <button class="diag-bucket active" data-bucket="__all__"><span class="b-name">all</span></button>
        </div>
      </div>` : "";

    // Per-model clickable rows; each has a hidden twin drawer row below.
    const rows = detail.models.map((m, idx) => {
      const pill = `<span class="status-pill ${escape(m.status)}">${escape(m.status)}</span>`;
      const shortErr = m.error ? escape(m.error.split("\n")[0].slice(0, 140)) : "";
      return `
        <tr class="model-row" data-row-idx="${idx}">
          <td><span class="chev">&#9656;</span> <strong>${escape(m.name)}</strong></td>
          <td>${pill}</td>
          <td>${m.elapsed_seconds.toFixed(2)}s</td>
          <td>${m.row_count ?? "&mdash;"}</td>
          <td>${shortErr}</td>
        </tr>
        <tr class="model-drawer" data-drawer-idx="${idx}" style="display:none;">
          <td colspan="5"><div class="drawer-inner" id="drawer-${idx}"></div></td>
        </tr>`;
    }).join("");

    // Global run-level tests block (second table per P0.6).
    const runTestsBlock = (detail.tests && detail.tests.length)
      ? `
        <h3 style="margin: 18px 0 4px; font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: .06em;">Data tests</h3>
        <table>
          <thead><tr><th>Model</th><th>Column</th><th>Test</th><th>Passed</th><th>Failing rows</th></tr></thead>
          <tbody>${detail.tests.map(t => `
            <tr>
              <td><code>${escape(t.model)}</code></td>
              <td><code>${escape(t.column)}</code></td>
              <td>${escape(t.name)}</td>
              <td><span class="status-pill ${t.passed ? "passed" : "failed"}">${t.passed ? "passed" : "failed"}</span></td>
              <td>${t.passed ? "&mdash;" : escape(String(t.failing_rows))}</td>
            </tr>`).join("")}
          </tbody>
        </table>`
      : "";

    panel.innerHTML = `
      <h2 style="margin:0; font-size: 16px;">Run <code>${escape(detail.run_id.substring(0, 7))}</code></h2>
      <div style="color:var(--text-dim); font-size: 12px; margin-top: 4px;">
        ${escape(formatTime(detail.started_at))} &middot; ${detail.elapsed_seconds.toFixed(2)}s &middot;
        ${detail.ok ? '<span class="ok-yes">ok</span>' : '<span class="ok-no">failed</span>'}
      </div>
      ${diagBlock}
      <table>
        <thead><tr><th>Model</th><th>Status</th><th>Elapsed</th><th>Rows</th><th>Error (summary)</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
      ${runTestsBlock}`;

    // Bucket filter: clicking a bucket chip shows only the model rows
    // whose per_model classification touches that bucket. Clicking "all"
    // resets.
    if (diag) {
      panel.querySelectorAll(".diag-bucket").forEach(btn => {
        btn.addEventListener("click", () => {
          panel.querySelectorAll(".diag-bucket").forEach(b => b.classList.toggle("active", b === btn));
          const bucket = btn.dataset.bucket;
          detail.models.forEach((m, idx) => {
            const entries = diag.per_model[m.name] || [];
            const show = bucket === "__all__" || entries.some(e => e.bucket === bucket);
            const row = panel.querySelector(`tr.model-row[data-row-idx="${idx}"]`);
            const drawer = panel.querySelector(`tr.model-drawer[data-drawer-idx="${idx}"]`);
            if (row) row.style.display = show ? "" : "none";
            if (drawer && !show) drawer.style.display = "none";
          });
        });
      });
    }

    // Click → expand drawer with every statement error and per-model tests.
    panel.querySelectorAll("tr.model-row").forEach(row => {
      row.addEventListener("click", () => {
        const idx = Number(row.dataset.rowIdx);
        const drawer = panel.querySelector(`tr.model-drawer[data-drawer-idx="${idx}"]`);
        const inner = document.getElementById(`drawer-${idx}`);
        const isOpen = row.classList.toggle("expanded");
        drawer.style.display = isOpen ? "" : "none";
        if (isOpen && !drawer.dataset.loaded) {
          const m = detail.models[idx];
          renderModelDrawer(inner, m, testsByModel[m.name] || []);
          drawer.dataset.loaded = "1";
        }
      });
    });
  }

  async function renderModelDrawer(container, model, tests) {
    let html = "";
    if (model.error) {
      html += `<h3>Error</h3><div class="err-msg">${escape(model.error)}</div>`;
    }
    const sErrs = model.statement_errors || [];
    if (sErrs.length) {
      html += `<h3>Statement errors (${sErrs.length})</h3>`;
      html += `<ul class="drawer-errors">${sErrs.map(se => `
        <li>
          <div class="err-head">
            statement <code>#${se.index}</code>${se.layer != null ? ` &middot; layer <code>${se.layer}</code>` : ""}
          </div>
          <div class="err-msg">${escape(se.error)}</div>
          ${se.sql_snippet ? `<pre class="stmt-sql"><code class="language-sql">${escape(se.sql_snippet)}</code></pre>` : ""}
        </li>`).join("")}</ul>`;
    }

    // Lazy-load SQL so a failed statement can be pointed at when ``sql_snippet``
    // wasn't recorded. Runs older than this feature have no snippet; fall back
    // to the rendered body.
    if (sErrs.length && !sErrs.some(se => se.sql_snippet)) {
      try {
        const detail = await api.model(model.name);
        if (detail.sql_rendered) {
          html += `<h3>Rendered SQL</h3>
            <pre class="stmt-sql"><code class="language-sql">${escape(detail.sql_rendered)}</code></pre>`;
        }
      } catch (_) { /* model may have been removed; ignore */ }
    }

    if (tests.length) {
      html += `<h3>Data tests for this model</h3>
        <table class="tests-table">
          <thead><tr><th>Column</th><th>Test</th><th>Result</th><th>Failing rows</th></tr></thead>
          <tbody>${tests.map(t => `
            <tr>
              <td><code>${escape(t.column)}</code></td>
              <td class="col-test">${escape(t.name)}</td>
              <td><span class="status-pill ${t.passed ? "passed" : "failed"}">${t.passed ? "passed" : "failed"}</span></td>
              <td>${t.passed ? "&mdash;" : escape(String(t.failing_rows))}</td>
            </tr>`).join("")}
          </tbody>
        </table>`;
    }

    if (!html) {
      html = '<p style="color:var(--text-dim); font-style: italic;">No additional detail for this model.</p>';
    }
    container.innerHTML = html;
    if (window.Prism) {
      container.querySelectorAll("pre code").forEach(c => Prism.highlightElement(c));
    }
  }

  // --- Seeds tab ------------------------------------------------------
  async function renderSeedsTab() {
    const pane = document.getElementById("seeds-pane");
    pane.innerHTML = '<div class="detail-empty">Loading seeds&hellip;</div>';
    try {
      const { seeds } = await api.seeds();
      if (!seeds.length) {
        pane.innerHTML = '<div class="detail-empty">No seeds declared in this project.</div>';
        return;
      }
      const filterBox = `<div class="seeds-filter">
        <input id="seeds-search" type="search" placeholder="Filter seeds (Esc clears)" autocomplete="off">
        <label><input type="checkbox" id="seeds-only-sentinels"> only with sentinels</label>
      </div>`;
      const rows = seeds.map((s, idx) => {
        const colsCount = Object.keys(s.inferred_types || {}).length;
        const sentinelCols = Object.keys(s.sentinels || {}).length;
        return `
          <tr class="seed-row" data-idx="${idx}">
            <td><span class="chev">&#9656;</span> <strong>${escape(s.name)}</strong></td>
            <td><code>${escape(s.format)}</code></td>
            <td>${sentinelCols ? `<span class="status-pill failed">${sentinelCols}</span>` : "&mdash;"}</td>
            <td>${colsCount}</td>
            <td>${s.row_count ?? "&mdash;"}</td>
            <td>${s.format === "parquet" ? s.parquet_files : "&mdash;"}</td>
            <td><code>${escape(s.path || "")}</code></td>
          </tr>
          <tr class="seed-drawer" data-drawer-idx="${idx}" style="display:none;">
            <td colspan="7"><div class="drawer-inner" id="seed-drawer-${idx}"></div></td>
          </tr>`;
      }).join("");
      pane.innerHTML = `
        <h2>Seeds (${seeds.length})</h2>
        ${filterBox}
        <div class="proj-card" style="padding:0;">
          <table class="seeds-table">
            <thead><tr>
              <th>Name</th><th>Format</th><th>Sentinels</th><th>Columns</th>
              <th>Rows (last run)</th><th>Parquet files</th><th>Path</th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
      // Row click expands drawer with column × inferred type + sentinel list.
      pane.querySelectorAll("tr.seed-row").forEach(row => {
        row.addEventListener("click", () => {
          const idx = Number(row.dataset.idx);
          const drawer = pane.querySelector(`tr.seed-drawer[data-drawer-idx="${idx}"]`);
          const inner = document.getElementById(`seed-drawer-${idx}`);
          const open = row.classList.toggle("expanded");
          drawer.style.display = open ? "" : "none";
          if (open && !drawer.dataset.loaded) {
            renderSeedDrawer(inner, seeds[idx]);
            drawer.dataset.loaded = "1";
          }
        });
      });
      // Client-side filtering.
      const si = document.getElementById("seeds-search");
      const chk = document.getElementById("seeds-only-sentinels");
      const applyFilter = () => {
        const q = (si.value || "").trim().toLowerCase();
        const onlySent = chk.checked;
        seeds.forEach((s, idx) => {
          const show = (!q || s.name.toLowerCase().includes(q) || (s.path || "").toLowerCase().includes(q))
            && (!onlySent || Object.keys(s.sentinels || {}).length > 0);
          pane.querySelector(`tr.seed-row[data-idx="${idx}"]`).style.display = show ? "" : "none";
          const drawer = pane.querySelector(`tr.seed-drawer[data-drawer-idx="${idx}"]`);
          if (!show) drawer.style.display = "none";
        });
      };
      si.addEventListener("input", applyFilter);
      si.addEventListener("keydown", e => { if (e.key === "Escape") { si.value = ""; applyFilter(); } });
      chk.addEventListener("change", applyFilter);
    } catch (e) {
      pane.innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
    }
  }

  function renderSeedDrawer(container, seed) {
    const types = seed.inferred_types || {};
    const sentinels = seed.sentinels || {};
    if (!Object.keys(types).length) {
      // CSV seeds are loaded via DuckDB's read_csv_auto() at runtime; the
      // schema cache (.juncture/seed_schemas.json) intentionally only holds
      // parquet seeds, so `juncture run` won't populate anything here. Make
      // that explicit so users don't go re-running looking for output.
      const msg = seed.format === "csv"
        ? 'CSV seeds are type-inferred at runtime via <code>read_csv_auto()</code> — not cached. Add a <code>seeds/schema.yml</code> entry to pin column types explicitly.'
        : 'No inferred types cached. Run <code>juncture run</code> to populate the seed schema cache.';
      container.innerHTML = `<p style="color:var(--text-dim); font-style: italic;">${msg}</p>`;
      return;
    }
    const rows = Object.entries(types).map(([col, t]) => {
      const sp = sentinels[col];
      const sentinelList = sp && sp.null_sentinels && sp.null_sentinels.length
        ? sp.null_sentinels.map(v => `<code>${escape(v === "" ? "''" : v)}</code>`).join(" ")
        : "&mdash;";
      return `<tr><td><code>${escape(col)}</code></td><td><code>${escape(t)}</code></td><td>${sentinelList}</td></tr>`;
    }).join("");
    container.innerHTML = `
      <h3>Columns &amp; inferred types</h3>
      <table class="tests-table">
        <thead><tr><th>Column</th><th>Type</th><th>Sentinels</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  // --- Reliability sparkline ------------------------------------------
  function renderSparkline(runs) {
    if (!runs.length) return "<em>No runs recorded.</em>";
    const statusColor = { success: "#2ea043", failed: "#d1242f", partial: "#b08800", skipped: "#b08800", disabled: "#b3b5b9" };
    // Right-to-left: the most recent run is on the right, same direction as the runs table.
    const bars = runs.slice(0, 20).reverse().map(r =>
      `<span class="spark-bar" style="background:${statusColor[r.status] || "#c6c8cd"};" title="${escape(r.started_at)} — ${escape(r.status)}"></span>`
    ).join("");
    return `<div class="spark-row">${bars}</div>`;
  }

  // --- Portfolio tab --------------------------------------------------
  async function renderPortfolioTab() {
    const pane = document.getElementById("portfolio-pane");
    pane.innerHTML = '<div class="detail-empty">Loading portfolio&hellip;</div>';
    try {
      const { models } = await api.portfolio();
      const rows = models.map(m => {
        const g = m.governance || {};
        const srPct = m.success_rate_30d != null ? `${(m.success_rate_30d * 100).toFixed(0)}%` : "&mdash;";
        const breachPill = (label, cond) => cond ? `<span class="status-pill failed">${escape(label)}</span>` : "";
        const lastAge = m.last_success_age_hours != null ? `${m.last_success_age_hours.toFixed(1)}h ago` : "&mdash;";
        return `
          <tr>
            <td><strong>${escape(m.name)}</strong></td>
            <td><code>${escape(g.criticality || "—")}</code></td>
            <td>${escape(g.owner || "—")}</td>
            <td>${escape(g.team || "—")}</td>
            <td>${escape(g.business_unit || "—")}</td>
            <td>${g.sla_freshness_hours != null ? `${g.sla_freshness_hours}h` : "—"}</td>
            <td>${lastAge}</td>
            <td>${srPct} <span style="color:var(--text-dim); font-size: 10.5px;">(${m.sample_30d})</span></td>
            <td>${breachPill("SLA freshness", m.freshness_breach)} ${breachPill("SLA success", m.success_breach)}</td>
          </tr>`;
      }).join("");
      pane.innerHTML = `
        <h2>Portfolio (${models.length} models)</h2>
        <div class="proj-card" style="padding:0;">
          <table class="portfolio-table">
            <thead><tr>
              <th>Model</th><th>Tier</th><th>Owner</th><th>Team</th><th>BU</th>
              <th>SLA freshness</th><th>Last success</th>
              <th>30-day success</th><th>Breaches</th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
    } catch (e) {
      pane.innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
    }
  }

  // --- Reliability dashboard tab --------------------------------------
  async function renderReliabilityTab() {
    const pane = document.getElementById("reliability-pane");
    pane.innerHTML = '<div class="detail-empty">Loading reliability&hellip;</div>';
    try {
      const r = await api.reliability();
      const tierBlock = window => `
        <div class="proj-card">
          <h2 style="margin-top:0;">${escape(window)} attainment by tier</h2>
          <table class="schema-table">
            <thead><tr><th>Tier</th><th>Attainment</th><th>Sample</th></tr></thead>
            <tbody>
              ${Object.entries(r.tiers[window] || {}).map(([tier, data]) => `
                <tr>
                  <td><code>${escape(tier)}</code></td>
                  <td>${data.attainment != null ? `${(data.attainment * 100).toFixed(0)}%` : "—"}</td>
                  <td>${data.sample}</td>
                </tr>`).join("") || '<tr><td colspan="3"><em>No runs in window.</em></td></tr>'}
            </tbody>
          </table>
        </div>`;
      const slowBlock = `
        <div class="proj-card">
          <h2 style="margin-top:0;">Slowest 10 by p95 elapsed</h2>
          <table class="schema-table">
            <thead><tr><th>Model</th><th>Tier</th><th>p95 elapsed</th></tr></thead>
            <tbody>
              ${r.slowest.length ? r.slowest.map(s => `
                <tr>
                  <td><code>${escape(s.name)}</code></td>
                  <td><code>${escape(s.tier)}</code></td>
                  <td>${s.p95_elapsed_seconds != null ? `${s.p95_elapsed_seconds.toFixed(2)}s` : "—"}</td>
                </tr>`).join("") : '<tr><td colspan="3"><em>Not enough data.</em></td></tr>'}
            </tbody>
          </table>
        </div>`;
      const bucketBlock = `
        <div class="proj-card">
          <h2 style="margin-top:0;">Top failure buckets (last 30 runs)</h2>
          ${Object.keys(r.failure_buckets).length ? `
            <div class="diag-buckets">
              ${Object.entries(r.failure_buckets).map(([b, n]) => `
                <button class="diag-bucket" disabled><span class="b-name">${escape(b)}</span> <span class="b-count">${n}</span></button>`).join("")}
            </div>` : '<em>No classified statement errors in the recent history.</em>'}
        </div>`;
      pane.innerHTML = `
        ${tierBlock("7d")}
        ${tierBlock("30d")}
        ${slowBlock}
        ${bucketBlock}`;
    } catch (e) {
      pane.innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
    }
  }

  // --- Project tab ----------------------------------------------------
  async function renderProjectTab() {
    const pane = document.getElementById("project-pane");
    pane.innerHTML = '<div class="detail-empty">Loading project overview&hellip;</div>';
    try {
      const [project, cfg, readme, git] = await Promise.all([
        api.project(), api.projectCfg(), api.projectReadme(), api.projectGit(),
      ]);
      const readmeHtml = readme.markdown && window.markdownit
        ? window.markdownit({ html: false, linkify: true, typographer: true }).render(readme.markdown)
        : null;
      const gitBlock = git.available ? `
        <div class="proj-card">
          <h2 style="margin-top:0;">Git</h2>
          <div class="proj-git-row">branch: <code>${escape(git.branch)}</code></div>
          <div class="proj-git-row">commit: <code>${escape(git.sha.substring(0, 10))}</code> &mdash; ${escape(git.subject)}</div>
          <div class="proj-git-row">author: ${escape(git.author)} &lt;${escape(git.email)}&gt;</div>
          <div class="proj-git-row">date: ${escape(git.date)}</div>
        </div>` : "";
      const cfgParsed = cfg.parsed || {};
      const connRows = Object.entries(cfgParsed.connections || {}).map(([name, c]) => `
        <tr><td><code>${escape(name)}</code></td><td><code>${escape(c.type || "")}</code></td>
            <td>${Object.entries(c).filter(([k]) => k !== "type").map(([k, v]) =>
              `<code>${escape(k)}=${escape(String(v))}</code>`).join(" ")}</td></tr>`).join("");
      const varsRows = Object.entries(cfgParsed.vars || {}).map(([k, v]) =>
        `<tr><td><code>${escape(k)}</code></td><td><code>${escape(String(v))}</code></td></tr>`).join("");
      pane.innerHTML = `
        <h2>Overview</h2>
        <div class="proj-card">
          <dl class="kv">
            <dt>Name</dt><dd><strong>${escape(project.name)}</strong></dd>
            <dt>Version</dt><dd><code>${escape(project.version)}</code></dd>
            <dt>Profile</dt><dd><code>${escape(project.profile)}</code></dd>
            <dt>Path</dt><dd><code>${escape(project.path)}</code></dd>
            <dt>Default materialization</dt><dd><code>${escape(project.default_materialization)}</code></dd>
            <dt>Default schema</dt><dd><code>${escape(project.default_schema)}</code></dd>
          </dl>
        </div>

        ${gitBlock}

        <h2>juncture.yaml &mdash; connections</h2>
        <div class="proj-card">
          ${connRows ? `<table class="schema-table">
            <thead><tr><th>Name</th><th>Type</th><th>Params</th></tr></thead>
            <tbody>${connRows}</tbody>
          </table>` : "<em>No connections declared.</em>"}
        </div>

        <h2>juncture.yaml &mdash; vars</h2>
        <div class="proj-card">
          ${varsRows ? `<table class="schema-table">
            <thead><tr><th>Key</th><th>Value</th></tr></thead>
            <tbody>${varsRows}</tbody>
          </table>` : "<em>No vars declared.</em>"}
        </div>

        <h2>juncture.yaml &mdash; raw</h2>
        <div class="proj-card"><pre><code class="language-yaml">${escape(cfg.raw || "")}</code></pre></div>

        <h2>README</h2>
        <div class="proj-card">
          ${readmeHtml
            ? `<div class="proj-readme">${readmeHtml}</div>`
            : "<em>No README.md found at the project root.</em>"}
        </div>`;
    } catch (e) {
      pane.innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
    }
  }

  // --- Utilities ------------------------------------------------------
  function escape(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function formatTime(iso) {
    try {
      const d = new Date(iso);
      return d.toLocaleString(undefined, { dateStyle: "short", timeStyle: "medium" });
    } catch { return iso; }
  }

  // --- Boot -----------------------------------------------------------
  (async () => {
    try {
      const project = await api.project();
      $("#project-name").textContent = project.name;
      $("#project-sub").textContent = `${project.path} · ${project.profile}`;
      document.title = `${project.name} — Juncture`;
      await renderRuns();
      await renderDag();
    } catch (e) {
      $("#status-line").textContent = `error: ${e.message}`;
    }
  })();
})();
