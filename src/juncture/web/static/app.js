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
    project:  () => fetch("/api/project").then(r => r.json()),
    manifest: () => fetch("/api/manifest").then(r => r.json()),
    model:    (name) => fetch(`/api/models/${encodeURIComponent(name)}`).then(r => r.json()),
    runs:     (limit = 50) => fetch(`/api/runs?limit=${limit}`).then(r => r.json()),
    run:      (id) => fetch(`/api/runs/${encodeURIComponent(id)}`).then(r => r.json()),
  };

  // --- Tab switching (top bar) ----------------------------------------
  document.querySelectorAll(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.view;
      document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach(v => v.classList.toggle("active", v.id === `view-${target}`));
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
      elements.push({
        data: { id: m.name, label: m.name, kind: m.kind, disabled: m.disabled },
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
  }

  // --- Model detail (four-tab sidebar) ---------------------------------
  async function selectModel(name) {
    currentModelName = name;
    try {
      currentModelDetail = await api.model(name);
    } catch (e) {
      $("#model-detail").innerHTML = `<div class="detail-empty">Load failed: ${escape(e.message)}</div>`;
      return;
    }
    document.querySelector(".detail-tabs").classList.add("visible");
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
    const lastStatus = latestRunByModel[m.name] || "(never run)";
    const deps = m.depends_on.length
      ? m.depends_on.map(d => `<code>${escape(d)}</code>`).join(" ")
      : "<em>none</em>";
    const tags = m.tags && m.tags.length
      ? m.tags.map(t => `<code>${escape(t)}</code>`).join(" ") : null;
    return `
      <div class="detail-block">
        <dl>
          <dt>Name</dt><dd><strong>${escape(m.name)}</strong></dd>
          <dt>Kind / materialization</dt>
          <dd><code>${escape(m.kind)}</code> / <code>${escape(m.materialization)}</code></dd>
          ${m.path ? `<dt>Source path</dt><dd><code>${escape(m.path)}</code></dd>` : ""}
          <dt>Last run</dt><dd><span class="status-pill ${escape(lastStatus)}">${escape(lastStatus)}</span></dd>
          ${m.disabled ? '<dt>State</dt><dd><span class="status-pill disabled">disabled</span></dd>' : ""}
          ${m.description ? `<dt>Description</dt><dd>${escape(m.description)}</dd>` : ""}
          <dt>Depends on</dt><dd>${deps}</dd>
          ${m.schedule_cron ? `<dt>Schedule</dt><dd><code>${escape(m.schedule_cron)}</code></dd>` : ""}
          ${tags ? `<dt>Tags</dt><dd>${tags}</dd>` : ""}
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
    const detail = await api.run(runId);

    const panel = $("#run-detail");
    panel.classList.remove("detail-empty");
    const testsByModel = (detail.tests || []).reduce((acc, t) => {
      (acc[t.model] = acc[t.model] || []).push(t);
      return acc;
    }, {});

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
      <table>
        <thead><tr><th>Model</th><th>Status</th><th>Elapsed</th><th>Rows</th><th>Error (summary)</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
      ${runTestsBlock}`;

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
