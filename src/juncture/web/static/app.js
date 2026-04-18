/*
 * Juncture web — vanilla JS single-page app.
 *
 * Two views: DAG (cytoscape breadth-first layout) and Runs (history
 * table + per-run drilldown). All state is derived from three API
 * calls so a page refresh is always authoritative.
 */

(() => {
  const $ = (sel, root = document) => root.querySelector(sel);

  // --- API ------------------------------------------------------------
  const api = {
    project: () => fetch("/api/project").then(r => r.json()),
    manifest: () => fetch("/api/manifest").then(r => r.json()),
    runs: (limit = 50) => fetch(`/api/runs?limit=${limit}`).then(r => r.json()),
    run: (id) => fetch(`/api/runs/${encodeURIComponent(id)}`).then(r => r.json()),
  };

  // --- Tab switching --------------------------------------------------
  document.querySelectorAll(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.view;
      document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach(v => v.classList.toggle("active", v.id === `view-${target}`));
    });
  });

  // --- State ----------------------------------------------------------
  let latestRunByModel = {};  // model_name -> last-seen status
  let manifestCache = null;

  // --- DAG render -----------------------------------------------------
  async function renderDag() {
    const manifest = await api.manifest();
    manifestCache = manifest;
    // Compute layer for each node via a naive topological walk (the
    // manifest's `order` is topologically sorted, so depth-from-root
    // is enough).
    const depth = {};
    const depsByModel = Object.fromEntries(manifest.models.map(m => [m.name, m.depends_on]));
    const computeDepth = (name) => {
      if (depth[name] !== undefined) return depth[name];
      const parents = depsByModel[name] || [];
      depth[name] = parents.length ? Math.max(...parents.map(computeDepth)) + 1 : 0;
      return depth[name];
    };
    manifest.models.forEach(m => computeDepth(m.name));

    const elements = [];
    manifest.models.forEach(m => {
      const lastStatus = latestRunByModel[m.name];
      const classes = [`kind-${m.kind}`];
      if (m.disabled) classes.push("status-disabled");
      else if (lastStatus) classes.push(`status-${lastStatus}`);
      elements.push({ data: { id: m.name, label: m.name, kind: m.kind, disabled: m.disabled }, classes: classes.join(" ") });
    });
    manifest.edges.forEach(e => {
      elements.push({ data: { id: `${e.from}->${e.to}`, source: e.from, target: e.to } });
    });

    const cy = cytoscape({
      container: document.getElementById("cy"),
      elements,
      layout: {
        name: "breadthfirst",
        directed: true,
        spacingFactor: 1.15,
        padding: 24,
      },
      wheelSensitivity: 0.2,
      style: [
        { selector: "node", style: {
          "background-color": "#ffffff",
          "border-width": 2, "border-color": "#6fa8dc",
          "label": "data(label)", "font-size": 11, "color": "#1e2024",
          "text-valign": "center", "text-halign": "center",
          "text-wrap": "ellipsis", "text-max-width": "130px",
          "width": 140, "height": 34, "shape": "round-rectangle",
          "font-family": "-apple-system, BlinkMacSystemFont, sans-serif",
        }},
        { selector: ".kind-seed",   style: { "border-color": "#e0b96a" } },
        { selector: ".kind-sql",    style: { "border-color": "#6fa8dc" } },
        { selector: ".kind-python", style: { "border-color": "#93c47d" } },
        { selector: ".status-success",  style: { "background-color": "rgba(46,160,67,.10)" } },
        { selector: ".status-failed",   style: { "background-color": "rgba(209,36,47,.12)", "border-color": "#d1242f" } },
        { selector: ".status-partial",  style: { "background-color": "rgba(176,136,0,.12)", "border-color": "#b08800" } },
        { selector: ".status-disabled", style: { "opacity": 0.45, "border-style": "dashed" } },
        { selector: "node:selected", style: { "border-width": 3 } },
        { selector: "edge", style: {
          "width": 1.2, "line-color": "#c6c8cd", "target-arrow-color": "#c6c8cd",
          "target-arrow-shape": "triangle", "curve-style": "bezier",
        }},
      ],
    });

    cy.on("tap", "node", (evt) => {
      const name = evt.target.id();
      const model = manifestCache.models.find(m => m.name === name);
      if (model) renderModelDetail(model);
    });
  }

  function renderModelDetail(model) {
    const panel = $("#model-detail");
    const lastStatus = latestRunByModel[model.name] || "(never run)";
    panel.classList.remove("detail-empty");
    panel.innerHTML = `
      <div class="detail-block">
        <dl>
          <dt>Name</dt><dd><strong>${escape(model.name)}</strong></dd>
          <dt>Kind / materialization</dt>
          <dd><code>${escape(model.kind)}</code> / <code>${escape(model.materialization)}</code></dd>
          <dt>Last run</dt><dd><span class="status-pill ${escape(lastStatus)}">${escape(lastStatus)}</span></dd>
          ${model.disabled ? '<dt>State</dt><dd><span class="status-pill disabled">disabled</span></dd>' : ""}
          ${model.description ? `<dt>Description</dt><dd>${escape(model.description)}</dd>` : ""}
          <dt>Depends on</dt>
          <dd>${model.depends_on.length ? model.depends_on.map(d => `<code>${escape(d)}</code>`).join(" ") : "<em>none</em>"}</dd>
          ${model.schedule_cron ? `<dt>Schedule</dt><dd><code>${escape(model.schedule_cron)}</code></dd>` : ""}
          ${model.tags && model.tags.length ? `<dt>Tags</dt><dd>${model.tags.map(t => `<code>${escape(t)}</code>`).join(" ")}</dd>` : ""}
        </dl>
      </div>`;
  }

  // --- Runs view ------------------------------------------------------
  async function renderRuns() {
    const { runs } = await api.runs();
    const body = $("#runs-body");
    if (runs.length === 0) {
      body.innerHTML = '<tr><td colspan="8" style="color:var(--text-dim); padding: 12px 0;">No runs recorded yet — try <code>juncture run --project .</code>.</td></tr>';
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
    // Update per-model last status for the DAG colouring.
    if (runs.length) {
      const detail = await api.run(runs[0].run_id);
      latestRunByModel = {};
      detail.models.forEach(m => { latestRunByModel[m.name] = m.status; });
      selectRun(runs[0].run_id);
    }
    $("#status-line").textContent = `${runs.length} run(s) on record`;
  }

  async function selectRun(runId) {
    document.querySelectorAll("#runs-body tr").forEach(tr => tr.classList.toggle("selected", tr.dataset.runId === runId));
    const detail = await api.run(runId);
    const panel = $("#run-detail");
    panel.classList.remove("detail-empty");
    const rows = detail.models.map(m => {
      const pill = `<span class="status-pill ${escape(m.status)}">${escape(m.status)}</span>`;
      const err = m.error ? `<div class="stmt-errors">${escape(m.error)}</div>` : "";
      let errDetails = "";
      if (m.statement_errors && m.statement_errors.length) {
        errDetails = '<div class="stmt-errors">' + m.statement_errors.slice(0, 5).map(se =>
          `<div><code>#${se.index}${se.layer != null ? " layer=" + se.layer : ""}</code> ${escape(se.error.split("\n")[0].slice(0, 180))}</div>`
        ).join("") + `${m.statement_errors.length > 5 ? `<div>... +${m.statement_errors.length - 5} more</div>` : ""}</div>`;
      }
      return `
        <tr>
          <td><strong>${escape(m.name)}</strong></td>
          <td>${pill}</td>
          <td>${m.elapsed_seconds.toFixed(2)}s</td>
          <td>${m.row_count ?? "—"}</td>
          <td>${err}${errDetails}</td>
        </tr>`;
    }).join("");
    panel.innerHTML = `
      <h2 style="margin:0; font-size: 16px;">Run <code>${escape(detail.run_id.substring(0, 7))}</code></h2>
      <div style="color:var(--text-dim); font-size: 12px; margin-top: 4px;">
        ${escape(formatTime(detail.started_at))} · ${detail.elapsed_seconds.toFixed(2)}s · ${detail.ok ? '<span class="ok-yes">ok</span>' : '<span class="ok-no">failed</span>'}
      </div>
      <table>
        <thead><tr><th>Model</th><th>Status</th><th>Elapsed</th><th>Rows</th><th>Detail</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
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
