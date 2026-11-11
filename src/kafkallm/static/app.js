const state = { jobs: [], agents: [], sandboxes: [], features: { unlimited_jobs: false }, selected: null, selectedAgents: null, detail: null, detailError: null, logs: [], logSeq: -1, logAutoScroll: true, logScrollTop: 0, timer: null, jobFilter: "" };
const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));
}

function fmtTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function duration(job) {
  if (!job.started_at) return "—";
  const start = new Date(job.started_at).getTime();
  const end = job.finished_at ? new Date(job.finished_at).getTime() : Date.now();
  if (Number.isNaN(start) || Number.isNaN(end)) return "—";
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function badge(status) {
  return `<span class="badge ${esc(status)}">${esc(status)}</span>`;
}

function jobStatus(job) {
  return job.cancelled ? "cancelled" : job.status;
}

async function api(path, options) {
  const response = await fetch(path, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}

async function uploadFiles(files) {
  const form = new FormData();
  for (const file of files) form.append("files", file, file.name);
  const data = await api("/api/uploads", { method: "POST", body: form });
  return data.input_files || [];
}

const enabledAgents = () => state.agents.filter((agent) => agent.enabled);
const enabledSandboxes = () => state.sandboxes.filter((sandbox) => sandbox.enabled);
const sandboxSpec = (sandboxId) => state.sandboxes.find((sandbox) => sandbox.sandbox_id === sandboxId);
const sandboxLabel = (sandboxId) => sandboxSpec(sandboxId)?.name || sandboxId || "default";

function renderSandboxOptions() {
  const select = $("sandbox");
  const sandboxes = enabledSandboxes();
  if (!sandboxes.length) {
    select.innerHTML = `<option value="">No enabled sandboxes</option>`;
    select.disabled = true;
    $("sandbox-hint").textContent = "Add an environment below before submitting a job.";
    return;
  }
  const previous = select.value || "default";
  select.disabled = false;
  select.innerHTML = sandboxes.map((sandbox) => `
    <option value="${esc(sandbox.sandbox_id)}">${esc(sandbox.name)}</option>`).join("");
  if (sandboxes.some((sandbox) => sandbox.sandbox_id === previous)) {
    select.value = previous;
  } else if (sandboxes.some((sandbox) => sandbox.sandbox_id === "default")) {
    select.value = "default";
  } else {
    select.value = sandboxes[0].sandbox_id;
  }
  updateSandboxHint();
}

function updateSandboxHint() {
  const sandbox = sandboxSpec($("sandbox").value);
  $("sandbox-hint").textContent = sandbox
    ? `${sandbox.description || "No description."} Image: ${sandbox.image}.`
    : "The environment provides the job image and its tools.";
}

function renderSandboxes() {
  const container = $("sandboxes");
  if (!state.sandboxes.length) {
    container.innerHTML = `<span class="muted">No sandboxes yet.</span>`;
    return;
  }
  container.innerHTML = state.sandboxes.map((sandbox) => `
    <div class="agent-row ${sandbox.enabled ? "" : "disabled"}">
      <div class="info">
        <div class="name">${esc(sandbox.name)}</div>
        <div class="meta mono">${esc(sandbox.sandbox_id)} · ${esc(sandbox.image)}</div>
        <div class="meta">${esc(sandbox.description || "")}</div>
      </div>
      <div class="actions">
        <button type="button" data-sandbox-edit="${esc(sandbox.sandbox_id)}">Edit</button>
        ${sandbox.enabled && sandbox.sandbox_id !== "default" ? `<button type="button" data-sandbox-disable="${esc(sandbox.sandbox_id)}">Disable</button>` : ""}
      </div>
    </div>`).join("");
  for (const button of container.querySelectorAll("[data-sandbox-edit]")) {
    button.addEventListener("click", () => editSandbox(button.dataset.sandboxEdit));
  }
  for (const button of container.querySelectorAll("[data-sandbox-disable]")) {
    button.addEventListener("click", () => disableSandbox(button.dataset.sandboxDisable));
  }
}

async function loadSandboxes() {
  try {
    const data = await api("/api/sandboxes");
    state.sandboxes = data.sandboxes || [];
    renderSandboxOptions();
    renderSandboxes();
  } catch (error) {
    $("sandboxes").innerHTML = `<span class="message error">${esc(error.message)}</span>`;
    $("sandbox").innerHTML = `<option value="">Could not load sandboxes</option>`;
    $("sandbox").disabled = true;
    $("sandbox-hint").textContent = error.message;
  }
}

async function loadFeatures() {
  try {
    const data = await api("/api/features");
    state.features = { ...state.features, ...data };
  } catch (error) {
    state.features.unlimited_jobs = false;
  }
  $("unlimited").disabled = !state.features.unlimited_jobs;
  if (!state.features.unlimited_jobs) {
    $("unlimited").checked = false;
    $("unlimited-hint").textContent = "Unlimited jobs are disabled on this worker. Set ALLOW_UNLIMITED_JOBS=true to enable them.";
  }
  $("reflect").checked = state.features.reflect_default !== false;
  $("reflect-hint").textContent = state.features.reflect_default === false
    ? "Learning pass is off by default on this worker. Checking it enables the pass for this job."
    : "After the main run, a short second Goose session can update one skill in the shared skills directory.";
  syncUnlimited();
}

function syncUnlimited() {
  const unlimited = $("unlimited").checked;
  $("limit-fields").style.display = unlimited ? "none" : "";
  $("timeout").disabled = unlimited;
}

function resetSandboxForm() {
  $("sandbox-id").value = "";
  $("sandbox-name").value = "";
  $("sandbox-image").value = "";
  $("sandbox-description").value = "";
  $("sandbox-instructions").value = "";
  $("sandbox-env").value = "{}";
  $("sandbox-extensions").value = "";
  $("sandbox-enabled").checked = true;
  $("sandbox-message").className = "message";
  $("sandbox-message").textContent = "";
}

function openNewSandbox() {
  resetSandboxForm();
  $("sandbox-details").open = true;
  $("sandbox-name").focus();
}

function editSandbox(sandboxId) {
  const sandbox = sandboxSpec(sandboxId);
  if (!sandbox) return;
  $("sandbox-id").value = sandbox.sandbox_id;
  $("sandbox-name").value = sandbox.name;
  $("sandbox-image").value = sandbox.image;
  $("sandbox-description").value = sandbox.description || "";
  $("sandbox-instructions").value = sandbox.instructions || "";
  $("sandbox-env").value = JSON.stringify(sandbox.env || {}, null, 2);
  $("sandbox-extensions").value = (sandbox.extensions || []).join(", ");
  $("sandbox-enabled").checked = sandbox.enabled;
  $("sandbox-message").className = "message";
  $("sandbox-message").textContent = "";
  $("sandbox-details").open = true;
  $("sandbox-name").focus();
}

async function disableSandbox(sandboxId) {
  try {
    await api(`/api/sandboxes/${encodeURIComponent(sandboxId)}`, { method: "DELETE" });
    await loadSandboxes();
  } catch (error) {
    $("sandbox-message").className = "message error";
    $("sandbox-message").textContent = error.message;
  }
}

function renderAgentOptions() {
  const container = $("agent-options");
  const agents = enabledAgents();
  if (!agents.length) {
    container.innerHTML = `<span class="muted" style="font-size:12px">No enabled agents. The job will use the built-in goosetown roles.</span>`;
    return;
  }
  const selected = state.selectedAgents === null
    ? new Set(agents.map((agent) => agent.agent_id))
    : new Set(state.selectedAgents);
  container.innerHTML = agents.map((agent) => `
    <label class="agent-option">
      <input type="checkbox" value="${esc(agent.agent_id)}" ${selected.has(agent.agent_id) ? "checked" : ""}>
      <span>${esc(agent.name)}</span>
      <span class="role">${esc(agent.role)}</span>
    </label>`).join("");
  for (const input of container.querySelectorAll("input")) {
    input.addEventListener("change", () => {
      state.selectedAgents = Array.from(
        container.querySelectorAll("input:checked")
      ).map((item) => item.value);
    });
  }
}

function syncAgentOptions() {
  $("agent-options").hidden = !$("multi_agent").checked;
}

function renderAgents() {
  const container = $("agents");
  if (!state.agents.length) {
    container.innerHTML = `<span class="muted">No agents yet.</span>`;
    return;
  }
  container.innerHTML = state.agents.map((agent) => `
    <div class="agent-row ${agent.enabled ? "" : "disabled"}">
      <div class="info">
        <div class="name">${esc(agent.name)}${agent.builtin ? ` <span class="muted">· built-in</span>` : ""}</div>
        <div class="meta">${esc(agent.role)} · ${esc((agent.skills || []).join(", ") || "no skills")}</div>
        <div class="meta">${esc(agent.description || "")}</div>
      </div>
      <div class="actions">
        ${agent.builtin ? `<span class="muted">read-only</span>` : `
          <button type="button" data-edit="${esc(agent.agent_id)}">Edit</button>
          ${agent.enabled ? `<button type="button" data-disable="${esc(agent.agent_id)}">Disable</button>` : ""}
        `}
      </div>
    </div>`).join("");
  for (const button of container.querySelectorAll("[data-edit]")) {
    button.addEventListener("click", () => editAgent(button.dataset.edit));
  }
  for (const button of container.querySelectorAll("[data-disable]")) {
    button.addEventListener("click", () => disableAgent(button.dataset.disable));
  }
}

async function loadAgents() {
  try {
    const data = await api("/api/agents");
    state.agents = data.agents || [];
    renderAgentOptions();
    renderAgents();
  } catch (error) {
    $("agents").innerHTML = `<span class="message error">${esc(error.message)}</span>`;
  }
}

function resetAgentForm() {
  $("agent-id").value = "";
  $("agent-name").value = "";
  $("agent-role").value = "";
  $("agent-skills").value = "";
  $("agent-description").value = "";
  $("agent-instructions").value = "";
  $("agent-enabled").checked = true;
  $("agent-message").className = "message";
  $("agent-message").textContent = "";
}

function openNewAgent() {
  resetAgentForm();
  $("agents-panel").querySelector("details").open = true;
  $("agent-name").focus();
}

function editAgent(agentId) {
  const agent = state.agents.find((item) => item.agent_id === agentId);
  if (!agent) return;
  $("agent-id").value = agent.agent_id;
  $("agent-name").value = agent.name;
  $("agent-role").value = agent.role;
  $("agent-skills").value = (agent.skills || []).join(", ");
  $("agent-description").value = agent.description || "";
  $("agent-instructions").value = agent.instructions || "";
  $("agent-enabled").checked = agent.enabled;
  $("agent-message").className = "message";
  $("agent-message").textContent = "";
  $("agents-panel").querySelector("details").open = true;
  $("agent-name").focus();
}

async function disableAgent(agentId) {
  try {
    await api(`/api/agents/${encodeURIComponent(agentId)}`, { method: "DELETE" });
    await loadAgents();
  } catch (error) {
    $("agents").innerHTML = `<span class="message error">${esc(error.message)}</span>`;
  }
}

function renderJobs() {
  const query = (state.jobFilter || "").trim().toLowerCase();
  const jobs = query
    ? state.jobs.filter((job) => [
        job.job_id,
        job.instruction,
        job.sandbox,
        sandboxLabel(job.sandbox),
        job.status,
      ].join(" ").toLowerCase().includes(query))
    : state.jobs;
  if (!jobs.length) {
    $("jobs").className = "empty";
    $("jobs").textContent = state.jobs.length
      ? "No jobs match the filter."
      : "No jobs yet. Submit one.";
    return;
  }
  $("jobs").className = "";
  const rows = jobs.map((job) => {
    const selected = job.job_id === state.selected;
    return `
    <tr data-id="${esc(job.job_id)}" class="${selected ? "selected" : ""}">
      <td>${badge(jobStatus(job))}</td>
      <td class="mono">${esc(job.job_id)}</td>
      <td><div class="snippet" title="${esc(job.instruction)}">${esc(job.instruction)}</div></td>
      <td class="muted">${esc(sandboxLabel(job.sandbox))}</td>
      <td class="muted">${esc(fmtTime(job.submitted_at))}</td>
      <td class="muted">${esc(duration(job))}</td>
      <td class="muted">${esc(job.steps ?? 0)}</td>
    </tr>${selected ? renderDetailRow() : ""}`;
  }).join("");
  $("jobs").innerHTML = `<table>
    <colgroup>
      <col style="width: 11%">
      <col style="width: 22%">
      <col style="width: 28%">
      <col style="width: 8%">
      <col style="width: 15%">
      <col style="width: 10%">
      <col style="width: 6%">
    </colgroup>
    <thead><tr><th>Status</th><th>Job</th><th>Instruction</th><th>Sandbox</th><th>Submitted</th><th>Duration</th><th>Steps</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
  for (const row of $("jobs").querySelectorAll("tbody tr[data-id]")) {
    row.addEventListener("click", () => {
      if (state.selected === row.dataset.id) {
        closeJob();
      } else {
        selectJob(row.dataset.id);
      }
    });
  }
  const stop = $("jobs").querySelector("[data-stop-job]");
  if (stop) {
    stop.addEventListener("click", (event) => {
      event.stopPropagation();
      cancelJob(stop.dataset.stopJob);
    });
  }
  const restart = $("jobs").querySelector("[data-restart-job]");
  if (restart) {
    restart.addEventListener("click", (event) => {
      event.stopPropagation();
      restartJob(restart.dataset.restartJob);
    });
  }
  const live = $("jobs").querySelector(".live-logs");
  if (live) {
    live.addEventListener("scroll", () => {
      state.logScrollTop = live.scrollTop;
      state.logAutoScroll =
        live.scrollHeight - live.scrollTop - live.clientHeight < 40;
    });
    if (state.logAutoScroll) {
      live.scrollTop = live.scrollHeight;
    } else {
      live.scrollTop = state.logScrollTop;
    }
  }
}

function renderDetailRow() {
  let content;
  if (state.detailError) {
    content = `<section><h2>Job detail</h2><div class="message error">${esc(state.detailError)}</div></section>`;
  } else if (state.detail && state.detail.job_id === state.selected) {
    content = renderDetail(state.detail);
  } else {
    content = `<section><h2>Job detail</h2><span class="muted">Loading…</span></section>`;
  }
  return `<tr class="detail-row"><td colspan="7">${content}</td></tr>`;
}

async function loadJobs() {
  try {
    const data = await api("/api/jobs");
    state.jobs = data.jobs || [];
    renderJobs();
  } catch (error) {
    $("jobs").className = "empty";
    $("jobs").textContent = `Could not load jobs: ${error.message}`;
  }
}

function closeJob() {
  state.selected = null;
  state.detail = null;
  state.detailError = null;
  state.logs = [];
  state.logSeq = -1;
  renderJobs();
}

async function selectJob(jobId) {
  state.selected = jobId;
  state.detail = null;
  state.detailError = null;
  state.logs = [];
  state.logSeq = -1;
  state.logAutoScroll = true;
  state.logScrollTop = 0;
  renderJobs();
  await loadDetail();
  await loadLogs();
}

async function cancelJob(jobId) {
  try {
    await api(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
    });
    await loadJobs();
    await loadDetail();
  } catch (error) {
    state.detailError = error.message;
    renderJobs();
  }
}

async function restartJob(jobId) {
  try {
    const job = await api(`/api/jobs/${encodeURIComponent(jobId)}/restart`, {
      method: "POST",
    });
    await loadJobs();
    await selectJob(job.job_id);
  } catch (error) {
    state.detailError = error.message;
    renderJobs();
  }
}

async function clearFinishedJobs() {
  if (!window.confirm("Remove all finished jobs from the list? Their state records are deleted from Kafka.")) {
    return;
  }
  try {
    await api("/api/jobs/clear-finished", { method: "POST" });
    await loadJobs();
    if (state.selected && !state.jobs.some((job) => job.job_id === state.selected)) {
      closeJob();
    }
  } catch (error) {
    $("jobs").className = "empty";
    $("jobs").textContent = error.message;
  }
}

async function loadDetail() {
  if (!state.selected) {
    state.detail = null;
    state.detailError = null;
    return;
  }
  const selected = state.selected;
  try {
    const job = await api(`/api/jobs/${encodeURIComponent(selected)}`);
    if (state.selected !== selected) return;
    state.detail = job;
    state.detailError = null;
  } catch (error) {
    if (state.selected !== selected) return;
    state.detail = null;
    state.detailError = error.message;
  }
  renderJobs();
}

async function loadLogs() {
  if (!state.selected) return;
  const selected = state.selected;
  try {
    const data = await api(
      `/api/jobs/${encodeURIComponent(selected)}/logs?after=${state.logSeq}`
    );
    if (state.selected !== selected) return;
    const records = data.logs || [];
    if (records.length) {
      state.logs.push(...records);
      if (state.logs.length > 3000) {
        state.logs = state.logs.slice(-3000);
      }
      state.logSeq = typeof data.next === "number" ? data.next : state.logSeq;
      renderJobs();
    }
  } catch (error) {
    // Logs are best-effort; keep the job detail usable if the topic lags.
  }
}

function renderDetail(job) {
  const usage = job.usage && Object.keys(job.usage).length
    ? Object.entries(job.usage).map(([k, v]) => `${esc(k)}=${esc(v)}`).join(" · ")
    : "—";
  const inputFiles = (job.input_files || []).length
    ? `<div class="events">${job.input_files.map((a) => `<div class="mono">${esc(a.bucket)}/${esc(a.key)}</div>`).join("")}</div>`
    : `<span class="muted">none</span>`;
  const artifacts = (job.artifacts || []).length
    ? `<div class="events">${job.artifacts.map((a, index) => `
        <div class="artifact-line">
          <span class="mono">${esc(a.bucket)}/${esc(a.key)}</span>
          <a class="download-link" href="/api/jobs/${encodeURIComponent(job.job_id)}/artifacts/${index}" download>Download</a>
        </div>`).join("")}</div>`
    : `<span class="muted">none</span>`;
  const events = (job.events || []).length
    ? `<div class="events">${job.events.map((event) => `
        <div class="event">
          <div class="line">
            <span class="type">${esc(event.type)}</span>
            <span class="muted">${esc(event.status)}</span>
            <span class="muted mono">${esc(fmtTime(event.timestamp))}</span>
            ${event.duration_ms ? `<span class="muted mono">${esc(Math.round(event.duration_ms))} ms</span>` : ""}
          </div>
          ${event.error ? `<div class="err">${esc(event.error)}</div>` : ""}
        </div>`).join("")}</div>`
    : `<span class="muted">No events yet.</span>`;
  const liveLogs = state.logs.length
    ? `<pre class="live-logs">${state.logs.map((entry) => `${esc(fmtTime(entry.timestamp))} [${esc(entry.kind)}] ${esc(entry.text)}`).join("\n")}</pre>`
    : `<span class="muted">No live activity yet.</span>`;
  return `<section>
    <h2>Job detail</h2>
    <div class="grid">
      <div class="field"><div class="k">Status</div><div class="v">${badge(jobStatus(job))}</div></div>
      <div class="field"><div class="k">Job</div><div class="v mono">${esc(job.job_id)}</div></div>
      <div class="field"><div class="k">Run</div><div class="v mono">${esc(job.run_id)}</div></div>
      <div class="field"><div class="k">Worker</div><div class="v mono">${esc(job.worker_id || "—")}</div></div>
      <div class="field"><div class="k">Submitted</div><div class="v">${esc(fmtTime(job.submitted_at))}</div></div>
      <div class="field"><div class="k">Started</div><div class="v">${esc(fmtTime(job.started_at))}</div></div>
      <div class="field"><div class="k">Finished</div><div class="v">${esc(fmtTime(job.finished_at))}</div></div>
      <div class="field"><div class="k">Sandbox</div><div class="v">${esc(sandboxLabel(job.sandbox))}${job.sandbox_image ? ` <span class="muted mono">· ${esc(job.sandbox_image)}</span>` : ""}</div></div>
      <div class="field"><div class="k">Mode</div><div class="v">${job.multi_agent === false ? "single agent" : "goosetown flock"}</div></div>
      <div class="field"><div class="k">Runtime</div><div class="v">${job.unlimited ? "unlimited" : `${esc(job.timeout_seconds)}s`}</div></div>
      <div class="field"><div class="k">Learning pass</div><div class="v">${job.reflect === false ? "off" : job.reflect === true ? "on" : "worker default"}</div></div>
      <div class="field"><div class="k">Flock</div><div class="v">${esc((job.agent_names || []).join(", ") || "built-in roles")}</div></div>
      <div class="field"><div class="k">Usage</div><div class="v mono">${usage}</div></div>
    </div>
    ${job.status === "queued" || job.status === "running" ? `<div class="block">
      <button id="stop-job" type="button" class="danger" data-stop-job="${esc(job.job_id)}">Stop job</button>
      <div class="hint">Requests cancellation. The worker stops the microVM and marks the job as cancelled.</div>
    </div>` : job.status === "succeeded" || job.status === "failed" ? `<div class="block">
      <button id="restart-job" type="button" data-restart-job="${esc(job.job_id)}">Restart job</button>
      <div class="hint">Queues a new job with the same instruction, inputs, sandbox image, agents, and limits.</div>
    </div>` : ""}
    <div class="block"><h3>Instruction</h3><pre>${esc(job.instruction)}</pre></div>
    ${job.result ? `<div class="block">
      <div class="block-head">
        <h3>Result</h3>
        <a class="download-link" href="/api/jobs/${encodeURIComponent(job.job_id)}/result" download>Download result</a>
      </div>
      <pre>${esc(job.result)}</pre>
    </div>` : ""}
    ${job.error ? `<div class="block"><h3>Error</h3><pre>${esc(job.error)}</pre></div>` : ""}
    ${job.artifact_error ? `<div class="block"><h3>Artifact error</h3><pre>${esc(job.artifact_error)}</pre></div>` : ""}
    <div class="block"><h3>Input files</h3>${inputFiles}</div>
    <div class="block"><h3>Artifacts</h3>${artifacts}</div>
    <div class="block"><h3>Live activity</h3>${liveLogs}</div>
    <div class="block"><h3>Audit events</h3>${events}</div>
  </section>`;
}

$("multi_agent").addEventListener("change", syncAgentOptions);
$("sandbox").addEventListener("change", updateSandboxHint);
$("unlimited").addEventListener("change", syncUnlimited);
syncAgentOptions();

$("job-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("submit");
  const message = $("message");
  const files = Array.from($("files").files || []);
  button.disabled = true;
  message.className = "message";
  message.textContent = files.length ? "Uploading files…" : "Submitting…";
  try {
    const sandbox = $("sandbox").value;
    if (!sandbox) throw new Error("Select an enabled sandbox first.");
    const input = JSON.parse($("input").value || "{}");
    const inputFiles = files.length ? await uploadFiles(files) : [];
    message.textContent = "Submitting…";
    const multiAgent = $("multi_agent").checked;
    const agentIds = multiAgent
      ? Array.from($("agent-options").querySelectorAll("input:checked")).map((item) => item.value)
      : [];
    const payload = {
      instruction: $("instruction").value.trim(),
      input,
      input_files: inputFiles,
      sandbox,
      multi_agent: multiAgent,
      agent_ids: agentIds,
      unlimited: $("unlimited").checked,
      timeout_seconds: Number($("timeout").value),
      reflect: $("reflect").checked,
    };
    const job = await api("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    message.className = "message ok";
    message.textContent = `Queued ${job.job_id}`;
    $("instruction").value = "";
    $("files").value = "";
    await loadJobs();
    await selectJob(job.job_id);
  } catch (error) {
    message.className = "message error";
    message.textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

$("job-search").addEventListener("input", () => {
  state.jobFilter = $("job-search").value;
  renderJobs();
});

$("clear-finished").addEventListener("click", clearFinishedJobs);

$("agent-new").addEventListener("click", openNewAgent);    $("agent-new-inline").addEventListener("click", openNewAgent);
$("agent-reload").addEventListener("click", async () => {
  const button = $("agent-reload");
  button.disabled = true;
  button.textContent = "Reloading…";
  try {
    await api("/api/agents/reload", { method: "POST" });
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await loadAgents();
  } catch (error) {
    $("agents").innerHTML = `<span class="message error">${esc(error.message)}</span>`;
  } finally {
    button.disabled = false;
    button.textContent = "Reload from files";
  }
});
$("agent-reset").addEventListener("click", resetAgentForm);

$("agent-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("agent-save");
  const message = $("agent-message");
  button.disabled = true;
  message.className = "message";
  message.textContent = "";
  try {
    const payload = {
      agent_id: $("agent-id").value || undefined,
      name: $("agent-name").value.trim(),
      role: $("agent-role").value.trim() || "custom",
      description: $("agent-description").value.trim(),
      instructions: $("agent-instructions").value.trim(),
      skills: $("agent-skills").value.split(",").map((value) => value.trim()).filter(Boolean),
      enabled: $("agent-enabled").checked,
    };
    if (!payload.agent_id) delete payload.agent_id;
    const created = await api("/api/agents", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (created.enabled && state.selectedAgents !== null) {
      state.selectedAgents = Array.from(
        new Set([...state.selectedAgents, created.agent_id])
      );
    }
    resetAgentForm();
    await loadAgents();
    $("agent-message").className = "message ok";
    $("agent-message").textContent = `Saved ${created.name}.`;
  } catch (error) {
    $("agent-message").className = "message error";
    $("agent-message").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

$("sandbox-new").addEventListener("click", openNewSandbox);
$("sandbox-reset").addEventListener("click", resetSandboxForm);

$("sandbox-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("sandbox-save");
  const message = $("sandbox-message");
  button.disabled = true;
  message.className = "message";
  message.textContent = "";
  try {
    const rawEnv = $("sandbox-env").value.trim();
    const env = rawEnv ? JSON.parse(rawEnv) : {};
    if (!env || Array.isArray(env) || typeof env !== "object") {
      throw new Error("Extra environment must be a JSON object.");
    }
    const payload = {
      sandbox_id: $("sandbox-id").value || undefined,
      name: $("sandbox-name").value.trim(),
      image: $("sandbox-image").value.trim(),
      description: $("sandbox-description").value.trim(),
      instructions: $("sandbox-instructions").value.trim(),
      env,
      extensions: $("sandbox-extensions").value.split(",").map((value) => value.trim()).filter(Boolean),
      enabled: $("sandbox-enabled").checked,
    };
    if (!payload.sandbox_id) delete payload.sandbox_id;
    const saved = await api("/api/sandboxes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    resetSandboxForm();
    await loadSandboxes();
    $("sandbox-message").className = "message ok";
    $("sandbox-message").textContent = `Saved ${saved.name}.`;
  } catch (error) {
    $("sandbox-message").className = "message error";
    $("sandbox-message").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

loadJobs();
loadAgents();
loadSandboxes();
loadFeatures();
state.timer = setInterval(async () => {
  await loadJobs();
  await loadDetail();
  await loadLogs();
}, 2000);
