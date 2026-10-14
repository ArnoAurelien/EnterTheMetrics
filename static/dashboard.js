const COLORS = {
  cpu: "#087f70",
  memory: "#c95745",
  receive: "#087f70",
  transmit: "#d08c2c",
  read: "#52646a",
  write: "#c95745",
  grid: "#e9efec",
  text: "#718078",
};

const chartInstances = new Map();
const state = {
  configured: [],
  archived: [],
  selectedContainer: null,
  selectedSession: null,
  detail: null,
  pollInterval: 2,
};

const elements = {
  configured: document.querySelector("#configured-list"),
  archived: document.querySelector("#archived-list"),
  configuredCount: document.querySelector("#configured-count"),
  historyCount: document.querySelector("#history-count"),
  empty: document.querySelector("#empty-state"),
  view: document.querySelector("#dashboard-view"),
  title: document.querySelector("#page-title"),
  subtitle: document.querySelector("#page-subtitle"),
  kicker: document.querySelector("#page-kicker"),
  breadcrumb: document.querySelector("#breadcrumb-container"),
  statePill: document.querySelector("#live-state"),
  quality: document.querySelector("#quality-banner"),
  qualityTitle: document.querySelector("#quality-title"),
  qualityMessage: document.querySelector("#quality-message"),
  summary: document.querySelector("#summary-grid"),
  sessionsSection: document.querySelector("#sessions-section"),
  sessions: document.querySelector("#sessions-list"),
  sessionCount: document.querySelector("#session-count-label"),
  chartsTitle: document.querySelector("#charts-title"),
  sampleFooter: document.querySelector("#sample-footer"),
  updatedAt: document.querySelector("#updated-at"),
  allTimeButton: document.querySelector("#alltime-button"),
};

function formatBytes(value, perSecond = false) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "No data";
  let amount = Number(value);
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  while (Math.abs(amount) >= 1000 && index < units.length - 1) {
    amount /= 1000;
    index += 1;
  }
  return `${amount.toFixed(index === 0 ? 0 : 1)} ${units[index]}${perSecond ? "/s" : ""}`;
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return "—";
  const value = Math.max(0, Math.round(Number(seconds)));
  const days = Math.floor(value / 86400);
  const hours = Math.floor((value % 86400) / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  if (days) return `${days}d ${hours}h`;
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m`;
  return `${value}s`;
}

function formatCpuTime(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return "No data";
  const value = Number(seconds);
  return value < 60 ? `${value.toFixed(1)} s` : formatDuration(value);
}

function formatTime(timestamp, options = {}) {
  if (timestamp === null || timestamp === undefined) return "Unknown time";
  return new Intl.DateTimeFormat(undefined, {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    ...(options.year ? { year: "numeric" } : {}),
  }).format(new Date(Number(timestamp) * 1000));
}

function shortId(value) {
  if (!value) return "Legacy samples";
  return value.length > 18 ? `${value.slice(0, 10)}…${value.slice(-5)}` : value;
}

async function getJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function statusClass(container) {
  if (container.live_state === "live") return "live";
  if (container.live_state === "stale") return "stale";
  if (container.status === "waiting") return "waiting";
  return "";
}

function renderContainerList(target, items, archived = false) {
  target.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "sidebar-empty";
    empty.textContent = archived ? "No archived history" : "No containers configured";
    target.append(empty);
    return;
  }

  for (const item of items) {
    const button = document.createElement("button");
    button.className = "container-button";
    button.type = "button";
    button.setAttribute("aria-current", String(state.selectedContainer?.id === item.id));
    const dot = document.createElement("span");
    dot.className = `status-dot ${archived ? "" : statusClass(item)}`;
    dot.setAttribute("aria-hidden", "true");
    const name = document.createElement("span");
    name.className = "container-name";
    name.textContent = item.name;
    const meta = document.createElement("span");
    meta.className = archived ? "archived-label" : "container-meta";
    meta.textContent = archived ? "ARCHIVE" : item.live_state?.toUpperCase() || "WAITING";
    button.append(dot, name, meta);
    button.addEventListener("click", () => selectContainer(item));
    target.append(button);
  }
}

async function loadCatalog() {
  const catalog = await getJson("/api/containers");
  state.configured = catalog.configured;
  state.archived = catalog.archived;
  state.pollInterval = catalog.poll_interval_seconds;
  elements.configuredCount.textContent = String(state.configured.length);
  elements.historyCount.textContent = String(state.archived.length);
  renderContainerList(elements.configured, state.configured);
  renderContainerList(elements.archived, state.archived, true);
  if (!state.selectedContainer) {
    const initial = state.configured[0] || state.archived[0];
    if (initial) await selectContainer(initial);
    else showEmpty();
  } else {
    const refreshed = [...state.configured, ...state.archived].find((item) => item.id === state.selectedContainer.id);
    if (refreshed) state.selectedContainer = refreshed;
    renderContainerList(elements.configured, state.configured);
    renderContainerList(elements.archived, state.archived, true);
  }
}

function showEmpty() {
  elements.empty.hidden = false;
  elements.view.hidden = true;
  destroyCharts();
}

async function selectContainer(container) {
  state.selectedContainer = container;
  state.selectedSession = null;
  renderContainerList(elements.configured, state.configured);
  renderContainerList(elements.archived, state.archived, true);
  await loadContainerDetail();
}

async function loadContainerDetail() {
  if (!state.selectedContainer) return;
  try {
    const key = encodeURIComponent(state.selectedContainer.id);
    state.detail = await getJson(`/api/containers/${key}`);
    state.selectedSession = null;
    renderOverview();
  } catch (error) {
    showError(error.message);
  }
}

async function selectSession(sessionId) {
  state.selectedSession = sessionId;
  try {
    state.detail = await getJson(`/api/sessions/${sessionId}`);
    renderSession();
  } catch (error) {
    showError(error.message);
  }
}

function showError(message) {
  elements.empty.hidden = true;
  elements.view.hidden = false;
  elements.title.textContent = "History unavailable";
  elements.subtitle.textContent = message;
  elements.summary.replaceChildren();
  elements.sessionsSection.hidden = true;
  destroyCharts();
}

function renderState(status, lastSeen) {
  elements.statePill.className = "state-pill";
  const display = status === "finished_inferred" ? "finished · inferred" : status || "history";
  if (status === "live" || status === "running") elements.statePill.classList.add("live");
  else if (status === "stale") elements.statePill.classList.add("stale");
  else if (status === "finished") elements.statePill.classList.add("finished");
  const age = lastSeen ? ` · seen ${formatDuration(Date.now() / 1000 - lastSeen)} ago` : "";
  const dot = document.createElement("span");
  dot.className = "state-dot";
  const label = document.createElement("span");
  label.textContent = `${display}${age}`;
  elements.statePill.replaceChildren(dot, label);
}

function summaryItem(label, value, unit, accent = "") {
  const item = document.createElement("div");
  item.className = `summary-item ${accent}`;
  const labelElement = document.createElement("span");
  labelElement.className = "summary-label";
  labelElement.textContent = label;
  const valueElement = document.createElement("strong");
  valueElement.className = "summary-value";
  valueElement.textContent = value;
  const unitElement = document.createElement("span");
  unitElement.className = "summary-unit";
  unitElement.textContent = unit || " ";
  item.append(labelElement, valueElement, unitElement);
  return item;
}

function renderSummary(totals, context) {
  elements.summary.replaceChildren(
    summaryItem("CPU time", formatCpuTime(totals.cpu_time_seconds), "Accumulated", "accent-teal"),
    summaryItem("Network received", formatBytes(totals.network_received_bytes), "Cumulative counter", "accent-teal"),
    summaryItem("Network sent", formatBytes(totals.network_sent_bytes), "Cumulative counter", "accent-amber"),
    summaryItem("Peak memory", formatBytes(totals.peak_memory_bytes), "Highest observed", "accent-coral"),
    summaryItem("Observations", String(totals.sample_count || 0), `${totals.gap_count || 0} gaps`),
  );
  elements.sampleFooter.textContent = totals.first_sample_at
    ? `${totals.sample_count} samples · ${formatTime(totals.first_sample_at, { year: true })} to ${formatTime(totals.last_sample_at, { year: true })} · ${context}`
    : "No metric samples recorded";
}

function renderQuality(samples, sessions, legacy = false) {
  const dataGaps = samples.filter((sample) => sample.gap_detected).length;
  const startupGaps = sessions.filter((session) => session.startup_gap).length;
  const activePartial = sessions.some((session) => ["running", "stale", "finished_inferred"].includes(session.status_display));
  const totalGaps = dataGaps + startupGaps;
  elements.quality.hidden = totalGaps === 0 && !activePartial && !legacy;
  if (elements.quality.hidden) return;
  if (legacy) {
    elements.qualityTitle.textContent = "Legacy samples have no session identity";
    elements.qualityMessage.textContent = "These observations predate session tracking, so their original container start and work-session boundaries are unknown.";
  } else if (totalGaps) {
    elements.qualityTitle.textContent = `${totalGaps} unobserved interval${totalGaps === 1 ? "" : "s"}`;
    elements.qualityMessage.textContent = `${dataGaps} sampling gaps${startupGaps ? ` and ${startupGaps} late-start observation${startupGaps === 1 ? "" : "s"}` : ""}. Counter rates span the gap; memory peaks inside it cannot be recovered.`;
  } else {
    elements.qualityTitle.textContent = "Totals are through the latest observation";
    elements.qualityMessage.textContent = "This session is ongoing or its finish was not observed. Cumulative totals may not include activity after the last sample.";
  }
}

function renderSessionList(sessions) {
  elements.sessions.replaceChildren();
  elements.sessionsSection.hidden = sessions.length === 0;
  elements.sessionCount.textContent = `${sessions.length} session${sessions.length === 1 ? "" : "s"}`;
  for (const session of sessions) {
    const button = document.createElement("button");
    button.className = "session-button";
    button.type = "button";
    button.setAttribute("aria-current", String(state.selectedSession === session.id));
    const date = document.createElement("span");
    date.className = "session-date";
    date.textContent = formatTime(session.started_at, { year: true });
    const meta = document.createElement("span");
    meta.className = "session-meta";
    const last = session.totals.last_sample_at;
    meta.textContent = `${session.totals.sample_count} samples · ${formatCpuTime(session.totals.cpu_time_seconds)} CPU${last ? ` · seen ${formatDuration(Date.now() / 1000 - last)} ago` : ""}`;
    const status = document.createElement("span");
    status.className = "session-state";
    status.textContent = session.status_display.replaceAll("_", " ");
    button.append(date, meta, status);
    button.addEventListener("click", () => selectSession(session.id));
    elements.sessions.append(button);
  }
}

function renderOverview() {
  const detail = state.detail;
  const container = detail.container;
  elements.empty.hidden = true;
  elements.view.hidden = false;
  elements.allTimeButton.hidden = true;
  elements.breadcrumb.textContent = container.name;
  elements.kicker.textContent = container.configured ? "CONFIGURED CONTAINER" : "ARCHIVED CONTAINER";
  elements.title.textContent = container.name;
  elements.subtitle.textContent = container.container_id
    ? `Container ID ${shortId(container.container_id)}`
    : container.configured
      ? "Waiting for the first Docker observation"
      : "Historical samples without a recorded container ID";
  const active = detail.sessions.find((session) => session.status_display === "running" || session.status_display === "stale");
  renderState(active ? active.status_display : container.configured && !container.container_id ? "waiting" : "history", active?.last_seen_at || container.last_seen_at);
  renderSummary(detail.totals, "all sessions");
  renderSessionList(detail.sessions);
  elements.chartsTitle.textContent = detail.legacy ? "Legacy observations" : "All observed activity";
  renderQuality(detail.samples, detail.sessions, detail.legacy);
  renderCharts(detail.samples);
  elements.updatedAt.textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
}

function renderSession() {
  const session = state.detail;
  const currentContainer = state.selectedContainer;
  elements.empty.hidden = true;
  elements.view.hidden = false;
  elements.allTimeButton.hidden = false;
  elements.breadcrumb.textContent = currentContainer?.name || session.container;
  elements.kicker.textContent = "WORK SESSION";
  elements.title.textContent = formatTime(session.started_at, { year: true });
  elements.subtitle.textContent = `${currentContainer?.name || session.container} · ${shortId(session.container_id)} · first observed ${formatTime(session.first_observed_at, { year: true })}`;
  renderState(session.status_display, session.last_seen_at);
  renderSummary(session.totals, "this session");
  elements.sessionsSection.hidden = true;
  elements.chartsTitle.textContent = "Session activity";
  renderQuality(session.samples, [session]);
  renderCharts(session.samples);
  elements.updatedAt.textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
}

function chartPoints(samples, field, breakAtSession = true) {
  const points = [];
  let previous = null;
  for (const sample of samples) {
    const boundary = breakAtSession && previous && sample.session_id !== previous.session_id;
    if (sample.gap_before) {
      points.push({ x: Number(sample.gap_start) * 1000, y: null });
    }
    if ((previous && sample.gap_detected) || sample.gap_before || boundary) {
      const gapStart = sample.gap_before ? Number(sample.gap_start) : Number(previous.timestamp);
      const breakTime = (gapStart + Number(sample.timestamp)) * 500;
      points.push({ x: breakTime, y: null });
    }
    points.push({ x: Number(sample.timestamp) * 1000, y: sample[field] ?? null });
    previous = sample;
  }
  return points;
}

function makeChart(id, datasets, valueFormatter) {
  if (!window.Chart) {
    elements.quality.hidden = false;
    elements.qualityTitle.textContent = "Charts could not load";
    elements.qualityMessage.textContent = "The chart library is unavailable. Check connectivity and reload.";
    return;
  }
  const canvas = document.getElementById(id);
  const existing = chartInstances.get(id);
  if (existing) existing.destroy();
  const empty = canvas.parentElement.querySelector(".chart-empty");
  const hasValues = datasets.some((dataset) => dataset.data.some((point) => point.y !== null && point.y !== undefined));
  canvas.hidden = !hasValues;
  empty.hidden = hasValues;
  if (!hasValues) {
    chartInstances.delete(id);
    return;
  }
  const chart = new Chart(canvas, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: datasets.length > 1, position: "bottom", align: "start", labels: { color: COLORS.text, boxWidth: 8, boxHeight: 8, usePointStyle: true, pointStyle: "circle", padding: 16, font: { family: "Manrope", size: 9 } } },
        tooltip: { callbacks: { label: (item) => `${item.dataset.label}: ${valueFormatter(item.parsed.y)}` } },
      },
      scales: {
        x: {
          type: "linear",
          grid: { color: COLORS.grid, drawTicks: false },
          border: { display: false },
          ticks: {
            color: COLORS.text,
            maxTicksLimit: 6,
            font: { family: "DM Mono", size: 8 },
            callback: (value) => new Date(Number(value)).toLocaleString([], { timeZone: "UTC", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false }),
          },
          title: { display: true, text: "TIME · UTC", color: COLORS.text, font: { family: "DM Mono", size: 8 }, padding: { top: 8 } },
        },
        y: {
          beginAtZero: true,
          grid: { color: COLORS.grid, drawTicks: false },
          border: { display: false },
          ticks: { color: COLORS.text, maxTicksLimit: 5, font: { family: "DM Mono", size: 8 }, callback: valueFormatter },
        },
      },
      elements: { point: { radius: 0, hoverRadius: 3 }, line: { borderWidth: 2, tension: 0, spanGaps: false } },
    },
  });
  chartInstances.set(id, chart);
}

function renderCharts(samples) {
  makeChart("cpu-chart", [{ label: "CPU", data: chartPoints(samples, "cpu_percent"), borderColor: COLORS.cpu, backgroundColor: "rgba(8,127,112,.08)", fill: true }], (value) => value === null ? "No sample" : `${Number(value).toFixed(1)}%`);
  makeChart("memory-chart", [{ label: "Working set", data: chartPoints(samples, "memory_usage_bytes"), borderColor: COLORS.memory, backgroundColor: "rgba(201,87,69,.07)", fill: true }], formatBytes);
  makeChart("network-chart", [
    { label: "Received", data: chartPoints(samples, "network_rx_bytes_per_second"), borderColor: COLORS.receive, backgroundColor: COLORS.receive, fill: false },
    { label: "Sent", data: chartPoints(samples, "network_tx_bytes_per_second"), borderColor: COLORS.transmit, backgroundColor: COLORS.transmit, fill: false },
  ], (value) => formatBytes(value, true));
  makeChart("block-chart", [
    { label: "Read", data: chartPoints(samples, "block_read_bytes_per_second"), borderColor: COLORS.read, backgroundColor: COLORS.read, fill: false },
    { label: "Write", data: chartPoints(samples, "block_write_bytes_per_second"), borderColor: COLORS.write, backgroundColor: COLORS.write, fill: false },
  ], (value) => formatBytes(value, true));
}

function destroyCharts() {
  for (const chart of chartInstances.values()) chart.destroy();
  chartInstances.clear();
}

async function refreshSelected() {
  if (!state.selectedContainer) return;
  const container = [...state.configured, ...state.archived].find((item) => item.id === state.selectedContainer.id) || state.selectedContainer;
  const needsLiveRefresh = container.configured && (
    container.live_state === "live" || container.live_state === "stale" ||
    state.detail?.sessions?.some((session) => ["running", "stale"].includes(session.status_display)) ||
    ["running", "stale"].includes(state.detail?.status_display)
  );
  if (!needsLiveRefresh) return;
  if (state.selectedSession !== null) await selectSession(state.selectedSession);
  else await loadContainerDetail();
}

async function refreshAll() {
  try {
    await loadCatalog();
    await refreshSelected();
  } catch (error) {
    showError(error.message);
  }
}

document.querySelector("#refresh-button").addEventListener("click", refreshAll);
elements.allTimeButton.addEventListener("click", async () => {
  state.selectedSession = null;
  await loadContainerDetail();
});
refreshAll();
window.setInterval(refreshAll, 3000);