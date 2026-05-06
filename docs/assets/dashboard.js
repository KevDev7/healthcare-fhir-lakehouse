const DATA_URL = "./data/dashboard.json";
const COLORS = ["#2563eb", "#0f766e", "#6d28d9", "#b7791f", "#475569", "#be123c"];

const format = new Intl.NumberFormat("en-US");

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function maxValue(rows) {
  return Math.max(...rows.map((row) => Number(row.value) || 0), 1);
}

function renderMetrics(data) {
  const metrics = [
    ["FHIR resources", data.meta.source_resources, "Bronze input volume"],
    ["Patients", data.meta.patients, "Silver patient rows"],
    ["Encounters", data.meta.encounters, "Silver encounter rows"],
    ["Observations", data.meta.observations, "Silver observation rows"],
    ["Conditions", data.meta.conditions, "Silver condition rows"],
    ["Quality checks", data.quality_checks.length, "0 failed checks"],
  ];

  const grid = document.querySelector("#metric-grid");
  grid.replaceChildren(
    ...metrics.map(([label, value, note]) => {
      const card = el("article", "metric");
      card.append(el("div", "label", label));
      card.append(el("div", "value", format.format(value)));
      card.append(el("div", "note", note));
      return card;
    }),
  );
}

function renderBars(target, rows, options = {}) {
  const root = document.querySelector(target);
  const max = maxValue(rows);
  const color = options.color || "#2563eb";
  const limit = options.limit || rows.length;
  const list = el("div", "bar-list");

  rows.slice(0, limit).forEach((row) => {
    const value = Number(row.value) || 0;
    const item = el("div", "bar-row");
    const label = el("div", "bar-label", row.label || "unknown");
    label.title = row.label || "unknown";

    const track = el("div", "bar-track");
    const fill = el("div", "bar-fill");
    fill.style.width = `${Math.max((value / max) * 100, 2)}%`;
    fill.style.background = color;
    track.append(fill);

    item.append(label, track, el("div", "bar-value", format.format(value)));
    list.append(item);
  });

  root.replaceChildren(list);
}

function renderTableCounts(rows) {
  const root = document.querySelector("#table-counts");
  const panel = el("article", "chart-panel wide");
  const table = el("div", "layer-table");

  rows.forEach((row) => {
    const item = el("div", "layer-row");
    item.append(
      el("div", `layer ${row.layer.toLowerCase()}`, row.layer),
      el("div", "check-name", row.table),
      el("div", "bar-value", format.format(row.rows)),
    );
    table.append(item);
  });

  panel.append(table);
  root.replaceChildren(panel);
}

function seriesFrom(rows) {
  const names = [...new Set(rows.map((row) => row.measurement_name))].slice(0, 6);
  return names.map((name, index) => ({
    name,
    color: COLORS[index % COLORS.length],
    values: rows.filter((row) => row.measurement_name === name),
  }));
}

function renderTrend(target, rows) {
  const root = document.querySelector(target);
  const wrap = el("div", "trend-wrap");
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", "trend-svg");
  svg.setAttribute("viewBox", "0 0 900 330");

  const series = seriesFrom(rows);
  const allValues = rows.map((row) => Number(row.avg_value)).filter(Number.isFinite);
  const yMin = Math.min(...allValues);
  const yMax = Math.max(...allValues);
  const xMin = 0;
  const xMax = 7;
  const left = 60;
  const right = 720;
  const top = 30;
  const bottom = 280;

  function x(day) {
    return left + ((Number(day) - xMin) / (xMax - xMin || 1)) * (right - left);
  }

  function y(value) {
    return bottom - ((Number(value) - yMin) / (yMax - yMin || 1)) * (bottom - top);
  }

  for (let day = 0; day <= 7; day += 1) {
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", x(day));
    text.setAttribute("y", 310);
    text.setAttribute("text-anchor", "middle");
    text.setAttribute("class", "trend-label");
    text.textContent = `D${day}`;
    svg.append(text);
  }

  series.forEach((line, index) => {
    const points = line.values
      .map((row) => `${x(row.event_day_index)},${y(row.avg_value)}`)
      .join(" ");
    const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    polyline.setAttribute("points", points);
    polyline.setAttribute("class", "trend-line");
    polyline.setAttribute("stroke", line.color);
    svg.append(polyline);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", 742);
    label.setAttribute("y", 44 + index * 26);
    label.setAttribute("class", "trend-label");
    label.setAttribute("fill", line.color);
    label.textContent = line.name;
    svg.append(label);
  });

  wrap.append(svg);
  root.replaceChildren(wrap);
}

function renderQuality(data) {
  const relationship = data.relationship_audit;
  const auditRows = [
    ["Patient rows", relationship.patient_rows],
    ["Encounter rows", relationship.encounter_rows],
    ["Observation rows", relationship.observation_rows],
    ["Condition rows", relationship.condition_rows],
    ["Observation orphan patients", relationship.observation_orphan_patient_id],
    ["Observation orphan encounters", relationship.observation_orphan_encounter_id],
    ["Condition orphan patients", relationship.condition_orphan_patient_id],
    ["Condition orphan encounters", relationship.condition_orphan_encounter_id],
  ];

  const auditList = el("div", "audit-list");
  auditRows.forEach(([label, value]) => {
    const item = el("div", "audit-item");
    item.append(el("span", "audit-name", label), el("span", "bar-value", format.format(value)));
    auditList.append(item);
  });
  document.querySelector("#relationship-summary").replaceChildren(auditList);

  const checks = el("div", "check-list");
  data.quality_checks.forEach((check) => {
    const item = el("div", "check-item");
    item.append(
      el("span", "check-name", check.name.replaceAll("_", " ")),
      el("span", `check-status ${check.status}`, check.status),
    );
    checks.append(item);
  });
  document.querySelector("#quality-checks").replaceChildren(checks);
}

function wireTabs() {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((node) => node.classList.remove("active"));
      document.querySelectorAll(".view").forEach((node) => node.classList.remove("active"));
      tab.classList.add("active");
      document.querySelector(`#${tab.dataset.view}`).classList.add("active");
    });
  });
}

async function main() {
  const response = await fetch(DATA_URL);
  const data = await response.json();

  document.querySelector("#dataset-title").textContent =
    `${data.meta.dataset} ${data.meta.dataset_version}`;
  document.querySelector("#quality-status").textContent =
    `${data.meta.failed_checks} failed checks`;
  document.querySelector("#cloud-status").textContent = data.meta.cloud_status;

  renderMetrics(data);
  renderTableCounts(data.table_counts);
  renderBars("#encounter-class-chart", data.encounters.by_class, { color: "#2563eb" });
  renderBars("#encounter-status-chart", data.encounters.by_status, { color: "#0f766e" });
  renderBars("#los-chart", data.encounters.length_of_stay_bins, { color: "#6d28d9" });
  renderBars("#observation-load-chart", data.encounters.observation_load, {
    color: "#b7791f",
  });
  renderBars("#condition-chart", data.conditions.top, { color: "#2563eb" });
  renderBars("#vitals-volume-chart", data.measurements.vitals_volume, {
    color: "#0f766e",
  });
  renderBars("#labs-volume-chart", data.measurements.labs_volume, {
    color: "#6d28d9",
  });
  renderTrend("#vitals-trend-chart", data.measurements.vitals_trend);
  renderTrend("#labs-trend-chart", data.measurements.labs_trend);
  renderQuality(data);
  wireTabs();
}

main();
