const DATA_URL = "./data/dashboard.json";
const COLORS = ["#2563eb", "#0f766e", "#6d28d9", "#b7791f", "#475569", "#be123c"];

const format = new Intl.NumberFormat("en-US");
const DISPLAY_LABELS = {
  "AGAINST ADVICE": "Against advice",
  "CHRONIC/LONG TERM ACUTE CARE": "Chronic/LTAC",
  "HOME HEALTH CARE": "Home health care",
  "LEFT WITHOUT BEING SEEN": "Left without being seen",
  "Non Invasive Blood Pressure diastolic": "BP diastolic",
  "Non Invasive Blood Pressure mean": "BP mean",
  "Non Invasive Blood Pressure systolic": "BP systolic",
  "O2 saturation pulseoxymetry": "O2 saturation",
  "SKILLED NURSING FACILITY": "Skilled nursing facility",
  "Temperature Fahrenheit": "Temperature",
  unknown: "Unknown",
};

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function maxValue(rows) {
  return Math.max(...rows.map((row) => Number(row.value) || 0), 1);
}

function displayLabel(label) {
  return DISPLAY_LABELS[label] || label || "unknown";
}

function renderMetrics(data) {
  const metrics = [
    ["FHIR resources", data.meta.source_resources, "Raw EHR-style input"],
    ["Patients", data.meta.patients, "Normalized patient records"],
    ["Encounters", data.meta.encounters, "Normalized encounter records"],
    ["Observations", data.meta.observations, "Normalized clinical observations"],
    ["Conditions", data.meta.conditions, "Diagnosis and condition records"],
    [
      "Medication events",
      data.meta.medication_events,
      "Orders, administrations, dispenses, statements",
    ],
    ["Procedures", data.meta.procedures, "Normalized procedure records"],
    [
      "Quality checks",
      data.quality_checks.length,
      `${data.meta.failed_checks} failed, ${data.meta.warning_checks} warning`,
    ],
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
    const labelWrap = el("div", "bar-label-wrap");
    const label = el("div", "bar-label", displayLabel(row.label));
    label.title = row.label || "unknown";
    labelWrap.append(label);
    if (options.detail) {
      labelWrap.append(el("div", "bar-detail", options.detail(row)));
    }

    const track = el("div", "bar-track");
    const fill = el("div", "bar-fill");
    fill.style.width = `${Math.max((value / max) * 100, 2)}%`;
    fill.style.background = color;
    track.append(fill);

    item.append(labelWrap, track, el("div", "bar-value", format.format(value)));
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
  const allValues = rows.map((row) => Number(row.index_value)).filter(Number.isFinite);
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

  [yMin, 100, yMax].forEach((tick) => {
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", 48);
    text.setAttribute("y", y(tick) + 4);
    text.setAttribute("text-anchor", "end");
    text.setAttribute("class", "trend-label");
    text.textContent = Math.round(tick);
    svg.append(text);
  });

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
      .map((row) => `${x(row.event_day_index)},${y(row.index_value)}`)
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
    label.textContent = displayLabel(line.name);
    svg.append(label);
  });

  const axisLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
  axisLabel.setAttribute("x", left);
  axisLabel.setAttribute("y", 18);
  axisLabel.setAttribute("class", "trend-label");
  axisLabel.textContent = "Index value, first observed day = 100";
  svg.append(axisLabel);

  wrap.append(svg);
  root.replaceChildren(wrap);
}

function renderQuality(data) {
  const relationship = data.relationship_audit;
  const orphanReferences = Object.entries(relationship)
    .filter(([key, value]) => key.includes("_orphan_") && typeof value === "number")
    .reduce((total, [, value]) => total + value, 0);

  const auditHeroRows = [
    ["Orphan references", orphanReferences, "clinical references"],
    [
      "Missing encounter context",
      relationship.observation_missing_encounter_id,
      "Observation rows",
    ],
    [
      "Quality result",
      `${data.meta.failed_checks}/${data.meta.warning_checks}`,
      "failed / warning",
    ],
  ];

  const auditHero = document.querySelector("#audit-hero");
  auditHero.replaceChildren(
    ...auditHeroRows.map(([label, value, note]) => {
      const card = el("div", "audit-hero-card");
      card.append(el("div", "audit-hero-label", label));
      card.append(
        el(
          "div",
          "audit-hero-value",
          typeof value === "number" ? format.format(value) : value,
        ),
      );
      card.append(el("div", "audit-hero-note", note));
      return card;
    }),
  );

  const auditRows = [
    ["Patients", relationship.patient_rows],
    ["Encounters", relationship.encounter_rows],
    ["Observations", relationship.observation_rows],
    ["Conditions", relationship.condition_rows],
    ["Medication requests", relationship.medication_request_rows],
    ["Medication administrations", relationship.medication_administration_rows],
    ["Medication dispenses", relationship.medication_dispense_rows],
    ["Medication statements", relationship.medication_statement_rows],
    ["Procedures", relationship.procedure_rows],
    ["Observation orphan patient refs", relationship.observation_orphan_patient_id],
    ["Observation orphan encounter refs", relationship.observation_orphan_encounter_id],
    ["Condition orphan patient refs", relationship.condition_orphan_patient_id],
    ["Condition orphan encounter refs", relationship.condition_orphan_encounter_id],
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

function showLoadError(error) {
  console.error(error);
  document.querySelector("#load-error").hidden = false;
  document.querySelector("#quality-status").textContent = "Data unavailable";
}

async function main() {
  let data;
  try {
    const response = await fetch(DATA_URL);
    if (!response.ok) {
      throw new Error(`Dashboard data request failed: ${response.status}`);
    }
    data = await response.json();
  } catch (error) {
    showLoadError(error);
    wireTabs();
    return;
  }

  document.querySelector("#dataset-title").textContent =
    `${data.meta.display_dataset} v${data.meta.dataset_version}`;
  const qualityStatus = document.querySelector("#quality-status");
  qualityStatus.textContent =
    `${data.meta.failed_checks} failed, ${data.meta.warning_checks} warning`;
  if (data.meta.warning_checks > 0) {
    qualityStatus.classList.add("warning");
  }
  document.querySelector("#cloud-status").textContent = data.meta.cloud_status;

  renderMetrics(data);
  renderTableCounts(data.table_counts);
  renderBars("#encounter-class-chart", data.encounters.by_class, { color: "#2563eb" });
  renderBars("#discharge-disposition-chart", data.encounters.by_discharge_disposition, {
    color: "#0f766e",
    limit: 8,
  });
  renderBars("#los-chart", data.encounters.length_of_stay_bins, { color: "#6d28d9" });
  renderBars("#observation-load-chart", data.encounters.observation_load, {
    color: "#b7791f",
  });
  renderBars("#condition-chart", data.conditions.top, {
    color: "#2563eb",
    detail: (row) => `${format.format(row.patient_count)} patients · ${format.format(row.encounter_count)} encounters`,
  });
  renderBars("#vitals-volume-chart", data.measurements.vitals_volume, {
    color: "#0f766e",
  });
  renderBars("#labs-volume-chart", data.measurements.labs_volume, {
    color: "#6d28d9",
  });
  renderTrend("#vitals-trend-chart", data.measurements.vitals_trend);
  renderTrend("#labs-trend-chart", data.measurements.labs_trend);
  renderBars("#medication-activity-type-chart", data.medications.by_activity_type, {
    color: "#0f766e",
  });
  renderBars("#medication-fulfillment-chart", data.medications.fulfillment_paths, {
    color: "#6d28d9",
  });
  renderBars("#medication-top-chart", data.medications.top_activity, {
    color: "#2563eb",
    detail: (row) =>
      `${row.activity_type} · ${row.source_system} · ${format.format(row.patient_count)} patients`,
  });
  renderBars("#procedure-chart", data.procedures.top, {
    color: "#b7791f",
    detail: (row) =>
      `${row.source_system} · ${format.format(row.patient_count)} patients · ${format.format(row.encounter_count)} encounters`,
  });
  renderQuality(data);
  wireTabs();
}

main();
