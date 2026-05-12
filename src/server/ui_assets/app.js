// SPDX-License-Identifier: Apache-2.0

const views = [
  { id: "dashboard", label: "Dashboard", title: "Dashboard", copy: "Operational overview for local S3Lab state." },
  { id: "requests", label: "Requests", title: "Requests", copy: "Recent local S3 API traffic captured by the trace store." },
  { id: "request-detail", label: "Request detail", title: "Request detail", copy: "Trace events for one local request." },
  { id: "buckets", label: "Buckets", title: "Buckets", copy: "Buckets known to the local storage backend." },
  { id: "objects", label: "Objects", title: "Objects", copy: "Objects listed from the selected bucket." },
  { id: "multipart", label: "Multipart uploads", title: "Multipart uploads", copy: "In-progress multipart upload state from local storage." },
  { id: "snapshots", label: "Snapshots", title: "Snapshots", copy: "Saved local storage snapshots." },
  { id: "replay", label: "Replay sessions", title: "Replay sessions", copy: "Replay support status for this local build." },
  { id: "failure", label: "Failure rules", title: "Failure rules", copy: "Failure injection support status for this local build." },
  { id: "compatibility", label: "Compatibility matrix", title: "Compatibility matrix", copy: "Evidence-oriented compatibility status for local S3 workflows." },
];

const state = {
  view: location.hash.replace("#", "") || "dashboard",
  requests: [],
  buckets: [],
  objects: [],
  uploads: [],
  snapshots: [],
  selectedRequestId: null,
  selectedBucket: null,
};

const navigation = document.querySelector("#navigation");
const content = document.querySelector("#content");
const statusLine = document.querySelector("#status-line");
const title = document.querySelector("#view-title");
const copy = document.querySelector("#view-copy");
const refresh = document.querySelector("#refresh");

function initialize() {
  navigation.innerHTML = views.map((view) => (
    `<button class="nav-item" type="button" data-view="${view.id}">${escapeHtml(view.label)}</button>`
  )).join("");
  navigation.addEventListener("click", (event) => {
    const button = event.target.closest("[data-view]");
    if (!button) {
      return;
    }
    setView(button.dataset.view);
  });
  refresh.addEventListener("click", loadData);
  window.addEventListener("hashchange", () => setView(location.hash.replace("#", "") || "dashboard", false));
  setView(state.view, false);
  loadData();
}

async function loadData() {
  setStatus("Loading local inspector data...");
  try {
    const [requests, buckets, uploads, snapshots] = await Promise.all([
      fetchJson("/api/requests"),
      fetchJson("/api/buckets"),
      fetchJson("/api/multipart-uploads"),
      fetchJson("/api/snapshots"),
    ]);
    state.requests = requests.requests || [];
    state.buckets = buckets.buckets || [];
    state.uploads = uploads.multipart_uploads || [];
    state.snapshots = snapshots.snapshots || [];
    if (!state.selectedBucket && state.buckets.length > 0) {
      state.selectedBucket = state.buckets[0].name;
    }
    await loadObjects();
    setStatus("Loaded from local inspector APIs.");
  } catch (error) {
    setStatus(`Unable to load local inspector data: ${error.message}`, true);
  }
  render();
}

async function loadObjects() {
  if (!state.selectedBucket) {
    state.objects = [];
    return;
  }
  const objects = await fetchJson(`/api/buckets/${encodeURIComponent(state.selectedBucket)}/objects`);
  state.objects = objects.objects || [];
}

async function fetchJson(path) {
  const response = await fetch(path, { headers: { accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json();
}

function setView(viewId, updateHash = true) {
  state.view = views.some((view) => view.id === viewId) ? viewId : "dashboard";
  if (updateHash) {
    location.hash = state.view;
  }
  render();
}

function render() {
  const current = views.find((view) => view.id === state.view) || views[0];
  title.textContent = current.title;
  copy.textContent = current.copy;
  for (const button of navigation.querySelectorAll("[data-view]")) {
    button.classList.toggle("active", button.dataset.view === state.view);
  }

  const renderers = {
    dashboard: renderDashboard,
    requests: renderRequests,
    "request-detail": renderRequestDetail,
    buckets: renderBuckets,
    objects: renderObjects,
    multipart: renderMultipart,
    snapshots: renderSnapshots,
    replay: () => renderPlaceholder("Replay sessions are not implemented in this build.", "No replay sessions are available yet. Future support should show imported traces, replay runs, diffs, and reproducible commands."),
    failure: () => renderPlaceholder("Failure rules are not implemented in this build.", "No failure injection rules are active. Future support should show seeded rules, matching scope, and deterministic activation evidence."),
    compatibility: renderCompatibility,
  };
  content.innerHTML = renderers[state.view]();
  bindContentActions();
}

function renderDashboard() {
  return `
    <section class="summary-grid" aria-label="Local state summary">
      ${metric("Requests", state.requests.length)}
      ${metric("Buckets", state.buckets.length)}
      ${metric("Objects", state.objects.length)}
      ${metric("Multipart uploads", state.uploads.length)}
    </section>
    ${panel("Recent requests", requestTable(state.requests.slice(-8)))}
    ${panel("Storage", table(["Bucket", "Objects"], state.buckets.map((bucket) => [
      bucketButton(bucket.name),
      state.selectedBucket === bucket.name ? String(state.objects.length) : "Select to load",
    ])))}
  `;
}

function renderRequests() {
  return panel("Request trace summaries", requestTable(state.requests));
}

function renderRequestDetail() {
  const options = state.requests.map((request) => (
    `<option value="${escapeAttr(request.request_id)}"${request.request_id === state.selectedRequestId ? " selected" : ""}>${escapeHtml(request.request_id)}</option>`
  )).join("");
  return `
    <section class="panel">
      <div class="panel-header">
        <h2>Trace events</h2>
        <select id="request-select" aria-label="Request">${options}</select>
      </div>
      <div id="request-detail-body" class="placeholder">Select a request to inspect trace events.</div>
    </section>
  `;
}

function renderBuckets() {
  return panel("Buckets", table(["Name", "Objects"], state.buckets.map((bucket) => [
    bucketButton(bucket.name),
    state.selectedBucket === bucket.name ? String(state.objects.length) : "Select to load",
  ])));
}

function renderObjects() {
  const options = state.buckets.map((bucket) => (
    `<option value="${escapeAttr(bucket.name)}"${bucket.name === state.selectedBucket ? " selected" : ""}>${escapeHtml(bucket.name)}</option>`
  )).join("");
  const rows = state.objects.map((object) => [
    object.key,
    object.content_length,
    object.content_type || "",
    object.etag,
    object.last_modified,
  ]);
  return `
    <section class="panel">
      <div class="panel-header">
        <h2>Objects</h2>
        <select id="bucket-select" aria-label="Bucket">${options}</select>
      </div>
      ${table(["Key", "Bytes", "Type", "ETag", "Last modified"], rows)}
    </section>
  `;
}

function renderMultipart() {
  const rows = state.uploads.map((upload) => [
    upload.bucket,
    upload.key,
    upload.upload_id,
    upload.part_count,
    upload.initiated,
  ]);
  return panel("Multipart uploads", table(["Bucket", "Key", "Upload ID", "Parts", "Initiated"], rows));
}

function renderSnapshots() {
  return panel("Snapshots", table(["Name"], state.snapshots.map((snapshot) => [snapshot.name])));
}

function renderCompatibility() {
  return panel("Compatibility evidence", table(["Area", "Status", "Evidence"], [
    ["Bucket lifecycle", "Local API implemented", "Covered by local S3 API routes and tests."],
    ["Object lifecycle", "Local API implemented", "PUT, GET, HEAD, DELETE, and ListObjectsV2 are exposed through the local server."],
    ["Multipart uploads", "Local API implemented", "Create, upload part, list parts, complete, and abort paths are exposed through local APIs."],
    ["Replay", "Not implemented", "No replay API is present in this build."],
    ["Failure injection", "Not implemented", "No failure-rule API is present in this build."],
  ]));
}

function bindContentActions() {
  for (const button of content.querySelectorAll("[data-request-id]")) {
    button.addEventListener("click", async () => {
      state.selectedRequestId = button.dataset.requestId;
      setView("request-detail");
      await renderSelectedRequest();
    });
  }
  for (const button of content.querySelectorAll("[data-bucket]")) {
    button.addEventListener("click", async () => {
      state.selectedBucket = button.dataset.bucket;
      await loadObjects();
      setView("objects");
      setStatus(`Loaded objects for ${state.selectedBucket}.`);
    });
  }
  const bucketSelect = content.querySelector("#bucket-select");
  if (bucketSelect) {
    bucketSelect.addEventListener("change", async () => {
      state.selectedBucket = bucketSelect.value;
      await loadObjects();
      render();
    });
  }
  const requestSelect = content.querySelector("#request-select");
  if (requestSelect) {
    requestSelect.addEventListener("change", async () => {
      state.selectedRequestId = requestSelect.value;
      await renderSelectedRequest();
    });
    if (!state.selectedRequestId && requestSelect.value) {
      state.selectedRequestId = requestSelect.value;
    }
    renderSelectedRequest();
  }
}

async function renderSelectedRequest() {
  const target = document.querySelector("#request-detail-body");
  if (!target || !state.selectedRequestId) {
    return;
  }
  target.textContent = "Loading trace events...";
  try {
    const detail = await fetchJson(`/api/requests/${encodeURIComponent(state.selectedRequestId)}`);
    target.innerHTML = `<pre>${escapeHtml(JSON.stringify(detail, null, 2))}</pre>`;
  } catch (error) {
    target.textContent = `Unable to load request detail: ${error.message}`;
  }
}

function requestTable(requests) {
  return table(["Request ID", "Method", "Path", "Operation", "Status", "Events"], requests.map((request) => [
    requestButton(request.request_id),
    request.method || "",
    request.path || "",
    request.operation || "",
    request.status_code || "",
    request.event_count,
  ]));
}

function metric(label, value) {
  return `<div class="metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(String(value))}</strong></div>`;
}

function panel(label, body) {
  return `<section class="panel"><div class="panel-header"><h2>${escapeHtml(label)}</h2></div>${body}</section>`;
}

function table(headers, rows) {
  if (!rows.length) {
    return `<div class="placeholder empty">No local records.</div>`;
  }
  return `
    <div class="table-wrap">
      <table>
        <thead><tr>${headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("")}</tr></thead>
        <tbody>${rows.map((row) => `<tr>${row.map((cell) => `<td>${tableCell(cell)}</td>`).join("")}</tr>`).join("")}</tbody>
      </table>
    </div>
  `;
}

function renderPlaceholder(heading, body) {
  return panel(heading, `<div class="placeholder"><p>${escapeHtml(body)}</p></div>`);
}

function requestButton(requestId) {
  return trustedHtml(`<button class="link-button" type="button" data-request-id="${escapeAttr(requestId)}">${escapeHtml(requestId)}</button>`);
}

function tableCell(value) {
  if (value && typeof value === "object" && Object.hasOwn(value, "trustedHtml")) {
    return value.trustedHtml;
  }
  return escapeHtml(String(value));
}

function bucketButton(bucket) {
  return trustedHtml(`<button class="link-button" type="button" data-bucket="${escapeAttr(bucket)}">${escapeHtml(bucket)}</button>`);
}

function trustedHtml(markup) {
  return { trustedHtml: markup };
}

function setStatus(message, error = false) {
  statusLine.textContent = message;
  statusLine.classList.toggle("error", error);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

initialize();
