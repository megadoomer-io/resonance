/* Server-backed lineup builder controller (#133).
 *
 * The profile is the system of record: every edit (add/remove/toggle source,
 * parameters, name) persists via PATCH /api/v1/generator-profiles/{id}, guarded
 * by the profile's optimistic version. Enrichment and generation are explicit
 * actions. Pure state logic lives in lineup.core.js (unit-tested under vitest);
 * this module is the DOM + network controller.
 */
import {
  absoluteWindow,
  addEventGroup,
  addManualArtist,
  allArtistIds,
  artistMeta,
  clampSeedCount,
  escapeHtml,
  groupKey,
  hasArtist,
  isEmpty,
  isoDate,
  isWindowValid,
  presetIdForWindow,
  removeArtistFromGroup,
  removeGroup,
  serializeInputReferences,
  serializeRediscovery,
  setEventArtists,
  toggleArtist,
  windowDates,
  windowFromPreset,
} from "./lineup.core.js";

const dataEl = document.getElementById("lineup-data");
const initial = dataEl ? JSON.parse(dataEl.textContent) : { groups: [], version: 0 };
const profileId = window.LINEUP_PROFILE_ID;
const similarAvailable = window.LINEUP_SIMILAR_AVAILABLE === true;

const state = { groups: initial.groups || [], version: initial.version || 0 };
let conflicted = false;

/* ---- rediscovery window mode (#rediscovery-ui) ---- */
const isRediscovery = window.LINEUP_GENERATOR_TYPE === "rediscovery";
// Window recipe + seed count. Hydrated from the stored listening_range source, or
// defaulted to the rolling "Last 2 Weeks" preset for a fresh draft.
let windowState =
  (initial.listening_range && initial.listening_range.window) || {
    kind: "relative",
    lookback_days: 14,
  };
let seedCount =
  (initial.listening_range && initial.listening_range.seed_artist_count) || 20;
// Preview drives the Generate-disable guard: an invalid window or a window with
// no listening data cannot generate. `previewDirty` gates refreshing the preview
// to persisted-window changes only (the endpoint reads the stored window).
let windowValid = isWindowValid(windowState);
let previewEmpty = false;
let previewDirty = false;

/* The stored input_references for the current mode: rediscovery leads with the
 * window source (preserving enrich discoveries); concert_prep uses the lineup. */
function currentInputRefs() {
  if (isRediscovery) {
    return serializeRediscovery(windowState, seedCount, state.groups);
  }
  return serializeInputReferences(state.groups);
}

const groupsEl = document.getElementById("lineup-groups");
const emptyEl = document.getElementById("lineup-empty");
const autosaveEl = document.getElementById("autosave");
const autosaveLabel = autosaveEl ? autosaveEl.querySelector(".autosave-label") : null;
const conflictEl = document.getElementById("lineup-conflict");

/* ---- autosave indicator ---- */

function setSaving(saving) {
  if (!autosaveEl) return;
  autosaveEl.hidden = false;
  autosaveEl.classList.toggle("saving", saving);
  if (autosaveLabel) autosaveLabel.textContent = saving ? "Saving…" : "Saved";
}

function showConflict() {
  conflicted = true;
  if (conflictEl) conflictEl.hidden = false;
  if (autosaveEl) autosaveEl.hidden = true;
}

/* ---- rendering ---- */

function rowHtml(artist, key, opts) {
  const removable = opts.removable;
  const related = opts.related;
  const findable = opts.findable && similarAvailable;
  const excluded = artist.included ? "" : " excluded";
  const checked = artist.included ? " checked" : "";
  const meta = artist.meta ? '<span class="m">' + escapeHtml(artist.meta) + "</span>" : "";
  // Double-pill cleanup: an excluded row shows only the EXCLUDED pill.
  let tag = "";
  if (!artist.included) {
    tag = '<span class="extag">excluded</span>';
  } else if (related) {
    tag = '<span class="ptag">related</span>';
  }
  const find = findable
    ? '<button type="button" class="afind" data-find-similar="' +
      escapeHtml(artist.id) +
      '" title="Find similar artists">+ similar</button>'
    : "";
  const remove = removable
    ? '<button type="button" class="aremove" data-remove-artist="' +
      escapeHtml(artist.id) +
      '" data-key="' +
      escapeHtml(key) +
      '" title="Remove" aria-label="Remove ' +
      escapeHtml(artist.name) +
      '">&times;</button>'
    : "";
  return (
    '<div class="artist-row' +
    excluded +
    '">' +
    '<input type="checkbox" data-artist-toggle data-key="' +
    escapeHtml(key) +
    '" data-artist-id="' +
    escapeHtml(artist.id) +
    '"' +
    checked +
    ' aria-label="Include ' +
    escapeHtml(artist.name) +
    '">' +
    '<span class="aname"><span class="n">' +
    escapeHtml(artist.name) +
    "</span>" +
    meta +
    "</span>" +
    tag +
    find +
    remove +
    "</div>"
  );
}

function eventGroupHtml(g) {
  const key = groupKey(g);
  let rows = "";
  if (g.artists.length === 0) {
    rows =
      '<div class="artist-row"><span class="aname"><span class="m">Loading lineup…</span></span></div>';
  } else {
    for (const a of g.artists) {
      rows += rowHtml(a, key, { removable: false, related: false, findable: true });
    }
  }
  return (
    '<div class="lineup-group">' +
    '<div class="lineup-group-head">' +
    '<span><span class="gtitle">' +
    escapeHtml(g.title) +
    '</span> <span class="gsub">' +
    escapeHtml(g.sub || "") +
    "</span></span>" +
    '<div class="ghead-actions">' +
    '<button type="button" class="gremove" data-remove-group="' +
    escapeHtml(key) +
    '" title="Remove event" aria-label="Remove event ' +
    escapeHtml(g.title) +
    '">&times;</button>' +
    "</div></div>" +
    rows +
    "</div>"
  );
}

function manualGroupHtml(g) {
  const key = groupKey(g);
  let rows = "";
  for (const a of g.artists) {
    rows += rowHtml(a, key, { removable: true, related: false, findable: true });
  }
  return (
    '<div class="lineup-group">' +
    '<div class="lineup-group-head"><span class="gtitle">' +
    escapeHtml(g.title) +
    "</span></div>" +
    rows +
    "</div>"
  );
}

function relatedGroupHtml(g) {
  const key = groupKey(g);
  const count = g.artists.length;
  let rows = "";
  for (const a of g.artists) {
    rows += rowHtml(a, key, { removable: true, related: true, findable: false });
  }
  const replace = similarAvailable
    ? '<button type="button" class="gaction" data-replace-scope="' +
      escapeHtml(g.scope) +
      '" title="Re-run this set">↻ replace</button>'
    : "";
  // Related groups render collapsed by default (core-first hierarchy); native
  // <details> handles the toggle accessibly. The data-group-key lets render()
  // preserve each group's open/closed state across re-renders (so toggling a
  // checkbox inside a group doesn't snap it shut).
  return (
    '<details class="lineup-group related" data-group-key="' +
    escapeHtml(key) +
    '">' +
    '<summary class="lineup-group-head">' +
    '<span><span class="gtitle">' +
    escapeHtml(g.title) +
    '</span> <span class="gsub">· ' +
    count +
    " added</span></span>" +
    '<div class="ghead-actions">' +
    replace +
    '<button type="button" class="gremove" data-remove-group="' +
    escapeHtml(key) +
    '" title="Remove these" aria-label="Remove ' +
    escapeHtml(g.title) +
    '">&times;</button>' +
    "</div></summary>" +
    rows +
    "</details>"
  );
}

function render() {
  // Preserve which related groups are expanded so a re-render (e.g. after a
  // checkbox toggle) doesn't collapse them back to their default-closed state.
  const openKeys = new Set();
  for (const d of groupsEl.querySelectorAll("details[data-group-key]")) {
    if (d.open) openKeys.add(d.getAttribute("data-group-key"));
  }

  let html = "";
  // Core (event + manual) first, then related groups (collapsed).
  for (const g of state.groups) {
    if (g.kind === "event") html += eventGroupHtml(g);
  }
  for (const g of state.groups) {
    if (g.kind === "manual") html += manualGroupHtml(g);
  }
  for (const g of state.groups) {
    if (g.kind === "related") html += relatedGroupHtml(g);
  }
  groupsEl.innerHTML = html;

  for (const d of groupsEl.querySelectorAll("details[data-group-key]")) {
    if (openKeys.has(d.getAttribute("data-group-key"))) d.open = true;
  }
  if (emptyEl) emptyEl.hidden = !isEmpty(state.groups);
  if (isRediscovery) updateNewStreamHint();
}

/* Show the "all deep cuts" hint when a rediscovery pool has no enrich-discovered
 * new artists yet (a silent empty new stream makes new_ratio a no-op). Hint, not
 * a block — all-deep-cuts is a legitimate output. */
function updateNewStreamHint() {
  const hint = document.getElementById("new-stream-hint");
  if (!hint) return;
  const hasNew = state.groups.some(
    (g) => g.kind === "related" && g.artists.length > 0
  );
  hint.hidden = hasNew;
}

/* ---- persistence (PATCH) ---- */

let saveTimer = null;
let pending = false;

function scheduleSave() {
  if (conflicted) return;
  pending = true;
  setSaving(true);
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(flushSave, 400);
}

async function flushSave(extra) {
  if (conflicted) return null;
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  pending = false;
  const body = Object.assign(
    {
      input_references: currentInputRefs(),
      expected_version: state.version,
    },
    extra || {}
  );
  setSaving(true);
  let resp;
  try {
    resp = await fetch("/api/v1/generator-profiles/" + profileId, {
      method: "PATCH",
      credentials: "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    setSaving(false);
    return null;
  }
  if (resp.status === 409) {
    showConflict();
    return null;
  }
  if (!resp.ok) {
    setSaving(false);
    return null;
  }
  const data = await resp.json();
  if (typeof data.version === "number") state.version = data.version;
  setSaving(false);
  // The seed preview reads the PERSISTED window, so refresh it only after a
  // window/seed-count change has committed (previewDirty), never on a name/param
  // save.
  if (isRediscovery && previewDirty) {
    previewDirty = false;
    refreshSeedPreview();
  }
  return data;
}

/* Flush any pending save and resolve when persisted (used before enrich/generate). */
async function ensureSaved() {
  if (pending || saveTimer) return flushSave();
  return null;
}

/* ---- enrich ---- */

async function runEnrich(scope, btn) {
  if (conflicted) return;
  if (btn) btn.disabled = true;
  await ensureSaved();
  if (conflicted) {
    if (btn) btn.disabled = false;
    return;
  }
  const nInput = document.getElementById("add-related-n");
  const n = nInput ? parseInt(nInput.value, 10) || 5 : 5;
  const seed = scope === "lineup" ? "lineup" : [scope];
  let resp;
  try {
    resp = await fetch("/api/v1/generator-profiles/" + profileId + "/enrich", {
      method: "POST",
      credentials: "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ seed_artist_ids: seed, n: n }),
    });
  } catch (e) {
    if (btn) btn.disabled = false;
    return;
  }
  if (resp.status === 409) {
    showEnrichBanner("A generation or enrichment is already running. Try again shortly.");
    if (btn) btn.disabled = false;
    return;
  }
  if (!resp.ok) {
    if (btn) btn.disabled = false;
    return;
  }
  const data = await resp.json();
  showEnrichBanner('<span class="spinner"></span> Finding related artists…');
  pollEnrich(data.task_id);
}

function showEnrichBanner(html) {
  let el = document.getElementById("enrich-banner");
  if (!el) {
    el = document.createElement("div");
    el.id = "enrich-banner";
    el.className = "enrich-status";
    groupsEl.parentNode.insertBefore(el, groupsEl);
  }
  el.innerHTML = html;
  el.hidden = false;
}

function clearEnrichBanner() {
  const el = document.getElementById("enrich-banner");
  if (el) el.hidden = true;
}

async function pollEnrich(taskId) {
  let resp;
  try {
    resp = await fetch("/playlists/task-status/" + taskId, {
      credentials: "same-origin",
    });
  } catch (e) {
    return;
  }
  if (!resp.ok) return;
  const data = await resp.json();
  if (data.status === "completed") {
    const result = data.result || {};
    const found = result.found != null ? result.found : 0;
    if (result.message) {
      showEnrichBanner("Enrichment: " + escapeHtml(result.message));
      setTimeout(clearEnrichBanner, 4000);
      return;
    }
    showEnrichBanner(
      '<span class="count">Added ' + found + " related artists.</span> Reloading…"
    );
    // Re-hydrate from the server so the new related group(s) render.
    window.location.reload();
    return;
  }
  if (data.status === "failed") {
    showEnrichBanner("Enrichment failed. " + escapeHtml(data.error || ""));
    return;
  }
  const cur = data.progress_current || 0;
  const total = data.progress_total;
  const count = total ? "found " + cur + " of " + total : "found " + cur;
  showEnrichBanner(
    '<span class="spinner"></span> Finding related artists… <span class="count">' +
      count +
      "</span>"
  );
  setTimeout(() => pollEnrich(taskId), 1500);
}

/* ---- artist picker ---- */

const searchInput = document.getElementById("artist-search");
const resultsEl = document.getElementById("artist-results");
let searchTimer = null;

function renderResults(items) {
  if (!items.length) {
    resultsEl.innerHTML = '<div class="picker-empty">No matching artists in your library.</div>';
    resultsEl.hidden = false;
    return;
  }
  let html = "";
  for (const a of items) {
    const meta = artistMeta(a);
    const disabled = hasArtist(state.groups, a.id);
    const tag = a.in_library
      ? '<span class="ptag lib">in library</span>'
      : '<span class="ptag">no tracks yet</span>';
    html +=
      '<div class="picker-item" data-add-artist="' +
      escapeHtml(a.id) +
      '" data-name="' +
      escapeHtml(a.name) +
      '" data-meta="' +
      escapeHtml(meta) +
      '"' +
      (disabled ? ' style="opacity:.45;pointer-events:none;"' : "") +
      "><span><span class=\"pname\">" +
      escapeHtml(a.name) +
      "</span>" +
      (meta ? ' <span class="pmeta">— ' + escapeHtml(meta) + "</span>" : "") +
      "</span>" +
      (disabled ? '<span class="ptag">added</span>' : tag) +
      "</div>";
  }
  resultsEl.innerHTML = html;
  resultsEl.hidden = false;
}

function onSearch() {
  const q = searchInput.value.trim();
  if (searchTimer) clearTimeout(searchTimer);
  if (q.length < 2) {
    resultsEl.hidden = true;
    resultsEl.innerHTML = "";
    return;
  }
  // Seed genre-affinity ranking with the artists already in the builder, so an
  // ambiguous name resolves toward the pool's genre (#136). Dedup so an artist in
  // two groups doesn't double-weight the seed profile or bloat the URL.
  const seedParams = [...new Set(allArtistIds(state.groups))]
    .map((id) => "&seed_artist_ids=" + encodeURIComponent(id))
    .join("");
  searchTimer = setTimeout(() => {
    fetch(
      "/api/v1/artists/search?q=" + encodeURIComponent(q) + "&limit=8" + seedParams,
      {
        credentials: "same-origin",
      },
    )
      .then((r) => (r.ok ? r.json() : { items: [] }))
      .then((data) => renderResults(data.items || []))
      .catch(() => {
        resultsEl.hidden = true;
      });
  }, 250);
}

/* ---- event add ---- */

const eventSelect = document.getElementById("add-event-select");

function addEvent(id, title, sub) {
  state.groups = addEventGroup(state.groups, id, title, sub);
  render();
  scheduleSave();
  fetch("/api/v1/events/" + encodeURIComponent(id) + "/lineup", {
    credentials: "same-origin",
  })
    .then((r) => (r.ok ? r.json() : { artists: [] }))
    .then((data) => {
      const rows = (data.artists || []).map((a) => ({
        id: a.id,
        name: a.name,
        meta: artistMeta(a),
        included: true,
      }));
      state.groups = setEventArtists(state.groups, id, rows);
      render();
      scheduleSave();
    })
    .catch(() => render());
}

/* ---- wiring ---- */

if (eventSelect) {
  eventSelect.addEventListener("change", () => {
    const opt = eventSelect.options[eventSelect.selectedIndex];
    const id = eventSelect.value;
    if (!id) return;
    const title = opt.getAttribute("data-title") || "Event";
    const date = opt.getAttribute("data-date") || "";
    const venue = opt.getAttribute("data-venue") || "";
    const sub = "· " + date + (venue ? " · " + venue : "") + " · live lineup";
    addEvent(id, title, sub);
    maybeAutoName(title, venue);
    eventSelect.selectedIndex = 0;
  });
}

/* Auto-fill the playlist name from the first event the user adds, UNLESS they've
 * typed their own. A draft opens named "New Playlist" (empty pool), so without
 * this it would generate as "New Playlist". */
function isDefaultName() {
  if (!nameInput) return false;
  const v = nameInput.value.trim();
  return v === "" || v === "New Playlist" || v === "New Rediscovery";
}

function maybeAutoName(title, venue) {
  if (!nameInput || !isDefaultName()) return;
  const derived = "Concert Prep: " + title + (venue ? " @ " + venue : "");
  nameInput.value = derived;
  flushSave({ name: derived });
}

if (searchInput) {
  searchInput.addEventListener("input", onSearch);
  searchInput.addEventListener("focus", onSearch);
}

if (resultsEl) {
  resultsEl.addEventListener("click", (e) => {
    const item = e.target.closest("[data-add-artist]");
    if (!item) return;
    state.groups = addManualArtist(state.groups, {
      id: item.getAttribute("data-add-artist"),
      name: item.getAttribute("data-name") || "",
      meta: item.getAttribute("data-meta") || "",
    });
    searchInput.value = "";
    resultsEl.hidden = true;
    resultsEl.innerHTML = "";
    render();
    scheduleSave();
  });
}

document.addEventListener("click", (e) => {
  if (resultsEl && !e.target.closest(".picker")) resultsEl.hidden = true;
});

groupsEl.addEventListener("change", (e) => {
  const cb = e.target.closest("[data-artist-toggle]");
  if (!cb) return;
  state.groups = toggleArtist(
    state.groups,
    cb.getAttribute("data-key"),
    cb.getAttribute("data-artist-id"),
    cb.checked
  );
  render();
  scheduleSave();
});

groupsEl.addEventListener("click", (e) => {
  const rmGroup = e.target.closest("[data-remove-group]");
  if (rmGroup) {
    state.groups = removeGroup(state.groups, rmGroup.getAttribute("data-remove-group"));
    render();
    scheduleSave();
    return;
  }
  const rmArtist = e.target.closest("[data-remove-artist]");
  if (rmArtist) {
    state.groups = removeArtistFromGroup(
      state.groups,
      rmArtist.getAttribute("data-key"),
      rmArtist.getAttribute("data-remove-artist")
    );
    render();
    scheduleSave();
    return;
  }
  const find = e.target.closest("[data-find-similar]");
  if (find) {
    runEnrich(find.getAttribute("data-find-similar"), find);
    return;
  }
  const replace = e.target.closest("[data-replace-scope]");
  if (replace) {
    runEnrich(replace.getAttribute("data-replace-scope"), replace);
  }
});

const addRelatedBtn = document.getElementById("add-related-btn");
if (addRelatedBtn) {
  addRelatedBtn.addEventListener("click", () => runEnrich("lineup", addRelatedBtn));
}

/* Parameters + name persist too. */
for (const slider of document.querySelectorAll("[data-param]")) {
  slider.addEventListener("change", () => {
    const params = {};
    for (const s of document.querySelectorAll("[data-param]")) {
      params[s.getAttribute("data-param")] = parseInt(s.value, 10);
    }
    flushSave({ parameter_values: params });
  });
}

const nameInput = document.getElementById("name");
if (nameInput) {
  nameInput.addEventListener("change", () => {
    flushSave({ name: nameInput.value.trim() });
  });
}

const conflictReload = document.getElementById("conflict-reload");
if (conflictReload) {
  conflictReload.addEventListener("click", () => window.location.reload());
}

/* ---- rediscovery window controls (#rediscovery-ui) ---- */

const seedPreviewEl = document.getElementById("seed-preview");
const customWindowEl = document.getElementById("custom-window");
const windowStartEl = document.getElementById("window-start");
const windowEndEl = document.getElementById("window-end");
const windowErrorEl = document.getElementById("window-error");
const seedCountEl = document.getElementById("seed-count");

/* Generate is disabled while the window is invalid (custom end < start) or the
 * window resolves to no listening data — both would crash or no-op generation.
 * Looks the button up lazily rather than closing over the `generateBtn` const:
 * this runs during module init (and on pill clicks) BEFORE that const is
 * evaluated later in the file, so a closure reference would hit the temporal dead
 * zone and abort the whole script — leaving the seed preview blank. */
function updateGenerateEnabled() {
  if (!isRediscovery) return;
  const btn = document.getElementById("generate-btn");
  if (btn) btn.disabled = !windowValid || previewEmpty;
}

function setActivePreset(presetId) {
  for (const btn of document.querySelectorAll("[data-window-preset]")) {
    btn.classList.toggle(
      "preset-active",
      btn.getAttribute("data-window-preset") === presetId
    );
  }
  if (customWindowEl) customWindowEl.hidden = presetId !== "custom";
}

const DAY_MS = 86400000;

/* Concrete YYYY-MM-DD (start, end) for any window, resolving a relative window to
 * [now - lookback, now] so switching to Custom pre-fills valid dates. */
function resolvedDatesForWindow(w, now) {
  if (!w) return { start: "", end: "" };
  if (w.kind === "absolute") return windowDates(w);
  const days = w.lookback_days || 14;
  return { start: isoDate(now - days * DAY_MS), end: isoDate(now) };
}

async function refreshSeedPreview() {
  if (!seedPreviewEl) return;
  seedPreviewEl.classList.add("loading");
  let resp;
  try {
    resp = await fetch("/playlists/" + profileId + "/seed-preview", {
      credentials: "same-origin",
    });
  } catch (e) {
    seedPreviewEl.classList.remove("loading");
    return;
  }
  seedPreviewEl.classList.remove("loading");
  if (!resp.ok) return;
  seedPreviewEl.innerHTML = await resp.text();
  const inner = document.getElementById("seed-preview-inner");
  previewEmpty = inner ? inner.getAttribute("data-empty") === "true" : false;
  updateGenerateEnabled();
}

/* Apply a window change: validate, reflect the custom-date error, and (if valid)
 * persist + mark the preview dirty so it refreshes after the PATCH commits. */
function applyWindow(newWindow, presetId) {
  windowState = newWindow;
  windowValid = isWindowValid(windowState);
  if (windowErrorEl) {
    windowErrorEl.hidden = windowValid || presetId !== "custom";
  }
  updateGenerateEnabled();
  if (!windowValid) return; // never PATCH an invalid window (the API 400s it)
  previewDirty = true;
  scheduleSave();
}

function windowFromCustomInputs() {
  const start = windowStartEl ? windowStartEl.value : "";
  const end = windowEndEl ? windowEndEl.value : "";
  if (!start || !end) return null;
  return absoluteWindow(start, end);
}

for (const presetBtn of document.querySelectorAll("[data-window-preset]")) {
  presetBtn.addEventListener("click", () => {
    const presetId = presetBtn.getAttribute("data-window-preset");
    setActivePreset(presetId);
    if (presetId === "custom") {
      // Pre-fill empty custom inputs from the current window so Custom starts
      // valid (design decision 4).
      if (windowStartEl && windowEndEl && (!windowStartEl.value || !windowEndEl.value)) {
        const d = resolvedDatesForWindow(windowState, Date.now());
        windowStartEl.value = windowStartEl.value || d.start;
        windowEndEl.value = windowEndEl.value || d.end;
      }
      applyWindow(windowFromCustomInputs(), "custom");
      return;
    }
    const w = windowFromPreset(presetId, Date.now());
    // Reflect an absolute preset's dates into the custom inputs so a later switch
    // to Custom is pre-filled + tweakable.
    const d = windowDates(w);
    if (windowStartEl) windowStartEl.value = d.start;
    if (windowEndEl) windowEndEl.value = d.end;
    applyWindow(w, presetId);
  });
}

for (const dateEl of [windowStartEl, windowEndEl]) {
  if (dateEl) {
    dateEl.addEventListener("change", () =>
      applyWindow(windowFromCustomInputs(), "custom")
    );
  }
}

if (seedCountEl) {
  seedCountEl.addEventListener("change", () => {
    seedCount = clampSeedCount(seedCountEl.value);
    seedCountEl.value = String(seedCount);
    previewDirty = true;
    scheduleSave();
  });
}

const hintFind = document.getElementById("hint-find-related");
if (hintFind) {
  hintFind.addEventListener("click", () => runEnrich("lineup", hintFind));
}

if (isRediscovery) {
  setActivePreset(presetIdForWindow(windowState));
  if (seedCountEl) seedCountEl.value = String(seedCount);
  const initialDates = windowDates(windowState);
  if (windowStartEl && initialDates.start) windowStartEl.value = initialDates.start;
  if (windowEndEl && initialDates.end) windowEndEl.value = initialDates.end;
  updateGenerateEnabled();
  refreshSeedPreview();
}

/* Generate: persist, then trigger generation and go to the status page. */
const generateBtn = document.getElementById("generate-btn");
if (generateBtn) {
  generateBtn.addEventListener("click", async () => {
    if (conflicted) return;
    // Rediscovery's pool comes from the window (seeds are derived), so an empty
    // groups list is fine — the window guard (disabled state) covers validity.
    // concert_prep needs at least one hand-picked source.
    if (!isRediscovery && isEmpty(state.groups)) {
      if (emptyEl) {
        emptyEl.textContent = "Add at least one event or artist before generating.";
        emptyEl.hidden = false;
      }
      return;
    }
    generateBtn.disabled = true;
    // Last-resort naming so nothing ever generates as a bare default: rediscovery
    // names from its window, concert_prep from the first event, else a dated
    // fallback. (Event-add already auto-names; this covers pools the user never
    // named.)
    if (isDefaultName()) {
      let fallback;
      if (isRediscovery) {
        fallback = "Rediscovery " + new Date().toISOString().slice(0, 10);
      } else {
        const ev = state.groups.find((g) => g.kind === "event");
        fallback = ev
          ? "Concert Prep: " + ev.title
          : "Playlist " + new Date().toISOString().slice(0, 10);
      }
      nameInput.value = fallback;
      await flushSave({ name: fallback });
    }
    await ensureSaved();
    if (conflicted) {
      generateBtn.disabled = false;
      return;
    }
    const maxEl = document.getElementById("max_tracks");
    const maxTracks = maxEl ? parseInt(maxEl.value, 10) || 50 : 50;
    let resp;
    try {
      resp = await fetch("/api/v1/generator-profiles/" + profileId + "/generate", {
        method: "POST",
        credentials: "same-origin",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ max_tracks: maxTracks }),
      });
    } catch (e) {
      generateBtn.disabled = false;
      return;
    }
    if (!resp.ok) {
      generateBtn.disabled = false;
      return;
    }
    const data = await resp.json();
    window.location.href = "/playlists/generating/" + data.task_id;
  });
}

render();
