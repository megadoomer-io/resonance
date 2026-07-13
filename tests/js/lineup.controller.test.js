// @vitest-environment happy-dom
//
// Controller smoke tests for lineup.js (#rediscovery-ui). The pure logic lives in
// lineup.core.js (lineup.core.test.js); this file exercises the DOM controller by
// importing it against a mock DOM + mock fetch. It exists because a temporal-dead-
// zone bug (updateGenerateEnabled closed over the `generateBtn` const declared
// LATER in the file, referenced during module-init) threw at import and aborted the
// whole script — so the rediscovery seed preview never loaded (blank box on prod).
// A top-level throw rejects the dynamic import(), so "loads without throwing" is the
// regression guard.
import { beforeEach, describe, expect, it, vi } from "vitest";

const EDITOR_DOM = (generatorType) => `
  <p><a class="backlink" href="/playlists">Back</a></p>
  <div class="editor-top">
    <input id="name" value="New Rediscovery">
    <span class="autosave" id="autosave" hidden><span class="dot"></span> <span class="autosave-label">Saved</span></span>
  </div>
  <div class="lineup-conflict" id="lineup-conflict" hidden>
    <button id="conflict-reload"></button>
  </div>
  <div class="filter-presets">
    <button type="button" class="preset-btn" data-window-preset="last_2_weeks">Last 2 Weeks</button>
    <button type="button" class="preset-btn" data-window-preset="a_month_ago">A Month Ago</button>
    <button type="button" class="preset-btn" data-window-preset="this_time_last_year">This Time Last Year</button>
    <button type="button" class="preset-btn" data-window-preset="custom">Custom</button>
  </div>
  <div class="custom-window" id="custom-window" hidden>
    <input type="date" id="window-start">
    <input type="date" id="window-end">
    <div class="cw-error" id="window-error" hidden></div>
  </div>
  <input type="number" id="seed-count" value="20">
  <div class="seed-preview" id="seed-preview"></div>
  <input type="number" id="add-related-n" value="15">
  <button type="button" class="ghost" id="add-related-btn">Find related artists</button>
  <div id="lineup-groups"></div>
  <div class="new-stream-hint" id="new-stream-hint" hidden><button id="hint-find-related"></button></div>
  <button type="button" class="primary" id="generate-btn">Generate Playlist</button>
  <script id="lineup-data" type="application/json">${JSON.stringify({
    generator_type: generatorType,
    version: 1,
    groups: [],
    name: "New Rediscovery",
    parameter_values: {},
    profile_id: "test-profile",
    listening_range: { window: { kind: "relative", lookback_days: 14 }, seed_artist_count: 20 },
  })}</script>
`;

function setupDom(generatorType) {
  document.body.innerHTML = EDITOR_DOM(generatorType);
  window.LINEUP_PROFILE_ID = "test-profile";
  window.LINEUP_SIMILAR_AVAILABLE = true;
  window.LINEUP_GENERATOR_TYPE = generatorType;
}

function mockFetch() {
  const fetchMock = vi.fn(async (url) => {
    if (String(url).includes("/seed-preview")) {
      return {
        ok: true,
        text: async () =>
          '<div id="seed-preview-inner" data-empty="false">You&rsquo;ll rediscover 3 artists: TOOL, Rainbow, Dopelord</div>',
      };
    }
    return { ok: true, json: async () => ({ version: 1 }) };
  });
  global.fetch = fetchMock;
  return fetchMock;
}

const settle = () => new Promise((r) => setTimeout(r, 0));

describe("lineup.js controller — rediscovery", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.restoreAllMocks();
  });

  it("imports without throwing and populates the seed preview on init", async () => {
    setupDom("rediscovery");
    const fetchMock = mockFetch();
    // The import IS the regression check: the TDZ bug threw here, rejecting this.
    await import("../../src/resonance/static/lineup.js");
    await settle();
    const preview = document.getElementById("seed-preview");
    expect(preview.innerHTML).toContain("You");
    expect(preview.innerHTML).toContain("rediscover");
    expect(
      fetchMock.mock.calls.some((c) => String(c[0]).includes("/seed-preview"))
    ).toBe(true);
  });

  it("wires the Generate button (handler attaches after init runs)", async () => {
    // The TDZ bug also aborted the script before the generate-btn handler + render()
    // ran. If import succeeds, module eval reached the end of the file.
    setupDom("rediscovery");
    mockFetch();
    await expect(
      import("../../src/resonance/static/lineup.js")
    ).resolves.toBeDefined();
    await settle();
    // Generate is disabled while the preview is unresolved/empty; after a populated
    // preview it is enabled — proving updateGenerateEnabled ran without throwing.
    const btn = document.getElementById("generate-btn");
    expect(btn.disabled).toBe(false);
  });
});

describe("lineup.js controller — concert_prep", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.restoreAllMocks();
  });

  it("imports without throwing and never fetches the seed preview", async () => {
    setupDom("concert_prep");
    const fetchMock = mockFetch();
    await import("../../src/resonance/static/lineup.js");
    await settle();
    // concert_prep is not rediscovery: the window init/preview must not run.
    expect(
      fetchMock.mock.calls.some((c) => String(c[0]).includes("/seed-preview"))
    ).toBe(false);
    expect(document.getElementById("seed-preview").innerHTML).toBe("");
  });
});
