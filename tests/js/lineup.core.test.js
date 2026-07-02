import { describe, expect, it } from "vitest";

import {
  addEventGroup,
  addManualArtist,
  allArtistIds,
  artistMeta,
  escapeHtml,
  groupKey,
  hasArtist,
  isEmpty,
  pruneEmpty,
  removeArtistFromGroup,
  removeGroup,
  serializeInputReferences,
  setEventArtists,
  toggleArtist,
} from "../../src/resonance/static/lineup.core.js";

const row = (id, included = true) => ({ id, name: id, meta: "", included });

describe("escapeHtml", () => {
  it("escapes HTML-significant characters", () => {
    expect(escapeHtml('<a href="x">&')).toBe("&lt;a href=&quot;x&quot;&gt;&amp;");
  });
  it("treats null/undefined as empty", () => {
    expect(escapeHtml(null)).toBe("");
    expect(escapeHtml(undefined)).toBe("");
  });
});

describe("artistMeta", () => {
  it("joins present fields with a middot", () => {
    expect(artistMeta({ disambiguation: "mathcore", area: "US", begin_year: 1997 })).toBe(
      "mathcore · US · 1997"
    );
  });
  it("omits empty fields", () => {
    expect(artistMeta({ area: "US" })).toBe("US");
    expect(artistMeta({})).toBe("");
  });
  it("leads with genres when present", () => {
    expect(artistMeta({ genres: ["black metal", "thrash"], area: "NO" })).toBe(
      "black metal, thrash · NO"
    );
  });
  it("ignores an empty genres list", () => {
    expect(artistMeta({ genres: [], area: "US" })).toBe("US");
  });
});

describe("allArtistIds", () => {
  it("collects ids across all groups", () => {
    const groups = [
      { artists: [{ id: "a" }, { id: "b" }] },
      { artists: [] },
      { artists: [{ id: "c" }] },
    ];
    expect(allArtistIds(groups)).toEqual(["a", "b", "c"]);
  });
  it("is empty for no artists", () => {
    expect(allArtistIds([{ artists: [] }])).toEqual([]);
  });
});

describe("groupKey", () => {
  it("is stable per group kind", () => {
    expect(groupKey({ kind: "event", event_id: "e1" })).toBe("event:e1");
    expect(groupKey({ kind: "manual" })).toBe("manual");
    expect(groupKey({ kind: "related", scope: "lineup" })).toBe("related:lineup");
    expect(groupKey({ kind: "related", scope: "a1" })).toBe("related:a1");
  });
});

describe("hasArtist / isEmpty", () => {
  const groups = [
    { kind: "event", event_id: "e1", artists: [row("a")] },
    { kind: "manual", artists: [row("b")] },
  ];
  it("finds artists across groups", () => {
    expect(hasArtist(groups, "a")).toBe(true);
    expect(hasArtist(groups, "b")).toBe(true);
    expect(hasArtist(groups, "z")).toBe(false);
  });
  it("isEmpty reflects total artist count", () => {
    expect(isEmpty(groups)).toBe(false);
    expect(isEmpty([{ kind: "event", event_id: "e1", artists: [] }])).toBe(true);
    expect(isEmpty([])).toBe(true);
  });
});

describe("toggleArtist", () => {
  it("flips included on the matching artist only, immutably", () => {
    const groups = [{ kind: "manual", artists: [row("a"), row("b")] }];
    const next = toggleArtist(groups, "manual", "a", false);
    expect(next[0].artists[0].included).toBe(false);
    expect(next[0].artists[1].included).toBe(true);
    expect(groups[0].artists[0].included).toBe(true); // original untouched
  });
});

describe("removeArtistFromGroup", () => {
  it("removes the artist and prunes an emptied related group", () => {
    const groups = [
      { kind: "related", scope: "lineup", artists: [row("a")] },
      { kind: "manual", artists: [row("b")] },
    ];
    const next = removeArtistFromGroup(groups, "related:lineup", "a");
    expect(next.find((g) => g.kind === "related")).toBeUndefined();
    expect(next.find((g) => g.kind === "manual")).toBeDefined();
  });
  it("keeps an emptied event group (lineup may still be loading)", () => {
    const groups = [{ kind: "event", event_id: "e1", artists: [row("a")] }];
    const next = removeArtistFromGroup(groups, "event:e1", "a");
    expect(next).toHaveLength(1);
    expect(next[0].artists).toHaveLength(0);
  });
});

describe("removeGroup / pruneEmpty", () => {
  it("removeGroup drops the targeted group", () => {
    const groups = [
      { kind: "related", scope: "a1", artists: [row("x")] },
      { kind: "manual", artists: [row("y")] },
    ];
    expect(removeGroup(groups, "related:a1")).toHaveLength(1);
  });
  it("pruneEmpty drops empty non-event groups", () => {
    const groups = [
      { kind: "manual", artists: [] },
      { kind: "event", event_id: "e", artists: [] },
      { kind: "related", scope: "x", artists: [row("a")] },
    ];
    const kinds = pruneEmpty(groups).map((g) => g.kind);
    expect(kinds).toEqual(["event", "related"]);
  });
});

describe("addManualArtist", () => {
  it("creates the manual group on first add", () => {
    const next = addManualArtist([], { id: "a", name: "A" });
    expect(next).toHaveLength(1);
    expect(next[0].kind).toBe("manual");
    expect(next[0].artists[0].id).toBe("a");
  });
  it("appends to an existing manual group and dedupes", () => {
    let g = addManualArtist([], { id: "a", name: "A" });
    g = addManualArtist(g, { id: "b", name: "B" });
    g = addManualArtist(g, { id: "a", name: "A" }); // dup ignored
    expect(g[0].artists.map((a) => a.id)).toEqual(["a", "b"]);
  });
});

describe("addEventGroup / setEventArtists", () => {
  it("adds an event group once, then fills its artists", () => {
    let g = addEventGroup([], "e1", "Show", "· sub");
    g = addEventGroup(g, "e1", "Show", "· sub"); // dup ignored
    expect(g).toHaveLength(1);
    g = setEventArtists(g, "e1", [row("a"), row("b")]);
    expect(g[0].artists).toHaveLength(2);
  });
});

describe("serializeInputReferences", () => {
  it("emits event, manual, and related (via_seed) sources + excludes", () => {
    const groups = [
      { kind: "event", event_id: "e1", artists: [row("a"), row("op", false)] },
      { kind: "manual", artists: [row("m")] },
      { kind: "related", scope: "lineup", artists: [row("r1"), row("r2", false)] },
      { kind: "related", scope: "seed1", artists: [row("r3")] },
    ];
    const out = serializeInputReferences(groups);
    expect(out.sources).toEqual([
      { kind: "event", event_id: "e1", enabled: true },
      { kind: "artist", artist_id: "m", enabled: true },
      { kind: "artist", artist_id: "r1", enabled: true, via_seed: "lineup" },
      { kind: "artist", artist_id: "r2", enabled: true, via_seed: "lineup" },
      { kind: "artist", artist_id: "r3", enabled: true, via_seed: "seed1" },
    ]);
    expect(out.exclude_artist_ids.sort()).toEqual(["op", "r2"]);
  });

  it("produces an empty spec for empty groups", () => {
    expect(serializeInputReferences([])).toEqual({
      sources: [],
      exclude_artist_ids: [],
    });
  });
});
