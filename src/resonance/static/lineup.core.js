/* Lineup builder core: pure state logic, no DOM, no fetch.
 *
 * The client holds the lineup as an array of *groups* (the same shape the server
 * hydrates from a profile's input_references):
 *
 *   { kind: "event",   event_id, title, sub, artists: [row] }
 *   { kind: "manual",  title, artists: [row] }
 *   { kind: "related", scope, title, artists: [row] }   // scope = "lineup" | "<artist_id>"
 *
 * where row = { id, name, meta, included }.
 *
 * These functions are pure (return new structures, never mutate inputs) so they
 * can be unit-tested under vitest without a browser. The DOM controller in
 * lineup.js imports them.
 */

export function escapeHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function artistMeta(a) {
  const bits = [];
  // Genres lead — the strongest disambiguator between same-name artists
  // (#136: the metal "Nite" vs the electronic one).
  if (a.genres && a.genres.length) bits.push(a.genres.join(", "));
  if (a.disambiguation) bits.push(a.disambiguation);
  if (a.area) bits.push(a.area);
  if (a.begin_year) bits.push(String(a.begin_year));
  return bits.join(" · ");
}

/* All artist IDs currently in the builder, for seeding genre-affinity search. */
export function allArtistIds(groups) {
  const ids = [];
  for (const g of groups) {
    for (const a of g.artists) ids.push(a.id);
  }
  return ids;
}

/* Stable identity for a group, used to target toggle/remove operations. */
export function groupKey(group) {
  if (group.kind === "event") return "event:" + group.event_id;
  if (group.kind === "related") return "related:" + group.scope;
  return "manual";
}

export function hasArtist(groups, id) {
  for (const g of groups) {
    for (const a of g.artists) {
      if (a.id === id) return true;
    }
  }
  return false;
}

export function isEmpty(groups) {
  return groups.every((g) => g.artists.length === 0);
}

/* Drop related/manual groups that have no artists left (keep event groups so a
 * just-added event with a pending lineup fetch still shows its header). */
export function pruneEmpty(groups) {
  return groups.filter((g) => g.kind === "event" || g.artists.length > 0);
}

export function toggleArtist(groups, key, id, included) {
  return groups.map((g) =>
    groupKey(g) !== key
      ? g
      : {
          ...g,
          artists: g.artists.map((a) =>
            a.id === id ? { ...a, included: included } : a
          ),
        }
  );
}

export function removeArtistFromGroup(groups, key, id) {
  const next = groups.map((g) =>
    groupKey(g) !== key
      ? g
      : { ...g, artists: g.artists.filter((a) => a.id !== id) }
  );
  return pruneEmpty(next);
}

export function removeGroup(groups, key) {
  return groups.filter((g) => groupKey(g) !== key);
}

export function addManualArtist(groups, artist) {
  if (hasArtist(groups, artist.id)) return groups;
  const row = {
    id: artist.id,
    name: artist.name || "",
    meta: artist.meta || "",
    included: true,
  };
  const existing = groups.find((g) => g.kind === "manual");
  if (existing) {
    return groups.map((g) =>
      g === existing ? { ...g, artists: [...g.artists, row] } : g
    );
  }
  return [...groups, { kind: "manual", title: "Added artists", artists: [row] }];
}

export function addEventGroup(groups, eventId, title, sub) {
  if (groups.some((g) => g.kind === "event" && g.event_id === eventId)) {
    return groups;
  }
  return [
    ...groups,
    { kind: "event", event_id: eventId, title: title, sub: sub, artists: [] },
  ];
}

export function setEventArtists(groups, eventId, rows) {
  return groups.map((g) =>
    g.kind === "event" && g.event_id === eventId ? { ...g, artists: rows } : g
  );
}

/* Serialize the grouped client state into the stored input_references shape.
 * Mirrors generators.pool.serialize_input_references on the server so a PATCH
 * round-trips: event sources, then manual artists, then related artists tagged
 * with their scope's via_seed; the union of unchecked rows becomes excludes. */
export function serializeInputReferences(groups) {
  const sources = [];
  const excludeSet = {};
  for (const g of groups) {
    if (g.kind === "event") {
      sources.push({ kind: "event", event_id: g.event_id, enabled: true });
      for (const a of g.artists) if (!a.included) excludeSet[a.id] = true;
    } else if (g.kind === "manual") {
      for (const a of g.artists) {
        sources.push({ kind: "artist", artist_id: a.id, enabled: true });
        if (!a.included) excludeSet[a.id] = true;
      }
    } else if (g.kind === "related") {
      for (const a of g.artists) {
        sources.push({
          kind: "artist",
          artist_id: a.id,
          enabled: true,
          via_seed: g.scope,
        });
        if (!a.included) excludeSet[a.id] = true;
      }
    }
  }
  return { sources: sources, exclude_artist_ids: Object.keys(excludeSet) };
}

/* ===== Rediscovery window (#rediscovery-ui) =====
 *
 * The rediscovery editor's recipe is a listening-history window + dials, not a
 * hand-picked lineup. These pure helpers own the window's client state and its
 * serialization into the same stored input_references shape the server parses
 * (a listening_range source, plus any enrich-discovered via_seed artists).
 */

const DAY_MS = 86400000;

export const SEED_COUNT_MIN = 5;
export const SEED_COUNT_MAX = 50;
export const SEED_COUNT_DEFAULT = 20;

/* Window preset pills, in display order. "custom" reveals two date inputs; the
 * other three are one-tap presets. Only "last_2_weeks" persists its identity
 * (as a relative window); the two absolute presets round-trip as "custom" (see
 * presetIdForWindow). */
export const REDISCOVERY_PRESETS = [
  { id: "last_2_weeks", label: "Last 2 Weeks" },
  { id: "a_month_ago", label: "A Month Ago" },
  { id: "this_time_last_year", label: "This Time Last Year" },
  { id: "custom", label: "Custom" },
];

/* Format an epoch-ms instant to a YYYY-MM-DD (UTC) date string. */
export function isoDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

/* Build an absolute window from two YYYY-MM-DD strings. The end snaps to
 * end-of-day so the end date is included (the backend bounds are inclusive), and
 * so a same-day window (start === end) is still a valid non-empty range. */
export function absoluteWindow(startDate, endDate) {
  return {
    kind: "absolute",
    start: startDate + "T00:00:00",
    end: endDate + "T23:59:59",
  };
}

/* Resolve a preset id to a stored window object against `now` (epoch ms).
 * "last_2_weeks" is relative (rolls forward on every generate); the two
 * "…ago" presets snap to concrete absolute day-bounds at selection time (a
 * frozen artifact). "custom" returns null — the caller builds it from the date
 * inputs via absoluteWindow. */
export function windowFromPreset(presetId, now) {
  if (presetId === "last_2_weeks") {
    return { kind: "relative", lookback_days: 14 };
  }
  if (presetId === "a_month_ago") {
    return absoluteWindow(isoDate(now - 44 * DAY_MS), isoDate(now - 16 * DAY_MS));
  }
  if (presetId === "this_time_last_year") {
    return absoluteWindow(isoDate(now - 379 * DAY_MS), isoDate(now - 351 * DAY_MS));
  }
  return null;
}

/* Which pill is active for a stored window. A relative window is the rolling
 * "Last 2 Weeks" preset; any absolute window reads as "Custom" with its dates —
 * preset identity for the two absolute presets is intentionally not persisted
 * (the backend rebuilds the window from typed fields only), matching the design's
 * relative-vs-custom round-trip contract. A missing window defaults to the
 * rolling preset. */
export function presetIdForWindow(window) {
  if (!window || window.kind === "relative") return "last_2_weeks";
  return "custom";
}

/* The YYYY-MM-DD (start, end) pair to prefill the custom date inputs from a
 * window. A relative window has no stored dates (empty strings); the controller
 * fills them from the resolved preset when the user switches to Custom. */
export function windowDates(window) {
  if (!window || window.kind !== "absolute") return { start: "", end: "" };
  return {
    start: (window.start || "").slice(0, 10),
    end: (window.end || "").slice(0, 10),
  };
}

/* Clamp a raw seed-count input to [MIN, MAX], falling back to the default on a
 * non-numeric value. */
export function clampSeedCount(n) {
  const v = parseInt(n, 10);
  if (!Number.isFinite(v)) return SEED_COUNT_DEFAULT;
  return Math.max(SEED_COUNT_MIN, Math.min(SEED_COUNT_MAX, v));
}

/* Is a window valid to preview/generate? Relative needs a positive lookback;
 * absolute needs start strictly before end. Drives the Generate-disable guard. */
export function isWindowValid(window) {
  if (!window) return false;
  if (window.kind === "relative") return (window.lookback_days || 0) > 0;
  if (window.kind === "absolute") {
    return Boolean(window.start && window.end && window.start < window.end);
  }
  return false;
}

/* Serialize the rediscovery recipe into stored input_references. The
 * listening_range source leads; enrich-discovered via_seed artists (carried as
 * `related` groups) and their excludes are PRESERVED via serializeInputReferences,
 * so editing the window never drops the enriched new-artist stream. v1 scores
 * lifetime-only, so the basis flags are constant. */
export function serializeRediscovery(window, seedCount, groups) {
  const base = serializeInputReferences(groups || []);
  const listeningRange = {
    kind: "listening_range",
    enabled: true,
    window: window,
    seed_artist_count: seedCount,
    deep_cut_basis: "lifetime",
    novelty_basis: "lifetime",
  };
  return {
    sources: [listeningRange, ...base.sources],
    exclude_artist_ids: base.exclude_artist_ids,
  };
}
