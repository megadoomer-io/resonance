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
