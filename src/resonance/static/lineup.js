/* Lineup builder controller (New Playlist).
 *
 * Manages an in-memory lineup of event sources (expanded to their individual
 * artists) and manually-added artists, each with an include/exclude checkbox.
 * On submit it serializes the lineup into the layered input_references shape
 * the server validates (#128):
 *
 *   { "sources": [ {kind:"event",event_id}, {kind:"artist",artist_id} ],
 *     "exclude_artist_ids": [ <unchecked artist ids> ] }
 *
 * Reads come from the session-authenticated JSON API (/api/v1/...); the create
 * itself is a normal form POST so the proven redirect-to-generating flow is kept.
 */
(function () {
  "use strict";

  var state = { events: [], manual: [] };

  var groupsEl = document.getElementById("lineup-groups");
  var emptyEl = document.getElementById("lineup-empty");
  var eventSelect = document.getElementById("add-event-select");
  var searchInput = document.getElementById("artist-search");
  var resultsEl = document.getElementById("artist-results");
  var form = document.getElementById("lineup-form");
  var hiddenField = document.getElementById("input-references-json");

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function artistMeta(a) {
    var bits = [];
    if (a.disambiguation) bits.push(a.disambiguation);
    if (a.area) bits.push(a.area);
    if (a.begin_year) bits.push(String(a.begin_year));
    return bits.join(" · ");
  }

  function hasArtist(id) {
    var i, j;
    for (i = 0; i < state.manual.length; i++) {
      if (state.manual[i].id === id) return true;
    }
    for (i = 0; i < state.events.length; i++) {
      for (j = 0; j < state.events[i].artists.length; j++) {
        if (state.events[i].artists[j].id === id) return true;
      }
    }
    return false;
  }

  function normArtist(a) {
    return { id: a.id, name: a.name, meta: artistMeta(a), included: true };
  }

  function rowHtml(artist, scope, removable) {
    var excluded = artist.included ? "" : " excluded";
    var checked = artist.included ? " checked" : "";
    var meta = artist.meta ? '<span class="m">' + esc(artist.meta) + "</span>" : "";
    var tag = artist.included ? "" : '<span class="extag">excluded</span>';
    var remove = removable
      ? '<button type="button" class="aremove" data-remove-manual="' +
        esc(artist.id) +
        '" title="Remove" aria-label="Remove ' +
        esc(artist.name) +
        '">&times;</button>'
      : "";
    return (
      '<div class="artist-row' +
      excluded +
      '">' +
      '<input type="checkbox" data-artist-toggle data-scope="' +
      esc(scope) +
      '" data-artist-id="' +
      esc(artist.id) +
      '"' +
      checked +
      ' aria-label="Include ' +
      esc(artist.name) +
      '">' +
      '<span class="aname"><span class="n">' +
      esc(artist.name) +
      "</span>" +
      meta +
      "</span>" +
      tag +
      remove +
      "</div>"
    );
  }

  function render() {
    var html = "";
    var i;

    for (i = 0; i < state.events.length; i++) {
      var ev = state.events[i];
      var rows = "";
      if (ev.artists.length === 0) {
        rows =
          '<div class="artist-row"><span class="aname"><span class="m">No artists found for this event.</span></span></div>';
      } else {
        for (var j = 0; j < ev.artists.length; j++) {
          rows += rowHtml(ev.artists[j], "event:" + ev.id, false);
        }
      }
      html +=
        '<div class="lineup-group" data-event-id="' +
        esc(ev.id) +
        '">' +
        '<div class="lineup-group-head">' +
        "<span><span class=\"gtitle\">" +
        esc(ev.title) +
        "</span> <span class=\"gsub\">" +
        esc(ev.sub) +
        "</span></span>" +
        '<button type="button" class="gremove" data-remove-event="' +
        esc(ev.id) +
        '" title="Remove event" aria-label="Remove event ' +
        esc(ev.title) +
        '">&times;</button>' +
        "</div>" +
        rows +
        "</div>";
    }

    if (state.manual.length > 0) {
      var mrows = "";
      for (i = 0; i < state.manual.length; i++) {
        mrows += rowHtml(state.manual[i], "manual", true);
      }
      html +=
        '<div class="lineup-group">' +
        '<div class="lineup-group-head"><span class="gtitle">Added artists</span></div>' +
        mrows +
        "</div>";
    }

    groupsEl.innerHTML = html;
    var isEmpty = state.events.length === 0 && state.manual.length === 0;
    emptyEl.hidden = !isEmpty;
  }

  function findArtist(scope, id) {
    if (scope === "manual") {
      return state.manual.find(function (a) {
        return a.id === id;
      });
    }
    var eid = scope.slice("event:".length);
    var ev = state.events.find(function (e) {
      return e.id === eid;
    });
    if (!ev) return undefined;
    return ev.artists.find(function (a) {
      return a.id === id;
    });
  }

  function addEvent(id, title, sub) {
    var exists = state.events.some(function (e) {
      return e.id === id;
    });
    if (exists) return;
    var ev = { id: id, title: title, sub: sub, artists: [] };
    state.events.push(ev);
    render();
    fetch("/api/v1/events/" + encodeURIComponent(id) + "/lineup", {
      credentials: "same-origin",
    })
      .then(function (r) {
        return r.ok ? r.json() : { artists: [] };
      })
      .then(function (data) {
        ev.artists = (data.artists || []).map(normArtist);
        render();
      })
      .catch(function () {
        render();
      });
  }

  function renderResults(items) {
    if (!items.length) {
      resultsEl.innerHTML = '<div class="picker-empty">No matching artists in your library.</div>';
      resultsEl.hidden = false;
      return;
    }
    var html = "";
    for (var i = 0; i < items.length; i++) {
      var a = items[i];
      var meta = artistMeta(a);
      var disabled = hasArtist(a.id);
      var tag = a.in_library
        ? '<span class="ptag lib">in library</span>'
        : '<span class="ptag">no tracks yet</span>';
      html +=
        '<div class="picker-item" data-add-artist="' +
        esc(a.id) +
        '" data-name="' +
        esc(a.name) +
        '" data-meta="' +
        esc(meta) +
        '"' +
        (disabled ? ' style="opacity:.45;pointer-events:none;"' : "") +
        ">" +
        '<span><span class="pname">' +
        esc(a.name) +
        "</span>" +
        (meta ? ' <span class="pmeta">— ' + esc(meta) + "</span>" : "") +
        "</span>" +
        (disabled ? '<span class="ptag">added</span>' : tag) +
        "</div>";
    }
    resultsEl.innerHTML = html;
    resultsEl.hidden = false;
  }

  var searchTimer = null;
  function onSearch() {
    var q = searchInput.value.trim();
    if (searchTimer) clearTimeout(searchTimer);
    if (q.length < 2) {
      resultsEl.hidden = true;
      resultsEl.innerHTML = "";
      return;
    }
    searchTimer = setTimeout(function () {
      fetch("/api/v1/artists/search?q=" + encodeURIComponent(q) + "&limit=8", {
        credentials: "same-origin",
      })
        .then(function (r) {
          return r.ok ? r.json() : { items: [] };
        })
        .then(function (data) {
          renderResults(data.items || []);
        })
        .catch(function () {
          resultsEl.hidden = true;
        });
    }, 250);
  }

  function serialize() {
    var sources = [];
    var excludes = {};
    var i, j;
    for (i = 0; i < state.events.length; i++) {
      var ev = state.events[i];
      sources.push({ kind: "event", event_id: ev.id, enabled: true });
      for (j = 0; j < ev.artists.length; j++) {
        if (!ev.artists[j].included) excludes[ev.artists[j].id] = true;
      }
    }
    for (i = 0; i < state.manual.length; i++) {
      var a = state.manual[i];
      sources.push({ kind: "artist", artist_id: a.id, enabled: true });
      if (!a.included) excludes[a.id] = true;
    }
    return { sources: sources, exclude_artist_ids: Object.keys(excludes) };
  }

  /* ---- wiring ---- */

  eventSelect.addEventListener("change", function () {
    var opt = eventSelect.options[eventSelect.selectedIndex];
    var id = eventSelect.value;
    if (!id) return;
    var title = opt.getAttribute("data-title") || "Event";
    var date = opt.getAttribute("data-date") || "";
    var venue = opt.getAttribute("data-venue") || "";
    var sub = "· " + date + (venue ? " · " + venue : "") + " · live lineup";
    addEvent(id, title, sub);
    eventSelect.selectedIndex = 0;
  });

  searchInput.addEventListener("input", onSearch);
  searchInput.addEventListener("focus", onSearch);

  resultsEl.addEventListener("click", function (e) {
    var item = e.target.closest("[data-add-artist]");
    if (!item) return;
    var id = item.getAttribute("data-add-artist");
    if (hasArtist(id)) return;
    state.manual.push({
      id: id,
      name: item.getAttribute("data-name") || "",
      meta: item.getAttribute("data-meta") || "",
      included: true,
    });
    searchInput.value = "";
    resultsEl.hidden = true;
    resultsEl.innerHTML = "";
    render();
  });

  document.addEventListener("click", function (e) {
    if (!e.target.closest(".picker")) {
      resultsEl.hidden = true;
    }
  });

  groupsEl.addEventListener("change", function (e) {
    var cb = e.target.closest("[data-artist-toggle]");
    if (!cb) return;
    var artist = findArtist(cb.getAttribute("data-scope"), cb.getAttribute("data-artist-id"));
    if (artist) {
      artist.included = cb.checked;
      render();
    }
  });

  groupsEl.addEventListener("click", function (e) {
    var rmEvent = e.target.closest("[data-remove-event]");
    if (rmEvent) {
      var eid = rmEvent.getAttribute("data-remove-event");
      state.events = state.events.filter(function (ev) {
        return ev.id !== eid;
      });
      render();
      return;
    }
    var rmManual = e.target.closest("[data-remove-manual]");
    if (rmManual) {
      var aid = rmManual.getAttribute("data-remove-manual");
      state.manual = state.manual.filter(function (a) {
        return a.id !== aid;
      });
      render();
    }
  });

  form.addEventListener("submit", function (e) {
    var payload = serialize();
    if (payload.sources.length === 0) {
      e.preventDefault();
      emptyEl.textContent = "Add at least one event or artist before generating.";
      emptyEl.style.color = "var(--error)";
      emptyEl.style.borderColor = "var(--error)";
      emptyEl.hidden = false;
      return;
    }
    hiddenField.value = JSON.stringify(payload);
  });

  /* Pre-seed from ?event_id= */
  if (window.LINEUP_PRESEED_EVENT) {
    var presEl = eventSelect.querySelector(
      'option[value="' + window.LINEUP_PRESEED_EVENT + '"]'
    );
    if (presEl) {
      var t = presEl.getAttribute("data-title") || "Event";
      var d = presEl.getAttribute("data-date") || "";
      var v = presEl.getAttribute("data-venue") || "";
      addEvent(window.LINEUP_PRESEED_EVENT, t, "· " + d + (v ? " · " + v : "") + " · live lineup");
    }
  }

  render();
})();
