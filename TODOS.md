# TODOs

Deferred work items with full context. Each entry explains what, why, and where to start.

## Investigate capability-scoped tables for ServiceConnection

**Status:** Investigated 2026-05-28. Deferred — revisit when adding a 3rd+ identity-only connector.

**What:** Evaluate whether ServiceConnection should only hold bare-minimum fields (user_id, service_type, external_user_id, connected_at, enabled), with capability-specific data living in separate tables joined by connection ID.

**Why:** Identity-only connectors (GitHub/Dex) create ServiceConnection rows with sync-related fields (sync_watermark, last_synced_at, encrypted tokens for API calls) that will always be None. As more connector types are added (each with different capability profiles), the single-table model accumulates nullable fields that only apply to subsets of connectors. Capability-scoped tables would let each capability declare its own storage needs.

**Investigation findings:** ServiceConnection has 14 fields. GitHub (identity-only) uses 6 of them; the remaining 8 are always None (78% field waste per row). Calendar feed connections have a similar pattern (6 unused fields). Music API connectors (Spotify, Last.fm, ListenBrainz) use all 14 fields efficiently. With only one identity-only connector today, the waste is trivial — nullable fields cost minimal storage and don't affect query performance. The problem becomes real at 3+ identity-only connectors or if capability-specific fields start diverging further.

**Revisit criteria:** Adding a 3rd identity-only connector, or needing capability-specific fields that don't fit the current schema.

**Where to start:** Map each ServiceConnection field to the capability that uses it. Sketch a normalized schema (e.g., `connection_sync_state`, `connection_tokens`, `connection_identity`). Evaluate whether the query complexity of joins outweighs the schema clarity.

**Source:** /plan-eng-review D15, 2026-05-28. Outside voice flagged ServiceConnection overhead for identity-only connectors.

## Refactor sync dispatch to be connector-capability-aware

**Status:** Investigated 2026-05-28. Deferred — current guards are sufficient; revisit before adding a new connector type.

**What:** Sync dispatch code (worker, orphan recovery, task dispatch) should check whether a connector declares sync-related capabilities before attempting to dispatch. Currently these call sites assume all connectors have a sync_function.

**Why:** Adding identity-only connectors (no sync) means sync dispatch could encounter None sync_function values. Rather than adding None guards at each call site (which must be repeated for every future sync-less connector), the dispatch code should query connector capabilities and skip non-sync connectors automatically.

**Investigation findings:** The API layer already blocks identity-only connectors from sync dispatch — `sync.py` checks `sync_function is not None` and raises 400, and the UI only shows sync buttons for connectors with a sync_function. The deeper dispatch code (`_TASK_DISPATCH`, `_check_parent_completion`, `_reenqueue_orphaned_tasks`) does not check capabilities, but these code paths can only be reached by tasks created through the guarded API layer. This means the refactor is a code quality improvement (defense in depth), not a bug fix.

**Revisit criteria:** Before adding a new connector type, especially one with partial sync support (e.g., recommendations but not listening history).

**Where to start:** Add a `_TASK_TYPE_CAPABILITIES` mapping from TaskType to required ConnectorCapability. Add capability checks in `_check_parent_completion()` and `_reenqueue_orphaned_tasks()`. Add runtime validation in `startup()` to verify registered strategies match declared capabilities.

**Source:** /plan-eng-review D12+D17, 2026-05-28. Outside voice caught the call-site audit gap; user requested forward-looking refactor.

## Phase B — local MusicBrainz DB as discovery engine (#71 follow-on)

**Status:** Deferred 2026-06-18. Revisit after Phase A (hosted-mapper MBID backfill) ships and its coverage report is in hand.

**What:** Stand up a local MusicBrainz mirror — the DB-only variant (`docker-compose.alt.db-only-mirror.yml` in metabrainz/musicbrainz-docker, no web/search/API containers). Use it as (1) a discovery engine for deep cuts, unheard tracks by known artists, and never-before-seen related artists, and (2) an alternate, rate-limit-free matching source if the hosted mapper's coverage disappoints.

**Why:** The hosted MetaBrainz mapper (Phase A) can backfill MBIDs but cannot power discovery (querying recordings/relationships at will), and may have a low match ceiling on a concert-discovery library skewed to live/bootleg/long-tail recordings. A local mirror removes both limits. Deployed nowhere — laptop-only, one-time-ish tool.

**Depends on / blocked by:** Phase A coverage report (the match-rate gate, T3-A) telling us whether the hosted ceiling actually justifies the mirror. The Phase A matcher is built source-agnostic (adapter pattern) so the local DB slots in as a third adapter.

**Where to start:** When ready to download, see https://musicbrainz.org/doc/MusicBrainz_Database/Download (and the db-only-mirror compose file). Needs Docker (colima on macOS). Expect a multi-GB download + import taking hours. Then add a local-DB adapter behind the Phase A backfill core, and a query layer for discovery.

**Source:** /plan-eng-review 2026-06-18, #71. Hosted-mapper-first sequencing (D1); local DB deferred per coverage gate.

## Surface MBID collisions as merge candidates (#71 follow-on)

**Status:** Deferred 2026-06-18. Revisit once Phase A produces collision logs worth acting on.

**What:** Turn logged MBID collisions (two library rows resolving to the same MusicBrainz id, recorded by the Phase A backfill per T5-A) into an actionable merge-candidate queue.

**Why:** `dedup.py` finds name-based duplicates, but an identical MBID is a stronger, higher-precision dedup signal that Phase A currently only logs and skips. Acting on it would catch duplicates name-matching misses.

**Depends on / blocked by:** Phase A backfill emitting collision logs (counted in the coverage report).

**Where to start:** Add a merge-candidate store (a small candidates table, or reuse `EventArtistCandidate`-style storage) that the backfill writes collisions into, surfaced through the existing `api/v1/matching.py` pairwise merge preview/confirm. No auto-merge.

**Source:** /plan-eng-review 2026-06-18, #71, Tension 4 (T5-A). Outside voice flagged that the merge endpoints are pairwise/UI-only with no ingest path.
