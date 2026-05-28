# TODOs

Deferred work items with full context. Each entry explains what, why, and where to start.

## Investigate capability-scoped tables for ServiceConnection

**What:** Evaluate whether ServiceConnection should only hold bare-minimum fields (user_id, service_type, external_user_id, connected_at, enabled), with capability-specific data living in separate tables joined by connection ID.

**Why:** Identity-only connectors (GitHub/Dex) create ServiceConnection rows with sync-related fields (sync_watermark, last_synced_at, encrypted tokens for API calls) that will always be None. As more connector types are added (each with different capability profiles), the single-table model accumulates nullable fields that only apply to subsets of connectors. Capability-scoped tables would let each capability declare its own storage needs.

**Current state:** ServiceConnection has ~15 fields. Identity-only connectors use ~6 of them. The unused fields are nullable and harmless today, but the pattern doesn't scale cleanly.

**Where to start:** Map each ServiceConnection field to the capability that uses it. Sketch a normalized schema (e.g., `connection_sync_state`, `connection_tokens`, `connection_identity`). Evaluate whether the query complexity of joins outweighs the schema clarity.

**Depends on:** GitHub/Dex auth feature landing first (provides a concrete identity-only connector to test against).

**Source:** /plan-eng-review D15, 2026-05-28. Outside voice flagged ServiceConnection overhead for identity-only connectors.

## Refactor sync dispatch to be connector-capability-aware

**What:** Sync dispatch code (worker, orphan recovery, task dispatch) should check whether a connector declares sync-related capabilities before attempting to dispatch. Currently these call sites assume all connectors have a sync_function.

**Why:** Adding identity-only connectors (no sync) means sync dispatch could encounter None sync_function values. Rather than adding None guards at each call site (which must be repeated for every future sync-less connector), the dispatch code should query connector capabilities and skip non-sync connectors automatically.

**Current state:** Will be partially addressed during the GitHub/Dex auth feature (capability checks added to dispatch). This TODO tracks the broader refactor to make the pattern self-documenting.

**Where to start:** Trace `_TASK_DISPATCH`, orphan recovery in `sync/lifecycle.py`, and worker job dispatch. Add capability checks using `registry.get_by_capability()` to filter connectors before dispatch.

**Source:** /plan-eng-review D12+D17, 2026-05-28. Outside voice caught the call-site audit gap; user requested forward-looking refactor.
