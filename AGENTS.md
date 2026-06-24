# Resonance Agent Instructions

Agent-agnostic instructions for working with the Resonance codebase.

## Architecture Directives

**API-first and agentic-first — nothing is purely client-side.** Every capability the UI
offers must be reachable via the `/api/v1` JSON API and the `resonance-api` CLI, so an
agent can do anything a person can. Pool/profile mutations update server-side records
first, then render to the client; a builder's in-browser state is a render convenience,
never the source of truth.

**The profile is the system of record.** `GeneratorProfile.input_references` is the
durable "how this playlist is made" recipe. Generation is re-runnable against a stored
profile via `POST /api/v1/generator-profiles/{id}/generate` — it re-resolves the pool and
re-snapshots `GenerationRecord`, so changed listening counts flow into track scoring on a
re-run. The mental model: the artist pool stays fixed; re-running re-scores tracks (e.g.,
to keep an "unlistened" goal honest). Design pool/profile-editing features as server-backed
mutations of the profile, not ephemeral client state.

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore
