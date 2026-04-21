# Feed Management UI Design

**Date**: 2026-04-21
**Status**: Accepted
**Issue**: [#51](https://github.com/megadoomer-io/resonance/issues/51)

## Goal

Let users connect Songkick accounts and trigger syncs from the browser.
Songkick connections live on the Account page alongside OAuth services.
Sync controls live on the Dashboard alongside existing service syncs.

## User Experience

### Connecting a Songkick Account

The Account page shows "Connect Songkick" alongside existing OAuth buttons.
The button stays visible even when accounts are already connected, since users
can add multiple Songkick usernames.

**Flow:**

1. Click "Connect Songkick" — inline HTMX swap reveals a username input
   field and "Look up" button
2. Enter username, submit — backend fetches both iCal feeds from Songkick
   to validate
3. **Username not found** — inline error: "No Songkick user found with that
   username"
4. **Username valid** — confirmation card appears:
   "**michael-dougherty** — 3 upcoming plans, 104 tracked artist shows"
   with [Connect] and [Cancel] buttons
5. Click Connect — creates two `UserCalendarFeed` rows, connections list
   reloads showing the new entry
6. Click Cancel — collapses back to the button

Event counts come from counting `BEGIN:VEVENT` in the two iCal feeds
fetched during validation. No scraping needed.

### Connected State

Each Songkick username appears in the connections list as its own entry,
same as OAuth services:

```
Songkick — michael-dougherty   [Disconnect]
Songkick — songkick            [Disconnect]
Spotify  — mike123             [Disconnect]
```

Disconnect removes only that username's two feeds (attendance + tracked
artist), not all Songkick feeds.

### Syncing

Each connection gets its own sync button on the Dashboard, matching the
existing per-service pattern:

```
Spotify  — mike123             [Sync]
Songkick — michael-dougherty   [Sync]
Songkick — songkick            [Sync]
```

Clicking Sync triggers both feeds (attendance + tracked artist) for that
username. Progress appears in the existing sync status table, which
already polls via HTMX.

## API Changes

### New Endpoints

**`POST /api/v1/calendar-feeds/songkick/lookup`**

Validate a Songkick username by fetching both iCal feeds. Read-only — no
database writes.

Request: `{"username": "michael-dougherty"}`

Response (200):
```json
{
  "username": "michael-dougherty",
  "plans_count": 3,
  "tracked_artist_count": 104
}
```

Returns 404 if Songkick returns 404 for the iCal feed URL.

**`DELETE /api/v1/calendar-feeds/songkick/{username}`**

Delete both feeds for a Songkick username. Matches feeds by URL pattern
(`songkick.com/users/{username}/calendars.ics`). Returns 404 if no feeds
found for that username.

### Existing Endpoints (used as-is)

- `POST /api/v1/calendar-feeds/songkick` — create the two feeds
- `POST /api/v1/calendar-feeds/{id}/sync` — sync a single feed (dashboard
  calls this once per feed for the username)
- `GET /api/v1/calendar-feeds` — list feeds (used to determine connected
  state and populate dashboard sync buttons)

## Template Changes

### Account Page (`account.html`)

Add after existing service connection buttons:

- "Connect Songkick" button (always visible)
- Songkick entries in the connections list (per username, with Disconnect)

### Dashboard (`dashboard.html`)

Add to the sync controls section:

- Per-Songkick-username sync buttons, conditional on having feeds
- Same HTMX pattern as existing sync buttons (`hx-post`, result in sync
  status table)

### New Partial

`partials/songkick_connect.html` — the three-state connect flow
(button → lookup form → confirmation card), swapped via HTMX.

## UI Routes

New route handlers in `ui/routes.py`:

- Dashboard needs to query `UserCalendarFeed` to render Songkick sync
  buttons (grouped by username)
- Account page needs to query `UserCalendarFeed` to render connected
  Songkick accounts

## Future Considerations

- **Generic iCal feeds**: the Account page currently only handles Songkick.
  Generic iCal URL input can be added later with a similar pattern
  (input URL → validate → confirm → connect).
- **Unified connection model**: `ServiceConnection` and `UserCalendarFeed`
  could be unified into a single "connection" abstraction. Worth a refactor
  when adding more non-OAuth services.
- **User avatar**: Songkick doesn't expose user avatars easily. If they
  add an API or make avatars scrapeable, the confirmation card could show
  the user's profile image.
