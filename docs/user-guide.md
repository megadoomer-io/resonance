# User Guide

Resonance is a personal media discovery platform that pulls your listening
history from multiple music services into one place. This guide walks you
through getting started, syncing your data, and browsing your library.

## Getting Started

Open Resonance in your browser. The login page presents three options:

- **Connect with Spotify** -- uses Spotify OAuth
- **Connect with ListenBrainz** -- uses MusicBrainz OAuth
- **Connect with Last.fm** -- uses Last.fm web authentication

Pick whichever service you already use. You will be redirected to that
service's login/authorization page, where you grant Resonance read access to
your listening data. Once you approve, you land on the dashboard.

You only need one service to get started. You can connect additional services
later from the Account page.

## Dashboard

The dashboard is the home screen after you log in. It shows three things at a
glance:

1. **Stats cards** -- your total artist count, track count, and listening
   event count.
2. **Connected Services** -- a table listing every service you have linked,
   when it was connected, when it was last used, and a sync control for each
   one.
3. **Sync Status** -- a live view of recent sync jobs, including progress
   bars, time estimates, and results.

### Sync controls

Each connected service has a **Sync Now** button. Clicking it starts a
background sync for that service. While a sync is running, the button changes
to a "Syncing..." indicator with a **Cancel** button if you need to stop it
early.

If a sync gets rate-limited by the upstream service, it enters a "Deferred"
state and automatically resumes after the cooldown period. You will see the
estimated resume time on the dashboard.

### Live progress

The Sync Status section polls the server every few seconds while a sync is
active. You can see:

- Which step of the sync is running (e.g., fetching recent plays, fetching
  saved tracks)
- A progress bar with item counts and estimated time remaining
- Completed steps marked with a checkmark
- Final results showing how many items were created or updated

You can safely navigate away from the dashboard while a sync is running. Come
back any time to check progress.

## Connecting Services

You can connect multiple services to Resonance. Each service provides
different types of data:

| Service       | What it provides                                      |
|---------------|-------------------------------------------------------|
| Spotify       | Listening history, followed artists, saved/liked tracks |
| Last.fm       | Listening history (scrobbles), loved tracks           |
| ListenBrainz  | Listening history                                     |

To connect an additional service, go to the **Account** page and use the
"Connect Another Service" buttons at the bottom.

When you connect a new service, Resonance checks whether that service account
is already linked to a different Resonance account (for example, if you
previously logged in with Spotify and are now logging in with Last.fm). If a
conflict is found, you are taken to a merge page where you can combine the two
accounts. See [Account Management](#account-management) for details.

Data from all connected services is merged and deduplicated automatically.
Artists and tracks that appear across services are matched and consolidated so
you see one unified library.

## Syncing Your Data

### Triggering a sync

There are two ways to start a sync:

- **Dashboard** -- click the **Sync Now** button next to any connected
  service
- **CLI** -- run `resonance-api sync <service>` (e.g., `resonance-api sync
  spotify`)

By default, a sync is **incremental**: it only fetches data that is new since
the last sync. This is fast and avoids re-processing your entire history.

To force a **full sync** that re-fetches everything from the beginning, use
the CLI with the `--full` flag:

```
resonance-api sync spotify --full
```

### Background processing

Syncs run in the background. You do not need to keep the page open. The
dashboard's sync status section will show current progress whenever you check
back.

A sync for a service with a large listening history (tens of thousands of
scrobbles on Last.fm, for example) can take several minutes. The progress
display shows estimated time remaining so you know what to expect.

### Rate limiting

Music service APIs have rate limits. If Resonance hits a rate limit during a
sync, the job is automatically deferred and resumes after the cooldown. You
will see a "Deferred until HH:MM" status on the dashboard. No action is
needed on your part.

### Auto-dedup

After each successful sync completes, Resonance automatically runs
deduplication to merge any duplicate artists, tracks, or listening events that
were created from overlapping data across services.

## Browsing Your Library

Once you have synced data, three library pages become useful:

### Artists

The **Artists** page (`/artists`) shows all artists in your library, sorted
alphabetically. Each row displays the artist name and which services they were
found on (e.g., "spotify, lastfm").

The list is paginated -- use the **Previous** and **Next** buttons to move
through pages.

### Tracks

The **Tracks** page (`/tracks`) lists all tracks sorted by title. Each row
shows the track title, artist name, and which services the track was found
on.

Like the Artists page, tracks are paginated.

### History

The **History** page (`/history`) shows your listening events in reverse
chronological order (most recent first). Each row includes:

- Track title
- Artist name
- When you listened (date and time, adjusted to your timezone)
- Which service reported the listen (e.g., "spotify", "lastfm",
  "listenbrainz")

History is also paginated. All pages use fast in-page navigation, so flipping
between pages does not require a full page reload.

## Account Management

The **Account** page (`/account`) is where you manage your profile and
service connections.

### Profile

Your display name, email (if provided by the service you logged in with), and
the date you joined are shown at the top.

### Timezone

Resonance displays all timestamps in your local timezone. On first login, it
auto-detects your timezone from your browser. You can change it manually from
the Account page by selecting a timezone from the dropdown and clicking
**Save**.

### Connected services

The Account page lists all your connected services with their external
account IDs and connection dates. You can **Disconnect** a service if you no
longer want to sync from it.

There is one restriction: you cannot disconnect your last remaining service.
At least one connection must remain so you can still log in. If a service is
your only connection, the Disconnect button is replaced with a "Last
connection" label.

### Account merge

If you logged in with different services on separate occasions and
accidentally created two Resonance accounts, the system detects this when you
try to connect a service that is already linked to another account.

When this happens, you are taken to the **Merge Accounts** page. It shows a
summary of the data that would be merged into your current account:

- Service connections
- Listening events
- Artist relations
- Track relations
- Sync job history

Click **Merge into my account** to combine everything. The other account is
deleted after the merge. If you change your mind, click **Cancel** to go back
to the Account page without merging.

### Logging out

A **Log Out** button at the bottom of the Account page ends your session.
