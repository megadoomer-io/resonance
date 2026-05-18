# Concert Archives Integration

Research and planning for integrating Concert Archives as a data source.

## Background

Concert Archives (concertarchives.org) is a community-driven platform for
documenting live music history -- concert attendance, setlists, venues, photos,
and videos. It has significantly richer historical coverage than Songkick,
which is currently the primary event data source for Resonance.

Concert Archives does not currently offer a public API, but accepts data
requests and is tracking community interest in API access. They may provide
spreadsheet exports with a licensing agreement, or lightweight access on a
case-by-case basis.

## Value to Resonance

- **Historical concert data** -- Concert Archives covers past shows far better
  than Songkick's iCal feeds, which are primarily forward-looking
- **Upcoming events** -- users can mark future concerts they plan to attend
- **Richer artist-to-event mapping** -- setlist integration via Setlist.fm
  already exists on their platform
- **Cross-source timeline** -- combining Concert Archives attendance with
  Songkick, Spotify listening history, and Last.fm/ListenBrainz scrobbles
  creates a more complete picture for playlist generation

## Existing Playlist Overlap

Concert Archives already offers per-concert playlist links (YouTube and
Spotify) based on setlists sourced from Setlist.fm. Resonance's approach is
complementary -- generating "inspired by" playlists that blend concert
attendance with the user's listening history and taste signals, rather than
recreating the setlist verbatim.

## Data Request

Concert Archives requests the following information for data access:

- Organization name and type
- Planned purpose and project description
- Distribution model
- Spreadsheet export vs API access preference
- Scope of data requested
- Additional context

A draft email covering all of these points has been prepared and is ready to
send to data@concertarchives.org. The request emphasizes:

- Personal, non-commercial, open-source project
- Narrow scope: single-user attendance history, not bulk data
- API access preferred for ongoing sync (similar to MusicBrainz/ListenBrainz)
- Offer to demo the app via Last.fm/ListenBrainz OAuth or whitelisted Spotify

## Next Steps

1. **Review and send** the data request email to data@concertarchives.org
2. **Wait for response** -- they review requests case-by-case
3. **If API access granted:** Design a Concert Archives connector following the
   existing `Connectable` protocol pattern (similar to Songkick)
4. **If spreadsheet export offered:** Evaluate whether a one-time import is
   useful as a bootstrap, with API access as the long-term goal
5. **If deferred:** Stay on the API interest list; revisit when they announce
   availability

## Technical Notes

- Concert Archives would likely follow the lightweight `Connectable` protocol
  (like Songkick) rather than the full `BaseConnector` class, since it doesn't
  involve OAuth token management
- Authentication would probably be username-based (user provides their Concert
  Archives username, Resonance fetches their public profile data)
- Rate limiting, caching, and incremental sync are already part of the
  connector architecture
- The concert data model (Venue, Event, EventArtist, UserEventAttendance) is
  already in place from the Songkick integration
