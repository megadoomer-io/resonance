# Design System — Resonance

## Product Context
- **What this is:** Personal music discovery platform that aggregates listening data and concert information
- **Who it's for:** Music enthusiasts who track listening across services and attend concerts
- **Space/industry:** Music listening trackers (Last.fm, stats.fm, ListenBrainz) and concert discovery
- **Project type:** Data-dense web app / personal dashboard with themeable UI

## Aesthetic Direction
- **Direction:** Industrial/Utilitarian with editorial warmth
- **Decoration level:** Intentional (subtle sound wave motif as structural element, not ornamental)
- **Mood:** Calm confidence. A well-designed music magazine that happens to be your concert diary. Function-first, data-dense, but with a distinctive typographic voice.
- **Positioning:** Warm + editorial in a category dominated by cold, dark, sans-serif dashboards. Serif headings and cream-tinted neutrals occupy uncontested visual space.

## Typography
- **Display/Headings:** Instrument Serif — modern serif with personality, distinctive vs every sans-serif music tracker. Used for h1-h4.
- **Body:** DM Sans — geometric sans that pairs well with Instrument Serif. Good tabular-nums support for data tables.
- **UI/Labels:** DM Sans (same as body)
- **Data/Tables:** DM Sans with `font-variant-numeric: tabular-nums`
- **Code:** JetBrains Mono
- **Loading:** Google Fonts CDN (`fonts.googleapis.com`)
- **Scale:** 1.25 major third — 16px (base) / 20px (h4) / 25px (h3) / 31px (h2) / 39px (h1)
- **Tracking:**
  - Logo wordmark: `-0.04em` (tight, letters flow together like a sound wave)
  - Headings (h1-h4): `0.04em` (open, airy, readable)
  - Body: `normal` (default)
- **Line height:** 1.6 body, 1.2 headings

## Color
- **Approach:** Restrained (one accent + warm neutrals)
- **Primary accent:** `#2563EB` — clear, confident blue. Neutral enough to work with album art of any color. Not Spotify-green or Last.fm-red.
- **Primary hover:** `#1D4ED8`
- **Primary subtle:** `#EFF6FF` (light mode backgrounds for info states)
- **Warm neutrals:**
  - Background: `#FAFAF8` (cream-tinted, not pure white)
  - Surface: `#FFFFFF` (cards, panels)
  - Surface alt: `#F3F3F0` (code blocks, specimens)
  - Border: `#E5E5E0`
  - Border strong: `#D4D4CF`
  - Text muted: `#6B6B66`
  - Text: `#1C1C1A`
- **Service colors** (brand-dictated, not themeable):
  - Spotify: `#1DB954`
  - Last.fm: `#D51007`
  - ListenBrainz: `#E66000`
- **Semantic:** success `#16A34A`, warning `#D97706`, error `#DC2626`, info `#2563EB`
- **Dark mode:**
  - Background: `#121214`
  - Surface: `#1E1E21`
  - Surface alt: `#2A2A2E`
  - Border: `#2E2E32`
  - Border strong: `#3E3E42`
  - Text: `#E8E8E6`
  - Text muted: `#9B9B96`
  - Primary: `#3B82F6` (desaturated 10%)

## Spacing
- **Base unit:** 8px
- **Density:** Comfortable
- **Scale:** 2xs(4) xs(8) sm(12) md(16) lg(24) xl(32) 2xl(48) 3xl(64)

## Layout
- **Approach:** Grid-disciplined (Pico CSS provides the grid)
- **Max content width:** 1200px
- **Border radius:** sm(4px) md(8px) lg(12px) full(9999px for pills/presets)

## Motion
- **Approach:** Minimal-functional
- **Easing:** enter(ease-out) exit(ease-in) move(ease-in-out)
- **Duration:** micro(50-100ms) short(150-250ms) medium(250-400ms)
- **Rules:** Transitions only for state changes (filter results, pagination). No entrance animations. Respects `prefers-reduced-motion`.

## Sound Wave Motif
- Subtle SVG sine wave pattern used as section dividers and page background texture
- Opacity 3-8% depending on context (structural, not decorative)
- Ties the product name to the visual identity without being a gimmick
- Not used as a logo (the italic Instrument Serif wordmark with tight tracking is the logo)

## Theme System
A theme provides these CSS custom properties. Any valid theme supplies these variables and the whole app adapts:

```css
/* Required theme variables */
--bg              /* Page background */
--surface         /* Card/panel background */
--surface-alt     /* Alternate surface (code blocks, alt rows) */
--text            /* Primary text color */
--text-muted      /* Secondary/muted text */
--primary         /* Accent color (links, buttons, active states) */
--primary-hover   /* Accent hover state */
--primary-subtle  /* Light tint for info backgrounds */
--border          /* Default border color */
--border-strong   /* Emphasized borders (table headers) */
--success         /* Semantic: success */
--warning         /* Semantic: warning */
--error           /* Semantic: error */
--heading-font    /* Font family for h1-h4 */
--body-font       /* Font family for body text */
--mono-font       /* Font family for code/data */
```

Service brand colors (`--spotify`, `--lastfm`, `--listenbrainz`) are NOT themeable. They are brand-dictated constants.

## CSS Utility Classes
Defined in `static/filters.css`:
- `.btn-sm` / `.btn-xs` — compact button sizes for in-table actions
- `.inline-form` — side-by-side form buttons in table cells
- `.service-badge` + `.service-badge-spotify` / `.service-badge-lastfm` / `.service-badge-lb`
- `.text-muted` / `.row-accepted` / `.row-rejected` — text and row state styling

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-05-22 | Initial design system | Created by /design-consultation. Serif headings + warm neutrals chosen to differentiate from the cold/dark/sans-serif category norm. Theme system designed for future multi-user support. |
| 2026-05-22 | Instrument Serif for headings | No music tracker uses serif type. Makes Resonance look editorial, not SaaS. |
| 2026-05-22 | Logo tracking -0.04em | Tight tracking on italic Instrument Serif makes letterforms flow together like a sound wave. Contrasts with open heading tracking (0.04em). |
| 2026-05-22 | Warm neutrals (#FAFAF8 bg) | Category drowns in pure whites and cool grays. Cream tint feels analog, like a concert program or vinyl sleeve. |
