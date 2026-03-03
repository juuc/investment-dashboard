<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-02 | Updated: 2026-03-02 -->

# static

## Purpose

Vanilla HTML/CSS/JS frontend for the NPS portfolio dashboard. Dark-themed, Korean-localized, no build step. Served directly by `dashboard_server.py`.

## Key Files

| File | Description |
|------|-------------|
| `index.html` | (~265 lines) Dashboard layout: hero header, controls panel, KPI cards, NPS bars, Korea holdings chart/table, Korea emerging section, SEC holdings chart/table, SEC history, snapshots list |
| `app.js` | (~520 lines) Client-side logic: fetches `/api/dashboard`, renders all sections, handles refresh trigger, polls job status, term tooltips, source tooltip panel |
| `styles.css` | (~515 lines) Dark theme with CSS custom properties, glassmorphism panels, gradient bar charts, responsive breakpoints at 960px and 620px |

## For AI Agents

### Working In This Directory

- **No framework**: Pure vanilla JS with DOM manipulation. No React, no bundler.
- **Korean localization**: All UI text is in Korean. Keep new text in Korean.
- **CSS variables**: Use `var(--ink)`, `var(--brand)`, `var(--ok)`, `var(--err)`, etc. from `:root`.
- **Immutability**: Follow `{ ...obj, key: value }` pattern when working with objects in JS.
- **`$()` helper**: `const $ = (id) => document.getElementById(id)` — used throughout `app.js`.

### UI Sections (top to bottom)

1. **Hero header** — Title, metadata (run ID, generated timestamp), data source tooltip
2. **Controls panel** — SEC/Korea options, refresh button, job status pill, log output
3. **KPI grid** — 4 cards: NPS month, AUM, latest 13F date, SEC filings processed
4. **NPS asset allocation** — Horizontal bar chart from `nps.rows`
5. **Korea holdings chart** — Top 30 Korea stocks by weight (orange gradient bars)
6. **Korea holdings table** — Full detail: name, market, code, weight, stake, value, date
7. **Korea emerging** — "Recently attractive" scored stocks (green gradient bars + table)
8. **SEC top holdings chart** — US holdings by weight (blue-teal-orange gradient bars)
9. **SEC top holdings table** — Issuer, class, CUSIP, value (USD bn), weight
10. **SEC filing history** — All 13F submissions with status and total value
11. **Snapshots list** — Recent run IDs with timestamps

### API Contract

| Endpoint | Method | Response Shape |
|----------|--------|---------------|
| `/api/dashboard` | GET | `{ generated_at_utc, latest: { run_manifest, nps, korea, sec_meta, sec_top_holdings, sec_history, report_markdown }, korea_emerging, snapshots, nps_history, job }` |
| `/api/job` | GET | `{ job: { running, started_at_utc, finished_at_utc, exit_code, stdout, stderr, last_error } }` |
| `/api/refresh` | POST | `{ ok, message, job }` — body accepts options like `sec_history`, `korea_lookback_days`, `skip_korea`, etc. |
| `/api/health` | GET | `{ status: "ok", time }` |

### Formatting Helpers in app.js

| Function | Purpose |
|----------|---------|
| `asText(value, fallback)` | Safe string with fallback |
| `fmtNumber(value, digits)` | Locale-formatted number |
| `fmtSigned(value, digits)` | Signed number with `+` prefix |
| `fmtUSDbn(value)` | Convert raw USD to billions |
| `fmtEok(value)` | Convert raw KRW to 억원 (hundred millions) |

### Polling Behavior

- `startPolling()` runs every 2.5 seconds via `setInterval`
- Polls `/api/job` for refresh status
- When job transitions from running to finished, auto-refreshes full dashboard data

### Testing Requirements

- No JS test framework configured
- Manual testing: start server (`make dashboard`), open `http://127.0.0.1:8787`
- Verify all sections render with data after a refresh

### Common Patterns

- Bar charts use `div.bar-row > div.bar-label + div.bar-track > div.bar-fill + div.bar-value`
- Tables use `<table><thead><tbody>` with dynamic row injection via `document.createElement`
- Term tooltips: elements with `data-term-key` attribute get dotted underline + title from `TERM_DEFINITIONS`

## Dependencies

### Internal

- Consumes JSON from `dashboard_server.py` API endpoints
- No JS imports or modules — single `app.js` script tag

### External

- Google Fonts: "Space Grotesk", "Noto Sans KR" (referenced in font-family, not imported in CSS)

<!-- MANUAL: -->
