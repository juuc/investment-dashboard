<!-- Generated: 2026-03-02 | Updated: 2026-03-02 -->

# NPS Portfolio Tracing System

## Purpose

Track and visualize Korean National Pension Service (국민연금) portfolio composition from official sources. Combines NPS monthly asset allocation, SEC 13F filings (US holdings), and OpenDART disclosure-based Korean market holdings into a single dark-themed web dashboard with Korean-localized UI.

## Key Files

| File | Description |
|------|-------------|
| `package.json` | Node.js manifest (playwright for testing only; no JS build system) |
| `Makefile` | CLI shortcuts: `make refresh`, `make dashboard`, `make test` |
| `.env.example` | Environment variable template (SEC_USER_AGENT, OPENDART_API_KEY, KIS keys) |
| `.gitignore` | Ignores `data/*` (except `.gitkeep`), `__pycache__`, `node_modules` |
| `README.md` | User-facing documentation with setup, CLI flags, and output structure |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `scripts/` | Python data pipeline and dashboard server (see `scripts/AGENTS.md`) |
| `web/` | Frontend static assets (see `web/AGENTS.md`) |
| `tests/` | Python unit tests (see `tests/AGENTS.md`) |
| `data/` | Runtime output directory (gitignored); holds `snapshots/` and `latest/` |

## For AI Agents

### Working In This Directory

- **Stack**: Pure Python backend (stdlib only, no pip dependencies). Frontend is vanilla HTML/CSS/JS (no build step).
- **No TypeScript/Node build**: The `package.json` exists only for playwright. All app logic is Python 3.
- **Env autoload**: Scripts auto-load `.env` and `../korean-dexter/.env` by default.
- **Immutability rule**: Follow `{ ...obj, key: value }` pattern in JS; create new dicts in Python rather than mutating.

### Core Commands

```bash
# Refresh data from all sources
python3 scripts/refresh_portfolio.py

# Start dashboard server
python3 scripts/dashboard_server.py --host 127.0.0.1 --port 8787

# Run tests
python3 -m unittest discover -s tests -p "test_*.py" -v
```

### Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `OPENDART_API_KEY` | Yes (for Korea) | Korea disclosure holdings from OpenDART |
| `KIS_APP_KEY` | Optional | KRW value-based Korea weights via KIS quotes |
| `KIS_APP_SECRET` | Optional | KRW value-based Korea weights via KIS quotes |
| `SEC_USER_AGENT` | Recommended | Improves SEC API reliability |

### Data Flow

```
refresh_portfolio.py
  ├── NPS portal (fund.nps.or.kr) → nps_asset_allocation.json
  ├── SEC EDGAR (data.sec.gov)    → sec_13f_*.json/csv
  └── OpenDART + KIS (optional)   → korea_stock_holdings.json/csv
        ↓
  data/snapshots/{timestamp}/     (archived)
  data/latest/                    (symlinked)
        ↓
dashboard_server.py
  ├── GET /api/dashboard          → aggregated JSON payload
  ├── GET /api/job                → refresh job status
  ├── POST /api/refresh           → trigger background refresh
  └── GET /*                      → static file serving (web/static/)
        ↓
web/static/ (browser)
  └── app.js fetches /api/dashboard and renders all sections
```

### Testing Requirements

- Run `make test` (or `python3 -m unittest discover -s tests -p "test_*.py" -v`)
- Tests import scripts as modules via `importlib.util`
- No external test dependencies required

### Known Interpretation Limits

- SEC 13F covers US-listed positions only (not full global NPS holdings)
- Korea holdings are disclosure-based and may not equal full domestic live portfolio
- "Recently attractive" scoring quality improves as more refresh snapshots accumulate

## Dependencies

### External (Python stdlib only)

- `urllib.request` - HTTP fetching
- `xml.etree.ElementTree` - SEC 13F XML parsing
- `json`, `csv`, `html`, `gzip`, `zlib` - Data processing
- `http.server` - Dashboard web server
- `threading` - Background refresh job execution
- `subprocess` - Dashboard spawns refresh script

### External (Node/npm)

- `playwright` - Browser testing only (not used by app)

## Korea "Recently Attractive" Scoring

Computed in `dashboard_server.py:build_korea_emerging_payload()`:

```
score = 1.8 * delta_short + 0.9 * delta_long + 0.15 * current_weight + new_bonus
```

- Short/long horizon deltas from snapshot history
- `new_bonus` for newly appeared stocks
- Ranked descending by score, then short delta, then current weight

<!-- MANUAL: -->
