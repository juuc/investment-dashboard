<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-02 | Updated: 2026-03-02 -->

# scripts

## Purpose

Python backend containing the data collection pipeline and web dashboard server. These two scripts are the entire backend — no frameworks, no pip dependencies, pure Python stdlib.

## Key Files

| File | Description |
|------|-------------|
| `refresh_portfolio.py` | (~1400 lines) Data pipeline: fetches NPS allocation, SEC 13F filings, OpenDART Korea holdings, optional KIS quote enrichment. Writes timestamped snapshots to `data/`. |
| `dashboard_server.py` | (~500 lines) `http.server`-based API + static file server. Serves dashboard JSON, triggers background refresh jobs, computes Korea "emerging/attractiveness" rankings from snapshot history. |

## For AI Agents

### Working In This Directory

- **No external dependencies**: Everything uses Python stdlib (`urllib.request`, `xml.etree.ElementTree`, `json`, `csv`, `http.server`, `threading`, `subprocess`).
- **Both scripts are importable as modules**: Tests use `importlib.util.spec_from_file_location()` to import them.
- **Type annotations**: Uses `from __future__ import annotations` and `typing.Any`. Follow this pattern.
- **Immutability**: Prefer creating new dicts over mutating existing ones.

### refresh_portfolio.py Architecture

| Section | Functions | Purpose |
|---------|-----------|---------|
| NPS scraping | `fetch_nps_page()`, `parse_nps_*()`, `split_nps_summary_row()` | Scrape NPS portal HTML for monthly asset allocation |
| SEC EDGAR | `fetch_sec_submissions()`, `extract_13f_filings()`, `fetch_13f_holdings()`, `parse_13f_information_table()` | Fetch/parse SEC 13F XML filing data |
| OpenDART Korea | `fetch_opendart_*()`, `collect_korea_holdings()` | Collect Korea KOSPI/KOSDAQ holdings from disclosure API |
| KIS enrichment | `fetch_kis_token()`, `fetch_kis_price()`, `enrich_korea_with_kis()` | Optional: add KRW market values via KIS quote API |
| Weighting | `apply_korea_weights()` | Compute weight_pct from estimated_value_krw or stake_pct |
| Quality | `build_data_quality_warnings()` | Detect anomalies in collected data |
| Snapshot | `write_snapshot()`, `prune_old_snapshots()` | Write timestamped snapshots, manage retention |
| CLI | `parse_args()`, `main()` | Argparse CLI with many flags |

Key CLI flags: `--skip-korea`, `--skip-nps`, `--skip-sec`, `--korea-lookback-days`, `--sec-history`, `--snapshot-retain`, `--top-holdings`

### dashboard_server.py Architecture

| Section | Functions | Purpose |
|---------|-----------|---------|
| Data loading | `load_latest_bundle()`, `load_snapshots_index()`, `load_nps_history()`, `load_korea_history()` | Read JSON/CSV from `data/` directory |
| Emerging calc | `build_korea_emerging_payload()` | Score Korea stocks by short/long-term weight changes |
| Job management | `start_refresh_job()`, `refresh_worker()`, `get_job_state()` | Background thread runs refresh_portfolio.py subprocess |
| HTTP handler | `DashboardHandler` (BaseHTTPRequestHandler) | Routes: GET `/api/dashboard`, `/api/job`, `/api/health`; POST `/api/refresh`; static files |

### Testing Requirements

- Tests are in `tests/test_parsers.py` and `tests/test_dashboard_server.py`
- Tests import these scripts as modules — ensure all top-level functions remain importable
- Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`

### Common Patterns

- `normalize_space(text)` — collapse whitespace for HTML text parsing
- `as_float(value, default)` — safe float conversion with NaN handling
- `read_json(path, default)` / `read_text(path, default)` — safe file reading with fallback
- HTTP responses always use `send_json()` or `send_text()` helper methods
- Background jobs use `threading.Thread(daemon=True)` with a global `JOB_LOCK`

## Dependencies

### Internal

- `dashboard_server.py` spawns `refresh_portfolio.py` as a subprocess
- Both read/write to `data/snapshots/` and `data/latest/`

### External

- Python stdlib only (no pip packages)

<!-- MANUAL: -->
