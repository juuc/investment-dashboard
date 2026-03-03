<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-02 | Updated: 2026-03-02 -->

# tests

## Purpose

Python unit tests covering the data pipeline parsers and dashboard server logic. Uses stdlib `unittest` only — no pytest or external test libraries.

## Key Files

| File | Description |
|------|-------------|
| `test_parsers.py` | Tests for `refresh_portfolio.py`: NPS HTML parsing, SEC 13F XML parsing, env file loading, NPS summary row splitting, Korea weighting logic, data quality warnings |
| `test_dashboard_server.py` | Tests for `dashboard_server.py`: Korea emerging ranking algorithm, dashboard payload contract, refresh job option wiring |

## For AI Agents

### Working In This Directory

- **Module loading pattern**: Both test files import scripts via `importlib.util.spec_from_file_location()` — they do NOT use normal `import`. This is because the scripts are standalone Python files, not installed packages.
- **No test dependencies**: Pure `unittest` — no pytest, no mocking libraries.
- **Monkey-patching**: `test_dashboard_server.py` patches module functions directly (e.g., `MODULE.load_korea_history = lambda ...`) and restores them in `finally` blocks.

### Test Coverage Map

| Test Class | Source File | What It Tests |
|------------|-------------|---------------|
| `NpsParserTests` | `refresh_portfolio.py` | `normalize_space()`, `parse_nps_as_of_month()`, `parse_nps_total_aum()`, `parse_nps_assets()` |
| `SecParserTests` | `refresh_portfolio.py` | `extract_13f_filings()`, `parse_13f_information_table()` |
| `EnvLoaderTests` | `refresh_portfolio.py` | `load_env_file()` — creates temp .env, verifies os.environ |
| `NpsModelingTests` | `refresh_portfolio.py` | `split_nps_summary_row()` — parent total detection vs regular row |
| `KoreaWeightingTests` | `refresh_portfolio.py` | `apply_korea_weights()` — estimated_value_krw vs stake_pct fallback |
| `DataQualityTests` | `refresh_portfolio.py` | `build_data_quality_warnings()` — anomaly detection |
| `KoreaEmergingTests` | `dashboard_server.py` | `build_korea_emerging_payload()` — ranking with mock history |
| `DashboardPayloadContractTests` | `dashboard_server.py` | `build_dashboard_payload()` — response shape contract |
| `RefreshOptionWiringTests` | `dashboard_server.py` | `start_refresh_job()` — CLI args constructed correctly |

### Running Tests

```bash
# All tests
python3 -m unittest discover -s tests -p "test_*.py" -v

# Or via Makefile
make test
```

### Common Patterns

- `load_refresh_module()` / `load_dashboard_module()` — module import helpers at top of each file
- `MODULE = load_*_module()` — global module reference used by all test classes
- Cleanup in `finally` blocks — restore monkey-patched functions and environment variables

## Dependencies

### Internal

- Imports `scripts/refresh_portfolio.py` as module
- Imports `scripts/dashboard_server.py` as module

### External

- Python stdlib: `unittest`, `importlib.util`, `tempfile`, `os`, `pathlib`

<!-- MANUAL: -->
