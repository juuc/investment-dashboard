<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-02 | Updated: 2026-03-02 -->

# web

## Purpose

Container directory for frontend web assets served by `dashboard_server.py`.

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `static/` | HTML, JS, CSS for the dashboard UI (see `static/AGENTS.md`) |

## For AI Agents

### Working In This Directory

- No build system — files are served directly by Python's `http.server`
- All static files live in `web/static/`
- The dashboard server resolves `web/static/` via `PROJECT_ROOT / "web" / "static"`

<!-- MANUAL: -->
