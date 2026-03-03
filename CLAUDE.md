# Project Rules

## Stack

- **Python 3** — pip dependencies listed in `requirements.txt` (`pip3 install -r requirements.txt`)
- **Vanilla HTML/CSS/JS** — no frameworks, no bundler, no npm build step
- The `package.json` exists only for playwright; it is not part of the app

## Language

- All dashboard UI text must be in **Korean**
- Code comments and variable names stay in English

## Commands

```bash
make install    # Create .venv and install pip dependencies
make refresh    # Refresh data from all sources (uses .venv)
make dashboard  # Start dashboard server at http://127.0.0.1:8787 (uses .venv)
make test       # Run all unit tests
```

## Before Committing

- Run `make test` — all 23 tests must pass
- Never commit `data/` contents (gitignored) or `.env` files

## Coding Conventions

- `from __future__ import annotations` at top of every Python file
- Use `typing.Any` — avoid bare `any` type
- Safe fallbacks: use `read_json(path, default)` / `as_float(value, default)` patterns
- Background jobs use `threading.Thread(daemon=True)` with `JOB_LOCK`

## Environment

- `OPENDART_API_KEY` required for Korea data
- `KIS_APP_KEY` + `KIS_APP_SECRET` optional (enables KRW value-based weights + KOSPI/KOSDAQ indices)
- `SEC_USER_AGENT` recommended for SEC API reliability
- `DATA_GO_KR_KOFIA_STATS_API_KEY` optional (enables 신용잔고/투자자예탁금 sentiment indicator)
- Scripts auto-load `.env` and `../korean-dexter/.env`
