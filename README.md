# Korean 국민연금 Portfolio Tracing System

Web dashboard + scripted refresh pipeline for tracing 국민연금(NPS) portfolio composition from official sources.

## Reliable Sources

- NPS Fund Management portal (official):
  `https://fund.nps.or.kr/eng/orinsm/ptflobrkdwn/getOHFD0002M0.do`
- SEC EDGAR submissions API (official):
  `https://data.sec.gov/submissions/CIK0001608046.json`
- SEC EDGAR filing archive (official):
  `https://www.sec.gov/Archives/edgar/data/1608046/`

## What Gets Fetched

1. NPS monthly fund-level asset allocation (official monthly disclosure page)
2. Korean market disclosure-based holdings (KOSPI/KOSDAQ) for 국민연금공단 via OpenDART
3. KIS quote-enriched Korea weights (when `KIS_APP_KEY` + `KIS_APP_SECRET` are available)
4. Market indices — KOSPI, KOSDAQ (KIS), NASDAQ, S&P500 (yfinance), Gold (yfinance), Bitcoin (CoinGecko)
5. Market sentiment — 신용잔고/투자자예탁금 ratio via data.go.kr KOFIA Stats API
6. SEC 13F filing history for NPS (default: full available history)
7. Latest filing full holdings CSV + per-filing holdings CSV exports

## Dashboard

Run:

```bash
make dashboard
```

Then open:

```text
http://127.0.0.1:8787
```

From the dashboard, click **Refresh All Data** to run collection. A live progress bar shows step labels and estimated remaining time while data is being fetched.

The dashboard refresh panel also lets you control Korea-specific options (lookback days, request delay, skip Korea) and snapshot retention count.

## CLI Refresh

```bash
make refresh
```

Or directly:

```bash
python3 scripts/refresh_portfolio.py
```

## Environment / Keys

- SEC works best with a clear user-agent string:

```bash
export SEC_USER_AGENT="your-name your-email@example.com"
```

- The script autoloads env files by default:
  - `./.env`
  - `../korean-dexter/.env`

So if keys already exist in `../korean-dexter/.env`, they are reused automatically.

- You can bootstrap local config from `.env.example`:

```bash
cp .env.example .env
```

- Korea section keys:
  - Required for disclosure holdings: `OPENDART_API_KEY`
  - Optional for price/value-based weights + KOSPI/KOSDAQ indices: `KIS_APP_KEY`, `KIS_APP_SECRET`
  - Optional for 신용잔고/투자자예탁금 sentiment: `DATA_GO_KR_KOFIA_STATS_API_KEY`

Disable autoload:

```bash
python3 scripts/refresh_portfolio.py --no-env-autoload
```

Add custom env file:

```bash
python3 scripts/refresh_portfolio.py --env-file ../korean-dexter/.env
```

## Output Structure

Each run creates a timestamped snapshot:

```text
data/
  snapshots/
    20260226T000000Z/
      nps_asset_allocation.json
      korea_stock_holdings.json
      korea_stock_holdings.csv
      sec_13f_meta.json
      sec_13f_top_holdings.json
      sec_13f_all_holdings.csv
      sec_13f_filings_history.json
      sec_13f_filing_holdings/
        <accession>.csv
      market_indices.json
      market_sentiment.json
      report.md
      run_manifest.json
  latest/
    ...same files from latest run...
```

`data/latest/` is always overwritten with the most recent successful run.

## Useful Flags

```bash
python3 scripts/refresh_portfolio.py --sec-history latest
python3 scripts/refresh_portfolio.py --sec-max-filings 20
python3 scripts/refresh_portfolio.py --top-holdings 50
python3 scripts/refresh_portfolio.py --korea-lookback-days 180
python3 scripts/refresh_portfolio.py --korea-request-delay 0.2
python3 scripts/refresh_portfolio.py --snapshot-retain 120
python3 scripts/refresh_portfolio.py --timeout 45 --retries 4
```

## Tests

```bash
make test
```

## Coverage Notes

- SEC 13F is a U.S.-listed long-equity subset, not the full global NPS portfolio.
- NPS monthly portfolio figures are published as provisional on the source page.
- Korea holdings are based on disclosure filings (OpenDART) and may not represent full domestic portfolio inventory.
