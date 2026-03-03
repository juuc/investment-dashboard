#!/usr/bin/env python3
"""
Fetch market data: indices, sentiment, gold, and bitcoin.

Data sources:
- KIS Open API: KOSPI, KOSDAQ indices
- yfinance: NASDAQ, S&P500, Gold futures
- CoinGecko: Bitcoin price (USD + KRW)
- data.go.kr KOFIA Stats API: 신용잔고, 투자자예탁금
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Shared helpers (keep in sync with refresh_portfolio.py conventions)
# ---------------------------------------------------------------------------

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
DEFAULT_USER_AGENT = "InvestDashboard/1.0 contact@example.com"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
        if num != num:  # NaN check
            return default
        return num
    except (TypeError, ValueError):
        return default


def _safe_get_json(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> dict[str, Any] | None:
    """Simple HTTP GET returning parsed JSON, or None on failure."""
    req_headers = {"Accept": "application/json", "User-Agent": DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)
    request = urllib.request.Request(url=url, headers=req_headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except Exception:
        return None


def _safe_post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 15,
) -> dict[str, Any] | None:
    """Simple HTTP POST returning parsed JSON, or None on failure."""
    req_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if headers:
        req_headers.update(headers)
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url=url, headers=req_headers, method="POST", data=body)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# KIS index data (KOSPI, KOSDAQ)
# ---------------------------------------------------------------------------

# KIS index inquiry uses the domestic index price endpoint.
# tr_id FHPUP02100000 = 업종(지수) 현재가 조회
KIS_INDEX_CODES: dict[str, str] = {
    "KOSPI": "0001",   # KOSPI composite
    "KOSDAQ": "1001",  # KOSDAQ composite
}


def fetch_kis_access_token(app_key: str, app_secret: str) -> str | None:
    """Get a KIS OAuth token. Returns None on failure."""
    result = _safe_post_json(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        payload={
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret,
        },
    )
    if not result:
        return None
    token = str(result.get("access_token", "")).strip()
    return token or None


def fetch_kis_index(
    access_token: str,
    app_key: str,
    app_secret: str,
    index_code: str,
) -> dict[str, Any] | None:
    """Fetch a single KIS market index. Returns {value, change_pct} or None."""
    query = urllib.parse.urlencode({
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": index_code,
    })
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-index-price?{query}"
    headers = {
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHPUP02100000",
        "content-type": "application/json; charset=utf-8",
    }
    result = _safe_get_json(url, headers=headers)
    if not result or str(result.get("rt_cd")) != "0":
        return None

    output = result.get("output", {})
    if not isinstance(output, dict):
        return None

    value = _as_float(output.get("bstp_nmix_prpr"), 0.0)  # 업종 지수 현재가
    change_pct = _as_float(output.get("bstp_nmix_prdy_ctrt"), 0.0)  # 전일 대비 등락율

    if value <= 0:
        return None

    return {"value": round(value, 2), "change_pct": round(change_pct, 2)}


def fetch_kis_indices(app_key: str, app_secret: str) -> dict[str, dict[str, Any]]:
    """Fetch KOSPI and KOSDAQ indices via KIS API. Returns partial results on failure."""
    if not app_key or not app_secret:
        return {}

    token = fetch_kis_access_token(app_key, app_secret)
    if not token:
        return {}

    results: dict[str, dict[str, Any]] = {}
    for name, code in KIS_INDEX_CODES.items():
        data = fetch_kis_index(token, app_key, app_secret, code)
        if data:
            results[name] = {**data, "source": "KIS"}

    return results


# ---------------------------------------------------------------------------
# Global indices via yfinance (NASDAQ, S&P500)
# ---------------------------------------------------------------------------


def fetch_global_indices_yfinance() -> dict[str, dict[str, Any]]:
    """Fetch NASDAQ and S&P500 via yfinance. Returns partial results on failure."""
    try:
        import yfinance as yf  # noqa: F811
    except ImportError:
        return {}

    symbols = {
        "NASDAQ": "^IXIC",
        "SP500": "^GSPC",
    }
    results: dict[str, dict[str, Any]] = {}

    for name, ticker_symbol in symbols.items():
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.fast_info
            price = _as_float(getattr(info, "last_price", None), 0.0)
            prev_close = _as_float(getattr(info, "previous_close", None), 0.0)

            if price <= 0:
                continue

            change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
            results[name] = {
                "value": round(price, 2),
                "change_pct": round(change_pct, 2),
                "source": "yfinance",
            }
        except Exception:
            continue

    return results


# ---------------------------------------------------------------------------
# Gold via yfinance
# ---------------------------------------------------------------------------


def fetch_gold_price_yfinance() -> dict[str, Any] | None:
    """Fetch gold futures (GC=F) via yfinance. Returns {value, change_pct} or None."""
    try:
        import yfinance as yf  # noqa: F811
    except ImportError:
        return None

    try:
        ticker = yf.Ticker("GC=F")
        info = ticker.fast_info
        price = _as_float(getattr(info, "last_price", None), 0.0)
        prev_close = _as_float(getattr(info, "previous_close", None), 0.0)

        if price <= 0:
            return None

        change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
        return {
            "value": round(price, 2),
            "change_pct": round(change_pct, 2),
            "source": "yfinance",
            "unit": "USD/oz",
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Bitcoin via CoinGecko
# ---------------------------------------------------------------------------


def fetch_bitcoin_price() -> dict[str, Any] | None:
    """Fetch Bitcoin price in USD and KRW from CoinGecko free API."""
    url = f"{COINGECKO_API_BASE}/simple/price?ids=bitcoin&vs_currencies=usd,krw&include_24hr_change=true"
    data = _safe_get_json(url)
    if not data or "bitcoin" not in data:
        return None

    btc = data["bitcoin"]
    usd_price = _as_float(btc.get("usd"), 0.0)
    krw_price = _as_float(btc.get("krw"), 0.0)
    change_pct = _as_float(btc.get("usd_24h_change"), 0.0)

    if usd_price <= 0:
        return None

    return {
        "value": round(usd_price, 2),
        "value_krw": round(krw_price, 0),
        "change_pct": round(change_pct, 2),
        "source": "CoinGecko",
        "unit": "USD",
    }


# ---------------------------------------------------------------------------
# Market indices orchestrator
# ---------------------------------------------------------------------------


def fetch_market_indices() -> dict[str, Any]:
    """Fetch all market indices. Returns combined payload. Partial failure is OK."""
    indices: dict[str, dict[str, Any]] = {}

    # KIS: KOSPI, KOSDAQ
    app_key = os.getenv("KIS_APP_KEY", "").strip()
    app_secret = os.getenv("KIS_APP_SECRET", "").strip()
    kis_data = fetch_kis_indices(app_key, app_secret)
    indices.update(kis_data)

    # yfinance: NASDAQ, S&P500
    global_data = fetch_global_indices_yfinance()
    indices.update(global_data)

    # Gold
    gold = fetch_gold_price_yfinance()
    if gold:
        indices["GOLD"] = gold

    # Bitcoin
    btc = fetch_bitcoin_price()
    if btc:
        indices["BTC"] = btc

    return {
        "fetched_at_utc": _now_utc_iso(),
        "indices": indices,
    }


# ---------------------------------------------------------------------------
# 신용잔고 / 투자자예탁금 Sentiment (data.go.kr KOFIA Stats API)
# ---------------------------------------------------------------------------

# dataset 15094809 — KOFIA 통합 통계 API
DATA_GO_KR_BASE = "https://apis.data.go.kr/1160100/GetKofiaStatService"


def fetch_credit_balance(api_key: str) -> float | None:
    """Fetch latest 신용공여잔고 (credit balance) in KRW from KOFIA Stats API.
    Returns amount in KRW or None on failure."""
    params = urllib.parse.urlencode({
        "serviceKey": api_key,
        "resultType": "json",
        "numOfRows": "1",
        "pageNo": "1",
    })
    url = f"{DATA_GO_KR_BASE}/getCreditTrdgList?{params}"
    data = _safe_get_json(url, timeout=20)
    if not data:
        return None

    try:
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if not isinstance(items, list) or not items:
            return None
        # Use the first (most recent) item
        balance_str = items[0].get("crdtBlnc", "0")
        return _as_float(balance_str, 0.0) * 1_000_000  # million KRW → KRW
    except Exception:
        return None


def fetch_investor_deposits(api_key: str) -> float | None:
    """Fetch latest 투자자예탁금 (investor deposits) in KRW from KOFIA Stats API.
    Returns amount in KRW or None on failure."""
    params = urllib.parse.urlencode({
        "serviceKey": api_key,
        "resultType": "json",
        "numOfRows": "1",
        "pageNo": "1",
    })
    url = f"{DATA_GO_KR_BASE}/getStockMktFundList?{params}"
    data = _safe_get_json(url, timeout=20)
    if not data:
        return None

    try:
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if not isinstance(items, list) or not items:
            return None
        deposit_str = items[0].get("invstrDpstAmt", "0")
        return _as_float(deposit_str, 0.0) * 1_000_000  # million KRW → KRW
    except Exception:
        return None


def compute_credit_ratio(credit_balance: float, investor_deposits: float) -> float:
    """Compute 신용잔고/투자자예탁금 ratio as a percentage."""
    if investor_deposits <= 0:
        return 0.0
    return (credit_balance / investor_deposits) * 100.0


def compute_sentiment_signal(ratio_pct: float) -> str:
    """Map credit ratio to a sentiment signal.

    ratio < 30%  → "normal"  (정상)
    30-35%       → "watch"   (관찰)
    35-36%       → "warning" (주의)
    ≥ 36%        → "danger"  (위험) — COVID peak
    """
    if ratio_pct < 30.0:
        return "normal"
    if ratio_pct < 35.0:
        return "watch"
    if ratio_pct < 36.0:
        return "warning"
    return "danger"


def fetch_market_sentiment() -> dict[str, Any]:
    """Fetch 신용잔고/투자자예탁금 data and compute sentiment signal."""
    api_key = os.getenv("DATA_GO_KR_KOFIA_STATS_API_KEY", "").strip()
    if not api_key:
        return {
            "fetched_at_utc": _now_utc_iso(),
            "status": "skipped",
            "reason": "DATA_GO_KR_KOFIA_STATS_API_KEY not set",
        }

    credit = fetch_credit_balance(api_key)
    deposits = fetch_investor_deposits(api_key)

    if credit is None or deposits is None:
        return {
            "fetched_at_utc": _now_utc_iso(),
            "status": "error",
            "reason": "failed to fetch credit/deposit data from KOFIA API",
            "credit_balance_krw": credit,
            "investor_deposits_krw": deposits,
        }

    ratio = compute_credit_ratio(credit, deposits)
    signal = compute_sentiment_signal(ratio)

    return {
        "fetched_at_utc": _now_utc_iso(),
        "status": "ok",
        "credit_balance_krw": credit,
        "investor_deposits_krw": deposits,
        "credit_ratio_pct": round(ratio, 2),
        "signal": signal,
        "danger_threshold_pct": 35.0,
        "covid_peak_pct": 36.0,
    }


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
