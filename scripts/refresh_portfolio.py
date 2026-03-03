#!/usr/bin/env python3
"""
Refresh Korean National Pension Service (NPS, 국민연금) portfolio traces.

Data sources:
- NPS official portfolio page (monthly, provisional): fund.nps.or.kr
- SEC EDGAR for NPS 13F filings (U.S. listed holdings only): sec.gov
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

NPS_PORTFOLIO_URL = "https://fund.nps.or.kr/eng/orinsm/ptflobrkdwn/getOHFD0002M0.do"
SEC_CIK = "0001608046"
SEC_SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{SEC_CIK.zfill(10)}.json"
DEFAULT_ENV_FILES = [
    str(Path.home() / ".claude" / "credentials" / "shared.env"),
    ".env",
    "../korean-dexter/.env",
]
DEFAULT_SEC_USER_AGENT = "NPS Portfolio Dashboard contact@example.com"
OPENDART_BASE_URL = "https://opendart.fss.or.kr/api"
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
NPS_KOREA_REPORTER_KEYWORD = "국민연금공단"

# Canonical output order for the NPS asset-allocation table.
NPS_ASSETS = [
    ("financials", "Financials"),
    ("domestic_equity", "Domestic Equity"),
    ("domestic_fixed_income", "Domestic Fixed Income"),
    ("global_equity", "Global Equity"),
    ("global_fixed_income", "Global Fixed Income"),
    ("alternatives", "Alternatives"),
    ("short_term_assets", "Short-term Assets"),
    ("welfare_others", "Welfare/Others"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh NPS portfolio composition from official sources."
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Base output directory (default: data).",
    )
    parser.add_argument(
        "--top-holdings",
        type=int,
        default=30,
        help="Top N 13F holdings to include in markdown report (default: 30).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retry count for transient failures (default: 3).",
    )
    parser.add_argument(
        "--skip-nps",
        action="store_true",
        help="Skip NPS portfolio collection.",
    )
    parser.add_argument(
        "--skip-sec",
        action="store_true",
        help="Skip SEC 13F collection.",
    )
    parser.add_argument(
        "--skip-korea",
        action="store_true",
        help="Skip Korea (KOSPI/KOSDAQ) disclosure-based holdings collection.",
    )
    parser.add_argument(
        "--skip-market-indices",
        action="store_true",
        help="Skip market indices collection (KOSPI, KOSDAQ, NASDAQ, S&P500, Gold, Bitcoin).",
    )
    parser.add_argument(
        "--skip-market-sentiment",
        action="store_true",
        help="Skip market sentiment collection (신용잔고/투자자예탁금 ratio).",
    )
    parser.add_argument(
        "--sec-user-agent",
        default=os.getenv("SEC_USER_AGENT", "").strip(),
        help=(
            "User-Agent for SEC requests. "
            "If omitted, falls back to SEC_USER_AGENT env var and then a default contact string."
        ),
    )
    parser.add_argument(
        "--sec-history",
        choices=["latest", "full"],
        default="full",
        help=(
            "SEC collection depth: latest filing only or full available 13F filing history "
            "(default: full)."
        ),
    )
    parser.add_argument(
        "--sec-max-filings",
        type=int,
        default=0,
        help=(
            "Maximum number of SEC 13F filings to process (0 means no limit; default: 0)."
        ),
    )
    parser.add_argument(
        "--sec-request-delay",
        type=float,
        default=0.2,
        help="Delay in seconds between SEC filing requests (default: 0.2).",
    )
    parser.add_argument(
        "--korea-lookback-days",
        type=int,
        default=365,
        help=(
            "Lookback days for scanning Korea disclosures to discover NPS-related filings "
            "(default: 365)."
        ),
    )
    parser.add_argument(
        "--korea-request-delay",
        type=float,
        default=0.1,
        help="Delay in seconds between OpenDART/KIS requests for Korea data (default: 0.1).",
    )
    parser.add_argument(
        "--snapshot-retain",
        type=int,
        default=120,
        help=(
            "Number of snapshot directories to keep under data/snapshots "
            "(0 means keep all; default: 120)."
        ),
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        help=(
            "Extra env file path to load KEY=VALUE pairs from. "
            "Can be passed multiple times."
        ),
    )
    parser.add_argument(
        "--no-env-autoload",
        action="store_true",
        help="Disable default env autoload (.env and ../korean-dexter/.env).",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> list[str]:
    if not path.exists() or not path.is_file():
        return []

    loaded_keys: list[str] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        if key in os.environ:
            continue
        os.environ[key] = value
        loaded_keys.append(key)
    return loaded_keys


def load_env_candidates(cwd: Path, cli_env_files: list[str], disable_autoload: bool) -> list[str]:
    loaded: list[str] = []
    candidates: list[Path] = []
    if not disable_autoload:
        for rel in DEFAULT_ENV_FILES:
            candidates.append((cwd / rel).resolve())
    for extra in cli_env_files:
        candidates.append((cwd / extra).resolve())

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        loaded.extend(load_env_file(candidate))
    return loaded


class HttpClient:
    def __init__(self, timeout: int, retries: int) -> None:
        self.timeout = timeout
        self.retries = retries

    def get_bytes(self, url: str, headers: dict[str, str] | None = None) -> bytes:
        req_headers = {"Accept": "*/*"}
        if headers:
            req_headers.update(headers)

        last_err: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = urllib.request.Request(url=url, headers=req_headers, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = response.read()
                    encoding = (response.headers.get("Content-Encoding") or "").lower()
                    if encoding == "gzip":
                        return gzip.decompress(payload)
                    if encoding == "deflate":
                        try:
                            return zlib.decompress(payload)
                        except zlib.error:
                            return zlib.decompress(payload, -zlib.MAX_WBITS)
                    return payload
            except urllib.error.HTTPError as err:
                last_err = err
                should_retry = 500 <= err.code < 600 and attempt < self.retries
                if should_retry:
                    time.sleep(0.8 * attempt)
                    continue
                raise
            except urllib.error.URLError as err:
                last_err = err
                if attempt < self.retries:
                    time.sleep(0.8 * attempt)
                    continue
                raise

        if last_err:
            raise last_err
        raise RuntimeError(f"Failed to fetch URL: {url}")

    def get_text(
        self, url: str, headers: dict[str, str] | None = None, encoding: str = "utf-8"
    ) -> str:
        return self.get_bytes(url, headers=headers).decode(encoding, errors="replace")

    def get_json(self, url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        raw = self.get_text(url, headers=headers)
        return json.loads(raw)

    def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }
        if headers:
            req_headers.update(headers)
        body = json.dumps(payload).encode("utf-8")

        last_err: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = urllib.request.Request(
                url=url, headers=req_headers, method="POST", data=body
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    raw_payload = response.read()
                    encoding = (response.headers.get("Content-Encoding") or "").lower()
                    if encoding == "gzip":
                        raw_payload = gzip.decompress(raw_payload)
                    elif encoding == "deflate":
                        try:
                            raw_payload = zlib.decompress(raw_payload)
                        except zlib.error:
                            raw_payload = zlib.decompress(raw_payload, -zlib.MAX_WBITS)
                    return json.loads(raw_payload.decode("utf-8", errors="replace"))
            except urllib.error.HTTPError as err:
                last_err = err
                should_retry = 500 <= err.code < 600 and attempt < self.retries
                if should_retry:
                    time.sleep(0.8 * attempt)
                    continue
                raise
            except urllib.error.URLError as err:
                last_err = err
                if attempt < self.retries:
                    time.sleep(0.8 * attempt)
                    continue
                raise

        if last_err:
            raise last_err
        raise RuntimeError(f"Failed to POST URL: {url}")


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def html_to_text(raw_html: str) -> str:
    text = re.sub(r"(?is)<!--.*?-->", " ", raw_html)
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return normalize_space(text)


def parse_float(value: str) -> float:
    return float(value.replace(",", "").strip())


def parse_nps_as_of_month(text: str) -> str:
    # English page pattern: "(As of end- Nov 2025, Unit : trillion won, %)"
    match = re.search(r"As of end-?\s*([A-Za-z]+)\.?\s+(\d{4})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not parse NPS as-of month.")

    month_token = match.group(1).strip(".")
    year = int(match.group(2))

    for date_fmt in ("%b %Y", "%B %Y"):
        try:
            parsed = datetime.strptime(f"{month_token} {year}", date_fmt)
            return parsed.strftime("%Y-%m")
        except ValueError:
            continue
    raise ValueError(f"Unrecognized month token: {month_token}")


def parse_nps_total_aum(text: str) -> float:
    patterns = [
        r"Total AUM\s+KRW\s*([0-9][0-9,\.]*)\s*tn",
        r"Total AUM\s*([0-9][0-9,\.]*)\s*KRW\s*tn",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return parse_float(match.group(1))
    raise ValueError("Could not parse NPS total AUM.")


def parse_nps_assets(text: str) -> list[dict[str, Any]]:
    asset_patterns: dict[str, list[str]] = {
        "financials": [r"Financials\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"],
        "domestic_equity": [
            r"Domestic Equity\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"
        ],
        "domestic_fixed_income": [
            r"Domestic Fixed income\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%",
            r"Domestic Fixed Income\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%",
        ],
        "global_equity": [r"Global Equity\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"],
        "global_fixed_income": [
            r"Global Fixed Income\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"
        ],
        "alternatives": [r"Alternatives\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"],
        "short_term_assets": [
            r"Short-term Assets\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"
        ],
        "welfare_others": [r"Welfare/Others\s+KRW\s+(-?[0-9,\.]+)\s*tn\s+(-?[0-9,\.]+)\s*%"],
    }

    rows: list[dict[str, Any]] = []
    for asset_id, asset_name in NPS_ASSETS:
        amount = None
        weight = None
        for pattern in asset_patterns[asset_id]:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                amount = parse_float(match.group(1))
                weight = parse_float(match.group(2))
                break

        if amount is None or weight is None:
            raise ValueError(f"Could not parse NPS row for asset: {asset_name}")

        rows.append(
            {
                "asset_id": asset_id,
                "asset_name": asset_name,
                "amount_trillion_krw": amount,
                "weight_pct": weight,
            }
        )
    return rows


def split_nps_summary_row(
    total_aum_trillion_krw: float,
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[str]]:
    """Detect parent-level summary rows and isolate category rows.

    The NPS page can expose a top-level "Financials" row that acts as the parent total
    (often 100%) while category rows underneath it also sum to ~100%.
    """

    warnings: list[str] = []
    summary_row: dict[str, Any] | None = None
    allocation_rows = list(rows)

    financials_row = next((row for row in rows if row.get("asset_id") == "financials"), None)
    if financials_row is not None:
        amount = float(financials_row.get("amount_trillion_krw") or 0.0)
        weight = float(financials_row.get("weight_pct") or 0.0)
        amount_gap = abs(amount - float(total_aum_trillion_krw or 0.0))
        relative_gap = amount_gap / max(abs(float(total_aum_trillion_krw or 0.0)), 1.0)

        # If Financials itself is ~100% and amount is close to total AUM,
        # treat it as a parent summary row and exclude it from allocation weights.
        if weight >= 99.0 and relative_gap <= 0.02:
            summary_row = financials_row
            allocation_rows = [row for row in rows if row.get("asset_id") != "financials"]
        elif weight >= 99.0:
            warnings.append(
                "NPS: Financials row looks like a total row but amount differs from total AUM "
                f"(gap={amount_gap:.2f} tn KRW)."
            )

    weights_sum = sum(float(row.get("weight_pct") or 0.0) for row in allocation_rows)
    if allocation_rows and not (98.0 <= weights_sum <= 102.0):
        warnings.append(
            "NPS: allocation weight sum is out of expected range "
            f"(sum={weights_sum:.3f}%)."
        )

    return summary_row, allocation_rows, warnings


def fetch_nps_asset_allocation(client: HttpClient) -> dict[str, Any]:
    page_html = client.get_text(NPS_PORTFOLIO_URL)
    text = html_to_text(page_html)

    as_of_month = parse_nps_as_of_month(text)
    total_aum = parse_nps_total_aum(text)
    rows_raw = parse_nps_assets(text)
    summary_row, allocation_rows, parsing_warnings = split_nps_summary_row(
        total_aum_trillion_krw=total_aum,
        rows=rows_raw,
    )

    return {
        "source_url": NPS_PORTFOLIO_URL,
        "as_of_month": as_of_month,
        "total_aum_trillion_krw": total_aum,
        "summary_row": summary_row,
        "rows": allocation_rows,
        "rows_raw": rows_raw,
        "allocation_weight_sum_pct": round(
            sum(float(row.get("weight_pct") or 0.0) for row in allocation_rows),
            6,
        ),
        "parsing_warnings": parsing_warnings,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def build_sec_headers(user_agent: str) -> dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json, text/xml;q=0.9, */*;q=0.8",
    }


def value_at(items: list[Any], index: int, default: Any = "") -> Any:
    if index < 0 or index >= len(items):
        return default
    return items[index]


def extract_13f_filings(feed: dict[str, Any], source_feed: str) -> list[dict[str, str]]:
    forms = feed.get("form", [])
    if not isinstance(forms, list):
        return []

    rows: list[dict[str, str]] = []
    for i, form in enumerate(forms):
        form_text = str(form).upper()
        if not form_text.startswith("13F-HR"):
            continue

        accession = str(value_at(feed.get("accessionNumber", []), i)).strip()
        if not accession:
            continue

        rows.append(
            {
                "form": str(value_at(forms, i)),
                "filing_date": str(value_at(feed.get("filingDate", []), i)),
                "report_date": str(value_at(feed.get("reportDate", []), i)),
                "acceptance_datetime": str(value_at(feed.get("acceptanceDateTime", []), i)),
                "accession_number": accession,
                "primary_document": str(value_at(feed.get("primaryDocument", []), i)),
                "source_feed": source_feed,
            }
        )
    return rows


def filing_sort_key(filing: dict[str, str]) -> tuple[str, str, str]:
    return (
        filing.get("filing_date", ""),
        filing.get("acceptance_datetime", ""),
        filing.get("accession_number", ""),
    )


def collect_13f_filings(
    submissions: dict[str, Any],
    client: HttpClient,
    headers: dict[str, str],
) -> list[dict[str, str]]:
    filings_block = submissions.get("filings", {})
    recent = filings_block.get("recent", {})
    rows = extract_13f_filings(recent, source_feed="recent")

    historical_files = filings_block.get("files", [])
    if isinstance(historical_files, list):
        for file_meta in historical_files:
            if not isinstance(file_meta, dict):
                continue
            name = str(file_meta.get("name", "")).strip()
            if not name:
                continue
            feed_url = f"https://data.sec.gov/submissions/{name}"
            historical = client.get_json(feed_url, headers=headers)
            rows.extend(extract_13f_filings(historical, source_feed=name))

    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        accession = row["accession_number"]
        prev = deduped.get(accession)
        if prev is None or filing_sort_key(row) > filing_sort_key(prev):
            deduped[accession] = row

    merged = list(deduped.values())
    merged.sort(key=filing_sort_key, reverse=True)
    return merged


def pick_info_table_xml(index_items: list[dict[str, Any]]) -> dict[str, Any]:
    xml_candidates = [
        item for item in index_items if str(item.get("name", "")).lower().endswith(".xml")
    ]
    if not xml_candidates:
        raise ValueError("No XML files found in SEC filing index.")

    non_primary = [
        item for item in xml_candidates if "primary_doc" not in str(item.get("name", "")).lower()
    ]
    pool = non_primary if non_primary else xml_candidates

    def size_as_int(item: dict[str, Any]) -> int:
        try:
            return int(item.get("size", 0))
        except (TypeError, ValueError):
            return 0

    return max(pool, key=size_as_int)


def xml_find_text(parent: ET.Element, tag: str) -> str:
    node = parent.find(f".//{{*}}{tag}")
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def parse_13f_information_table(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    holdings: list[dict[str, Any]] = []

    for info in root.findall(".//{*}infoTable"):
        value_kusd_raw = xml_find_text(info, "value")
        shares_raw = xml_find_text(info, "sshPrnamt")

        try:
            value_kusd = int(value_kusd_raw)
        except ValueError:
            value_kusd = 0
        try:
            shares = int(shares_raw)
        except ValueError:
            shares = 0

        holdings.append(
            {
                "issuer_name": xml_find_text(info, "nameOfIssuer"),
                "title_of_class": xml_find_text(info, "titleOfClass"),
                "cusip": xml_find_text(info, "cusip"),
                "value_usd": value_kusd * 1000,  # 13F value field is in thousands USD.
                "shares": shares,
                "shares_type": xml_find_text(info, "sshPrnamtType"),
                "investment_discretion": xml_find_text(info, "investmentDiscretion"),
                "put_call": xml_find_text(info, "putCall"),
            }
        )

    holdings.sort(key=lambda x: x["value_usd"], reverse=True)
    total_value_usd = sum(row["value_usd"] for row in holdings)
    for row in holdings:
        weight = (row["value_usd"] / total_value_usd * 100.0) if total_value_usd else 0.0
        row["weight_pct_of_13f"] = round(weight, 6)

    return holdings


def fetch_13f_filing_detail(
    client: HttpClient,
    headers: dict[str, str],
    filing: dict[str, str],
    top_holdings: int,
) -> dict[str, Any]:
    accession = filing["accession_number"]
    accession_compact = accession.replace("-", "")
    cik_no_leading_zero = str(int(SEC_CIK))

    index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_no_leading_zero}/"
        f"{accession_compact}/index.json"
    )
    filing_index = client.get_json(index_url, headers=headers)
    index_items = filing_index.get("directory", {}).get("item", [])
    if not isinstance(index_items, list):
        raise ValueError("Unexpected SEC filing index format.")

    info_xml = pick_info_table_xml(index_items)
    info_xml_name = str(info_xml.get("name"))
    info_xml_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_no_leading_zero}/"
        f"{accession_compact}/{info_xml_name}"
    )
    info_xml_text = client.get_text(info_xml_url, headers=headers)
    holdings = parse_13f_information_table(info_xml_text)

    filing_detail_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_no_leading_zero}/"
        f"{accession_compact}/{accession}-index.htm"
    )

    return {
        "source_urls": {
            "filing_index_json": index_url,
            "information_table_xml": info_xml_url,
            "filing_detail_page": filing_detail_url,
        },
        "filing": filing,
        "holdings_count": len(holdings),
        "total_value_usd": sum(row["value_usd"] for row in holdings),
        "top_holdings": holdings[:top_holdings],
        "all_holdings": holdings,
    }


def fetch_sec_13f_data(
    client: HttpClient,
    sec_user_agent: str,
    top_holdings: int,
    sec_history: str,
    sec_max_filings: int,
    sec_request_delay: float,
) -> dict[str, Any]:
    headers = build_sec_headers(sec_user_agent)
    submissions = client.get_json(SEC_SUBMISSIONS_URL, headers=headers)
    all_filings = collect_13f_filings(submissions=submissions, client=client, headers=headers)
    if not all_filings:
        raise ValueError("No 13F-HR filings found for the configured CIK.")

    selected = all_filings[:1] if sec_history == "latest" else list(all_filings)
    if sec_max_filings > 0:
        selected = selected[:sec_max_filings]

    history: list[dict[str, Any]] = []
    holdings_by_filing: dict[str, list[dict[str, Any]]] = {}

    for idx, filing in enumerate(selected):
        try:
            detail = fetch_13f_filing_detail(
                client=client, headers=headers, filing=filing, top_holdings=top_holdings
            )
            holdings_by_filing[filing["accession_number"]] = detail["all_holdings"]
            history.append(
                {
                    **filing,
                    "status": "ok",
                    "holdings_count": detail["holdings_count"],
                    "total_value_usd": detail["total_value_usd"],
                    "top_holdings": detail["top_holdings"],
                    "source_urls": detail["source_urls"],
                }
            )
        except Exception as err:
            history.append({**filing, "status": "error", "error": str(err)})

        if sec_request_delay > 0 and idx < len(selected) - 1:
            time.sleep(sec_request_delay)

    latest_success = next((row for row in history if row.get("status") == "ok"), None)
    if latest_success is None:
        raise ValueError("Failed to parse holdings from all selected SEC 13F filings.")

    latest_accession = str(latest_success["accession_number"])
    latest_all_holdings = holdings_by_filing.get(latest_accession, [])
    latest_source_urls = latest_success.get("source_urls", {})

    return {
        "source_urls": {
            "submissions_api": SEC_SUBMISSIONS_URL,
            "latest_filing_detail_page": latest_source_urls.get("filing_detail_page", ""),
        },
        "cik": SEC_CIK,
        "filings_available_count": len(all_filings),
        "filings_processed_count": len(selected),
        "filings_success_count": sum(1 for row in history if row.get("status") == "ok"),
        "filings_failed_count": sum(1 for row in history if row.get("status") == "error"),
        "history": history,
        "holdings_by_filing": holdings_by_filing,
        "latest_filing": {
            "form": latest_success.get("form", ""),
            "filing_date": latest_success.get("filing_date", ""),
            "report_date": latest_success.get("report_date", ""),
            "acceptance_datetime": latest_success.get("acceptance_datetime", ""),
            "accession_number": latest_success.get("accession_number", ""),
            "primary_document": latest_success.get("primary_document", ""),
        },
        "latest_holdings_count": len(latest_all_holdings),
        "latest_total_value_usd": sum(row["value_usd"] for row in latest_all_holdings),
        "latest_top_holdings": latest_all_holdings[:top_holdings],
        "latest_all_holdings": latest_all_holdings,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def parse_int_like(value: Any) -> int | None:
    text = str(value or "").replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_float_like(value: Any) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def as_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def normalize_date_digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def market_name_from_corp_cls(corp_cls: str) -> str:
    mapping = {
        "Y": "KOSPI",
        "K": "KOSDAQ",
        "N": "KONEX",
        "E": "ETC",
    }
    return mapping.get(str(corp_cls or "").strip().upper(), "UNKNOWN")


def opendart_request_json(
    client: HttpClient,
    endpoint: str,
    api_key: str,
    params: dict[str, Any],
    request_delay: float = 0.0,
    max_attempts: int = 5,
    allow_statuses: set[str] | None = None,
) -> dict[str, Any]:
    base_params = {"crtfc_key": api_key}
    for key, value in params.items():
        if value is None:
            continue
        text = str(value).strip()
        if text == "":
            continue
        base_params[key] = text

    query = urllib.parse.urlencode(base_params)
    url = f"{OPENDART_BASE_URL}/{endpoint}.json?{query}"

    for attempt in range(1, max_attempts + 1):
        payload = client.get_json(url)
        status = str(payload.get("status", ""))

        if status == "000" or (allow_statuses and status in allow_statuses):
            if request_delay > 0:
                time.sleep(request_delay)
            return payload

        # OpenDART rate-limit code.
        if status == "020" and attempt < max_attempts:
            time.sleep(max(0.5, request_delay) * attempt)
            continue

        message = str(payload.get("message", "Unknown OpenDART error"))
        raise ValueError(f"OpenDART {endpoint} failed: status={status}, message={message}")

    raise ValueError(f"OpenDART {endpoint} failed after retries")


def fetch_kis_access_token(
    client: HttpClient,
    app_key: str,
    app_secret: str,
) -> str | None:
    try:
        payload = client.post_json(
            f"{KIS_BASE_URL}/oauth2/tokenP",
            payload={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            headers={"User-Agent": DEFAULT_SEC_USER_AGENT},
        )
        token = str(payload.get("access_token", "")).strip()
        return token or None
    except Exception:
        return None


def fetch_kis_domestic_price(
    client: HttpClient,
    access_token: str,
    app_key: str,
    app_secret: str,
    stock_code: str,
) -> int | None:
    if not stock_code or not access_token:
        return None

    query = urllib.parse.urlencode(
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code.zfill(6)}
    )
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price?{query}"
    headers = {
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010100",
        "content-type": "application/json; charset=utf-8",
        "User-Agent": DEFAULT_SEC_USER_AGENT,
    }
    try:
        payload = client.get_json(url, headers=headers)
    except Exception:
        return None

    if str(payload.get("rt_cd")) != "0":
        return None
    output = payload.get("output", {})
    if not isinstance(output, dict):
        return None
    return parse_int_like(output.get("stck_prpr"))


def daterange_chunks(end_date: date, lookback_days: int, window_days: int = 89) -> list[tuple[date, date]]:
    days = max(1, lookback_days)
    start_date = end_date - timedelta(days=days - 1)
    chunks: list[tuple[date, date]] = []
    cursor_end = end_date

    while cursor_end >= start_date:
        cursor_start = max(start_date, cursor_end - timedelta(days=window_days - 1))
        chunks.append((cursor_start, cursor_end))
        cursor_end = cursor_start - timedelta(days=1)
    return chunks


def apply_korea_weights(holdings: list[dict[str, Any]]) -> str:
    """Populate `weight_pct` for each row and return the weight basis used."""

    value_basis_available = sum(
        row["estimated_value_krw"] or 0
        for row in holdings
        if row.get("estimated_value_krw") is not None
    )
    if value_basis_available > 0:
        total_value = float(value_basis_available)
        for row in holdings:
            value = float(row.get("estimated_value_krw") or 0.0)
            row["weight_pct"] = round((value / total_value) * 100.0, 6) if value > 0 else 0.0
        return "estimated_value_krw"

    total_stake = float(sum(row.get("stake_pct") or 0.0 for row in holdings))
    for row in holdings:
        stake = float(row.get("stake_pct") or 0.0)
        row["weight_pct"] = round((stake / total_stake) * 100.0, 6) if total_stake > 0 else 0.0
    return "stake_pct_normalized"


def fetch_korea_nps_holdings(
    client: HttpClient,
    lookback_days: int,
    request_delay: float,
) -> dict[str, Any]:
    opendart_api_key = os.getenv("OPENDART_API_KEY", "").strip()
    if not opendart_api_key:
        return {
            "status": "skipped",
            "reason": "OPENDART_API_KEY missing",
            "reporter_name": NPS_KOREA_REPORTER_KEYWORD,
            "holdings": [],
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_urls": {
                "opendart_list": f"{OPENDART_BASE_URL}/list.json",
                "opendart_elestock": f"{OPENDART_BASE_URL}/elestock.json",
            },
        }

    today = datetime.now(timezone.utc).date()
    chunks = daterange_chunks(end_date=today, lookback_days=lookback_days, window_days=89)

    scanned_rows = 0
    disclosure_candidates: list[dict[str, Any]] = []

    for bgn, end in chunks:
        page_no = 1
        total_page = 1
        while page_no <= total_page:
            payload = opendart_request_json(
                client=client,
                endpoint="list",
                api_key=opendart_api_key,
                params={
                    "bgn_de": as_yyyymmdd(bgn),
                    "end_de": as_yyyymmdd(end),
                    "pblntf_ty": "D",
                    "page_no": page_no,
                    "page_count": 100,
                },
                request_delay=request_delay,
                allow_statuses={"013"},
            )
            if str(payload.get("status", "")) == "013":
                break

            total_page = int(payload.get("total_page", 1) or 1)
            rows = payload.get("list", [])
            if not isinstance(rows, list):
                rows = []
            scanned_rows += len(rows)

            for row in rows:
                if not isinstance(row, dict):
                    continue
                flr_nm = str(row.get("flr_nm", "")).strip()
                report_nm = str(row.get("report_nm", "")).strip()
                if NPS_KOREA_REPORTER_KEYWORD not in flr_nm:
                    continue
                if "임원ㆍ주요주주" not in report_nm and "대량보유" not in report_nm:
                    continue

                disclosure_candidates.append(
                    {
                        "corp_code": str(row.get("corp_code", "")).strip(),
                        "corp_name": str(row.get("corp_name", "")).strip(),
                        "stock_code": str(row.get("stock_code", "")).strip(),
                        "corp_cls": str(row.get("corp_cls", "")).strip(),
                        "report_nm": report_nm,
                        "flr_nm": flr_nm,
                        "rcept_no": str(row.get("rcept_no", "")).strip(),
                        "rcept_dt": normalize_date_digits(str(row.get("rcept_dt", ""))),
                    }
                )
            page_no += 1

    latest_by_corp: dict[str, dict[str, Any]] = {}
    for row in disclosure_candidates:
        corp_code = row["corp_code"]
        if not corp_code:
            continue
        prev = latest_by_corp.get(corp_code)
        if prev is None:
            latest_by_corp[corp_code] = row
            continue
        prev_key = (prev.get("rcept_dt", ""), prev.get("rcept_no", ""))
        curr_key = (row.get("rcept_dt", ""), row.get("rcept_no", ""))
        if curr_key > prev_key:
            latest_by_corp[corp_code] = row

    app_key = os.getenv("KIS_APP_KEY", "").strip()
    app_secret = os.getenv("KIS_APP_SECRET", "").strip()
    kis_token: str | None = None
    kis_enabled = bool(app_key and app_secret)
    if kis_enabled:
        kis_token = fetch_kis_access_token(client, app_key=app_key, app_secret=app_secret)

    holdings: list[dict[str, Any]] = []
    for idx, (corp_code, disclosure) in enumerate(latest_by_corp.items()):
        elestock_payload = opendart_request_json(
            client=client,
            endpoint="elestock",
            api_key=opendart_api_key,
            params={"corp_code": corp_code},
            request_delay=request_delay,
            allow_statuses={"011", "013"},
        )
        if str(elestock_payload.get("status", "")) in {"011", "013"}:
            continue
        rows = elestock_payload.get("list", [])
        if not isinstance(rows, list):
            rows = []

        nps_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            repror = str(row.get("repror", "")).strip()
            if NPS_KOREA_REPORTER_KEYWORD not in repror:
                continue
            nps_rows.append(row)

        if not nps_rows:
            continue

        nps_rows.sort(
            key=lambda x: (
                normalize_date_digits(str(x.get("rcept_dt", ""))),
                str(x.get("rcept_no", "")),
            ),
            reverse=True,
        )
        latest = nps_rows[0]

        shares_held = parse_int_like(latest.get("sp_stock_lmp_cnt"))
        stake_pct = parse_float_like(latest.get("sp_stock_lmp_rate"))
        if shares_held is None and stake_pct is None:
            continue

        stock_code = str(disclosure.get("stock_code", "")).strip().zfill(6)
        market = market_name_from_corp_cls(str(disclosure.get("corp_cls", "")))
        estimated_price_krw = None
        estimated_value_krw = None

        if kis_token and app_key and app_secret and stock_code:
            estimated_price_krw = fetch_kis_domestic_price(
                client=client,
                access_token=kis_token,
                app_key=app_key,
                app_secret=app_secret,
                stock_code=stock_code,
            )
            if estimated_price_krw is not None and shares_held is not None:
                estimated_value_krw = estimated_price_krw * shares_held

        holdings.append(
            {
                "corp_name_ko": str(disclosure.get("corp_name", "")).strip(),
                "stock_code": stock_code,
                "market": market,
                "reporter_name": NPS_KOREA_REPORTER_KEYWORD,
                "latest_disclosure_date": disclosure.get("rcept_dt", ""),
                "latest_disclosure_rcept_no": disclosure.get("rcept_no", ""),
                "latest_disclosure_report_name": disclosure.get("report_nm", ""),
                "elestock_report_date": normalize_date_digits(str(latest.get("rcept_dt", ""))),
                "elestock_rcept_no": str(latest.get("rcept_no", "")),
                "shares_held": shares_held,
                "stake_pct": stake_pct,
                "estimated_price_krw": estimated_price_krw,
                "estimated_value_krw": estimated_value_krw,
            }
        )

        if request_delay > 0 and idx < len(latest_by_corp) - 1:
            time.sleep(request_delay)

    weight_basis = apply_korea_weights(holdings)

    holdings.sort(key=lambda x: x.get("weight_pct", 0.0), reverse=True)

    warnings: list[str] = []
    if not kis_enabled:
        warnings.append("KIS_APP_KEY/KIS_APP_SECRET not found. Using stake-ratio-normalized weights.")
    elif kis_enabled and not kis_token:
        warnings.append("KIS token issuance failed. Using stake-ratio-normalized weights.")
    if weight_basis != "estimated_value_krw":
        warnings.append(
            "Korea holdings weights are normalized from disclosed stake percentages, not full portfolio valuation."
        )

    return {
        "status": "ok",
        "reporter_name": NPS_KOREA_REPORTER_KEYWORD,
        "lookback_days": lookback_days,
        "weight_basis": weight_basis,
        "disclosures_scanned": scanned_rows,
        "disclosure_hits_count": len(disclosure_candidates),
        "companies_detected_count": len(latest_by_corp),
        "holdings_count": len(holdings),
        "holdings": holdings,
        "warnings": warnings,
        "source_urls": {
            "opendart_list": f"{OPENDART_BASE_URL}/list.json",
            "opendart_elestock": f"{OPENDART_BASE_URL}/elestock.json",
            "kis_quote": f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
        },
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }

def build_data_quality_warnings(
    nps_payload: dict[str, Any] | None,
    korea_payload: dict[str, Any] | None,
    sec_payload: dict[str, Any] | None,
) -> list[str]:
    warnings: list[str] = []

    if nps_payload:
        parsing_warnings = nps_payload.get("parsing_warnings", [])
        if isinstance(parsing_warnings, list):
            for warning in parsing_warnings:
                warnings.append(f"NPS quality: {warning}")

        rows = nps_payload.get("rows", [])
        if isinstance(rows, list) and rows:
            weight_sum = sum(float(row.get("weight_pct") or 0.0) for row in rows if isinstance(row, dict))
            if not (98.0 <= weight_sum <= 102.0):
                warnings.append(
                    f"NPS quality: allocation row weights do not sum close to 100% (sum={weight_sum:.3f}%)."
                )

    if korea_payload and str(korea_payload.get("status", "")).lower() == "ok":
        holdings = korea_payload.get("holdings", [])
        if isinstance(holdings, list) and holdings:
            weight_sum = sum(float(row.get("weight_pct") or 0.0) for row in holdings if isinstance(row, dict))
            if not (99.0 <= weight_sum <= 101.0):
                warnings.append(
                    f"Korea quality: holding weights do not sum close to 100% (sum={weight_sum:.3f}%)."
                )
            if any(float(row.get("weight_pct") or 0.0) < 0 for row in holdings if isinstance(row, dict)):
                warnings.append("Korea quality: detected negative holding weight.")

    if sec_payload:
        processed = int(sec_payload.get("filings_processed_count") or 0)
        success = int(sec_payload.get("filings_success_count") or 0)
        if success < processed:
            warnings.append(
                f"SEC quality: {processed - success} filing(s) failed during this run."
            )

        latest_all_holdings = sec_payload.get("latest_all_holdings", [])
        if isinstance(latest_all_holdings, list) and latest_all_holdings:
            weight_sum = sum(
                float(row.get("weight_pct_of_13f") or 0.0)
                for row in latest_all_holdings
                if isinstance(row, dict)
            )
            if not (99.0 <= weight_sum <= 101.0):
                warnings.append(
                    f"SEC quality: 13F holding weights do not sum close to 100% (sum={weight_sum:.3f}%)."
                )

    return warnings


def prune_snapshot_history(snapshots_dir: Path, retain: int) -> list[str]:
    if retain <= 0 or not snapshots_dir.exists() or not snapshots_dir.is_dir():
        return []

    snapshot_dirs = [path for path in snapshots_dir.iterdir() if path.is_dir()]
    snapshot_dirs.sort(key=lambda path: path.name, reverse=True)

    removed: list[str] = []
    for stale_dir in snapshot_dirs[retain:]:
        shutil.rmtree(stale_dir, ignore_errors=True)
        removed.append(stale_dir.name)
    return removed


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as fp:
            fp.write("")
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def format_nps_table(nps_payload: dict[str, Any]) -> str:
    lines = ["| Asset | Amount (KRW tn) | Weight (%) |", "|---|---:|---:|"]
    for row in nps_payload["rows"]:
        lines.append(
            f"| {row['asset_name']} | {row['amount_trillion_krw']:.1f} | {row['weight_pct']:.1f} |"
        )
    return "\n".join(lines)


def format_sec_table(sec_payload: dict[str, Any], top_holdings: int) -> str:
    rows = sec_payload["latest_top_holdings"][:top_holdings]
    lines = [
        "| Issuer | Class | CUSIP | Value (USD bn) | Weight in 13F (%) |",
        "|---|---|---|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {issuer} | {klass} | {cusip} | {value_bn:.3f} | {weight:.3f} |".format(
                issuer=row["issuer_name"],
                klass=row["title_of_class"],
                cusip=row["cusip"],
                value_bn=(row["value_usd"] / 1_000_000_000),
                weight=row["weight_pct_of_13f"],
            )
        )
    return "\n".join(lines)


def format_korea_table(korea_payload: dict[str, Any], top_holdings: int) -> str:
    rows = (korea_payload.get("holdings") or [])[:top_holdings]
    lines = [
        "| 종목명 | 시장 | 종목코드 | 가중치(%) | 지분율(%) | 추정평가액(억원) |",
        "|---|---|---|---:|---:|---:|",
    ]
    for row in rows:
        estimated_value_eok = (
            (float(row["estimated_value_krw"]) / 100_000_000)
            if row.get("estimated_value_krw") is not None
            else None
        )
        lines.append(
            "| {name} | {market} | {code} | {weight:.3f} | {stake} | {value} |".format(
                name=row.get("corp_name_ko", ""),
                market=row.get("market", ""),
                code=row.get("stock_code", ""),
                weight=float(row.get("weight_pct") or 0.0),
                stake=(
                    f"{float(row['stake_pct']):.3f}"
                    if row.get("stake_pct") is not None
                    else "-"
                ),
                value=(f"{estimated_value_eok:.1f}" if estimated_value_eok is not None else "-"),
            )
        )
    return "\n".join(lines)


def build_markdown_report(
    run_id: str,
    nps_payload: dict[str, Any] | None,
    korea_payload: dict[str, Any] | None,
    sec_payload: dict[str, Any] | None,
    top_holdings: int,
    warnings: list[str],
) -> str:
    lines: list[str] = []
    lines.append("# NPS Portfolio Trace")
    lines.append("")
    lines.append(f"- Generated at (UTC): `{run_id}`")
    lines.append("")

    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")

    if nps_payload is not None:
        lines.append("## Official NPS Asset Allocation (Fund-Level)")
        lines.append("")
        lines.append(f"- Source: {nps_payload['source_url']}")
        lines.append(f"- As of: `{nps_payload['as_of_month']}` (month-end)")
        lines.append(
            f"- Total AUM: `{nps_payload['total_aum_trillion_krw']:.1f} trillion KRW`"
        )
        summary_row = nps_payload.get("summary_row")
        if isinstance(summary_row, dict):
            lines.append(
                "- Financials summary row treated as parent total: "
                f"`{float(summary_row.get('amount_trillion_krw') or 0.0):.1f} tn / "
                f"{float(summary_row.get('weight_pct') or 0.0):.1f}%`"
            )
        lines.append(
            "- Allocation weight sum (excluding parent summary): "
            f"`{float(nps_payload.get('allocation_weight_sum_pct') or 0.0):.3f}%`"
        )
        lines.append("")
        lines.append(format_nps_table(nps_payload))
        lines.append("")

    if korea_payload is not None:
        lines.append("## Korean Stock Market (KOSPI/KOSDAQ, Disclosure-Based)")
        lines.append("")
        lines.append(f"- Source (OpenDART list): {korea_payload['source_urls']['opendart_list']}")
        lines.append(
            f"- Source (OpenDART elestock): {korea_payload['source_urls']['opendart_elestock']}"
        )
        lines.append(f"- Reporter: `{korea_payload.get('reporter_name', NPS_KOREA_REPORTER_KEYWORD)}`")
        lines.append(f"- Holdings count: `{korea_payload.get('holdings_count', 0)}`")
        lines.append(f"- Weight basis: `{korea_payload.get('weight_basis', '-')}`")
        lines.append("")
        lines.append(format_korea_table(korea_payload, top_holdings))
        if korea_payload.get("warnings"):
            lines.append("")
            for warning in korea_payload["warnings"]:
                lines.append(f"- Korea note: {warning}")
        lines.append("")

    if sec_payload is not None:
        latest = sec_payload["latest_filing"]
        lines.append("## SEC 13F Snapshot (U.S.-Listed Holdings Subset)")
        lines.append("")
        lines.append(f"- Source: {sec_payload['source_urls']['latest_filing_detail_page']}")
        lines.append(f"- CIK: `{sec_payload['cik']}`")
        lines.append(f"- Latest filing form: `{latest['form']}`")
        lines.append(f"- Filing date: `{latest['filing_date']}`")
        lines.append(f"- Report date: `{latest['report_date']}`")
        lines.append(
            f"- Filings processed this run: `{sec_payload['filings_processed_count']}` "
            f"(successful: `{sec_payload['filings_success_count']}`, failed: `{sec_payload['filings_failed_count']}`)"
        )
        lines.append(
            "- Coverage note: 13F captures long U.S.-listed equity positions only, "
            "not the full NPS global portfolio."
        )
        lines.append("")
        lines.append(format_sec_table(sec_payload, top_holdings))
        lines.append("")

    lines.append("## Reliability Notes")
    lines.append("")
    lines.append(
        "- NPS states the fund portfolio status is disclosed monthly and figures are provisional."
    )
    lines.append(
        "- SEC data is sourced from EDGAR APIs and filing archives, updated as filings are disseminated."
    )
    lines.append("")

    return "\n".join(lines)


def emit_progress(step: int, total: int, label: str, start_time: float) -> None:
    elapsed = int(time.time() - start_time)
    print(f"[PROGRESS] {step}/{total} | {label} | elapsed={elapsed}", flush=True)


def persist_latest_snapshot(snapshot_dir: Path, latest_dir: Path) -> None:
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(snapshot_dir, latest_dir)


def run_refresh(args: argparse.Namespace) -> dict[str, str]:
    all_skipped = (
        args.skip_nps
        and args.skip_sec
        and args.skip_korea
        and args.skip_market_indices
        and args.skip_market_sentiment
    )
    if all_skipped:
        raise ValueError("Nothing to do: all data sources are skipped.")

    _progress_start = time.time()
    _total_steps = sum([
        not args.skip_nps,
        not args.skip_korea,
        not args.skip_market_indices,
        not args.skip_market_sentiment,
        not args.skip_sec,
    ]) + 1  # +1 for finalize
    _current_step = 0

    loaded_env = load_env_candidates(
        cwd=Path.cwd(),
        cli_env_files=args.env_file,
        disable_autoload=args.no_env_autoload,
    )

    output_dir = Path(args.output_dir)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = output_dir / "snapshots" / run_id
    latest_dir = output_dir / "latest"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    client = HttpClient(timeout=args.timeout, retries=args.retries)
    warnings: list[str] = []
    if loaded_env:
        warnings.append(f"Loaded {len(loaded_env)} env keys from configured env files.")

    nps_payload: dict[str, Any] | None = None
    if not args.skip_nps:
        _current_step += 1
        emit_progress(_current_step, _total_steps, "NPS 자산배분 수집 중...", _progress_start)
        nps_payload = fetch_nps_asset_allocation(client)
        write_json(snapshot_dir / "nps_asset_allocation.json", nps_payload)

    korea_payload: dict[str, Any] | None = None
    if not args.skip_korea:
        _current_step += 1
        emit_progress(_current_step, _total_steps, "국내 공시 수집 중...", _progress_start)
        korea_payload = fetch_korea_nps_holdings(
            client=client,
            lookback_days=max(1, args.korea_lookback_days),
            request_delay=max(0.0, args.korea_request_delay),
        )
        write_json(snapshot_dir / "korea_stock_holdings.json", korea_payload)
        write_csv(snapshot_dir / "korea_stock_holdings.csv", korea_payload.get("holdings", []))
        if korea_payload.get("status") != "ok":
            warnings.append(f"Korea: {korea_payload.get('reason', 'data unavailable')}")
        for warning in korea_payload.get("warnings", []):
            warnings.append(f"Korea: {warning}")

    # Market indices (KOSPI, KOSDAQ, NASDAQ, S&P500, Gold, Bitcoin)
    _scripts_dir = str(Path(__file__).resolve().parent)
    if _scripts_dir not in sys.path:
        sys.path.insert(0, _scripts_dir)

    if not args.skip_market_indices:
        _current_step += 1
        emit_progress(_current_step, _total_steps, "시장 지수 수집 중...", _progress_start)
        try:
            from fetch_market import fetch_market_indices, write_json as fm_write_json

            market_indices_payload = fetch_market_indices()
            fm_write_json(snapshot_dir / "market_indices.json", market_indices_payload)
            idx_count = len(market_indices_payload.get("indices", {}))
            if idx_count == 0:
                warnings.append("Market indices: no index data was fetched.")
        except ImportError:
            warnings.append(
                "Market indices: fetch_market module not available. Run 'pip3 install -r requirements.txt'."
            )
        except Exception as exc:
            warnings.append(f"Market indices: {exc}")

    # Market sentiment (신용잔고/투자자예탁금 ratio)
    if not args.skip_market_sentiment:
        _current_step += 1
        emit_progress(_current_step, _total_steps, "시장 심리 지표 수집 중...", _progress_start)
        try:
            from fetch_market import fetch_market_sentiment, write_json as fm_write_json

            sentiment_payload = fetch_market_sentiment()
            fm_write_json(snapshot_dir / "market_sentiment.json", sentiment_payload)
            if sentiment_payload.get("status") == "skipped":
                warnings.append(f"Market sentiment: {sentiment_payload.get('reason', 'skipped')}")
            elif sentiment_payload.get("status") == "error":
                warnings.append(f"Market sentiment: {sentiment_payload.get('reason', 'error')}")
        except ImportError:
            warnings.append(
                "Market sentiment: fetch_market module not available. Run 'pip3 install -r requirements.txt'."
            )
        except Exception as exc:
            warnings.append(f"Market sentiment: {exc}")

    sec_payload: dict[str, Any] | None = None
    if not args.skip_sec:
        _current_step += 1
        emit_progress(_current_step, _total_steps, "SEC 13F 데이터 수집 중...", _progress_start)
        sec_user_agent = args.sec_user_agent
        if not sec_user_agent:
            sec_user_agent = DEFAULT_SEC_USER_AGENT
            warnings.append(
                "SEC_USER_AGENT is not set. Using default contact string; set your own SEC_USER_AGENT for reliability."
            )
        sec_payload = fetch_sec_13f_data(
            client=client,
            sec_user_agent=sec_user_agent,
            top_holdings=args.top_holdings,
            sec_history=args.sec_history,
            sec_max_filings=max(0, args.sec_max_filings),
            sec_request_delay=max(0.0, args.sec_request_delay),
        )
        sec_holdings_by_filing = sec_payload["holdings_by_filing"]
        sec_meta = {
            k: v
            for k, v in sec_payload.items()
            if k
            not in {
                "latest_all_holdings",
                "latest_top_holdings",
                "history",
                "holdings_by_filing",
            }
        }
        write_json(snapshot_dir / "sec_13f_meta.json", sec_meta)
        write_json(snapshot_dir / "sec_13f_top_holdings.json", sec_payload["latest_top_holdings"])
        write_csv(snapshot_dir / "sec_13f_all_holdings.csv", sec_payload["latest_all_holdings"])
        write_json(snapshot_dir / "sec_13f_filings_history.json", sec_payload["history"])

        sec_filing_holdings_dir = snapshot_dir / "sec_13f_filing_holdings"
        for accession, rows in sec_holdings_by_filing.items():
            safe_accession = re.sub(r"[^A-Za-z0-9._-]", "_", accession)
            write_csv(sec_filing_holdings_dir / f"{safe_accession}.csv", rows)

    _current_step += 1
    emit_progress(_current_step, _total_steps, "완료", _progress_start)

    warnings.extend(
        build_data_quality_warnings(
            nps_payload=nps_payload,
            korea_payload=korea_payload,
            sec_payload=sec_payload,
        )
    )

    snapshot_retain = max(0, int(args.snapshot_retain))
    pruned_snapshot_ids = prune_snapshot_history(
        snapshots_dir=output_dir / "snapshots",
        retain=snapshot_retain,
    )
    if pruned_snapshot_ids:
        warnings.append(
            f"Snapshot retention: pruned {len(pruned_snapshot_ids)} old snapshot(s) "
            f"(retain={snapshot_retain})."
        )

    report = build_markdown_report(
        run_id=run_id,
        nps_payload=nps_payload,
        korea_payload=korea_payload,
        sec_payload=sec_payload,
        top_holdings=args.top_holdings,
        warnings=warnings,
    )
    report_path = snapshot_dir / "report.md"
    report_path.write_text(report, encoding="utf-8")

    run_manifest = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "nps": NPS_PORTFOLIO_URL if not args.skip_nps else None,
            "opendart": f"{OPENDART_BASE_URL}/list.json" if not args.skip_korea else None,
            "kis_quote": (
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
                if not args.skip_korea
                else None
            ),
            "sec_submissions": SEC_SUBMISSIONS_URL if not args.skip_sec else None,
            "market_indices": "KIS+yfinance+CoinGecko" if not args.skip_market_indices else None,
            "market_sentiment": "data.go.kr KOFIA Stats" if not args.skip_market_sentiment else None,
        },
        "settings": {
            "top_holdings": args.top_holdings,
            "sec_history": args.sec_history,
            "sec_max_filings": args.sec_max_filings,
            "sec_request_delay": args.sec_request_delay,
            "korea_lookback_days": args.korea_lookback_days,
            "korea_request_delay": args.korea_request_delay,
            "snapshot_retain": snapshot_retain,
            "skip_market_indices": args.skip_market_indices,
            "skip_market_sentiment": args.skip_market_sentiment,
        },
        "warnings": warnings,
        "pruned_snapshots": pruned_snapshot_ids,
        "files": sorted(
            [str(path.relative_to(snapshot_dir)) for path in snapshot_dir.glob("**/*") if path.is_file()]
        ),
    }
    write_json(snapshot_dir / "run_manifest.json", run_manifest)

    persist_latest_snapshot(snapshot_dir=snapshot_dir, latest_dir=latest_dir)

    return {
        "run_id": run_id,
        "snapshot_dir": str(snapshot_dir),
        "latest_dir": str(latest_dir),
        "report_path": str(latest_dir / "report.md"),
    }


def main() -> int:

    args = parse_args()
    try:
        result = run_refresh(args)
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 2

    print(f"Snapshot saved: {result['snapshot_dir']}")
    print(f"Latest updated: {result['latest_dir']}")
    print(f"Report: {result['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
