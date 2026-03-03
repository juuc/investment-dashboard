#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import subprocess
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = PROJECT_ROOT / "web" / "static"
DATA_DIR = PROJECT_ROOT / "data"
REFRESH_SCRIPT = PROJECT_ROOT / "scripts" / "refresh_portfolio.py"

JOB_LOCK = threading.Lock()
JOB_STATE: dict[str, Any] = {
    "running": False,
    "started_at_utc": None,
    "finished_at_utc": None,
    "exit_code": None,
    "stdout": "",
    "stderr": "",
    "last_error": None,
    "progress": None,
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def read_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return default


def list_snapshot_dirs() -> list[Path]:
    snapshots_dir = DATA_DIR / "snapshots"
    if not snapshots_dir.exists():
        return []
    dirs = [path for path in snapshots_dir.iterdir() if path.is_dir()]
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs


def load_latest_bundle() -> dict[str, Any]:
    latest_dir = DATA_DIR / "latest"
    return {
        "run_manifest": read_json(latest_dir / "run_manifest.json", {}),
        "nps": read_json(latest_dir / "nps_asset_allocation.json", {}),
        "korea": read_json(latest_dir / "korea_stock_holdings.json", {}),
        "sec_meta": read_json(latest_dir / "sec_13f_meta.json", {}),
        "sec_top_holdings": read_json(latest_dir / "sec_13f_top_holdings.json", []),
        "sec_history": read_json(latest_dir / "sec_13f_filings_history.json", []),
        "report_markdown": read_text(latest_dir / "report.md", ""),
        "market_indices": read_json(latest_dir / "market_indices.json", {}),
        "market_sentiment": read_json(latest_dir / "market_sentiment.json", {}),
    }


def load_snapshots_index(limit: int = 100) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot_dir in list_snapshot_dirs()[:limit]:
        manifest = read_json(snapshot_dir / "run_manifest.json", {})
        rows.append(
            {
                "run_id": snapshot_dir.name,
                "created_at_utc": manifest.get("created_at_utc", ""),
                "warnings_count": len(manifest.get("warnings", []))
                if isinstance(manifest.get("warnings", []), list)
                else 0,
                "has_nps": (snapshot_dir / "nps_asset_allocation.json").exists(),
                "has_sec": (snapshot_dir / "sec_13f_meta.json").exists(),
                "has_korea": (snapshot_dir / "korea_stock_holdings.json").exists(),
            }
        )
    return rows


def load_nps_history(limit: int = 120) -> list[dict[str, Any]]:
    # Deduplicate by as-of month; keep newest snapshot for each month.
    by_month: dict[str, dict[str, Any]] = {}
    for snapshot_dir in list_snapshot_dirs():
        nps = read_json(snapshot_dir / "nps_asset_allocation.json", {})
        as_of_month = str(nps.get("as_of_month", "")).strip()
        if not as_of_month:
            continue
        if as_of_month not in by_month:
            by_month[as_of_month] = {
                "as_of_month": as_of_month,
                "total_aum_trillion_krw": nps.get("total_aum_trillion_krw", 0),
                "rows": nps.get("rows", []),
                "run_id": snapshot_dir.name,
            }
    months = sorted(by_month.keys())
    points = [by_month[month] for month in months]
    if limit > 0:
        points = points[-limit:]
    return points


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
        if num != num:  # NaN check
            return default
        return num
    except (TypeError, ValueError):
        return default


def load_korea_history(limit_runs: int = 60) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    snapshot_dirs = list_snapshot_dirs()
    snapshot_dirs.reverse()  # oldest -> newest
    for snapshot_dir in snapshot_dirs:
        korea_payload = read_json(snapshot_dir / "korea_stock_holdings.json", {})
        if not isinstance(korea_payload, dict):
            continue
        if str(korea_payload.get("status", "")).strip().lower() != "ok":
            continue

        holdings = korea_payload.get("holdings", [])
        if not isinstance(holdings, list) or not holdings:
            continue

        holdings_map: dict[str, dict[str, Any]] = {}
        for row in holdings:
            if not isinstance(row, dict):
                continue
            stock_code = str(row.get("stock_code", "")).strip()
            corp_name = str(row.get("corp_name_ko", "")).strip()
            key = stock_code or corp_name
            if not key:
                continue
            holdings_map[key] = {
                "stock_code": stock_code,
                "corp_name_ko": corp_name or key,
                "market": str(row.get("market", "")).strip() or "기타",
                "weight_pct": as_float(row.get("weight_pct", 0.0), 0.0),
                "latest_disclosure_date": str(row.get("latest_disclosure_date", "")).strip(),
            }

        if not holdings_map:
            continue

        manifest = read_json(snapshot_dir / "run_manifest.json", {})
        points.append(
            {
                "run_id": snapshot_dir.name,
                "created_at_utc": str(manifest.get("created_at_utc", "")).strip(),
                "weight_basis": str(korea_payload.get("weight_basis", "")).strip(),
                "holdings_map": holdings_map,
            }
        )

    if limit_runs > 0:
        points = points[-limit_runs:]
    return points


def build_korea_emerging_payload(
    limit_runs: int = 60,
    short_window: int = 6,
    long_window: int = 18,
    top_n: int = 25,
) -> dict[str, Any]:
    history_points = load_korea_history(limit_runs=limit_runs)
    if not history_points:
        return {
            "status": "no_data",
            "reason": "no_korea_history",
            "ranked": [],
            "history_runs": 0,
        }

    latest = history_points[-1]
    latest_map = latest.get("holdings_map", {})
    if not isinstance(latest_map, dict) or not latest_map:
        return {
            "status": "no_data",
            "reason": "no_latest_korea_holdings",
            "ranked": [],
            "history_runs": len(history_points),
        }

    short_n = max(2, min(short_window, len(history_points)))
    long_n = max(short_n, min(long_window, len(history_points)))

    short_slice = history_points[-short_n:]
    long_slice = history_points[-long_n:]
    older_slice = long_slice[:-short_n]

    ranked: list[dict[str, Any]] = []
    for key, latest_row in latest_map.items():
        if not isinstance(latest_row, dict):
            continue
        current_weight = as_float(latest_row.get("weight_pct"), 0.0)

        short_series = [
            as_float(snapshot.get("holdings_map", {}).get(key, {}).get("weight_pct", 0.0), 0.0)
            for snapshot in short_slice
        ]
        long_series = [
            as_float(snapshot.get("holdings_map", {}).get(key, {}).get("weight_pct", 0.0), 0.0)
            for snapshot in long_slice
        ]
        older_series = [
            as_float(snapshot.get("holdings_map", {}).get(key, {}).get("weight_pct", 0.0), 0.0)
            for snapshot in older_slice
        ]

        short_start = short_series[0] if short_series else 0.0
        long_start = long_series[0] if long_series else 0.0
        delta_short = current_weight - short_start
        delta_long = current_weight - long_start

        short_presence = sum(1 for value in short_series if value > 0)
        long_presence = sum(1 for value in long_series if value > 0)
        short_presence_ratio = short_presence / len(short_series) if short_series else 0.0
        long_presence_ratio = long_presence / len(long_series) if long_series else 0.0

        older_presence = sum(1 for value in older_series if value > 0)
        new_bonus = 0.0
        if older_series and older_presence == 0 and short_presence >= 2:
            new_bonus = 0.6
        elif long_presence_ratio <= 0.35 and short_presence_ratio >= 0.5:
            new_bonus = 0.25

        # Relaxed "attractiveness" score:
        # prioritize recent increase while still considering medium horizon and current size.
        score = (delta_short * 1.8) + (delta_long * 0.9) + (current_weight * 0.15) + new_bonus

        trend = "유지"
        if delta_short >= 0.05 or delta_long >= 0.1:
            trend = "상승"
        elif delta_short <= -0.05 and delta_long < 0:
            trend = "약화"

        ranked.append(
            {
                "stock_code": str(latest_row.get("stock_code", "")).strip(),
                "corp_name_ko": str(latest_row.get("corp_name_ko", "")).strip() or key,
                "market": str(latest_row.get("market", "")).strip() or "기타",
                "latest_disclosure_date": str(latest_row.get("latest_disclosure_date", "")).strip(),
                "current_weight_pct": round(current_weight, 6),
                "delta_short_pctp": round(delta_short, 6),
                "delta_long_pctp": round(delta_long, 6),
                "short_presence_ratio": round(short_presence_ratio, 4),
                "long_presence_ratio": round(long_presence_ratio, 4),
                "new_flag": bool(new_bonus > 0.0),
                "trend_label": trend,
                "score": round(score, 6),
                "sparkline_long": [round(value, 4) for value in long_series],
            }
        )

    ranked.sort(
        key=lambda row: (
            as_float(row.get("score"), 0.0),
            as_float(row.get("delta_short_pctp"), 0.0),
            as_float(row.get("current_weight_pct"), 0.0),
        ),
        reverse=True,
    )

    if top_n > 0:
        ranked = ranked[:top_n]

    return {
        "status": "ok",
        "as_of_run_id": str(latest.get("run_id", "")).strip(),
        "as_of_created_at_utc": str(latest.get("created_at_utc", "")).strip(),
        "weight_basis": str(latest.get("weight_basis", "")).strip(),
        "history_runs": len(history_points),
        "short_window_runs": short_n,
        "long_window_runs": long_n,
        "ranked": ranked,
    }


def get_job_state() -> dict[str, Any]:
    with JOB_LOCK:
        return dict(JOB_STATE)


def parse_progress_line(line: str) -> dict[str, Any] | None:
    if not line.startswith("[PROGRESS] "):
        return None
    try:
        rest = line[len("[PROGRESS] "):]
        parts = rest.split(" | ")
        step_part = parts[0].strip()
        label = parts[1].strip() if len(parts) > 1 else ""
        elapsed_str = parts[2].strip() if len(parts) > 2 else ""
        step_str, total_str = step_part.split("/")
        elapsed_sec = 0
        if elapsed_str.startswith("elapsed="):
            elapsed_sec = int(elapsed_str[len("elapsed="):])
        return {
            "step": int(step_str),
            "total": int(total_str),
            "label": label,
            "elapsed_sec": elapsed_sec,
        }
    except (ValueError, IndexError):
        return None


def refresh_worker(cmd: list[str]) -> None:
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        with JOB_LOCK:
            JOB_STATE["stdout"] += line
            parsed = parse_progress_line(line.rstrip("\n"))
            if parsed:
                JOB_STATE["progress"] = parsed
    proc.wait()
    assert proc.stderr is not None
    with JOB_LOCK:
        JOB_STATE["running"] = False
        JOB_STATE["finished_at_utc"] = now_utc_iso()
        JOB_STATE["exit_code"] = proc.returncode
        JOB_STATE["stdout"] = JOB_STATE["stdout"][-12000:]
        JOB_STATE["stderr"] = (proc.stderr.read() or "")[-12000:]
        JOB_STATE["last_error"] = None if proc.returncode == 0 else "refresh_failed"
        JOB_STATE["progress"] = None


def start_refresh_job(options: dict[str, Any]) -> tuple[bool, str]:
    try:
        top_holdings = max(1, int(options.get("top_holdings", 30)))
        sec_history = str(options.get("sec_history", "full"))
        if sec_history not in {"latest", "full"}:
            sec_history = "full"
        sec_max_filings = int(options.get("sec_max_filings", 0))
        sec_request_delay = float(options.get("sec_request_delay", 0.2))

        korea_lookback_days = max(1, int(options.get("korea_lookback_days", 365)))
        korea_request_delay = float(options.get("korea_request_delay", 0.1))
        snapshot_retain = max(0, int(options.get("snapshot_retain", 120)))
    except (TypeError, ValueError):
        return False, "invalid_refresh_options"

    with JOB_LOCK:
        if JOB_STATE["running"]:
            return False, "refresh_already_running"
        JOB_STATE["running"] = True
        JOB_STATE["started_at_utc"] = now_utc_iso()
        JOB_STATE["finished_at_utc"] = None
        JOB_STATE["exit_code"] = None
        JOB_STATE["stdout"] = ""
        JOB_STATE["stderr"] = ""
        JOB_STATE["last_error"] = None
        JOB_STATE["progress"] = None

    cmd = [
        sys.executable,
        str(REFRESH_SCRIPT),
        "--output-dir",
        str(options.get("output_dir", "data")),
        "--top-holdings",
        str(top_holdings),
        "--sec-history",
        sec_history,
        "--korea-lookback-days",
        str(korea_lookback_days),
        "--korea-request-delay",
        str(max(0.0, korea_request_delay)),
        "--snapshot-retain",
        str(snapshot_retain),
    ]

    if sec_max_filings >= 0:
        cmd.extend(["--sec-max-filings", str(sec_max_filings)])

    if sec_request_delay >= 0:
        cmd.extend(["--sec-request-delay", str(sec_request_delay)])

    sec_user_agent = str(options.get("sec_user_agent", "")).strip()
    if sec_user_agent:
        cmd.extend(["--sec-user-agent", sec_user_agent])

    if bool(options.get("skip_nps", False)):
        cmd.append("--skip-nps")
    if bool(options.get("skip_sec", False)):
        cmd.append("--skip-sec")
    if bool(options.get("skip_korea", False)):
        cmd.append("--skip-korea")
    if bool(options.get("skip_market_indices", False)):
        cmd.append("--skip-market-indices")
    if bool(options.get("skip_market_sentiment", False)):
        cmd.append("--skip-market-sentiment")
    if bool(options.get("no_env_autoload", False)):
        cmd.append("--no-env-autoload")

    env_files = options.get("env_files", [])
    if isinstance(env_files, list):
        for env_file in env_files:
            env_file_str = str(env_file).strip()
            if env_file_str:
                cmd.extend(["--env-file", env_file_str])

    thread = threading.Thread(target=refresh_worker, args=(cmd,), daemon=True)
    thread.start()
    return True, "refresh_started"


def build_dashboard_payload() -> dict[str, Any]:
    return {
        "generated_at_utc": now_utc_iso(),
        "latest": load_latest_bundle(),
        "korea_emerging": build_korea_emerging_payload(),
        "snapshots": load_snapshots_index(),
        "nps_history": load_nps_history(),
        "job": get_job_state(),
    }


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "InvestDashboard/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep stdout clean.
        return

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, status: int, text: str, content_type: str) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_static(self, path: str) -> None:
        requested = "index.html" if path == "/" else path.lstrip("/")
        file_path = (STATIC_DIR / requested).resolve()
        if STATIC_DIR.resolve() not in file_path.parents and file_path != STATIC_DIR.resolve():
            self.send_json(404, {"error": "not_found"})
            return
        if not file_path.exists() or not file_path.is_file():
            self.send_json(404, {"error": "not_found"})
            return

        mime_type, _ = mimetypes.guess_type(str(file_path))
        content_type = mime_type or "application/octet-stream"
        self.send_text(200, file_path.read_text(encoding="utf-8"), f"{content_type}; charset=utf-8")

    def parse_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        if not raw.strip():
            return {}
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
            return {}
        except json.JSONDecodeError:
            return {}

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/dashboard":
            self.send_json(200, build_dashboard_payload())
            return
        if parsed.path == "/api/job":
            self.send_json(200, {"job": get_job_state()})
            return
        if parsed.path == "/api/health":
            self.send_json(200, {"status": "ok", "time": now_utc_iso()})
            return
        self.serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/refresh":
            self.send_json(404, {"error": "not_found"})
            return
        payload = self.parse_json_body()
        started, message = start_refresh_job(payload)
        status = 202 if started else (400 if message == "invalid_refresh_options" else 409)
        self.send_json(status, {"ok": started, "message": message, "job": get_job_state()})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local web dashboard for NPS portfolio traces.")
    parser.add_argument("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="Port (default: 8787)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard: http://{args.host}:{args.port}")
    print("Use POST /api/refresh or the browser button to trigger new data collection.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
