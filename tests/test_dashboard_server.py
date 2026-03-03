from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from typing import Any


def load_dashboard_module():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "dashboard_server.py"
    spec = importlib.util.spec_from_file_location("dashboard_server", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MODULE = load_dashboard_module()


class KoreaEmergingTests(unittest.TestCase):
    def test_build_korea_emerging_payload_ranking(self) -> None:
        history_points: list[dict[str, Any]] = [
            {
                "run_id": "r1",
                "created_at_utc": "2026-01-01T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 1.0},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 3.0},
                },
            },
            {
                "run_id": "r2",
                "created_at_utc": "2026-01-02T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 1.2},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 2.9},
                },
            },
            {
                "run_id": "r3",
                "created_at_utc": "2026-01-03T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 1.4},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 2.8},
                },
            },
            {
                "run_id": "r4",
                "created_at_utc": "2026-01-04T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 1.8},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 2.7},
                    "C": {"stock_code": "000003", "corp_name_ko": "C", "market": "KOSDAQ", "weight_pct": 0.5},
                },
            },
            {
                "run_id": "r5",
                "created_at_utc": "2026-01-05T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 2.1},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 2.6},
                    "C": {"stock_code": "000003", "corp_name_ko": "C", "market": "KOSDAQ", "weight_pct": 0.8},
                },
            },
            {
                "run_id": "r6",
                "created_at_utc": "2026-01-06T00:00:00Z",
                "weight_basis": "estimated_value_krw",
                "holdings_map": {
                    "A": {"stock_code": "000001", "corp_name_ko": "A", "market": "KOSPI", "weight_pct": 2.5},
                    "B": {"stock_code": "000002", "corp_name_ko": "B", "market": "KOSPI", "weight_pct": 2.5},
                    "C": {"stock_code": "000003", "corp_name_ko": "C", "market": "KOSDAQ", "weight_pct": 1.1},
                },
            },
        ]

        original = MODULE.load_korea_history
        MODULE.load_korea_history = lambda limit_runs=60: history_points
        try:
            payload = MODULE.build_korea_emerging_payload(
                limit_runs=60,
                short_window=3,
                long_window=6,
                top_n=3,
            )
        finally:
            MODULE.load_korea_history = original

        self.assertEqual(payload.get("status"), "ok")
        ranked = payload.get("ranked", [])
        self.assertEqual(len(ranked), 3)
        self.assertEqual(ranked[0]["corp_name_ko"], "A")
        self.assertIn("C", [row["corp_name_ko"] for row in ranked[:2]])


class DashboardPayloadContractTests(unittest.TestCase):
    def test_build_dashboard_payload_contract(self) -> None:
        originals = {
            "now_utc_iso": MODULE.now_utc_iso,
            "load_latest_bundle": MODULE.load_latest_bundle,
            "build_korea_emerging_payload": MODULE.build_korea_emerging_payload,
            "load_snapshots_index": MODULE.load_snapshots_index,
            "load_nps_history": MODULE.load_nps_history,
            "get_job_state": MODULE.get_job_state,
        }

        MODULE.now_utc_iso = lambda: "2026-01-01T00:00:00+00:00"
        MODULE.load_latest_bundle = lambda: {"run_manifest": {"run_id": "abc"}}
        MODULE.build_korea_emerging_payload = lambda: {"status": "ok", "ranked": []}
        MODULE.load_snapshots_index = lambda limit=100: [{"run_id": "abc"}]
        MODULE.load_nps_history = lambda limit=120: [{"as_of_month": "2025-12"}]
        MODULE.get_job_state = lambda: {"running": False}

        try:
            payload = MODULE.build_dashboard_payload()
        finally:
            for key, value in originals.items():
                setattr(MODULE, key, value)

        self.assertIn("generated_at_utc", payload)
        self.assertIn("latest", payload)
        self.assertIn("korea_emerging", payload)
        self.assertIn("snapshots", payload)
        self.assertIn("nps_history", payload)
        self.assertIn("job", payload)


class RefreshOptionWiringTests(unittest.TestCase):
    def test_start_refresh_job_includes_korea_and_retention_options(self) -> None:
        captured: dict[str, Any] = {}

        class DummyThread:
            def __init__(self, target, args, daemon=False):
                captured["target"] = target
                captured["cmd"] = args[0]
                captured["daemon"] = daemon

            def start(self):
                captured["started"] = True

        original_thread = MODULE.threading.Thread
        original_state = dict(MODULE.JOB_STATE)
        try:
            MODULE.threading.Thread = DummyThread
            MODULE.JOB_STATE.update(
                {
                    "running": False,
                    "started_at_utc": None,
                    "finished_at_utc": None,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "last_error": None,
                }
            )

            ok, message = MODULE.start_refresh_job(
                {
                    "output_dir": "data",
                    "top_holdings": 25,
                    "sec_history": "full",
                    "sec_max_filings": 5,
                    "sec_request_delay": 0.3,
                    "korea_lookback_days": 180,
                    "korea_request_delay": 0.25,
                    "snapshot_retain": 42,
                    "skip_korea": True,
                }
            )

            self.assertTrue(ok)
            self.assertEqual(message, "refresh_started")
            cmd = captured.get("cmd", [])
            self.assertIn("--korea-lookback-days", cmd)
            self.assertIn("180", cmd)
            self.assertIn("--korea-request-delay", cmd)
            self.assertIn("0.25", cmd)
            self.assertIn("--snapshot-retain", cmd)
            self.assertIn("42", cmd)
            self.assertIn("--skip-korea", cmd)
            self.assertTrue(captured.get("started", False))
        finally:
            MODULE.threading.Thread = original_thread
            MODULE.JOB_STATE.clear()
            MODULE.JOB_STATE.update(original_state)


if __name__ == "__main__":
    unittest.main()
