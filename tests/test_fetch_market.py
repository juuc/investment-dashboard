from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from typing import Any


def load_fetch_market_module():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "fetch_market.py"
    spec = importlib.util.spec_from_file_location("fetch_market", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MODULE = load_fetch_market_module()


class MarketIndicesPayloadTests(unittest.TestCase):
    """Test the shape of the market indices payload."""

    def test_fetch_market_indices_returns_valid_shape(self) -> None:
        # Mock all fetchers to return known data without network calls
        original_kis = MODULE.fetch_kis_indices
        original_global = MODULE.fetch_global_indices_yfinance
        original_gold = MODULE.fetch_gold_price_yfinance
        original_btc = MODULE.fetch_bitcoin_price

        MODULE.fetch_kis_indices = lambda app_key, app_secret: {
            "KOSPI": {"value": 2580.12, "change_pct": -0.47, "source": "KIS"},
        }
        MODULE.fetch_global_indices_yfinance = lambda: {
            "NASDAQ": {"value": 17200.0, "change_pct": 0.26, "source": "yfinance"},
            "SP500": {"value": 5100.0, "change_pct": -0.15, "source": "yfinance"},
        }
        MODULE.fetch_gold_price_yfinance = lambda: {
            "value": 2050.5, "change_pct": 0.8, "source": "yfinance", "unit": "USD/oz",
        }
        MODULE.fetch_bitcoin_price = lambda: {
            "value": 65000.0, "value_krw": 87000000.0, "change_pct": 1.2,
            "source": "CoinGecko", "unit": "USD",
        }

        try:
            payload = MODULE.fetch_market_indices()
        finally:
            MODULE.fetch_kis_indices = original_kis
            MODULE.fetch_global_indices_yfinance = original_global
            MODULE.fetch_gold_price_yfinance = original_gold
            MODULE.fetch_bitcoin_price = original_btc

        self.assertIn("fetched_at_utc", payload)
        self.assertIn("indices", payload)
        indices = payload["indices"]
        self.assertIn("KOSPI", indices)
        self.assertIn("NASDAQ", indices)
        self.assertIn("SP500", indices)
        self.assertIn("GOLD", indices)
        self.assertIn("BTC", indices)

        # Each index should have value and change_pct
        for key in ("KOSPI", "NASDAQ", "SP500", "GOLD", "BTC"):
            self.assertIn("value", indices[key])
            self.assertIn("change_pct", indices[key])
            self.assertIn("source", indices[key])

    def test_partial_failure_returns_available_indices(self) -> None:
        """If KIS fails, yfinance results should still be returned."""
        original_kis = MODULE.fetch_kis_indices
        original_global = MODULE.fetch_global_indices_yfinance
        original_gold = MODULE.fetch_gold_price_yfinance
        original_btc = MODULE.fetch_bitcoin_price

        MODULE.fetch_kis_indices = lambda app_key, app_secret: {}
        MODULE.fetch_global_indices_yfinance = lambda: {
            "NASDAQ": {"value": 17200.0, "change_pct": 0.26, "source": "yfinance"},
        }
        MODULE.fetch_gold_price_yfinance = lambda: None
        MODULE.fetch_bitcoin_price = lambda: None

        try:
            payload = MODULE.fetch_market_indices()
        finally:
            MODULE.fetch_kis_indices = original_kis
            MODULE.fetch_global_indices_yfinance = original_global
            MODULE.fetch_gold_price_yfinance = original_gold
            MODULE.fetch_bitcoin_price = original_btc

        indices = payload["indices"]
        self.assertIn("NASDAQ", indices)
        self.assertNotIn("KOSPI", indices)
        self.assertNotIn("GOLD", indices)
        self.assertNotIn("BTC", indices)


class MarketSentimentSignalTests(unittest.TestCase):
    """Test the pure sentiment signal function."""

    def test_normal_signal(self) -> None:
        self.assertEqual(MODULE.compute_sentiment_signal(20.0), "normal")
        self.assertEqual(MODULE.compute_sentiment_signal(0.0), "normal")
        self.assertEqual(MODULE.compute_sentiment_signal(29.9), "normal")

    def test_watch_signal(self) -> None:
        self.assertEqual(MODULE.compute_sentiment_signal(30.0), "watch")
        self.assertEqual(MODULE.compute_sentiment_signal(32.5), "watch")
        self.assertEqual(MODULE.compute_sentiment_signal(34.9), "watch")

    def test_warning_signal(self) -> None:
        self.assertEqual(MODULE.compute_sentiment_signal(35.0), "warning")
        self.assertEqual(MODULE.compute_sentiment_signal(35.5), "warning")
        self.assertEqual(MODULE.compute_sentiment_signal(35.99), "warning")

    def test_danger_signal(self) -> None:
        self.assertEqual(MODULE.compute_sentiment_signal(36.0), "danger")
        self.assertEqual(MODULE.compute_sentiment_signal(40.0), "danger")
        self.assertEqual(MODULE.compute_sentiment_signal(100.0), "danger")


class CreditRatioComputationTests(unittest.TestCase):
    """Test the credit ratio computation."""

    def test_basic_ratio(self) -> None:
        # 30 / 100 = 30%
        ratio = MODULE.compute_credit_ratio(30_000_000_000_000, 100_000_000_000_000)
        self.assertAlmostEqual(ratio, 30.0, places=4)

    def test_zero_deposits(self) -> None:
        ratio = MODULE.compute_credit_ratio(10_000, 0)
        self.assertEqual(ratio, 0.0)

    def test_negative_deposits(self) -> None:
        ratio = MODULE.compute_credit_ratio(10_000, -5_000)
        self.assertEqual(ratio, 0.0)

    def test_covid_peak_ratio(self) -> None:
        # Simulate ratio near COVID peak: 36%
        credit = 36_000_000_000_000
        deposits = 100_000_000_000_000
        ratio = MODULE.compute_credit_ratio(credit, deposits)
        self.assertAlmostEqual(ratio, 36.0, places=4)
        self.assertEqual(MODULE.compute_sentiment_signal(ratio), "danger")


class SentimentPayloadTests(unittest.TestCase):
    """Test the sentiment orchestrator with mocked API key."""

    def test_skipped_when_no_api_key(self) -> None:
        import os
        original = os.environ.get("DATA_GO_KR_KOFIA_STATS_API_KEY")
        os.environ.pop("DATA_GO_KR_KOFIA_STATS_API_KEY", None)

        try:
            payload = MODULE.fetch_market_sentiment()
        finally:
            if original is not None:
                os.environ["DATA_GO_KR_KOFIA_STATS_API_KEY"] = original

        self.assertEqual(payload["status"], "skipped")
        self.assertIn("fetched_at_utc", payload)


if __name__ == "__main__":
    unittest.main()
