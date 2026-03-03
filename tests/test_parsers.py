from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path


def load_refresh_module():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "refresh_portfolio.py"
    spec = importlib.util.spec_from_file_location("refresh_portfolio", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MODULE = load_refresh_module()


class NpsParserTests(unittest.TestCase):
    def test_parse_nps_allocation_from_sample_text(self) -> None:
        sample = """
        (As of end- Nov 2025, Unit : trillion won, %)
        Total AUM KRW 1437.9 tn
        Financials KRW 1.4 tn 0.1 %
        Domestic Equity KRW 244.8 tn 17.0 %
        Domestic Fixed income KRW 435.8 tn 30.3 %
        Global Equity KRW 549.9 tn 38.2 %
        Global Fixed Income KRW 119.7 tn 8.3 %
        Alternatives KRW 250.4 tn 17.4 %
        Short-term Assets KRW 44.1 tn 3.1 %
        Welfare/Others KRW -208.2 tn -14.5 %
        """
        normalized = MODULE.normalize_space(sample)
        self.assertEqual(MODULE.parse_nps_as_of_month(normalized), "2025-11")
        self.assertEqual(MODULE.parse_nps_total_aum(normalized), 1437.9)

        rows = MODULE.parse_nps_assets(normalized)
        self.assertEqual(len(rows), 8)
        global_equity = [row for row in rows if row["asset_id"] == "global_equity"][0]
        self.assertEqual(global_equity["amount_trillion_krw"], 549.9)
        self.assertEqual(global_equity["weight_pct"], 38.2)


class SecParserTests(unittest.TestCase):
    def test_extract_13f_filings(self) -> None:
        feed = {
            "form": ["8-K", "13F-HR", "13F-HR/A"],
            "filingDate": ["2026-01-01", "2026-02-14", "2026-02-20"],
            "reportDate": ["", "2025-12-31", "2025-12-31"],
            "acceptanceDateTime": ["", "20260214120000", "20260220150000"],
            "accessionNumber": ["", "0001608046-26-000001", "0001608046-26-000002"],
            "primaryDocument": ["a.htm", "b.htm", "c.htm"],
        }
        rows = MODULE.extract_13f_filings(feed, source_feed="recent")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["accession_number"], "0001608046-26-000001")
        self.assertEqual(rows[1]["form"], "13F-HR/A")

    def test_parse_13f_information_table(self) -> None:
        sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
          <infoTable>
            <nameOfIssuer>APPLE INC</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>037833100</cusip>
            <value>123456</value>
            <shrsOrPrnAmt>
              <sshPrnamt>789000</sshPrnamt>
              <sshPrnamtType>SH</sshPrnamtType>
            </shrsOrPrnAmt>
            <investmentDiscretion>SOLE</investmentDiscretion>
          </infoTable>
          <infoTable>
            <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>594918104</cusip>
            <value>234567</value>
            <shrsOrPrnAmt>
              <sshPrnamt>654321</sshPrnamt>
              <sshPrnamtType>SH</sshPrnamtType>
            </shrsOrPrnAmt>
            <investmentDiscretion>SOLE</investmentDiscretion>
          </infoTable>
        </informationTable>
        """
        holdings = MODULE.parse_13f_information_table(sample_xml)
        self.assertEqual(len(holdings), 2)

        top = holdings[0]
        self.assertEqual(top["issuer_name"], "MICROSOFT CORP")
        self.assertEqual(top["cusip"], "594918104")
        self.assertEqual(top["value_usd"], 234567000)

        total_weight = sum(row["weight_pct_of_13f"] for row in holdings)
        self.assertAlmostEqual(total_weight, 100.0, places=5)


class EnvLoaderTests(unittest.TestCase):
    def test_load_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("FOO_KEY=foo\nBAR_KEY='bar value'\n#COMMENT=skip\n", encoding="utf-8")

            previous_foo = os.environ.get("FOO_KEY")
            try:
                if "FOO_KEY" in os.environ:
                    del os.environ["FOO_KEY"]
                if "BAR_KEY" in os.environ:
                    del os.environ["BAR_KEY"]

                loaded = MODULE.load_env_file(env_path)
                self.assertIn("FOO_KEY", loaded)
                self.assertIn("BAR_KEY", loaded)
                self.assertEqual(os.environ.get("FOO_KEY"), "foo")
                self.assertEqual(os.environ.get("BAR_KEY"), "bar value")
            finally:
                if previous_foo is None:
                    os.environ.pop("FOO_KEY", None)
                else:
                    os.environ["FOO_KEY"] = previous_foo
                os.environ.pop("BAR_KEY", None)


class NpsModelingTests(unittest.TestCase):
    def test_split_nps_summary_row_detects_parent_total(self) -> None:
        rows = [
            {
                "asset_id": "financials",
                "asset_name": "Financials",
                "amount_trillion_krw": 1457.5,
                "weight_pct": 100.0,
            },
            {
                "asset_id": "domestic_equity",
                "asset_name": "Domestic Equity",
                "amount_trillion_krw": 263.7,
                "weight_pct": 18.1,
            },
            {
                "asset_id": "global_equity",
                "asset_name": "Global Equity",
                "amount_trillion_krw": 550.5,
                "weight_pct": 37.8,
            },
            {
                "asset_id": "alternatives",
                "asset_name": "Alternatives",
                "amount_trillion_krw": 643.8,
                "weight_pct": 44.1,
            },
        ]

        summary, allocation_rows, warnings = MODULE.split_nps_summary_row(
            total_aum_trillion_krw=1458.0,
            rows=rows,
        )

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary["asset_id"], "financials")
        self.assertEqual(len(allocation_rows), 3)
        self.assertAlmostEqual(
            sum(float(row["weight_pct"]) for row in allocation_rows),
            100.0,
            places=4,
        )
        self.assertEqual(warnings, [])

    def test_split_nps_summary_row_keeps_regular_financials_row(self) -> None:
        rows = [
            {
                "asset_id": "financials",
                "asset_name": "Financials",
                "amount_trillion_krw": 1.4,
                "weight_pct": 0.1,
            },
            {
                "asset_id": "domestic_equity",
                "asset_name": "Domestic Equity",
                "amount_trillion_krw": 244.8,
                "weight_pct": 17.0,
            },
        ]

        summary, allocation_rows, warnings = MODULE.split_nps_summary_row(
            total_aum_trillion_krw=1437.9,
            rows=rows,
        )

        self.assertIsNone(summary)
        self.assertEqual(len(allocation_rows), 2)
        self.assertGreaterEqual(len(warnings), 1)


class KoreaWeightingTests(unittest.TestCase):
    def test_apply_korea_weights_with_estimated_value_basis(self) -> None:
        holdings = [
            {"corp_name_ko": "A", "estimated_value_krw": 100_000_000, "stake_pct": 5.0},
            {"corp_name_ko": "B", "estimated_value_krw": 300_000_000, "stake_pct": 7.0},
        ]

        basis = MODULE.apply_korea_weights(holdings)

        self.assertEqual(basis, "estimated_value_krw")
        self.assertAlmostEqual(float(holdings[0]["weight_pct"]), 25.0, places=4)
        self.assertAlmostEqual(float(holdings[1]["weight_pct"]), 75.0, places=4)

    def test_apply_korea_weights_fallback_to_stake_pct(self) -> None:
        holdings = [
            {"corp_name_ko": "A", "estimated_value_krw": None, "stake_pct": 3.0},
            {"corp_name_ko": "B", "estimated_value_krw": None, "stake_pct": 1.0},
        ]

        basis = MODULE.apply_korea_weights(holdings)

        self.assertEqual(basis, "stake_pct_normalized")
        self.assertAlmostEqual(float(holdings[0]["weight_pct"]), 75.0, places=4)
        self.assertAlmostEqual(float(holdings[1]["weight_pct"]), 25.0, places=4)


class DataQualityTests(unittest.TestCase):
    def test_build_data_quality_warnings_detects_anomalies(self) -> None:
        nps_payload = {
            "rows": [
                {"weight_pct": 40.0},
                {"weight_pct": 35.0},
            ],
            "parsing_warnings": ["example warning"],
        }
        korea_payload = {
            "status": "ok",
            "holdings": [
                {"weight_pct": 20.0},
                {"weight_pct": 20.0},
            ],
        }
        sec_payload = {
            "filings_processed_count": 3,
            "filings_success_count": 2,
            "latest_all_holdings": [
                {"weight_pct_of_13f": 60.0},
                {"weight_pct_of_13f": 20.0},
            ],
        }

        warnings = MODULE.build_data_quality_warnings(
            nps_payload=nps_payload,
            korea_payload=korea_payload,
            sec_payload=sec_payload,
        )

        self.assertTrue(any("NPS quality" in warning for warning in warnings))
        self.assertTrue(any("Korea quality" in warning for warning in warnings))
        self.assertTrue(any("SEC quality" in warning for warning in warnings))


if __name__ == "__main__":
    unittest.main()
