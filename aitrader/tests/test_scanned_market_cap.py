import unittest
from pathlib import Path


AITRADER_DIR = Path(__file__).resolve().parents[1]


class ScannedMarketCapContractTests(unittest.TestCase):
    def test_backend_keeps_scanned_market_cap_explicit_and_nullable(self):
        source = (AITRADER_DIR / "app.py").read_text(encoding="utf-8")

        self.assertIn(
            'MARKET_CAP_FIELD_ORDER = ("market_cap", "usd_market_cap")',
            source,
        )
        self.assertIn("def _market_cap_from_trending", source)
        self.assertIn("no price*supply inference", source)
        self.assertIn("return None, None", source)
        self.assertIn("mcap=f.mcap, mcap_source=f.mcap_source", source)

    def test_scanned_table_renders_market_cap_column(self):
        html = (AITRADER_DIR / "static" / "index.html").read_text(encoding="utf-8")

        self.assertIn('data-i18n="th_mcap"', html)
        self.assertIn("fmtScanMc", html)
        self.assertIn("N/A", html)
        self.assertIn("mcap:(f.mcap==null?null:Number(f.mcap))", html)
        self.assertIn("${fmtScanMc(t.mcap)}", html)
        self.assertIn('colspan="13"', html)


if __name__ == "__main__":
    unittest.main()
