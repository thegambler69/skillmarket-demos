import unittest

from discovery_v2 import DiscoveryV2Service


ADDR = "7xK3KQx1kM4QG5jYd9w3rTn2hV8pL6cB4mN1sA9qP2zR"


class FakeResearch:
    def validate_sol_address(self, address):
        if address != ADDR:
            raise ValueError("bad address")
        return address

    def _call(self, args, key, ttl_s=None):
        route = tuple(args[:2])
        if route == ("market", "trending"):
            return {"data": {"rank": [{
                "address": ADDR,
                "symbol": "OME",
                "market_cap": 84000,
                "liquidity": 52000,
                "price": 0.000084,
                "price_change_percent5m": 12,
                "price_change_percent1h": 68,
                "buys": 68,
                "sells": 32,
                "smart_degen_count": 3,
                "renowned_count": 1,
                "top_10_holder_rate": 0.18,
                "bundler_rate": 0.02,
                "rat_trader_amount_rate": 0.03,
                "rug_ratio": 0.04,
                "renounced_mint": 1,
                "renounced_freeze_account": 1,
                "creation_timestamp": 1,
            }]}}
        if route == ("market", "trenches"):
            return {"data": {"new_creation": [], "pump": [{
                "address": ADDR, "symbol": "OME", "usd_market_cap": 83000,
            }], "completed": []}}
        if route == ("market", "signal"):
            return {"data": [
                {"token_address": ADDR, "signal_type": 12, "market_cap": 84000},
                {"token_address": ADDR, "signal_type": 20, "market_cap": 84000},
            ]}
        if route == ("market", "kline"):
            return {"data": {"list": [{"close": 1, "volume": 2}, {"close": 2, "volume": 3}]}}
        raise AssertionError(args)


class DiscoveryV2Tests(unittest.TestCase):
    def test_snapshot_merges_sources_and_keeps_dimensions_separate(self):
        payload = DiscoveryV2Service(FakeResearch()).snapshot()
        self.assertEqual(payload["counts"]["all"], 1)
        row = payload["candidates"][0]
        self.assertEqual(row["address"], ADDR)
        self.assertEqual(row["stage"], "Near Grad")
        self.assertEqual(row["market_cap"], 84000)
        self.assertEqual(row["liquidity"], 52000)
        self.assertEqual(row["smart_money_count"], 3)
        self.assertEqual(row["kol_count"], 1)
        self.assertEqual(row["signal_counts"]["smart_money_buy"], 1)
        self.assertEqual(row["signal_counts"]["kol_buy"], 1)
        self.assertIn(row["quality"], {"PASS", "WATCH", "FAIL"})
        self.assertEqual(row["entry"], "EARLY")
        self.assertEqual(row["execution"], "PASS")
        self.assertEqual(set(row["sources"]), {"5m_trending", "market_signal", "trenches"})

    def test_kline_is_on_demand_and_resolution_checked(self):
        service = DiscoveryV2Service(FakeResearch())
        out = service.kline(ADDR, "5m")
        self.assertEqual(out["resolution"], "5m")
        self.assertEqual(len(out["data"]["list"]), 2)
        with self.assertRaises(ValueError):
            service.kline(ADDR, "1s")


if __name__ == "__main__":
    unittest.main()
