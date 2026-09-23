"""Read-only Smart Money/KOL collection and deterministic cluster signals."""
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from typing import Any


class SmartMoneyService:
    def __init__(self, research, store, settings_getter=None):
        self.research = research
        self.store = store
        self.settings_getter = settings_getter or (lambda k, d=None: d)
        self._lock = threading.RLock()
        self._cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self.last_success: int | None = None
        self.last_error: str | None = None
        self.stale = False

    @staticmethod
    def _rows(result: Any) -> list[dict[str, Any]]:
        if isinstance(result, dict):
            data = result.get("data", result)
            if isinstance(data, dict):
                return data.get("list") or data.get("activities") or data.get("trades") or []
            if isinstance(data, list): return data
        return result if isinstance(result, list) else []

    def _call(self, kind: str, ttl: float = 30.0) -> list[dict[str, Any]]:
        now = time.monotonic()
        cached = self._cache.get(kind)
        if cached and cached[0] > now: return cached[1]
        args = ["track", kind, "--chain", "sol", "--limit", "100"]
        result = self.research._call(args, f"track:sol:{kind}", ttl_s=ttl)
        rows = self._rows(result)
        self._cache[kind] = (now + ttl, rows)
        return rows

    @staticmethod
    def _num(value):
        try: return float(value)
        except (TypeError, ValueError): return None

    def normalize(self, row: dict[str, Any], wallet_type: str, market: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        maker_info = row.get("maker_info") or {}
        wallet = row.get("maker") or row.get("wallet") or maker_info.get("address")
        token = row.get("base_token") or row.get("token") or {}
        address = row.get("base_address") or row.get("token_address") or token.get("address") or token.get("token_address")
        if not wallet or not address: return None
        side = str(row.get("side") or row.get("event_type") or "").lower()
        close = row.get("is_open_or_close")
        if close in (1, "1", True) and side in ("buy", "add", "open", ""):
            side = "sell"
        if side in ("add", "open"): side = "buy"
        ts = int(self._num(row.get("timestamp")) or time.time())
        entry = self._num(row.get("price_usd") or row.get("price"))
        amount = self._num(row.get("amount_usd") or row.get("buy_cost_usd") or row.get("cost_usd"))
        m = market.get(address) or {}
        current = self._num(row.get("price_now") or row.get("current_price") or m.get("price"))
        perf = self._num(row.get("price_change"))
        if perf is None and entry and current: perf = current / entry - 1.0
        event_key = str(row.get("transaction_hash") or row.get("tx_hash") or row.get("id") or f"{wallet}:{address}:{ts}:{side}")
        return {
            "event_key": f"{wallet_type}:{event_key}", "timestamp": ts, "chain": "sol", "wallet": wallet,
            "wallet_type": wallet_type, "side": side or "unknown", "token_address": address,
            "symbol": row.get("symbol") or token.get("symbol") or m.get("symbol") or "?",
            "entry_price": entry, "entry_market_cap": self._num(row.get("entry_market_cap") or row.get("market_cap")),
            "trade_amount": amount, "current_price": current, "current_market_cap": self._num(row.get("current_market_cap") or m.get("market_cap")),
            "unrealized_performance": perf, "tags": maker_info.get("tags") or row.get("tags") or [], "raw": row,
        }

    def collect_once(self, force: bool = False) -> dict[str, Any]:
        with self._lock:
            try:
                if force: self._cache.clear()
                try:
                    trend = self.research.trending_sol(100)
                except Exception:
                    trend = []
                market = {str(x.get("address")): x for x in trend if x.get("address")}
                normalized: list[dict[str, Any]] = []
                for kind, label in (("smartmoney", "smart_money"), ("kol", "kol")):
                    for row in self._call(kind):
                        event = self.normalize(row, label, market)
                        if event: normalized.append(event)
                for event in normalized:
                    self.store.save_smart_money_event(event)
                    self.store.upsert_tracked_wallet(event["wallet"], event["timestamp"], classification=event["wallet_type"])
                    if event["side"] == "buy":
                        signal_type = "KOL_ENTRY" if event["wallet_type"] == "kol" else "SMART_MONEY_ENTRY"
                        self.store.save_signal({"timestamp": event["timestamp"], "chain": "sol", "address": event["token_address"], "symbol": event.get("symbol") or "?", "event_type": signal_type, "severity": "positive", "details": {"wallet": event["wallet"], "wallet_type": event["wallet_type"], "amount": event.get("trade_amount")}})
                    elif event["side"] == "sell":
                        signal_type = "KOL_EXIT" if event["wallet_type"] == "kol" else "SMART_MONEY_EXIT"
                        self.store.save_signal({"timestamp": event["timestamp"], "chain": "sol", "address": event["token_address"], "symbol": event.get("symbol") or "?", "event_type": signal_type, "severity": "warning", "details": {"wallet": event["wallet"], "wallet_type": event["wallet_type"], "amount": event.get("trade_amount")}})
                clusters = self._clusters(normalized)
                for cluster in clusters:
                    if self.store.save_cluster(cluster):
                        self.store.save_signal({"timestamp": cluster["timestamp"], "chain": "sol", "address": cluster["token_address"], "symbol": cluster.get("symbol") or "?", "event_type": "WALLET_CLUSTER_ENTRY", "severity": "positive", "details": cluster})
                # Deterministic acceleration/distribution: compare recent buy/sell counts per token.
                by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for event in normalized: by_token[event["token_address"]].append(event)
                for token, rows in by_token.items():
                    buys = sum(1 for x in rows if x["side"] == "buy"); sells = sum(1 for x in rows if x["side"] == "sell")
                    if buys >= 3:
                        x = rows[-1]; self.store.save_signal({"timestamp": x["timestamp"], "chain": "sol", "address": token, "symbol": x.get("symbol") or "?", "event_type": "SMART_MONEY_ACCELERATION", "severity": "positive", "details": {"buy_events": buys}})
                    if sells >= 2 and sells >= buys:
                        x = rows[-1]; self.store.save_signal({"timestamp": x["timestamp"], "chain": "sol", "address": token, "symbol": x.get("symbol") or "?", "event_type": "SMART_MONEY_DISTRIBUTION", "severity": "warning", "details": {"sell_events": sells, "buy_events": buys}})
                self.last_success = int(time.time()); self.last_error = None; self.stale = False
                return {"events": len(normalized), "clusters": len(clusters), "stale": False}
            except Exception as exc:
                self.last_error = str(exc); self.stale = True
                return {"events": len(self.store.smart_money_events(limit=100)), "clusters": 0, "stale": True, "error": self.last_error}

    def _clusters(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        minimum = int(self.settings_getter("cluster_min_wallets", 3) or 3)
        window = int(self.settings_getter("cluster_window_seconds", 1800) or 1800)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for e in events:
            if e["side"] == "buy": grouped[e["token_address"]].append(e)
        result = []
        for token, rows in grouped.items():
            rows.sort(key=lambda x: x["timestamp"])
            for i, first in enumerate(rows):
                cohort = [x for x in rows[i:] if x["timestamp"] - first["timestamp"] <= window]
                wallets = sorted({x["wallet"] for x in cohort})
                if len(wallets) < minimum: continue
                last = max(x["timestamp"] for x in cohort)
                key = f"sol:{token}:{','.join(wallets)}:{first['timestamp']//60}"
                result.append({"event_key": key, "timestamp": last, "chain": "sol", "token_address": token,
                    "symbol": next((x.get("symbol") for x in cohort if x.get("symbol")), "?"),
                    "participating_wallets": wallets, "wallet_count": len(wallets), "time_span_seconds": last-first["timestamp"],
                    "wallet_scores": {}, "entry_market_caps": {x["wallet"]: x.get("entry_market_cap") for x in cohort if x.get("entry_market_cap") is not None}})
                break
        return result


class SmartMoneyCollector:
    def __init__(self, service, interval_getter):
        self.service = service; self.interval_getter = interval_getter; self.stop = threading.Event(); self.thread = None

    def start(self):
        if self.thread and self.thread.is_alive(): return
        self.thread = threading.Thread(target=self._run, name="gmgn-smart-money", daemon=True); self.thread.start()

    def _run(self):
        while not self.stop.is_set():
            self.service.collect_once()
            self.stop.wait(max(10, int(self.interval_getter() or 60)))
