"""Read-only adapter around gmgn-cli for the browser research terminal.

All commands are argument arrays.  Credentials stay in the subprocess environment and are
never returned by this module.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any
from gmgn_guard import SHARED_GMGN_GUARD, RateLimitGuardError


SOL_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def truth(value: Any) -> bool:
    return value is True or value in (1, "1", "true", "yes", "YES", "True")


@dataclass
class CachedValue:
    value: Any
    expires_at: float


class GMGNResearchService:
    """Central cache, rate spacing, and last-known-good fallbacks for read-only calls."""

    def __init__(self, env_loader, ttl_s: float = 12.0, min_request_gap_s: float = 0.30):
        self.env_loader = env_loader
        self.ttl_s = ttl_s
        self.min_request_gap_s = min_request_gap_s
        self._cache: dict[str, CachedValue] = {}
        self._last_good: dict[str, Any] = {}
        self._last_errors: dict[str, str] = {}
        self._lock = threading.RLock()
        self._last_request = 0.0
        # GMGN Free is a 5/5 leaky bucket. Use that conservative baseline even
        # when a paid key may be faster; it prevents an inspector from turning
        # one browser click into several 429s. Values refill at five weight/sec.
        self._rate_tokens = 5.0
        self._rate_updated = time.monotonic()

    @staticmethod
    def _weight(args: list[str]) -> int:
        route = tuple(args[:2])
        if route == ("market", "trending"):
            return 3
        if route == ("token", "holders") or route == ("token", "traders"):
            return 5
        return 1

    def _acquire_weight(self, weight: int) -> None:
        now = time.monotonic()
        self._rate_tokens = min(5.0, self._rate_tokens + (now - self._rate_updated) * 5.0)
        self._rate_updated = now
        if self._rate_tokens < weight:
            time.sleep((weight - self._rate_tokens) / 5.0)
            now = time.monotonic()
            self._rate_tokens = min(5.0, self._rate_tokens + (now - self._rate_updated) * 5.0)
            self._rate_updated = now
        self._rate_tokens = max(0.0, self._rate_tokens - weight)

    @staticmethod
    def validate_sol_address(address: str) -> str:
        address = (address or "").strip()
        if not SOL_ADDRESS.fullmatch(address):
            raise ValueError("A Solana token address must be a base58 string of 32–44 characters.")
        return address

    def _env(self) -> dict[str, str]:
        # Keep the caller's Node environment intact.  Recent Node releases reject the
        # historical --use-system-ca option; gmgn-cli 1.6.x works with its default CA path.
        return {**os.environ, **self.env_loader()}

    def _call(self, args: list[str], cache_key: str, ttl_s: float | None = None) -> Any:
        ttl = self.ttl_s if ttl_s is None else ttl_s
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and cached.expires_at > now:
                return cached.value
            self._acquire_weight(self._weight(args))
            wait_for = self.min_request_gap_s - (now - self._last_request)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_request = time.monotonic()
            command = ["gmgn-cli", *args, "--raw"]
            try:
                SHARED_GMGN_GUARD.before_request()
                completed = subprocess.run(
                    command, capture_output=True, text=True, timeout=25, env=self._env(), check=False
                )
                if completed.returncode != 0:
                    raise RuntimeError(completed.stderr.strip() or "gmgn-cli exited unsuccessfully")
                result = json.loads(completed.stdout)
                if isinstance(result, dict) and result.get("code") not in (None, 0):
                    raise RuntimeError(result.get("message") or result.get("error") or "GMGN returned an error")
                self._cache[cache_key] = CachedValue(result, time.monotonic() + ttl)
                self._last_good[cache_key] = result
                self._last_errors.pop(cache_key, None)
                SHARED_GMGN_GUARD.record_success()
                return result
            except RateLimitGuardError as exc:
                self._last_errors[cache_key] = str(exc)
                if cache_key in self._last_good:
                    return self._last_good[cache_key]
                raise
            except Exception as exc:
                SHARED_GMGN_GUARD.record_failure(str(exc))
                self._last_errors[cache_key] = str(exc)
                if cache_key in self._last_good:
                    return self._last_good[cache_key]
                raise

    def last_error(self, cache_key: str) -> str | None:
        with self._lock: return self._last_errors.get(cache_key)

    @staticmethod
    def _data(result: Any) -> dict[str, Any]:
        return result.get("data", result) if isinstance(result, dict) else {}

    def trending_sol(self, limit: int = 50) -> list[dict[str, Any]]:
        result = self._call(
            ["market", "trending", "--chain", "sol", "--interval", "1h", "--order-by", "volume", "--limit", str(limit)],
            "trending:sol:1h", ttl_s=10,
        )
        return self._data(result).get("rank") or []

    def inspect_sol(self, address: str) -> dict[str, Any]:
        address = self.validate_sol_address(address)
        prefix = f"token:sol:{address}"
        info = self._data(self._call(["token", "info", "--chain", "sol", "--address", address], prefix + ":info", 30))
        security = self._data(self._call(["token", "security", "--chain", "sol", "--address", address], prefix + ":security", 30))
        pool = self._data(self._call(["token", "pool", "--chain", "sol", "--address", address], prefix + ":pool", 45))
        holders = self._data(self._call(["token", "holders", "--chain", "sol", "--address", address, "--limit", "20"], prefix + ":holders", 60)).get("list") or []
        traders = self._data(self._call(["token", "traders", "--chain", "sol", "--address", address, "--limit", "20"], prefix + ":traders", 60)).get("list") or []
        return {"info": info, "security": security, "pool": pool, "holders": holders, "traders": traders}


def deterministic_dev_score(dev_holding_pct: float, creator_status: str) -> int:
    """Exposure-only score used for the fast market screen, not a claim about creator history."""
    score = 60
    if creator_status in ("creator_close", "sell", "closed"):
        score += 25
    elif creator_status in ("creator_hold", "hold"):
        score -= 15
    score -= min(50, round(max(0.0, dev_holding_pct) * 100 * 2))
    return max(0, min(100, score))


def priority_score(row: dict[str, Any]) -> int:
    """Deterministic, bounded research priority: activity and quality, never an LLM verdict."""
    buy_ratio = row["buy_ratio"]
    score = 20
    score += min(25, max(0, number(row["change_5m"]) / 4))
    score += min(15, max(0, number(row["change_1h"]) / 20))
    score += min(15, max(0, (buy_ratio - 0.5) * 100))
    score += min(15, row["smart_money_count"] * 2 + row["kol_count"])
    score += min(10, max(0, 10 - row["bundler_pct"] * 30))
    score += min(10, max(0, 10 - row["top10_pct"] * 20))
    score += row["dev_score"] * 0.1
    return max(0, min(100, round(score)))


def normalize_trending(row: dict[str, Any], now: int) -> dict[str, Any]:
    buys, sells = int(number(row.get("buys"))), int(number(row.get("sells")))
    total = buys + sells
    dev_holding = number(row.get("dev_team_hold_rate"))
    creator_status = str(row.get("creator_token_status") or "")
    item = {
        "timestamp": now, "chain": "sol", "address": str(row.get("address") or ""),
        "symbol": str(row.get("symbol") or "?"), "price": number(row.get("price")),
        "market_cap": number(row.get("market_cap")), "liquidity": number(row.get("liquidity")),
        "age_seconds": max(0, now - int(number(row.get("creation_timestamp"), now))),
        "volume": number(row.get("volume")), "change_5m": number(row.get("price_change_percent5m")),
        "change_1h": number(row.get("price_change_percent1h")), "buys": buys, "sells": sells,
        "buy_ratio": round(buys / total, 4) if total else 0.0,
        "smart_money_count": int(number(row.get("smart_degen_count"))),
        "kol_count": int(number(row.get("renowned_count"))),
        "top10_pct": number(row.get("top_10_holder_rate")), "bundler_pct": number(row.get("bundler_rate")),
        "dev_holding_pct": dev_holding, "creator_status": creator_status,
        "rug_ratio": number(row.get("rug_ratio")), "is_wash_trading": bool(row.get("is_wash_trading")),
        "renounced_mint": truth(row.get("renounced_mint")),
        "renounced_freeze_account": truth(row.get("renounced_freeze_account")),
        "rank": int(number(row.get("rank"))), "platform": row.get("launchpad_platform") or "",
    }
    item["dev_score"] = deterministic_dev_score(dev_holding, creator_status)
    item["priority_score"] = priority_score(item)
    item["decision"] = (
        "skip" if item["rug_ratio"] > 0.3 or item["is_wash_trading"] else
        "watch" if item["top10_pct"] > 0.5 or item["bundler_pct"] > 0.3 else "research"
    )
    return item


def deterministic_risk(info: dict[str, Any], security: dict[str, Any]) -> dict[str, Any]:
    """Published threshold rules only; no model or natural-language safety decision."""
    stat, tags, dev = info.get("stat") or {}, info.get("wallet_tags_stat") or {}, info.get("dev") or {}
    get = lambda key, default=None: security.get(key, stat.get(key, dev.get(key, default)))
    factors = {
        "mint_renounced": truth(get("renounced_mint")),
        "freeze_renounced": truth(get("renounced_freeze_account")),
        "rug_ratio": number(get("rug_ratio")),
        "top10_pct": number(get("top_10_holder_rate")),
        "dev_holding_pct": number(get("dev_team_hold_rate")),
        "bundler_pct": number(get("bundler_trader_amount_rate", get("top_bundler_trader_percentage", 0))),
        "insider_pct": number(get("rat_trader_amount_rate", get("top_rat_trader_percentage", 0))),
        "wash_trading": bool(get("is_wash_trading")),
        "creator_status": str(get("creator_token_status") or "unknown"),
        "smart_money_count": int(number(tags.get("smart_wallets"))),
        "kol_count": int(number(tags.get("renowned_wallets"))),
    }
    red_flags: list[str] = []
    warnings: list[str] = []
    if not factors["mint_renounced"]: red_flags.append("Mint authority is not renounced")
    if not factors["freeze_renounced"]: red_flags.append("Freeze authority is not renounced")
    if factors["rug_ratio"] > 0.3: red_flags.append("GMGN rug ratio is above 0.30")
    if factors["top10_pct"] > 0.5: red_flags.append("Top-10 concentration is above 50%")
    if factors["wash_trading"]: red_flags.append("GMGN flags wash trading")
    if factors["bundler_pct"] > 0.3: warnings.append("Bundler activity is above 30%")
    if factors["dev_holding_pct"] > 0.1: warnings.append("Developer-team holding is above 10%")
    if factors["insider_pct"] > 0.3: warnings.append("Insider trading share is above 30%")
    decision = "skip" if red_flags else "watch" if warnings else "research"
    return {"method": "deterministic_thresholds_v1", "decision": decision,
            "red_flags": red_flags, "warnings": warnings, "factors": factors}
