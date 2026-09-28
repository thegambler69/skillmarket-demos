"""Budget-safe GMGN Discovery v2 aggregation.

This module is read-only. It combines a small number of cached GMGN market
queries into a candidate universe for research. It never calls swap/order APIs
and never starts a background collector.
"""
from __future__ import annotations

import time
from typing import Any

SIGNAL_NAMES = {
    1: "price_pattern",
    6: "price_spike",
    7: "ath",
    10: "bundler_sell",
    11: "cto",
    12: "smart_money_buy",
    20: "kol_buy",
}

_ALLOWED_KLINE = {"30s", "1m", "5m", "15m", "1h", "4h", "1d"}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truth(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _data(result: Any) -> Any:
    if isinstance(result, dict):
        return result.get("data", result)
    return result


def _rows_from_signal(result: Any) -> list[dict[str, Any]]:
    body = _data(result)
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("list", "signals", "items"):
            rows = body.get(key)
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        # Multi-group responses have varied between CLI versions. Degrade
        # gracefully by flattening only list-valued members.
        out: list[dict[str, Any]] = []
        for value in body.values():
            if isinstance(value, list):
                out.extend(x for x in value if isinstance(x, dict))
        return out
    return []


def _market_cap(row: dict[str, Any]) -> float | None:
    for key in ("market_cap", "usd_market_cap"):
        value = _num(row.get(key), 0.0)
        if value > 0:
            return value
    return None


def _created_ts(row: dict[str, Any]) -> int | None:
    for key in ("creation_timestamp", "created_timestamp", "open_timestamp"):
        value = int(_num(row.get(key), 0))
        if value > 0:
            return value
    return None


def _quality(row: dict[str, Any]) -> tuple[int, str, list[str]]:
    score = 100
    reasons: list[str] = []

    rug = _num(row.get("rug_ratio"))
    wash = _truth(row.get("is_wash_trading"))
    top10 = _num(row.get("top_10_holder_rate"))
    bundler = _num(row.get("bundler_rate") or row.get("bundler_trader_amount_rate"))
    rat = _num(row.get("rat_trader_amount_rate") or row.get("suspected_insider_hold_rate"))
    dev = _num(row.get("dev_team_hold_rate"))
    mint = row.get("renounced_mint")
    freeze = row.get("renounced_freeze_account")

    if wash:
        score -= 45
        reasons.append("wash trading flagged")
    if rug > 0.30:
        score -= 40
        reasons.append(f"rug ratio {rug:.0%}")
    elif rug > 0.15:
        score -= 15
        reasons.append(f"rug ratio {rug:.0%}")

    if top10 > 0.50:
        score -= 30
        reasons.append(f"top10 {top10:.0%}")
    elif top10 > 0.30:
        score -= 12
        reasons.append(f"top10 {top10:.0%}")

    if bundler > 0.30:
        score -= 25
        reasons.append(f"bundler {bundler:.0%}")
    elif bundler > 0.15:
        score -= 10
        reasons.append(f"bundler {bundler:.0%}")

    if rat > 0.30:
        score -= 20
        reasons.append(f"insider/rat {rat:.0%}")
    elif rat > 0.15:
        score -= 8
        reasons.append(f"insider/rat {rat:.0%}")

    if dev > 0.10:
        score -= 15
        reasons.append(f"dev hold {dev:.0%}")

    # Only score explicit authority values. Missing is unknown, not failure.
    if mint is not None and not _truth(mint):
        score -= 25
        reasons.append("mint authority active")
    if freeze is not None and not _truth(freeze):
        score -= 15
        reasons.append("freeze authority active")

    score = max(0, min(100, round(score)))
    label = "PASS" if score >= 75 else ("WATCH" if score >= 50 else "FAIL")
    return score, label, reasons


def _execution(mcap: float | None, liquidity: float) -> tuple[str, str]:
    if liquidity <= 0:
        return "N/A", "liquidity unavailable"
    ratio = (mcap / liquidity) if mcap and liquidity > 0 else None
    if liquidity >= 20_000 and (ratio is None or ratio <= 6):
        return "PASS", "usable liquidity"
    if liquidity >= 8_000:
        return "WATCH", "thin liquidity"
    return "FAIL", "very thin liquidity"


def _entry(stage: str, change5: float, change1h: float, buy_ratio: float,
           signal_types: set[int]) -> tuple[str, list[str]]:
    why: list[str] = []
    if 10 in signal_types:
        return "AVOID", ["bundler sell signal"]
    if stage in {"Early", "Near Grad"}:
        return "EARLY", ["pre-migration lifecycle"]
    if change5 >= 40 or change1h >= 200:
        return "LATE", ["move already extended"]
    if change5 < -6 and change1h < -12:
        return "AVOID", ["5m and 1h momentum both weak"]
    if 0 < change5 <= 25 and buy_ratio >= 0.52:
        why.append("positive 5m momentum")
        why.append("buyers still dominant")
        return "GOOD", why
    if change5 > 0:
        return "WAIT", ["positive momentum, confirmation incomplete"]
    return "WAIT", ["no clean entry setup yet"]


class DiscoveryV2Service:
    """Three-call discovery fan-in with per-source caching.

    One refresh uses at most:
      1x market trending (5m)
      1x market trenches (all lifecycle buckets)
      1x market signal (multi-group)
    The shared process scheduler remains the only subprocess path.
    """

    def __init__(self, research):
        self.research = research

    def _call(self, args: list[str], key: str, ttl: float) -> Any:
        return self.research._call(args, key, ttl_s=ttl)

    def snapshot(self) -> dict[str, Any]:
        started = time.time()
        errors: dict[str, str] = {}

        try:
            trend_result = self._call(
                ["market", "trending", "--chain", "sol", "--interval", "5m",
                 "--order-by", "volume", "--limit", "50"],
                "discovery:v2:trending5m", 45,
            )
            trend_rows = (_data(trend_result) or {}).get("rank") or []
        except Exception as exc:
            errors["trending_5m"] = str(exc)[:180]
            trend_rows = []

        try:
            trenches_result = self._call(
                ["market", "trenches", "--chain", "sol",
                 "--type", "new_creation", "--type", "near_completion", "--type", "completed",
                 "--filter-preset", "safe", "--limit", "40"],
                "discovery:v2:trenches", 60,
            )
            trenches = _data(trenches_result) or {}
        except Exception as exc:
            errors["trenches"] = str(exc)[:180]
            trenches = {}

        try:
            groups = '[{"signal_type":[12]},{"signal_type":[20]},{"signal_type":[6,7,10,11]}]'
            signal_result = self._call(
                ["market", "signal", "--chain", "sol", "--groups", groups],
                "discovery:v2:signals", 30,
            )
            signal_rows = _rows_from_signal(signal_result)
        except Exception as exc:
            errors["signals"] = str(exc)[:180]
            signal_rows = []

        candidates: dict[str, dict[str, Any]] = {}

        def ensure(address: str) -> dict[str, Any]:
            return candidates.setdefault(address, {
                "address": address,
                "symbol": "?",
                "name": "",
                "sources": set(),
                "signal_types": set(),
                "signal_counts": {},
                "stage": None,
                "row": {},
                "why": [],
            })

        # Lifecycle is authoritative when available.
        lifecycle_sets = [
            ("new_creation", "Early"),
            ("pump", "Near Grad"),  # API response key for near_completion
            ("completed", "Breakout"),
        ]
        for key, stage in lifecycle_sets:
            rows = trenches.get(key) if isinstance(trenches, dict) else []
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                address = str(row.get("address") or "")
                if not address:
                    continue
                item = ensure(address)
                item["stage"] = stage
                item["sources"].add("trenches")
                item["row"].update(row)
                item["symbol"] = str(row.get("symbol") or item["symbol"])
                item["name"] = str(row.get("name") or item["name"])

        # 5m trending supplies the freshest common market fields.
        for row in trend_rows:
            if not isinstance(row, dict):
                continue
            address = str(row.get("address") or "")
            if not address:
                continue
            item = ensure(address)
            item["sources"].add("5m_trending")
            item["row"].update(row)
            item["symbol"] = str(row.get("symbol") or item["symbol"])
            item["name"] = str(row.get("name") or item["name"])

        # Signals can surface tokens that are neither trenches nor top-50 trending.
        for sig in signal_rows:
            address = str(sig.get("token_address") or sig.get("address") or "")
            if not address:
                continue
            item = ensure(address)
            item["sources"].add("market_signal")
            stype = int(_num(sig.get("signal_type"), 0))
            if stype:
                item["signal_types"].add(stype)
                label = SIGNAL_NAMES.get(stype, f"signal_{stype}")
                item["signal_counts"][label] = item["signal_counts"].get(label, 0) + 1
            cur = sig.get("cur_data") or {}
            if isinstance(cur, dict):
                # Only fill fields not already provided by a richer rank row.
                for src, dst in (("liquidity", "liquidity"),
                                 ("top_10_holder_rate", "top_10_holder_rate"),
                                 ("holder_count", "holder_count")):
                    if dst not in item["row"] and cur.get(src) is not None:
                        item["row"][dst] = cur.get(src)
            if item["row"].get("market_cap") is None and sig.get("market_cap") is not None:
                item["row"]["market_cap"] = sig.get("market_cap")
            if not item["symbol"] or item["symbol"] == "?":
                item["symbol"] = str(sig.get("symbol") or "?")

        now = int(time.time())
        out: list[dict[str, Any]] = []
        for item in candidates.values():
            row = item["row"]
            mcap = _market_cap(row)
            liquidity = _num(row.get("liquidity"))
            buys = int(_num(row.get("buys") or row.get("buys_24h")))
            sells = int(_num(row.get("sells") or row.get("sells_24h")))
            total = buys + sells
            buy_ratio = buys / total if total else 0.0
            change5 = _num(row.get("price_change_percent5m") or row.get("price_change_percent"))
            change1h = _num(row.get("price_change_percent1h"))
            sm = int(_num(row.get("smart_degen_count")))
            kol = int(_num(row.get("renowned_count")))
            created = _created_ts(row)
            age_s = max(0, now - created) if created else None

            stage = item["stage"]
            if not stage:
                if created and age_s is not None and age_s <= 30 * 60:
                    stage = "Breakout"
                else:
                    stage = "Momentum"

            qscore, qlabel, qreasons = _quality(row)
            exec_label, exec_reason = _execution(mcap, liquidity)
            entry_label, entry_reasons = _entry(
                stage, change5, change1h, buy_ratio, item["signal_types"]
            )

            sm_signal = item["signal_counts"].get("smart_money_buy", 0)
            kol_signal = item["signal_counts"].get("kol_buy", 0)
            flow_points = min(10, sm * 2 + kol + sm_signal * 3 + kol_signal * 2)
            flow_label = "STRONG" if flow_points >= 6 else ("MEDIUM" if flow_points >= 3 else "WEAK")

            why: list[str] = []
            if stage == "Near Grad":
                why.append("Near graduation")
            elif stage == "Breakout":
                why.append("Recently completed / migrated")
            elif stage == "Early":
                why.append("Fresh launch")
            if "5m_trending" in item["sources"]:
                why.append("Top 5m activity")
            if sm_signal:
                why.append(f"Smart Money buy signal ×{sm_signal}")
            if kol_signal:
                why.append(f"KOL buy signal ×{kol_signal}")
            if item["signal_counts"].get("price_spike"):
                why.append("Price-spike signal")
            if item["signal_counts"].get("bundler_sell"):
                why.append("Bundler-sell warning")
            if sm:
                why.append(f"{sm} GMGN smart wallets")
            if kol:
                why.append(f"{kol} GMGN KOL wallets")

            out.append({
                "address": item["address"],
                "symbol": item["symbol"],
                "name": item["name"],
                "stage": stage,
                "sources": sorted(item["sources"]),
                "market_cap": mcap,
                "liquidity": liquidity or None,
                "price": _num(row.get("price")) or None,
                "age_seconds": age_s,
                "volume_5m": _num(row.get("volume")) if "5m_trending" in item["sources"] else None,
                "change_5m": change5,
                "change_1h": change1h,
                "buys": buys,
                "sells": sells,
                "buy_ratio": round(buy_ratio, 4),
                "smart_money_count": sm,
                "kol_count": kol,
                "signal_counts": item["signal_counts"],
                "quality_score": qscore,
                "quality": qlabel,
                "quality_reasons": qreasons,
                "flow": flow_label,
                "flow_points": flow_points,
                "entry": entry_label,
                "entry_reasons": entry_reasons,
                "execution": exec_label,
                "execution_reason": exec_reason,
                "top10_pct": _num(row.get("top_10_holder_rate")),
                "bundler_pct": _num(row.get("bundler_rate") or row.get("bundler_trader_amount_rate")),
                "rat_pct": _num(row.get("rat_trader_amount_rate") or row.get("suspected_insider_hold_rate")),
                "rug_ratio": _num(row.get("rug_ratio")),
                "holder_count": int(_num(row.get("holder_count"))),
                "platform": row.get("launchpad_platform") or "",
                "exchange": row.get("exchange") or "",
                "why": why[:6],
            })

        stage_rank = {"Breakout": 0, "Near Grad": 1, "Momentum": 2, "Early": 3}
        entry_rank = {"GOOD": 0, "EARLY": 1, "WAIT": 2, "LATE": 3, "AVOID": 4}
        out.sort(key=lambda x: (
            entry_rank.get(x["entry"], 9),
            -x["quality_score"],
            -x["flow_points"],
            stage_rank.get(x["stage"], 9),
        ))

        return {
            "chain": "sol",
            "timestamp": int(time.time()),
            "candidates": out,
            "counts": {
                "all": len(out),
                "early": sum(1 for x in out if x["stage"] == "Early"),
                "near_grad": sum(1 for x in out if x["stage"] == "Near Grad"),
                "breakout": sum(1 for x in out if x["stage"] == "Breakout"),
                "momentum": sum(1 for x in out if x["stage"] == "Momentum"),
                "good_entry": sum(1 for x in out if x["entry"] == "GOOD"),
            },
            "sources": {
                "trending_5m": len(trend_rows),
                "trenches": sum(len(trenches.get(k) or []) for k in ("new_creation", "pump", "completed")) if isinstance(trenches, dict) else 0,
                "signals": len(signal_rows),
            },
            "errors": errors,
            "elapsed_ms": round((time.time() - started) * 1000),
            "semantics": {
                "quality": "Deterministic structural heuristic; all factors are exposed.",
                "flow": "GMGN wallet counts + market-signal presence, not unique-wallet net USD flow yet.",
                "entry": "Deterministic timing label from lifecycle, short momentum, buy ratio and negative signals.",
                "execution": "Liquidity screen only; quote/price-impact validation is a later on-demand stage.",
            },
        }

    def kline(self, address: str, resolution: str = "5m") -> dict[str, Any]:
        address = self.research.validate_sol_address(address)
        if resolution not in _ALLOWED_KLINE:
            raise ValueError(f"Unsupported kline resolution: {resolution}")
        result = self._call(
            ["market", "kline", "--chain", "sol", "--address", address,
             "--resolution", resolution],
            f"discovery:v2:kline:{address}:{resolution}", 30,
        )
        return {"chain": "sol", "address": address, "resolution": resolution, "data": _data(result)}
