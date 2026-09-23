"""Deterministic lifecycle events derived from stored token snapshots."""

from __future__ import annotations

from typing import Any


MC_LEVELS = (10_000, 50_000, 100_000, 500_000, 1_000_000, 10_000_000)


def derive_signals(current: dict[str, Any], previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    def event(kind: str, severity: str, **details: Any) -> dict[str, Any]:
        return {"timestamp": current["timestamp"], "chain": current["chain"], "address": current["address"],
                "symbol": current["symbol"], "event_type": kind, "severity": severity, "details": details}

    if previous is None:
        return [event("FIRST_SEEN", "info", rank=current.get("rank"), priority=current.get("priority_score"))]
    events: list[dict[str, Any]] = []
    if current["smart_money_count"] > previous["smart_money_count"]:
        events.append(event("SMART_MONEY_ENTRY", "positive", before=previous["smart_money_count"], after=current["smart_money_count"]))
    if current["smart_money_count"] < previous["smart_money_count"]:
        events.append(event("SMART_MONEY_EXIT", "warning", before=previous["smart_money_count"], after=current["smart_money_count"]))
    if current["kol_count"] > previous["kol_count"]:
        events.append(event("KOL_ENTRY", "positive", before=previous["kol_count"], after=current["kol_count"]))
    if previous["volume"] > 0 and current["volume"] >= max(25_000, previous["volume"] * 3):
        events.append(event("VOLUME_BREAKOUT", "positive", before=previous["volume"], after=current["volume"]))
    if current["buy_ratio"] - previous["buy_ratio"] >= 0.20:
        events.append(event("BUY_PRESSURE_JUMP", "positive", before=previous["buy_ratio"], after=current["buy_ratio"]))
    if current["priority_score"] - previous["priority_score"] >= 15:
        events.append(event("SCORE_JUMP", "positive", before=previous["priority_score"], after=current["priority_score"]))
    for level in MC_LEVELS:
        if previous["market_cap"] < level <= current["market_cap"]:
            events.append(event("MC_CROSS", "positive", level=level, direction="up"))
        elif current["market_cap"] < level <= previous["market_cap"]:
            events.append(event("MC_CROSS", "warning", level=level, direction="down"))
    if current["dev_holding_pct"] < previous["dev_holding_pct"] - 0.03:
        events.append(event("DEV_SELL", "warning", before=previous["dev_holding_pct"], after=current["dev_holding_pct"]))
    if (not current["renounced_mint"] and previous.get("renounced_mint", False)) or current["rug_ratio"] > previous.get("rug_ratio", 0) + 0.2:
        events.append(event("SECURITY_DEGRADATION", "critical", rug_before=previous.get("rug_ratio"), rug_after=current["rug_ratio"]))
    return events
