"""Deterministic historical replay/backtesting over SQLite snapshots only."""
from __future__ import annotations

import statistics
import time
from collections import defaultdict
from typing import Any

SIGNALS = ("FIRST_SEEN", "SMART_MONEY_ENTRY", "KOL_ENTRY", "WALLET_CLUSTER_ENTRY", "HIGH_SCORE_WALLET_ENTRY",
           "SMART_MONEY_ACCELERATION", "SMART_MONEY_DISTRIBUTION", "VOLUME_BREAKOUT", "BUY_PRESSURE_JUMP",
           "SCORE_JUMP", "MC_CROSS", "DEV_SELL", "SECURITY_DEGRADATION")
TARGETS = (60, 300, 900, 1800, 3600)

def _num(x):
    try: return float(x)
    except (TypeError, ValueError): return None

def _pct(values):
    vals = sorted(v for v in values if v is not None)
    if not vals: return {"count": 0, "average": None, "median": None, "p25": None, "p50": None, "p75": None, "p90": None}
    def q(p):
        idx = min(len(vals)-1, max(0, int(round((len(vals)-1)*p))))
        return vals[idx]
    return {"count": len(vals), "average": sum(vals)/len(vals), "median": statistics.median(vals), "p25": q(.25), "p50": q(.50), "p75": q(.75), "p90": q(.90)}

class ReplayEngine:
    def __init__(self, store): self.store = store

    def _filtered(self, cfg: dict[str, Any], start_ts=None, end_ts=None):
        rows = self.store.historical_snapshots(cfg.get("chain", "sol"), cfg.get("token") or None, start_ts, end_ts)
        out=[]
        for r in rows:
            tests=(("priority_score", cfg.get("min_priority_score")),("smart_money_count",cfg.get("min_smart_money_count")),
                   ("liquidity",cfg.get("min_liquidity")),("bundler_pct",cfg.get("max_bundler_pct")),("top10_pct",cfg.get("max_top10_pct")),
                   ("dev_score",cfg.get("min_dev_score")))
            if cfg.get("min_priority_score") is not None and (_num(r.get("priority_score")) or 0) < float(cfg["min_priority_score"]): continue
            if cfg.get("min_smart_money_count") is not None and (_num(r.get("smart_money_count")) or 0) < float(cfg["min_smart_money_count"]): continue
            if cfg.get("min_liquidity") is not None and (_num(r.get("liquidity")) or 0) < float(cfg["min_liquidity"]): continue
            if cfg.get("max_bundler_pct") is not None and (_num(r.get("bundler_pct")) or 0) > float(cfg["max_bundler_pct"]): continue
            if cfg.get("max_top10_pct") is not None and (_num(r.get("top10_pct")) or 0) > float(cfg["max_top10_pct"]): continue
            if cfg.get("min_dev_score") is not None and (_num(r.get("dev_score")) or 0) < float(cfg["min_dev_score"]): continue
            if cfg.get("market_cap_min") is not None and (_num(r.get("market_cap")) or 0) < float(cfg["market_cap_min"]): continue
            if cfg.get("market_cap_max") is not None and (_num(r.get("market_cap")) or 0) > float(cfg["market_cap_max"]): continue
            if cfg.get("age_min_seconds") is not None and (_num(r.get("age_seconds")) or 0) < float(cfg["age_min_seconds"]): continue
            if cfg.get("age_max_seconds") is not None and (_num(r.get("age_seconds")) or 0) > float(cfg["age_max_seconds"]): continue
            if cfg.get("min_buy_ratio") is not None and (_num(r.get("buy_ratio")) or 0) < float(cfg["min_buy_ratio"]): continue
            out.append(r)
        return out

    def replay(self, cfg: dict[str, Any]) -> dict[str, Any]:
        start, end = cfg.get("start_ts"), cfg.get("end_ts")
        rows = self._filtered(cfg, start, end)
        signals = self.store.historical_signals(cfg.get("chain", "sol"), cfg.get("token") or None, start, end)
        events=[]
        for r in rows: events.append({"timestamp":r["timestamp"],"kind":"snapshot","token_address":r["address"],"symbol":r["symbol"],"row":r})
        for s in signals:
            if cfg.get("signal_type") and s["event_type"] != cfg["signal_type"]: continue
            events.append({"timestamp":s["timestamp"],"kind":"signal","token_address":s["address"],"symbol":s["symbol"],"event_type":s["event_type"],"details":s.get("details",{})})
        events.sort(key=lambda x:(x["timestamp"], 0 if x["kind"]=="snapshot" else 1, x["token_address"]))
        return {"events":events,"start_ts":start,"end_ts":end,"snapshot_count":len(rows),"signal_count":sum(1 for x in events if x["kind"]=="signal"),"data_quality":self.data_quality(rows)}

    def _entries(self, cfg):
        rows=self._filtered(cfg,cfg.get("start_ts"),cfg.get("end_ts")); by_token=defaultdict(list)
        for r in rows: by_token[r["address"]].append(r)
        signals=self.store.historical_signals(cfg.get("chain","sol"),cfg.get("token") or None,cfg.get("start_ts"),cfg.get("end_ts"))
        wanted=set(cfg.get("entry_signals") or ["FIRST_SEEN"]); entries=[]; seen=set()
        for s in signals:
            if s["event_type"] not in wanted: continue
            candidates=[r for r in by_token.get(s["address"],[]) if r["timestamp"]==s["timestamp"] or r["timestamp"]<=s["timestamp"]]
            if not candidates: continue
            r=max(candidates,key=lambda x:x["timestamp"]); key=(r["address"],r["timestamp"],s["event_type"])
            if key in seen: continue
            seen.add(key); entries.append((s,r,by_token[r["address"]]))
        return entries

    def backtest(self,cfg:dict[str,Any])->dict[str,Any]:
        entries=self._entries(cfg); trades=[]; incomplete=0
        for s,r,allrows in entries:
            entry=_num(r.get("price")); outcome={"complete":True,"returns":{},"targets":{}}
            future=sorted((x for x in allrows if x["timestamp"]>=r["timestamp"]),key=lambda x:x["timestamp"])
            if entry is None or entry<=0: outcome["complete"]=False; incomplete+=1
            else:
                for horizon in TARGETS:
                    hit=next((x for x in future if x["timestamp"]>=r["timestamp"]+horizon and _num(x.get("price")) is not None),None)
                    key=f"{horizon//60}m"; outcome["targets"][key]=bool(hit)
                    outcome["returns"][key]=((_num(hit["price"])/entry)-1) if hit else None
                    if hit is None: outcome["complete"]=False
                window=[_num(x.get("price")) for x in future if x["timestamp"]<=r["timestamp"]+3600 and _num(x.get("price")) is not None]
                outcome["max_upside"]=(max(window)/entry-1) if window else None; outcome["max_drawdown"]=(min(window)/entry-1) if window else None
                outcome["max_market_cap"]=max((_num(x.get("market_cap")) for x in future if _num(x.get("market_cap")) is not None),default=None)
                outcome["min_market_cap"]=min((_num(x.get("market_cap")) for x in future if _num(x.get("market_cap")) is not None),default=None)
                for threshold,label in ((.25,"time_to_plus25"),(.5,"time_to_plus50"),(1.0,"time_to_plus100"),(-.25,"time_to_minus25"),(-.5,"time_to_minus50")):
                    hit=next((x for x in future if _num(x.get("price")) is not None and _num(x["price"])/entry-1>=threshold),None)
                    outcome[label]=(hit["timestamp"]-r["timestamp"]) if hit else None
            trades.append({"token_address":r["address"],"symbol":r["symbol"],"signal_type":s["event_type"],"entry_timestamp":r["timestamp"],"entry_price":entry,"entry_market_cap":r.get("market_cap"),**outcome})
            if not outcome["complete"] and entry is not None: incomplete+=1
        returns=[t["returns"].get("60m") for t in trades if t["returns"].get("60m") is not None]
        result={"rule_versions":{"priority_score":"v1","signal_rules":"v1","risk_rules":"v1"},"number_signals":len(entries),"valid_entries":len(trades)-incomplete,"incomplete":incomplete,"returns_60m":_pct(returns),"max_upside":_pct([t.get("max_upside") for t in trades]),"drawdown":_pct([t.get("max_drawdown") for t in trades]),"reaches":{"+25%":sum(1 for t in trades if t.get("time_to_plus25") is not None),"+50%":sum(1 for t in trades if t.get("time_to_plus50") is not None),"+100%":sum(1 for t in trades if t.get("time_to_plus100") is not None)},"data_quality":self.data_quality(self._filtered(cfg,cfg.get("start_ts"),cfg.get("end_ts")))}
        return {"result":result,"trades":trades}

    def signal_performance(self,cfg):
        output=[]
        for signal in SIGNALS:
            c=dict(cfg); c["entry_signals"]=[signal]; bt=self.backtest(c); ts=bt["trades"]; output.append({"signal_type":signal,"sample_count":len(ts),"median_entry_mc":statistics.median([_num(t["entry_market_cap"]) for t in ts if _num(t["entry_market_cap"]) is not None]) if any(_num(t["entry_market_cap"]) is not None for t in ts) else None,"median_5m_return":_pct([t["returns"].get("5m") for t in ts if t["returns"].get("5m") is not None])["median"],"median_15m_return":_pct([t["returns"].get("15m") for t in ts if t["returns"].get("15m") is not None])["median"],"median_60m_return":_pct([t["returns"].get("60m") for t in ts if t["returns"].get("60m") is not None])["median"],"median_max_upside":_pct([t.get("max_upside") for t in ts])["median"],"median_max_drawdown":_pct([t.get("max_drawdown") for t in ts])["median"],"incomplete":bt["result"]["incomplete"]})
        return output

    def data_quality(self, rows):
        by=defaultdict(list); missing={"price":0,"market_cap":0,"liquidity":0}
        for r in rows:
            by[r["address"]].append(r)
            for k in missing:
                if _num(r.get(k)) is None or _num(r.get(k))==0: missing[k]+=1
        intervals=[b[i]["timestamp"]-b[i-1]["timestamp"] for b in by.values() for i in range(1,len(b))]
        return {"snapshot_count":len(rows),"token_count":len(by),"average_snapshot_interval":(sum(intervals)/len(intervals) if intervals else None),"missing":missing,"tokens_insufficient_future":sum(1 for b in by.values() if len(b)<2),"collector_gaps":sum(1 for x in intervals if x>300)}
