"""Small, local SQLite store for read-only GMGN research data."""

from __future__ import annotations

import json
import hashlib
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


SNAPSHOT_COLUMNS = (
    "timestamp, chain, address, symbol, price, market_cap, liquidity, age_seconds, "
    "volume, change_5m, change_1h, buys, sells, buy_ratio, smart_money_count, "
    "kol_count, top10_pct, bundler_pct, dev_holding_pct, dev_score, priority_score, decision, rank"
)


class ResearchStore:
    """Thread-safe SQLite persistence with one snapshot per token per time bucket."""

    def __init__(self, path: Path, snapshot_bucket_s: int = 30):
        self.path = path
        self.snapshot_bucket_s = snapshot_bucket_s
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS token_snapshots (
                  id INTEGER PRIMARY KEY,
                  timestamp INTEGER NOT NULL,
                  bucket INTEGER NOT NULL,
                  chain TEXT NOT NULL,
                  address TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  price REAL, market_cap REAL, liquidity REAL, age_seconds INTEGER,
                  volume REAL, change_5m REAL, change_1h REAL,
                  buys INTEGER, sells INTEGER, buy_ratio REAL,
                  smart_money_count INTEGER, kol_count INTEGER,
                  top10_pct REAL, bundler_pct REAL, dev_holding_pct REAL,
                  dev_score REAL, priority_score REAL, decision TEXT,
                  UNIQUE(chain, address, bucket)
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_token_time
                  ON token_snapshots(chain, address, timestamp DESC);
                CREATE TABLE IF NOT EXISTS research_signals (
                  id INTEGER PRIMARY KEY,
                  timestamp INTEGER NOT NULL,
                  bucket INTEGER NOT NULL,
                  chain TEXT NOT NULL,
                  address TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  event_type TEXT NOT NULL,
                  severity TEXT NOT NULL,
                  details_json TEXT NOT NULL,
                  UNIQUE(chain, address, event_type, bucket)
                );
                CREATE INDEX IF NOT EXISTS idx_signals_token_time
                  ON research_signals(chain, address, timestamp DESC);
                CREATE TABLE IF NOT EXISTS token_security_states (
                  id INTEGER PRIMARY KEY,
                  timestamp INTEGER NOT NULL,
                  chain TEXT NOT NULL,
                  address TEXT NOT NULL,
                  state_json TEXT NOT NULL,
                  state_hash TEXT NOT NULL,
                  UNIQUE(chain, address, state_hash)
                );
                CREATE TABLE IF NOT EXISTS smart_money_events (
                  id INTEGER PRIMARY KEY,
                  event_key TEXT NOT NULL UNIQUE,
                  timestamp INTEGER NOT NULL,
                  chain TEXT NOT NULL,
                  wallet TEXT NOT NULL,
                  wallet_type TEXT NOT NULL,
                  side TEXT NOT NULL,
                  token_address TEXT NOT NULL,
                  symbol TEXT,
                  entry_price REAL,
                  entry_market_cap REAL,
                  trade_amount REAL,
                  current_price REAL,
                  current_market_cap REAL,
                  unrealized_performance REAL,
                  raw_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_smart_events_time
                  ON smart_money_events(chain, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_smart_events_token
                  ON smart_money_events(chain, token_address, timestamp DESC);
                CREATE TABLE IF NOT EXISTS tracked_wallets (
                  wallet_address TEXT PRIMARY KEY,
                  first_seen INTEGER NOT NULL,
                  last_seen INTEGER NOT NULL,
                  classification TEXT,
                  track_record_score REAL,
                  copy_tradeability_score REAL,
                  token_count INTEGER,
                  trade_count INTEGER,
                  realized_pnl REAL,
                  win_rate REAL,
                  average_entry_market_cap REAL,
                  median_hold_time REAL,
                  activity_stats_json TEXT NOT NULL,
                  dev_flag INTEGER,
                  dev_score REAL,
                  last_evaluation_timestamp INTEGER
                );
                CREATE TABLE IF NOT EXISTS wallet_clusters (
                  id INTEGER PRIMARY KEY,
                  event_key TEXT NOT NULL UNIQUE,
                  timestamp INTEGER NOT NULL,
                  chain TEXT NOT NULL,
                  token_address TEXT NOT NULL,
                  symbol TEXT,
                  participating_wallets_json TEXT NOT NULL,
                  wallet_count INTEGER NOT NULL,
                  time_span_seconds INTEGER NOT NULL,
                  wallet_scores_json TEXT NOT NULL,
                  entry_market_caps_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clusters_token_time
                  ON wallet_clusters(chain, token_address, timestamp DESC);
                CREATE TABLE IF NOT EXISTS wallet_evaluations (
                  wallet_address TEXT PRIMARY KEY,
                  chain TEXT NOT NULL,
                  last_evaluated INTEGER NOT NULL,
                  result_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_search_history (
                  id INTEGER PRIMARY KEY,
                  wallet_address TEXT NOT NULL,
                  chain TEXT NOT NULL,
                  searched_at INTEGER NOT NULL,
                  last_evaluated INTEGER,
                  result_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_wallet_history_time
                  ON wallet_search_history(chain, wallet_address, searched_at DESC);
                CREATE TABLE IF NOT EXISTS research_settings (
                  key TEXT PRIMARY KEY,
                  value_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_cache (
                  cache_key TEXT PRIMARY KEY,
                  updated_at INTEGER NOT NULL,
                  payload_json TEXT NOT NULL,
                  stale INTEGER NOT NULL DEFAULT 0,
                  error TEXT
                );
                CREATE TABLE IF NOT EXISTS strategy_definitions (
                  id INTEGER PRIMARY KEY,
                  name TEXT NOT NULL,
                  config_json TEXT NOT NULL,
                  priority_score_version TEXT NOT NULL,
                  signal_rules_version TEXT NOT NULL,
                  risk_rules_version TEXT NOT NULL,
                  created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS backtest_runs (
                  id INTEGER PRIMARY KEY,
                  created_at INTEGER NOT NULL,
                  strategy_config_json TEXT NOT NULL,
                  start_ts INTEGER,
                  end_ts INTEGER,
                  priority_score_version TEXT NOT NULL,
                  signal_rules_version TEXT NOT NULL,
                  risk_rules_version TEXT NOT NULL,
                  sample_size INTEGER NOT NULL,
                  result_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS backtest_trades (
                  id INTEGER PRIMARY KEY,
                  run_id INTEGER NOT NULL,
                  token_address TEXT NOT NULL,
                  symbol TEXT,
                  signal_type TEXT,
                  entry_timestamp INTEGER NOT NULL,
                  entry_price REAL,
                  entry_market_cap REAL,
                  outcome_json TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES backtest_runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(run_id, entry_timestamp);
                CREATE INDEX IF NOT EXISTS idx_backtest_runs_created ON backtest_runs(created_at DESC);
                """
            )
            # Small forward-only migration: rank is retained to highlight material
            # movement without storing a second full snapshot representation.
            existing = {row[1] for row in conn.execute("PRAGMA table_info(token_snapshots)")}
            if "rank" not in existing:
                conn.execute("ALTER TABLE token_snapshots ADD COLUMN rank INTEGER")

    def latest_snapshot(self, chain: str, address: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM token_snapshots WHERE chain=? AND address=? ORDER BY timestamp DESC LIMIT 1",
                (chain, address),
            ).fetchone()
        return dict(row) if row else None

    def save_snapshot(self, snapshot: dict[str, Any]) -> bool:
        now = int(snapshot["timestamp"])
        bucket = now // self.snapshot_bucket_s
        values = [snapshot.get(k) for k in SNAPSHOT_COLUMNS.split(", ")]
        with self._lock, self._connect() as conn:
            result = conn.execute(
                f"INSERT OR IGNORE INTO token_snapshots (bucket, {SNAPSHOT_COLUMNS}) "
                f"VALUES (?, {','.join('?' for _ in values)})",
                [bucket, *values],
            )
        return result.rowcount > 0

    def save_signal(self, signal: dict[str, Any]) -> bool:
        now = int(signal["timestamp"])
        bucket = now // 60  # repeated observations of one event remain a single minute event
        with self._lock, self._connect() as conn:
            result = conn.execute(
                """INSERT OR IGNORE INTO research_signals
                (timestamp,bucket,chain,address,symbol,event_type,severity,details_json)
                VALUES (?,?,?,?,?,?,?,?)""",
                (now, bucket, signal["chain"], signal["address"], signal["symbol"],
                 signal["event_type"], signal["severity"],
                 json.dumps(signal.get("details", {}), separators=(",", ":"))),
            )
        return result.rowcount > 0

    def recent_signals(self, chain: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM research_signals WHERE chain=? ORDER BY timestamp DESC LIMIT ?",
                (chain, max(1, min(limit, 500))),
            ).fetchall()
        return [{**dict(row), "details": json.loads(row["details_json"])} for row in rows]

    def lifecycle(self, chain: str, address: str, limit: int = 1000) -> dict[str, Any]:
        with self._lock, self._connect() as conn:
            snapshots = conn.execute(
                "SELECT * FROM token_snapshots WHERE chain=? AND address=? ORDER BY timestamp ASC LIMIT ?",
                (chain, address, max(1, min(limit, 5000))),
            ).fetchall()
            signals = conn.execute(
                "SELECT * FROM research_signals WHERE chain=? AND address=? ORDER BY timestamp ASC LIMIT ?",
                (chain, address, max(1, min(limit, 5000))),
            ).fetchall()
        return {
            "snapshots": [dict(row) for row in snapshots],
            "signals": [{**dict(row), "details": json.loads(row["details_json"])} for row in signals],
        }

    def security_state(self, chain: str, address: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM token_security_states WHERE chain=? AND address=? ORDER BY timestamp DESC LIMIT 1",
                (chain, address),
            ).fetchone()
        return json.loads(row["state_json"]) if row else None

    def save_security_state(self, chain: str, address: str, state: dict[str, Any]) -> None:
        normalized = json.dumps(state, sort_keys=True, separators=(",", ":"))
        state_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO token_security_states(timestamp,chain,address,state_json,state_hash) VALUES(?,?,?,?,?)",
                (int(time.time()), chain, address, normalized, state_hash),
            )

    def save_smart_money_event(self, event: dict[str, Any]) -> bool:
        key = str(event.get("event_key") or "")
        if not key:
            key = hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()
        with self._lock, self._connect() as conn:
            cur = conn.execute("""INSERT OR IGNORE INTO smart_money_events
              (event_key,timestamp,chain,wallet,wallet_type,side,token_address,symbol,
               entry_price,entry_market_cap,trade_amount,current_price,current_market_cap,
               unrealized_performance,raw_json)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                key, int(event.get("timestamp") or time.time()), event.get("chain", "sol"),
                event.get("wallet", ""), event.get("wallet_type", "smart_money"), event.get("side", ""),
                event.get("token_address", ""), event.get("symbol") or "", event.get("entry_price"),
                event.get("entry_market_cap"), event.get("trade_amount"), event.get("current_price"),
                event.get("current_market_cap"), event.get("unrealized_performance"),
                json.dumps(event.get("raw", {}), separators=(",", ":"), default=str)))
        return cur.rowcount > 0

    def smart_money_events(self, chain: str = "sol", limit: int = 100, token_address: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM smart_money_events WHERE chain=?"; args: list[Any] = [chain]
        if token_address:
            sql += " AND token_address=?"; args.append(token_address)
        sql += " ORDER BY timestamp DESC LIMIT ?"; args.append(max(1, min(int(limit), 500)))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(row) for row in rows]

    def smart_money_intelligence(self, chain: str = "sol", since_ts: int | None = None,
                                 wallet_type: str | None = None, activity_limit: int = 250,
                                 max_events: int = 20000) -> dict[str, Any]:
        """Aggregate persisted Smart Money/KOL events without making any GMGN calls.

        "Observed holdings" deliberately means the latest side seen for a wallet/token pair is a buy.
        It is not presented as a complete on-chain balance because the track feed can start after a
        wallet entered a position and can omit transfers or activity outside the retained feed.
        """
        where = ["chain=?"]; args: list[Any] = [chain]
        if since_ts is not None:
            where.append("timestamp>=?"); args.append(int(since_ts))
        if wallet_type in {"smart_money", "kol"}:
            where.append("wallet_type=?"); args.append(wallet_type)
        cap = max(100, min(int(max_events), 100000))
        sql = f"""SELECT * FROM (
                    SELECT * FROM smart_money_events
                    WHERE {' AND '.join(where)}
                    ORDER BY timestamp DESC LIMIT ?
                  ) ORDER BY timestamp ASC, id ASC"""
        args.append(cap)
        with self._lock, self._connect() as conn:
            events = [dict(r) for r in conn.execute(sql, args).fetchall()]

        def _median(values):
            vals = sorted(float(v) for v in values if v is not None)
            if not vals: return None
            n = len(vals); mid = n // 2
            return vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2.0

        tokens: dict[str, dict[str, Any]] = {}
        wallets: dict[str, dict[str, Any]] = {}
        positions: dict[tuple[str, str], dict[str, Any]] = {}
        total_buys = total_sells = smart_events = kol_events = 0

        for e in events:
            wallet = str(e.get("wallet") or "")
            token = str(e.get("token_address") or "")
            if not wallet or not token:
                continue
            side = str(e.get("side") or "unknown").lower()
            kind = str(e.get("wallet_type") or "smart_money")
            amount = float(e["trade_amount"]) if e.get("trade_amount") is not None else None
            entry_mc = float(e["entry_market_cap"]) if e.get("entry_market_cap") is not None else None
            current_mc = float(e["current_market_cap"]) if e.get("current_market_cap") is not None else None
            ts = int(e.get("timestamp") or 0)
            if side == "buy": total_buys += 1
            elif side == "sell": total_sells += 1
            if kind == "kol": kol_events += 1
            else: smart_events += 1

            t = tokens.setdefault(token, {
                "token_address": token, "symbol": e.get("symbol") or "?", "wallets": set(),
                "smart_wallets": set(), "kol_wallets": set(), "buy_events": 0, "sell_events": 0,
                "buy_usd": 0.0, "sell_usd": 0.0, "entry_mcaps": [], "first_seen": ts,
                "last_seen": ts, "latest_current_market_cap": None,
            })
            t["symbol"] = e.get("symbol") or t["symbol"]
            t["wallets"].add(wallet)
            t["kol_wallets" if kind == "kol" else "smart_wallets"].add(wallet)
            t["first_seen"] = min(t["first_seen"], ts); t["last_seen"] = max(t["last_seen"], ts)
            if side == "buy": t["buy_events"] += 1
            elif side == "sell": t["sell_events"] += 1
            if amount is not None:
                if side == "buy": t["buy_usd"] += amount
                elif side == "sell": t["sell_usd"] += amount
            if entry_mc is not None and side == "buy": t["entry_mcaps"].append(entry_mc)
            if current_mc is not None: t["latest_current_market_cap"] = current_mc

            w = wallets.setdefault(wallet, {
                "wallet": wallet, "types": set(), "tokens": set(), "buy_events": 0, "sell_events": 0,
                "buy_usd": 0.0, "sell_usd": 0.0, "entry_mcaps": [], "first_seen": ts,
                "last_seen": ts, "observed_open_positions": 0,
            })
            w["types"].add(kind); w["tokens"].add(token)
            w["first_seen"] = min(w["first_seen"], ts); w["last_seen"] = max(w["last_seen"], ts)
            if side == "buy": w["buy_events"] += 1
            elif side == "sell": w["sell_events"] += 1
            if amount is not None:
                if side == "buy": w["buy_usd"] += amount
                elif side == "sell": w["sell_usd"] += amount
            if entry_mc is not None and side == "buy": w["entry_mcaps"].append(entry_mc)

            p = positions.setdefault((wallet, token), {
                "wallet": wallet, "token_address": token, "symbol": e.get("symbol") or "?",
                "types": set(), "buy_events": 0, "sell_events": 0, "buy_usd": 0.0, "sell_usd": 0.0,
                "entry_mcaps": [], "first_seen": ts, "last_seen": ts, "last_side": side,
                "latest_current_market_cap": None,
            })
            p["symbol"] = e.get("symbol") or p["symbol"]; p["types"].add(kind)
            p["first_seen"] = min(p["first_seen"], ts); p["last_seen"] = max(p["last_seen"], ts)
            p["last_side"] = side
            if side == "buy": p["buy_events"] += 1
            elif side == "sell": p["sell_events"] += 1
            if amount is not None:
                if side == "buy": p["buy_usd"] += amount
                elif side == "sell": p["sell_usd"] += amount
            if entry_mc is not None and side == "buy": p["entry_mcaps"].append(entry_mc)
            if current_mc is not None: p["latest_current_market_cap"] = current_mc

        token_rows = []
        for t in tokens.values():
            median_mc = _median(t.pop("entry_mcaps"))
            latest_mc = t["latest_current_market_cap"]
            t["unique_wallets"] = len(t.pop("wallets"))
            t["smart_wallets"] = len(t["smart_wallets"]); t["kol_wallets"] = len(t["kol_wallets"])
            t["median_entry_market_cap"] = median_mc
            t["observed_net_flow_usd"] = round(t["buy_usd"] - t["sell_usd"], 6)
            t["mcap_multiple"] = (latest_mc / median_mc) if latest_mc and median_mc and median_mc > 0 else None
            token_rows.append(t)
        token_rows.sort(key=lambda x: (x["unique_wallets"], x["buy_events"], x["last_seen"]), reverse=True)

        holding_rows = []
        for p in positions.values():
            if p["last_side"] != "buy":
                continue
            median_mc = _median(p.pop("entry_mcaps"))
            latest_mc = p["latest_current_market_cap"]
            p["wallet_type"] = "+".join(sorted(p.pop("types")))
            p["median_entry_market_cap"] = median_mc
            p["observed_net_flow_usd"] = round(p["buy_usd"] - p["sell_usd"], 6)
            p["mcap_multiple"] = (latest_mc / median_mc) if latest_mc and median_mc and median_mc > 0 else None
            p["status"] = "OBSERVED_OPEN"
            p["status_method"] = "last-observed-side"
            holding_rows.append(p)
            if p["wallet"] in wallets:
                wallets[p["wallet"]]["observed_open_positions"] += 1
        holding_rows.sort(key=lambda x: x["last_seen"], reverse=True)

        wallet_rows = []
        for w in wallets.values():
            w["wallet_type"] = "+".join(sorted(w.pop("types")))
            w["unique_tokens"] = len(w.pop("tokens"))
            w["median_entry_market_cap"] = _median(w.pop("entry_mcaps"))
            w["observed_net_flow_usd"] = round(w["buy_usd"] - w["sell_usd"], 6)
            wallet_rows.append(w)
        wallet_rows.sort(key=lambda x: (x["observed_open_positions"], x["buy_events"], x["last_seen"]), reverse=True)

        activity = []
        for e in reversed(events[-max(1, min(int(activity_limit), 1000)):]):
            item = {k: e.get(k) for k in (
                "id", "timestamp", "wallet", "wallet_type", "side", "token_address", "symbol",
                "entry_price", "entry_market_cap", "trade_amount", "current_price",
                "current_market_cap", "unrealized_performance")}
            if item.get("entry_market_cap") and item.get("current_market_cap"):
                item["mcap_multiple"] = item["current_market_cap"] / item["entry_market_cap"]
            else:
                item["mcap_multiple"] = None
            activity.append(item)

        return {
            "summary": {
                "events": len(events), "buy_events": total_buys, "sell_events": total_sells,
                "smart_money_events": smart_events, "kol_events": kol_events,
                "unique_wallets": len(wallets), "unique_tokens": len(tokens),
                "observed_open_positions": len(holding_rows),
                "first_event_at": events[0]["timestamp"] if events else None,
                "last_event_at": events[-1]["timestamp"] if events else None,
                "event_cap": cap, "event_cap_reached": len(events) >= cap,
            },
            "activity": activity, "holdings": holding_rows[:1000],
            "wallets": wallet_rows[:1000], "tokens": token_rows[:1000],
            "methodology": {
                "holdings": "Observed open interest: latest collected side for wallet/token is BUY; not a complete on-chain balance.",
                "market_caps": "Entry/current market caps are shown only when present in collected GMGN data.",
                "collection": "Built from persisted track smartmoney + track kol events; viewing this endpoint makes no GMGN call.",
            },
        }

    def upsert_tracked_wallet(self, wallet: str, now: int, **fields: Any) -> None:
        with self._lock, self._connect() as conn:
            old = conn.execute("SELECT * FROM tracked_wallets WHERE wallet_address=?", (wallet,)).fetchone()
            first = int(old["first_seen"]) if old else now
            vals = {
                "classification": fields.get("classification") or (old["classification"] if old else None),
                "track_record_score": fields.get("track_record_score", old["track_record_score"] if old else None),
                "copy_tradeability_score": fields.get("copy_tradeability_score", old["copy_tradeability_score"] if old else None),
                "token_count": fields.get("token_count", old["token_count"] if old else None),
                "trade_count": fields.get("trade_count", old["trade_count"] if old else None),
                "realized_pnl": fields.get("realized_pnl", old["realized_pnl"] if old else None),
                "win_rate": fields.get("win_rate", old["win_rate"] if old else None),
                "average_entry_market_cap": fields.get("average_entry_market_cap", old["average_entry_market_cap"] if old else None),
                "median_hold_time": fields.get("median_hold_time", old["median_hold_time"] if old else None),
                "activity_stats_json": json.dumps(fields.get("activity_stats", {}), separators=(",", ":")),
                "dev_flag": fields.get("dev_flag", old["dev_flag"] if old else None),
                "dev_score": fields.get("dev_score", old["dev_score"] if old else None),
                "last_evaluation_timestamp": fields.get("last_evaluation_timestamp", old["last_evaluation_timestamp"] if old else None),
            }
            conn.execute("""INSERT OR REPLACE INTO tracked_wallets
              (wallet_address,first_seen,last_seen,classification,track_record_score,copy_tradeability_score,
               token_count,trade_count,realized_pnl,win_rate,average_entry_market_cap,median_hold_time,
               activity_stats_json,dev_flag,dev_score,last_evaluation_timestamp)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (wallet, first, now, vals["classification"],
                vals["track_record_score"], vals["copy_tradeability_score"], vals["token_count"], vals["trade_count"],
                vals["realized_pnl"], vals["win_rate"], vals["average_entry_market_cap"], vals["median_hold_time"],
                vals["activity_stats_json"], vals["dev_flag"], vals["dev_score"], vals["last_evaluation_timestamp"]))

    def tracked_wallets(self, chain: str = "sol", limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM tracked_wallets ORDER BY last_seen DESC LIMIT ?", (max(1, min(limit, 500)),)).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["activity_stats"] = json.loads(item.pop("activity_stats_json") or "{}")
            out.append(item)
        return out

    def save_cluster(self, cluster: dict[str, Any]) -> bool:
        with self._lock, self._connect() as conn:
            cur = conn.execute("""INSERT OR IGNORE INTO wallet_clusters
              (event_key,timestamp,chain,token_address,symbol,participating_wallets_json,wallet_count,time_span_seconds,wallet_scores_json,entry_market_caps_json)
              VALUES (?,?,?,?,?,?,?,?,?,?)""", (cluster["event_key"], int(cluster["timestamp"]), cluster.get("chain", "sol"),
                cluster["token_address"], cluster.get("symbol") or "", json.dumps(cluster.get("participating_wallets", [])),
                int(cluster["wallet_count"]), int(cluster.get("time_span_seconds", 0)), json.dumps(cluster.get("wallet_scores", {})),
                json.dumps(cluster.get("entry_market_caps", {}))))
        return cur.rowcount > 0

    def wallet_clusters(self, chain: str = "sol", token_address: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT * FROM wallet_clusters WHERE chain=?"; args: list[Any] = [chain]
        if token_address:
            sql += " AND token_address=?"; args.append(token_address)
        sql += " ORDER BY timestamp DESC LIMIT ?"; args.append(max(1, min(limit, 500)))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            for key, dest in (("participating_wallets_json", "participating_wallets"), ("wallet_scores_json", "wallet_scores"), ("entry_market_caps_json", "entry_market_caps")):
                item[dest] = json.loads(item.pop(key) or ("[]" if dest == "participating_wallets" else "{}"))
            out.append(item)
        return out

    def save_wallet_evaluation(self, address: str, chain: str, result: dict[str, Any], now: int | None = None) -> None:
        ts = int(now or time.time()); encoded = json.dumps(result, separators=(",", ":"), default=str)
        with self._lock, self._connect() as conn:
            conn.execute("INSERT OR REPLACE INTO wallet_evaluations(wallet_address,chain,last_evaluated,result_json) VALUES(?,?,?,?)", (address, chain, ts, encoded))
            conn.execute("INSERT INTO wallet_search_history(wallet_address,chain,searched_at,last_evaluated,result_json) VALUES(?,?,?,?,?)", (address, chain, ts, ts, encoded))

    def wallet_evaluation(self, address: str, chain: str = "sol") -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM wallet_evaluations WHERE wallet_address=? AND chain=?", (address, chain)).fetchone()
        if not row: return None
        return {"last_evaluated": row["last_evaluated"], "result": json.loads(row["result_json"])}

    def wallet_history(self, chain: str = "sol", address: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        sql = "SELECT wallet_address,chain,searched_at,last_evaluated FROM wallet_search_history WHERE chain=?"; args: list[Any] = [chain]
        if address: sql += " AND wallet_address=?"; args.append(address)
        sql += " ORDER BY searched_at DESC LIMIT ?"; args.append(max(1, min(limit, 200)))
        with self._lock, self._connect() as conn: return [dict(row) for row in conn.execute(sql, args).fetchall()]

    def setting(self, key: str, default: Any = None) -> Any:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT value_json FROM research_settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_setting(self, key: str, value: Any) -> None:
        with self._lock, self._connect() as conn: conn.execute("INSERT OR REPLACE INTO research_settings(key,value_json) VALUES(?,?)", (key, json.dumps(value)))

    def counts(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            return {name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in ("token_snapshots", "research_signals", "smart_money_events", "tracked_wallets", "wallet_clusters")}

    def save_cache(self, key: str, payload: Any, updated_at: int | None = None, stale: bool = False, error: str | None = None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("INSERT OR REPLACE INTO research_cache(cache_key,updated_at,payload_json,stale,error) VALUES(?,?,?,?,?)",
                         (key, int(updated_at or time.time()), json.dumps(payload, separators=(",", ":"), default=str), int(stale), error))

    def load_cache(self, key: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn: row = conn.execute("SELECT * FROM research_cache WHERE cache_key=?", (key,)).fetchone()
        if not row: return None
        return {"updated_at": row["updated_at"], "payload": json.loads(row["payload_json"]), "stale": bool(row["stale"]), "error": row["error"]}

    def db_size(self) -> int:
        try: return int(self.path.stat().st_size)
        except OSError: return 0

    def maintenance(self, action: str) -> dict[str, Any]:
        if action not in {"checkpoint", "vacuum", "integrity"}: raise ValueError("unsupported maintenance action")
        with self._lock, self._connect() as conn:
            if action == "checkpoint": result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            elif action == "vacuum": conn.execute("VACUUM"); result = [["ok"]]
            else: result = conn.execute("PRAGMA integrity_check").fetchall()
        return {"action": action, "result": result}

    def backup(self, destination: Path) -> dict[str, Any]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            src = self._connect(); dst = sqlite3.connect(destination)
            try: src.backup(dst)
            finally: dst.close(); src.close()
        return {"path": str(destination), "size": int(destination.stat().st_size)}

    def historical_snapshots(self, chain: str = "sol", address: str | None = None,
                             start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM token_snapshots WHERE chain=?"; args: list[Any] = [chain]
        if address: sql += " AND address=?"; args.append(address)
        if start_ts is not None: sql += " AND timestamp>=?"; args.append(int(start_ts))
        if end_ts is not None: sql += " AND timestamp<=?"; args.append(int(end_ts))
        sql += " ORDER BY timestamp ASC, address ASC"
        with self._lock, self._connect() as conn: return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def historical_signals(self, chain: str = "sol", address: str | None = None,
                           start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM research_signals WHERE chain=?"; args: list[Any] = [chain]
        if address: sql += " AND address=?"; args.append(address)
        if start_ts is not None: sql += " AND timestamp>=?"; args.append(int(start_ts))
        if end_ts is not None: sql += " AND timestamp<=?"; args.append(int(end_ts))
        sql += " ORDER BY timestamp ASC, address ASC"
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [{**dict(r), "details": json.loads(r["details_json"])} for r in rows]

    def save_backtest(self, config: dict[str, Any], result: dict[str, Any], trades: list[dict[str, Any]],
                      start_ts: int | None, end_ts: int | None) -> int:
        now = int(time.time())
        with self._lock, self._connect() as conn:
            cur = conn.execute("""INSERT INTO backtest_runs(created_at,strategy_config_json,start_ts,end_ts,
              priority_score_version,signal_rules_version,risk_rules_version,sample_size,result_json)
              VALUES(?,?,?,?,?,?,?,?,?)""", (now, json.dumps(config, sort_keys=True), start_ts, end_ts,
                config.get("priority_score_version", "v1"), config.get("signal_rules_version", "v1"),
                config.get("risk_rules_version", "v1"), len(trades), json.dumps(result, sort_keys=True)))
            run_id = int(cur.lastrowid)
            for trade in trades:
                conn.execute("""INSERT INTO backtest_trades(run_id,token_address,symbol,signal_type,entry_timestamp,entry_price,entry_market_cap,outcome_json)
                  VALUES(?,?,?,?,?,?,?,?)""", (run_id, trade.get("token_address", ""), trade.get("symbol"), trade.get("signal_type"),
                    trade.get("entry_timestamp"), trade.get("entry_price"), trade.get("entry_market_cap"), json.dumps(trade, sort_keys=True)))
        return run_id

    def backtest_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn: rows = conn.execute("SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?", (max(1, min(limit, 200)),)).fetchall()
        return [{**dict(r), "strategy_config": json.loads(r["strategy_config_json"]), "result": json.loads(r["result_json"])} for r in rows]

    def backtest_trades(self, run_id: int) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn: rows = conn.execute("SELECT * FROM backtest_trades WHERE run_id=? ORDER BY entry_timestamp", (run_id,)).fetchall()
        return [{**dict(r), "outcome": json.loads(r["outcome_json"])} for r in rows]
