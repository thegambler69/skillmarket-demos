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
                  entry_market_cap_source TEXT,
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

            smart_existing = {row[1] for row in conn.execute("PRAGMA table_info(smart_money_events)")}
            if "entry_market_cap_source" not in smart_existing:
                conn.execute("ALTER TABLE smart_money_events ADD COLUMN entry_market_cap_source TEXT")

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
               entry_price,entry_market_cap,entry_market_cap_source,trade_amount,current_price,current_market_cap,
               unrealized_performance,raw_json)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                key, int(event.get("timestamp") or time.time()), event.get("chain", "sol"),
                event.get("wallet", ""), event.get("wallet_type", "smart_money"), event.get("side", ""),
                event.get("token_address", ""), event.get("symbol") or "", event.get("entry_price"),
                event.get("entry_market_cap"), event.get("entry_market_cap_source"),
                event.get("trade_amount"), event.get("current_price"),
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
                                 wallet_type: str | None = None, activity_limit: int = 250) -> dict[str, Any]:
        """Aggregate persisted Smart Money/KOL events without making any GMGN calls.

        V2 feature data is deliberately descriptive rather than scored. It separates
        independent-wallet activity from repeated transactions, classifies obvious
        base/stable assets, and measures fresh-flow/acceleration over short windows.
        """
        where = ["chain=?"]; args: list[Any] = [chain]
        if since_ts is not None:
            where.append("timestamp>=?"); args.append(int(since_ts))
        if wallet_type in {"smart_money", "kol"}:
            where.append("wallet_type=?"); args.append(wallet_type)

        sql = f"""SELECT * FROM smart_money_events
                  WHERE {' AND '.join(where)}
                  ORDER BY timestamp ASC, id ASC"""

        now_ts = int(time.time())

        # Short-window features need 2x the largest window so current 1h can
        # always be compared with the previous 1h, even when API scope is 1h.
        feature_history_seconds = 7200
        feature_where = ["chain=?", "timestamp>=?"]
        feature_args: list[Any] = [chain, now_ts - feature_history_seconds]

        lifecycle_where = ["chain=?"]
        lifecycle_args: list[Any] = [chain]

        if wallet_type in {"smart_money", "kol"}:
            feature_where.append("wallet_type=?")
            feature_args.append(wallet_type)
            lifecycle_where.append("wallet_type=?")
            lifecycle_args.append(wallet_type)

        feature_sql = f"""SELECT * FROM smart_money_events
                          WHERE {' AND '.join(feature_where)}
                          ORDER BY timestamp ASC, id ASC"""

        lifecycle_sql = f"""
            SELECT
                wallet,
                token_address,
                MIN(timestamp) AS first_seen_ts,
                MIN(CASE WHEN LOWER(side)='buy' THEN timestamp END) AS first_buy_ts,
                MIN(CASE WHEN LOWER(side)='sell' THEN timestamp END) AS first_sell_ts,
                SUM(CASE WHEN LOWER(side)='buy' THEN 1 ELSE 0 END) AS buy_events,
                SUM(CASE WHEN LOWER(side)='sell' THEN 1 ELSE 0 END) AS sell_events
            FROM smart_money_events
            WHERE {' AND '.join(lifecycle_where)}
            GROUP BY wallet, token_address
        """

        with self._lock, self._connect() as conn:
            events = [dict(r) for r in conn.execute(sql, args)]
            feature_events = [
                dict(r) for r in conn.execute(feature_sql, feature_args)
            ]
            lifecycle_rows = [
                dict(r) for r in conn.execute(lifecycle_sql, lifecycle_args)
            ]

        lifecycle_first_buy = {
            (str(r["wallet"]), str(r["token_address"])):
                (int(r["first_buy_ts"]) if r["first_buy_ts"] is not None else None)
            for r in lifecycle_rows
        }

        lifecycle_by_token: dict[str, list[dict[str, Any]]] = {}
        for row in lifecycle_rows:
            lifecycle_by_token.setdefault(
                str(row["token_address"]), []
            ).append(row)

        feature_events_by_token: dict[str, list[dict[str, Any]]] = {}
        for event in feature_events:
            token = str(event.get("token_address") or "")
            if token:
                feature_events_by_token.setdefault(token, []).append(event)

        feature_windows = {
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
        }

        # Exact-address exclusions only. Symbols are intentionally not trusted because
        # arbitrary launch tokens can spoof WSOL/USDC/etc.
        known_assets = {
            "So11111111111111111111111111111111111111112":
                ("BASE_ASSET", False, "known WSOL address"),
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":
                ("STABLE_ASSET", False, "known USDC address"),
            "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB":
                ("STABLE_ASSET", False, "known USD1 address"),
            "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij":
                ("MAJOR_WRAPPED_ASSET", False, "known cbBTC address"),
        }

        def _num(value):
            try:
                return float(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        def _median(values):
            vals = sorted(float(v) for v in values if v is not None)
            if not vals:
                return None
            n = len(vals); mid = n // 2
            return vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2.0

        def _launchpad(event):
            try:
                raw = json.loads(event.get("raw_json") or "{}")
            except Exception:
                return None
            token = raw.get("base_token") or raw.get("token") or {}
            if not isinstance(token, dict):
                token = {}
            value = (
                token.get("launchpad")
                or token.get("launchpad_platform")
                or raw.get("launchpad")
                or raw.get("launchpad_platform")
            )
            value = str(value or "").strip()
            return value or None

        def _classify(address, launchpads):
            if address in known_assets:
                asset_class, eligible, reason = known_assets[address]
                return {
                    "asset_class": asset_class,
                    "eligible": eligible,
                    "eligibility_reason": reason,
                }

            observed = sorted(x for x in launchpads if x)
            if observed:
                return {
                    "asset_class": "LAUNCHPAD_TOKEN",
                    "eligible": True,
                    "eligibility_reason": "launchpad metadata observed: " + ", ".join(observed),
                }

            return {
                "asset_class": "NON_LAUNCH_TOKEN",
                "eligible": True,
                "eligibility_reason": "non-base/non-stable asset; no launchpad metadata observed",
            }

        def _window_metrics(rows):
            buyers = set()
            smart_buyers = set()
            kol_buyers = set()
            sellers = set()
            buy_sizes = []
            entry_mcaps = []
            buy_usd = 0.0
            sell_usd = 0.0
            qualified_sell_usd = 0.0
            preexisting_exit_sell_usd = 0.0
            buy_events = 0
            sell_events = 0
            wallet_buy_events = {}
            wallet_buy_usd = {}

            for event in rows:
                wallet = str(event.get("wallet") or "")
                side = str(event.get("side") or "").lower()
                kind = str(event.get("wallet_type") or "smart_money")
                amount = _num(event.get("trade_amount"))
                entry_mc = _num(event.get("entry_market_cap"))

                if side == "buy":
                    buy_events += 1
                    buyers.add(wallet)
                    if kind == "kol":
                        kol_buyers.add(wallet)
                    else:
                        smart_buyers.add(wallet)

                    wallet_buy_events[wallet] = wallet_buy_events.get(wallet, 0) + 1
                    if amount is not None:
                        buy_usd += amount
                        buy_sizes.append(amount)
                        wallet_buy_usd[wallet] = wallet_buy_usd.get(wallet, 0.0) + amount
                    if entry_mc is not None:
                        entry_mcaps.append(entry_mc)

                elif side == "sell":
                    sell_events += 1
                    sellers.add(wallet)
                    if amount is not None:
                        sell_usd += amount

                        token_address = str(event.get("token_address") or "")
                        event_ts = int(event.get("timestamp") or 0)
                        first_buy_ts = lifecycle_first_buy.get(
                            (wallet, token_address)
                        )

                        if (
                            first_buy_ts is not None
                            and first_buy_ts <= event_ts
                        ):
                            qualified_sell_usd += amount
                        else:
                            preexisting_exit_sell_usd += amount

            largest_buyer_share = None
            if buy_usd > 0 and wallet_buy_usd:
                largest_buyer_share = max(wallet_buy_usd.values()) / buy_usd

            return {
                "unique_buyers": len(buyers),
                "smart_buyers": len(smart_buyers),
                "kol_buyers": len(kol_buyers),
                "unique_sellers": len(sellers),
                "buy_events": buy_events,
                "sell_events": sell_events,
                "buy_usd": round(buy_usd, 6),
                "sell_usd": round(sell_usd, 6),

                # Legacy/raw observed flow retained for compatibility.
                "net_flow_usd": round(buy_usd - sell_usd, 6),
                "observed_net_flow_usd": round(buy_usd - sell_usd, 6),

                # Lifecycle-qualified flow does not treat a first-observed SELL
                # as proof of fresh bearish positioning.
                "qualified_sell_usd": round(qualified_sell_usd, 6),
                "preexisting_exit_sell_usd":
                    round(preexisting_exit_sell_usd, 6),
                "qualified_net_flow_usd":
                    round(buy_usd - qualified_sell_usd, 6),

                "median_buy_size_usd": _median(buy_sizes),
                "median_entry_market_cap": _median(entry_mcaps),
                "smart_kol_presence": bool(smart_buyers and kol_buyers),
                "smart_kol_convergence": bool(
                    smart_buyers and kol_buyers and len(buyers) >= 2
                ),
                "dual_classified_buyers": len(smart_buyers & kol_buyers),
                "smart_only_buyers": len(smart_buyers - kol_buyers),
                "kol_only_buyers": len(kol_buyers - smart_buyers),
                "repeat_buyer_wallets": sum(1 for n in wallet_buy_events.values() if n >= 2),
                "transactions_per_unique_buyer":
                    (buy_events / len(buyers)) if buyers else None,
                "largest_buyer_flow_share": largest_buyer_share,
            }

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
            amount = _num(e.get("trade_amount"))
            entry_mc = _num(e.get("entry_market_cap"))
            current_mc = _num(e.get("current_market_cap"))
            ts = int(e.get("timestamp") or 0)
            launchpad = _launchpad(e)

            if side == "buy":
                total_buys += 1
            elif side == "sell":
                total_sells += 1

            if kind == "kol":
                kol_events += 1
            else:
                smart_events += 1

            t = tokens.setdefault(token, {
                "token_address": token,
                "symbol": e.get("symbol") or "?",
                "wallets": set(),
                "smart_wallets": set(),
                "kol_wallets": set(),
                "buy_events": 0,
                "sell_events": 0,
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "entry_mcaps": [],
                "first_seen": ts,
                "last_seen": ts,
                "latest_current_market_cap": None,
                "_events": [],
                "_launchpads": set(),
            })

            t["symbol"] = e.get("symbol") or t["symbol"]
            t["wallets"].add(wallet)
            t["kol_wallets" if kind == "kol" else "smart_wallets"].add(wallet)
            t["first_seen"] = min(t["first_seen"], ts)
            t["last_seen"] = max(t["last_seen"], ts)
            t["_events"].append(e)

            if launchpad:
                t["_launchpads"].add(launchpad)

            if side == "buy":
                t["buy_events"] += 1
            elif side == "sell":
                t["sell_events"] += 1

            if amount is not None:
                if side == "buy":
                    t["buy_usd"] += amount
                elif side == "sell":
                    t["sell_usd"] += amount

            if entry_mc is not None and side == "buy":
                t["entry_mcaps"].append(entry_mc)

            if current_mc is not None:
                t["latest_current_market_cap"] = current_mc

            w = wallets.setdefault(wallet, {
                "wallet": wallet,
                "types": set(),
                "tokens": set(),
                "buy_events": 0,
                "sell_events": 0,
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "entry_mcaps": [],
                "first_seen": ts,
                "last_seen": ts,
                "observed_open_positions": 0,
            })

            w["types"].add(kind)
            w["tokens"].add(token)
            w["first_seen"] = min(w["first_seen"], ts)
            w["last_seen"] = max(w["last_seen"], ts)

            if side == "buy":
                w["buy_events"] += 1
            elif side == "sell":
                w["sell_events"] += 1

            if amount is not None:
                if side == "buy":
                    w["buy_usd"] += amount
                elif side == "sell":
                    w["sell_usd"] += amount

            if entry_mc is not None and side == "buy":
                w["entry_mcaps"].append(entry_mc)

            pkey = (wallet, token)
            pstate = positions.setdefault(pkey, {
                "wallet": wallet,
                "token_address": token,
                "symbol": e.get("symbol") or "?",
                "types": set(),
                "buy_events": 0,
                "sell_events": 0,
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "entry_mcaps": [],
                "first_seen": ts,
                "last_seen": ts,
                "first_side": side,
                "last_side": side,
                "latest_current_market_cap": None,
            })

            pstate["symbol"] = e.get("symbol") or pstate["symbol"]
            pstate["types"].add(kind)
            pstate["first_seen"] = min(pstate["first_seen"], ts)
            pstate["last_seen"] = max(pstate["last_seen"], ts)
            pstate["last_side"] = side

            if side == "buy":
                pstate["buy_events"] += 1
            elif side == "sell":
                pstate["sell_events"] += 1

            if amount is not None:
                if side == "buy":
                    pstate["buy_usd"] += amount
                elif side == "sell":
                    pstate["sell_usd"] += amount

            if entry_mc is not None and side == "buy":
                pstate["entry_mcaps"].append(entry_mc)

            if current_mc is not None:
                pstate["latest_current_market_cap"] = current_mc

        positions_by_token: dict[str, list[dict[str, Any]]] = {}
        for pstate in positions.values():
            positions_by_token.setdefault(pstate["token_address"], []).append(pstate)

        token_rows = []
        asset_class_counts: dict[str, int] = {}
        eligible_token_count = 0

        for t in tokens.values():
            median_mc = _median(t.pop("entry_mcaps"))
            latest_mc = t["latest_current_market_cap"]
            # _events contains the selected API scope. Short-window feature
            # calculations instead use their own guaranteed 2h context.
            t.pop("_events")
            window_events = feature_events_by_token.get(
                t["token_address"], []
            )
            launchpads = t.pop("_launchpads")

            classification = _classify(t["token_address"], launchpads)
            t.update(classification)
            t["launchpad"] = ", ".join(sorted(launchpads)) if launchpads else None

            if t["eligible"]:
                eligible_token_count += 1
            asset_class_counts[t["asset_class"]] = asset_class_counts.get(t["asset_class"], 0) + 1

            t["unique_wallets"] = len(t.pop("wallets"))
            t["smart_wallets"] = len(t["smart_wallets"])
            t["kol_wallets"] = len(t["kol_wallets"])
            t["median_entry_market_cap"] = median_mc
            t["observed_net_flow_usd"] = round(t["buy_usd"] - t["sell_usd"], 6)
            t["mcap_multiple"] = (
                latest_mc / median_mc
                if latest_mc and median_mc and median_mc > 0
                else None
            )

            token_positions = positions_by_token.get(t["token_address"], [])
            lifecycle_token_rows = lifecycle_by_token.get(
                t["token_address"], []
            )

            t["observation_quality"] = {
                "first_event_buy_wallets": sum(
                    1 for row in lifecycle_token_rows
                    if row["first_buy_ts"] is not None
                    and (
                        row["first_sell_ts"] is None
                        or int(row["first_buy_ts"]) <= int(row["first_sell_ts"])
                    )
                ),
                "first_event_sell_wallets": sum(
                    1 for row in lifecycle_token_rows
                    if row["first_sell_ts"] is not None
                    and (
                        row["first_buy_ts"] is None
                        or int(row["first_sell_ts"]) < int(row["first_buy_ts"])
                    )
                ),
                "observed_buyer_wallets": sum(
                    1 for row in lifecycle_token_rows
                    if int(row["buy_events"] or 0) > 0
                ),
                "repeat_accumulator_wallets": sum(
                    1 for row in lifecycle_token_rows
                    if int(row["buy_events"] or 0) >= 2
                ),
                "exit_only_wallets": sum(
                    1 for row in lifecycle_token_rows
                    if int(row["buy_events"] or 0) == 0
                    and int(row["sell_events"] or 0) > 0
                ),
                "scope": "all_matching_persisted_events",
            }

            t["windows"] = {}
            for label, seconds in feature_windows.items():
                current_cutoff = now_ts - seconds
                previous_cutoff = now_ts - (2 * seconds)

                current_rows = [
                    event for event in window_events
                    if current_cutoff <= int(event.get("timestamp") or 0) <= now_ts
                ]
                previous_rows = [
                    event for event in window_events
                    if previous_cutoff <= int(event.get("timestamp") or 0) < current_cutoff
                ]

                current = _window_metrics(current_rows)
                previous = _window_metrics(previous_rows)

                newly_observed_wallets = set()
                for event in current_rows:
                    if str(event.get("side") or "").lower() != "buy":
                        continue

                    wallet = str(event.get("wallet") or "")
                    first_buy_ts = lifecycle_first_buy.get(
                        (wallet, t["token_address"])
                    )

                    if (
                        first_buy_ts is not None
                        and first_buy_ts >= current_cutoff
                    ):
                        newly_observed_wallets.add(wallet)

                newly_observed_buyers = len(newly_observed_wallets)

                current.update({
                    "newly_observed_buyers": newly_observed_buyers,
                    "previous_unique_buyers": previous["unique_buyers"],
                    "previous_net_flow_usd": previous["net_flow_usd"],
                    "previous_qualified_net_flow_usd":
                        previous["qualified_net_flow_usd"],
                    "buyer_acceleration":
                        current["unique_buyers"] - previous["unique_buyers"],
                    "buyer_acceleration_ratio":
                        (current["unique_buyers"] / previous["unique_buyers"])
                        if previous["unique_buyers"] > 0 else None,
                    "flow_acceleration_usd":
                        round(
                            current["net_flow_usd"]
                            - previous["net_flow_usd"],
                            6,
                        ),
                    "qualified_flow_acceleration_usd":
                        round(
                            current["qualified_net_flow_usd"]
                            - previous["qualified_net_flow_usd"],
                            6,
                        ),
                })

                t["windows"][label] = current

            token_rows.append(t)

        token_rows.sort(
            key=lambda x: (x["unique_wallets"], x["buy_events"], x["last_seen"]),
            reverse=True
        )

        holding_rows = []
        for pstate in positions.values():
            if pstate["last_side"] != "buy":
                continue

            median_mc = _median(pstate.pop("entry_mcaps"))
            latest_mc = pstate["latest_current_market_cap"]
            pstate["wallet_type"] = "+".join(sorted(pstate.pop("types")))
            pstate["median_entry_market_cap"] = median_mc
            pstate["observed_net_flow_usd"] = round(
                pstate["buy_usd"] - pstate["sell_usd"], 6
            )
            pstate["mcap_multiple"] = (
                latest_mc / median_mc
                if latest_mc and median_mc and median_mc > 0
                else None
            )
            pstate["status"] = "OBSERVED_OPEN"
            pstate["status_method"] = "last-observed-side"
            holding_rows.append(pstate)

            if pstate["wallet"] in wallets:
                wallets[pstate["wallet"]]["observed_open_positions"] += 1

        holding_rows.sort(key=lambda x: x["last_seen"], reverse=True)

        wallet_rows = []
        for w in wallets.values():
            w["wallet_type"] = "+".join(sorted(w.pop("types")))
            w["unique_tokens"] = len(w.pop("tokens"))
            w["median_entry_market_cap"] = _median(w.pop("entry_mcaps"))
            w["observed_net_flow_usd"] = round(w["buy_usd"] - w["sell_usd"], 6)
            wallet_rows.append(w)

        wallet_rows.sort(
            key=lambda x: (
                x["observed_open_positions"],
                x["buy_events"],
                x["last_seen"],
            ),
            reverse=True
        )

        activity = []
        for e in reversed(events[-max(1, min(int(activity_limit), 1000)):]):
            item = {k: e.get(k) for k in (
                "id", "timestamp", "wallet", "wallet_type", "side",
                "token_address", "symbol", "entry_price", "entry_market_cap",
                "entry_market_cap_source", "trade_amount", "current_price",
                "current_market_cap", "unrealized_performance",
            )}

            if item.get("entry_market_cap") and item.get("current_market_cap"):
                item["mcap_multiple"] = (
                    item["current_market_cap"] / item["entry_market_cap"]
                )
            else:
                item["mcap_multiple"] = None

            activity.append(item)

        return {
            "summary": {
                "events": len(events),
                "buy_events": total_buys,
                "sell_events": total_sells,
                "smart_money_events": smart_events,
                "kol_events": kol_events,
                "unique_wallets": len(wallets),
                "unique_tokens": len(tokens),
                "observed_open_positions": len(holding_rows),
                "first_event_at": events[0]["timestamp"] if events else None,
                "last_event_at": events[-1]["timestamp"] if events else None,
                "event_cap": None,
                "event_cap_reached": False,
                "aggregation_scope": "all_matching_persisted_events",
            },
            "feature_engine": {
                "version": "smart-kol-v2-features-1",
                "anchor_timestamp": now_ts,
                "windows": list(feature_windows),
                "feature_history_seconds": feature_history_seconds,
                "lifecycle_scope": "all_matching_persisted_events",
                "eligible_tokens": eligible_token_count,
                "asset_class_counts": asset_class_counts,
                "scoring_enabled": False,
            },
            "activity": activity,
            "holdings": holding_rows[:1000],
            "wallets": wallet_rows[:1000],
            "tokens": token_rows[:1000],
            "methodology": {
                "holdings":
                    "Observed open interest: latest collected side for wallet/token is BUY; not a complete on-chain balance.",
                "market_caps":
                    "Buy market caps use persisted event market cap, normally derived from event price x token supply.",
                "collection":
                    "Built only from persisted track smartmoney + track kol events; viewing this endpoint makes no GMGN call.",
                "eligibility":
                    "Known base/stable/major wrapped assets are excluded by exact address. Launchpad and other non-base assets remain eligible regardless of market cap.",
                "freshness":
                    "Short-window features use unique wallets so repeated transactions from one wallet do not masquerade as independent consensus.",
                "newly_observed_buyers":
                    "First BUY observed in the full persisted matching history; not proof that it was the wallet's first-ever on-chain purchase.",
                "flows":
                    "Observed flow includes every collected sell. Qualified flow subtracts sells only after a prior observed BUY; first-observed/pre-existing exits are reported separately.",
                "scoring":
                    "No composite score is assigned in v2 feature mode; features are retained for later outcome-based calibration.",
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
