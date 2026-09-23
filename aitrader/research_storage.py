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
