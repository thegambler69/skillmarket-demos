"""One process-wide, serialized scheduler for every read-only gmgn-cli call.

The scheduler deliberately paces below the Free-tier ceiling.  Callers submit
argument arrays and wait for a shared Future; identical queued/in-flight keys
are coalesced.  No caller is allowed to invoke subprocess for gmgn-cli.
"""
from __future__ import annotations

import heapq
import os
import subprocess
import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

from gmgn_guard import SHARED_GMGN_GUARD, RateLimitGuardError

GMGN_ENDPOINT_WEIGHTS = {
    "token.info": 1, "token.security": 1, "token.pool": 1,
    "token.holders": 5, "token.traders": 5,
    "track.kol": 1, "track.smartmoney": 1,
    "track.follow_wallet": 10, "track.follow_tokens": 3,
    "track.follow_token_groups": 1,
}
UNKNOWN_ENDPOINT_WEIGHT = max(5, int(os.getenv("GMGN_UNKNOWN_ENDPOINT_WEIGHT", "5")))

def endpoint_name(command: list[str]) -> str:
    words = [x for x in command[1:] if not x.startswith("-")]
    if len(words) >= 2:
        if words[0] == "track" and words[1] == "smartmoney": return "track.smartmoney"
        if words[0] == "track" and words[1] == "kol": return "track.kol"
        return f"{words[0]}.{words[1].replace('-', '_')}"
    return "unknown"

def endpoint_weight(command: list[str]) -> tuple[int, str, bool]:
    name = endpoint_name(command)
    weight = GMGN_ENDPOINT_WEIGHTS.get(name)
    return (weight if weight is not None else UNKNOWN_ENDPOINT_WEIGHT, name, weight is None)


@dataclass(order=True)
class _Job:
    sort_key: tuple[float, int] = field(init=False, repr=False)
    priority: int
    sequence: int
    queued_at: float
    key: str = field(compare=False)
    command: list[str] = field(compare=False)
    env: dict[str, str] = field(compare=False)
    timeout: float = field(compare=False)
    category: str = field(compare=False)
    weight: int = field(compare=False)
    endpoint: str = field(compare=False)
    unknown_weight: bool = field(compare=False)
    future: Future = field(compare=False)

    def __post_init__(self) -> None:
        self.sort_key = (float(self.priority), self.sequence)


class GMGNScheduler:
    def __init__(self) -> None:
        self.plan_profile = os.getenv("GMGN_PLAN_PROFILE", "FREE").upper()
        self.rps = float(os.getenv("GMGN_REQUESTS_PER_SECOND", "2"))
        self.min_gap = max(0.5, float(os.getenv("GMGN_MIN_REQUEST_GAP_MS", "500")) / 1000.0)
        self.max_queue = max(1, int(os.getenv("GMGN_MAX_QUEUE_SIZE", "128")))
        self._cv = threading.Condition(threading.RLock())
        self._queue: list[_Job] = []
        self._pending: dict[str, Future] = {}
        self._pending_weights: dict[str, int] = {}
        self._sequence = 0
        self._last_request = 0.0
        self.weight_budget = float(os.getenv("GMGN_WEIGHT_BUDGET_PER_SEC", "4.0"))
        self.official_plan_weight = float(os.getenv("GMGN_OFFICIAL_PLAN_WEIGHT", "5"))
        self._next_virtual_time = 0.0
        self._weight_history = deque()
        self._stop = False
        self._thread = threading.Thread(target=self._worker, name="gmgn-scheduler", daemon=True)
        self._thread.start()
        self.metrics: dict[str, Any] = {
            "total_attempted": 0, "successful": 0, "cache_hits": 0,
            "deduplicated": 0, "suppressed": 0, "queued": 0,
            "queue_wait_seconds": 0.0, "429": 0, "ban_events": 0,
            "last_request_timestamp": None, "by_category": {}, "by_endpoint": {},
            "total_weight": 0, "cache_weight_avoided": 0, "dedup_weight_avoided": 0,
            "unknown_weight_requests": 0,
        }
        self._rate_errors = 0
        self._degraded_until = 0.0

    def _effective_gap(self) -> float:
        # A small deterministic circuit breaker for non-ban failures.
        return self.min_gap * (2.0 if self._degraded_until > time.monotonic() else 1.0)

    def submit(self, command: list[str], env: dict[str, str], *, key: str,
               priority: int = 3, category: str = "other", timeout: float = 25.0) -> Any:
        wait_for: Future | None = None
        with self._cv:
            existing = self._pending.get(key)
            if existing is not None:
                # Existing jobs carry their own documented cost; a duplicate
                # does not consume budget and is counted as avoided weight.
                self.metrics["deduplicated"] += 1
                self.metrics["dedup_weight_avoided"] += self._pending_weights.get(key, UNKNOWN_ENDPOINT_WEIGHT)
                wait_for = existing
            if wait_for is not None:
                pass
            elif len(self._queue) >= self.max_queue:
                raise RuntimeError("GMGN scheduler queue is full; cached data remains available")
            else:
                future: Future = Future()
                self._sequence += 1
                weight, endpoint, unknown = endpoint_weight(command)
                job = _Job(priority, self._sequence, time.monotonic(), key, list(command), env, timeout, category, weight, endpoint, unknown, future)
                self._pending[key] = future
                self._pending_weights[key] = weight
                heapq.heappush(self._queue, job)
                self.metrics["queued"] = len(self._queue)
                self._cv.notify()
        return (wait_for or future).result()

    def cache_hit(self, weight: int = UNKNOWN_ENDPOINT_WEIGHT) -> None:
        with self._cv:
            self.metrics["cache_hits"] += 1
            self.metrics["cache_weight_avoided"] += int(weight)

    def _pop(self) -> _Job | None:
        # Aging prevents low-priority work from starving: each 5 seconds in the
        # queue buys one priority level, while preserving deterministic order.
        if not self._queue:
            return None
        now = time.monotonic()
        best = min(range(len(self._queue)), key=lambda i: (self._queue[i].priority - int((now - self._queue[i].queued_at) / 5), self._queue[i].sequence))
        job = self._queue.pop(best)
        heapq.heapify(self._queue)
        self.metrics["queued"] = len(self._queue)
        return job

    def _worker(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._stop:
                    self._cv.wait()
                if self._stop:
                    return
                job = self._pop()
            assert job is not None
            try:
                # Never retry or drain work while the persisted circuit breaker
                # is active. Jobs remain pending until reset_at.
                while SHARED_GMGN_GUARD.blocked():
                    state = SHARED_GMGN_GUARD.snapshot()
                    delay = max(0.1, float(state.get("banned_until", 0) or 0) - time.time())
                    with self._cv:
                        self.metrics["suppressed"] += 1
                    time.sleep(delay)
                # Virtual-time leaky bucket: each endpoint consumes its
                # documented weight, with no concurrent or burst release.
                now_mono = time.monotonic()
                slot = max(now_mono, self._next_virtual_time)
                if slot > now_mono: time.sleep(slot - now_mono)
                SHARED_GMGN_GUARD.before_request()
                self._last_request = time.monotonic()
                self._next_virtual_time = self._last_request + (job.weight / self.weight_budget)
                started = time.monotonic()
                with self._cv:
                    self.metrics["total_attempted"] += 1
                    self.metrics["last_request_timestamp"] = time.time()
                    self.metrics["total_weight"] += job.weight
                    now_hist = time.monotonic(); self._weight_history.append((now_hist, job.weight))
                    while self._weight_history and now_hist - self._weight_history[0][0] > 10: self._weight_history.popleft()
                    if job.unknown_weight: self.metrics["unknown_weight_requests"] += 1
                    self.metrics["by_category"][job.category] = self.metrics["by_category"].get(job.category, 0) + 1
                    ep = self.metrics["by_endpoint"].setdefault(job.endpoint, {"requests": 0, "weight": 0})
                    ep["requests"] += 1; ep["weight"] += job.weight
                result = subprocess.run(job.command, capture_output=True, text=True, timeout=job.timeout, env=job.env, check=False)
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or "gmgn-cli exited unsuccessfully")
                SHARED_GMGN_GUARD.record_success()
                with self._cv:
                    self.metrics["successful"] += 1
                    self.metrics["queue_wait_seconds"] += max(0.0, started - job.queued_at)
                    self._rate_errors = 0
                job.future.set_result(result)
            except RateLimitGuardError as exc:
                with self._cv: self.metrics["suppressed"] += 1
                job.future.set_exception(exc)
            except Exception as exc:
                text = str(exc)
                if "429" in text or "rate limit" in text.lower() or "rate_limit" in text.lower():
                    with self._cv:
                        self.metrics["429"] += 1; self.metrics["ban_events"] += 1
                    SHARED_GMGN_GUARD.record_failure(text)
                else:
                    with self._cv:
                        self._rate_errors += 1
                        if self._rate_errors >= 2: self._degraded_until = time.monotonic() + 60
                job.future.set_exception(exc)
            finally:
                with self._cv:
                    self._pending.pop(job.key, None)
                    self._pending_weights.pop(job.key, None)

    def snapshot(self) -> dict[str, Any]:
        with self._cv:
            data = dict(self.metrics)
            data["queue_depth"] = len(self._queue)
            data["queue_weight"] = sum(j.weight for j in self._queue)
            data["oldest_queued_seconds"] = max(0.0, time.monotonic() - min((j.queued_at for j in self._queue), default=time.monotonic()))
            data["configured_rps"] = self.rps
            data["official_plan_weight"] = self.official_plan_weight
            data["configured_weight_budget"] = self.weight_budget
            data["plan_profile"] = self.plan_profile
            data["configured_min_gap_ms"] = int(self.min_gap * 1000)
            data["state"] = "RATE LIMITED" if SHARED_GMGN_GUARD.blocked() else ("DEGRADED" if self._degraded_until > time.monotonic() else "NORMAL")
            data["effective_weight_rate"] = self.weight_budget
            now = time.monotonic()
            data["current_weight_rate"] = sum(w for t, w in self._weight_history if now - t <= 10) / 10.0
            data["max_queue_size"] = self.max_queue
            return data


SHARED_GMGN_SCHEDULER = GMGNScheduler()
