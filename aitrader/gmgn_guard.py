"""Process-shared, persisted GMGN rate-limit circuit breaker."""
from __future__ import annotations
import json, os, re, threading, time
from pathlib import Path

class RateLimitGuardError(RuntimeError):
    def __init__(self, message: str, state: dict):
        super().__init__(message); self.state = state

class GMGNRateLimitGuard:
    def __init__(self, path: Path | None = None):
        self.path = path or Path(os.getenv("GMGN_GUARD_PATH", "outputs/gmgn_guard.json")); self.lock=threading.RLock(); self.state={}
        self._load()
    def _load(self):
        try: self.state=json.loads(self.path.read_text())
        except Exception: self.state={}
    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True); tmp=self.path.with_suffix(".tmp"); tmp.write_text(json.dumps(self.state,sort_keys=True)); tmp.replace(self.path)
    def snapshot(self):
        with self.lock:
            self._load(); return dict(self.state)
    def blocked(self) -> bool:
        with self.lock:
            self._load(); return float(self.state.get("banned_until",0) or 0) > time.time()
    def before_request(self):
        with self.lock:
            self._load()
            if float(self.state.get("banned_until",0) or 0) > time.time():
                raise RateLimitGuardError("GMGN_RATE_LIMITED", dict(self.state))
    def record_failure(self, message: str):
        text=str(message); limited=bool("429" in text or "rate_limit" in text.lower() or "rate limit" in text.lower())
        if not limited: return
        with self.lock:
            now=time.time(); match=re.search(r"resets at ([^.;]+)",text,re.I); reset_text=match.group(1).strip() if match else None
            reset_epoch=now+300
            # CLI supplies a human-readable reset but not a stable epoch; retain a conservative cooldown.
            self.state.update({"banned_until":reset_epoch,"last_error":text,"rate_limit_reset":reset_text,"consecutive_failures":int(self.state.get("consecutive_failures",0))+1,"last_failure":int(now),"next_retry":int(reset_epoch)})
            self._save()
    def record_success(self):
        with self.lock:
            if self.state:
                self.state.update({"banned_until":0,"consecutive_failures":0,"last_error":None,"rate_limit_reset":None,"next_retry":None,"last_success":int(time.time())}); self._save()

SHARED_GMGN_GUARD = GMGNRateLimitGuard()
