"""Read-only Survivor endpoints for the local GMGN aitrader app.

Installed by bsc-sol-scan/scripts/install-gmgn-survivor-bridge.sh.
No credentials are accepted or returned by these endpoints. Calls use the
agent's existing adapter, lock, scheduler and local GMGN configuration.
"""
from typing import Literal
from pydantic import BaseModel
from fastapi import HTTPException


class SurvivorTokenIn(BaseModel):
    chain: str = "sol"
    address: str
    depth: Literal["basic", "deep"] = "basic"


def _unwrap(value):
    if isinstance(value, dict) and isinstance(value.get("data"), dict):
        return value["data"]
    return value


def install_survivor_bridge(app, ST, valid_chain):
    # Idempotent when development reload imports this module twice.
    if any(getattr(route, "path", None) == "/api/survivor/token" for route in app.routes):
        return

    @app.post("/api/survivor/token")
    def api_survivor_token(req: SurvivorTokenIn):
        ch = valid_chain(req.chain)
        address = (req.address or "").strip()
        if not address:
            raise HTTPException(400, "missing token address")

        with ST.lock:
            g = ST.adapter_for(ch)
            try:
                info = g.token_info(address)
                security = g.token_security(address)
            except Exception as exc:
                raise HTTPException(502, f"GMGN basic enrichment failed: {exc}")

            pool = None
            cli = getattr(g, "_cli", None)
            if callable(cli):
                try:
                    pool = cli("token", "pool", "--address", address)
                except Exception:
                    pool = None

            holders = traders = None
            if req.depth == "deep":
                try:
                    holders = g.token_holders(address)
                except Exception as exc:
                    holders = {"error": str(exc)}
                if callable(cli):
                    try:
                        traders = cli("token", "traders", "--address", address)
                    except Exception as exc:
                        traders = {"error": str(exc)}

        return {
            "chain": ch,
            "address": address,
            "depth": req.depth,
            "live_adapter": bool(getattr(ST, "is_live_adapter", False)),
            "mode": getattr(ST, "mode", None),
            "info": _unwrap(info),
            "security": _unwrap(security),
            "pool": _unwrap(pool),
            "holders": _unwrap(holders),
            "traders": _unwrap(traders),
        }
