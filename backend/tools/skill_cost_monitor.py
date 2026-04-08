"""
Saturn Skill: Cost Monitor
Tracks daily and monthly token spend vs budget.
Used by: Pulse, Sentinel
Requires: token_log table in SQLite (skips gracefully if missing).
"""

import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger("saturn.skill_cost_monitor")
_DB = os.environ.get(
    "SATURN_DB_PATH",
    str(Path.home() / "Workspace" / "Saturn" / "database" / "saturn.db"),
)
_BUDGET = float(os.environ.get("SATURN_TOKEN_BUDGET_USD", "20.0"))
_COST_PER_1K = 0.0002


def _usd(tokens: int) -> float:
    return round(tokens * _COST_PER_1K / 1000, 4)


def _table_exists(conn, name: str) -> bool:
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return r is not None


def today_usage() -> dict:
    try:
        conn = sqlite3.connect(_DB, timeout=10)
        if not _table_exists(conn, "token_log"):
            conn.close()
            return {"status": "skipped", "reason": "token_log table not found"}
        total = conn.execute(
            "SELECT COALESCE(SUM(tokens),0) FROM token_log "
            "WHERE DATE(logged_at)=DATE('now','localtime')"
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT agent, SUM(tokens) FROM token_log "
            "WHERE DATE(logged_at)=DATE('now','localtime') GROUP BY agent"
        ).fetchall()
        conn.close()
        usd = _usd(total)
        remaining = max(0.0, _BUDGET - usd)
        return {
            "status": "ok" if usd < _BUDGET else "budget_exceeded",
            "total_tokens": total,
            "total_usd": usd,
            "budget_usd": _BUDGET,
            "remaining_usd": round(remaining, 4),
            "pct_used": round(usd / _BUDGET * 100, 1) if _BUDGET else 0,
            "by_agent": {r[0]: {"tokens": r[1], "usd": _usd(r[1])} for r in rows},
        }
    except Exception as e:
        logger.warning("skill_cost_monitor today_usage failed", exc_info=e)
        return {"status": "error", "reason": str(e)}


def monthly_usage() -> dict:
    try:
        conn = sqlite3.connect(_DB, timeout=10)
        if not _table_exists(conn, "token_log"):
            conn.close()
            return {"status": "skipped", "reason": "token_log table not found"}
        total = conn.execute(
            "SELECT COALESCE(SUM(tokens),0) FROM token_log "
            "WHERE strftime('%Y-%m',logged_at)=strftime('%Y-%m','now')"
        ).fetchone()[0]
        conn.close()
        usd = _usd(total)
        budget_m = _BUDGET * 30
        return {
            "status": "ok",
            "total_tokens": total,
            "total_usd": usd,
            "monthly_budget": budget_m,
            "pct_used": round(usd / budget_m * 100, 1) if budget_m else 0,
        }
    except Exception as e:
        logger.warning("skill_cost_monitor monthly_usage failed", exc_info=e)
        return {"status": "error", "reason": str(e)}
