"""
Saturn Skill: Self Healing
Health checks and service recovery for Sentinel.
No external dependencies except subprocess + sqlite3 (stdlib only).
"""

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

_DB = os.environ.get(
    "SATURN_DB_PATH",
    str(Path.home() / "Workspace" / "Saturn" / "database" / "saturn.db"),
)
_TIMERS = [
    "saturn-morning",
    "saturn-hourly",
    "saturn-progress",
    "saturn-report",
    "saturn-email-reader",
]


def check_api(port: int = 8787) -> dict:
    try:
        import urllib.request

        with urllib.request.urlopen(f"http://localhost:{port}/api/saturn/health/full", timeout=5) as r:
            return {"status": "ok" if r.status == 200 else "degraded", "code": r.status}
    except Exception as e:
        return {"status": "unreachable", "reason": str(e)}


def check_n8n(port: int = 5678) -> dict:
    try:
        import urllib.request

        with urllib.request.urlopen(f"http://localhost:{port}/healthz", timeout=5) as r:
            return {"status": "ok" if r.status == 200 else "degraded"}
    except Exception as e:
        return {"status": "unreachable", "reason": str(e)}


def check_db() -> dict:
    try:
        conn = sqlite3.connect(_DB, timeout=5)
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check;").fetchone()[0]
        conn.close()
        return {"status": "ok", "journal_mode": mode, "integrity": integrity}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def check_disk(min_gb: float = 1.0) -> dict:
    try:
        total, used, free = shutil.disk_usage("/")
        free_gb = round(free / (1024 ** 3), 2)
        used_pct = round(used / total * 100, 1)
        return {"status": "ok" if free_gb >= min_gb else "warn", "free_gb": free_gb, "used_pct": used_pct}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def check_timers() -> dict:
    results = {}
    for t in _TIMERS:
        try:
            r = subprocess.run(
                ["systemctl", "--user", "is-active", f"{t}.timer"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            results[t] = r.stdout.strip()
        except Exception as e:
            results[t] = f"error:{e}"
    ok = all(v == "active" for v in results.values())
    return {"status": "ok" if ok else "warn", "timers": results}


def full_report() -> dict:
    return {
        "api": check_api(),
        "n8n": check_n8n(),
        "db": check_db(),
        "disk": check_disk(),
        "timers": check_timers(),
    }


def restart_api() -> dict:
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "saturn-api"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        import time

        time.sleep(3)
        return {"status": "restarted", "health": check_api()}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def auto_heal() -> dict:
    actions = []
    report = full_report()
    if report.get("api", {}).get("status") == "unreachable":
        r = restart_api()
        actions.append({"service": "saturn-api", "action": "restart", "result": r})
    for timer, state in report.get("timers", {}).get("timers", {}).items():
        if state != "active":
            try:
                subprocess.run(
                    ["systemctl", "--user", "start", f"{timer}.timer"],
                    capture_output=True,
                    timeout=10,
                )
                actions.append({"service": timer, "action": "started"})
            except Exception as e:
                actions.append({"service": timer, "action": "failed", "reason": str(e)})
    return {"status": "complete", "actions": len(actions), "detail": actions, "report": report}
