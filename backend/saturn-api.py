from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import re
import sqlite3
import subprocess
import shutil
import threading
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from flask import Flask, jsonify, request
try:
    from flask_cors import CORS
except ModuleNotFoundError as exc:
    logging.getLogger("saturn.api").warning(
        "flask_cors unavailable; continuing without CORS middleware: %s",
        exc,
    )

    def CORS(app, *args, **kwargs):  # type: ignore[override]
        return app

logger = logging.getLogger("saturn.api")
RATE_LIMIT_USER_MESSAGE = "⚠️ Rate limit reached. System is safe. Retry in a few minutes."

try:
    from modules.tool_registry import ToolExecutionError, ToolNotFoundError, ToolRegistry
except ModuleNotFoundError:
    from backend.modules.tool_registry import ToolExecutionError, ToolNotFoundError, ToolRegistry

# --- Path Setup ---
BASE_PATH = Path(
    os.environ.get("SATURN_BASE_PATH", str(Path.home() / "Workspace" / "Saturn"))
).expanduser().resolve()
DB_PATH = BASE_PATH / "database" / "saturn.db"
MCP_SERVER_PATH = BASE_PATH / "configs" / "saturn-server.py"
MCPORTER_CONFIG_PATH = Path(
    os.environ.get("SATURN_MCPORTER_CONFIG", "/home/navin/config/mcporter.json")
).expanduser().resolve()
SERVER_STARTED_AT = dt.datetime.now(tz=dt.timezone(dt.timedelta(hours=5, minutes=30)))
TOKEN_QUOTA_DAILY = 500_000
TOOL_CALL_TIMEOUT_SEC = 30
_SCHEMA_INIT_LOCK = threading.Lock()
_SCHEMA_READY = False
UI_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("SATURN_ALLOWED_ORIGINS", "http://localhost:8787").split(",")
    if origin.strip()
]

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": UI_ALLOWED_ORIGINS}})

AGENT_CONFIG = {
    "saturn": {"name": "Saturn", "role": "Commander"},
    "hunter": {"name": "Hunter", "role": "Lead Discovery"},
    "echo": {"name": "Echo", "role": "Outreach & Content"},
    "forge": {"name": "Forge", "role": "Build & Delivery"},
    "sentinel": {"name": "Sentinel", "role": "Monitoring & Security"},
    "pulse": {"name": "Pulse", "role": "Planning & Operations"},
}

PIPELINE_STAGES = ["new", "contacted", "qualified", "lost"]
ERROR_TYPES = {
    "API_ERROR",
    "AUTH_ERROR",
    "RATE_LIMIT",
    "NETWORK_ERROR",
    "DB_ERROR",
    "LOGIC_ERROR",
}
STATUS_TRANSITIONS = {
    "new": {"contacted"},
    "contacted": {"qualified", "lost"},
    "qualified": set(),
    "lost": set(),
}


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def add_column_if_missing(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name not in table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def ensure_operational_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        company TEXT,
        contact TEXT,
        source TEXT,
        status TEXT DEFAULT 'new',
        value_estimate REAL DEFAULT 0,
        notes TEXT,
        last_contact TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        priority TEXT DEFAULT 'normal',
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS revenue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client TEXT,
        service TEXT,
        amount REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        invoice_date TIMESTAMP,
        paid_date TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS content_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        status TEXT DEFAULT 'pending',
        processed_at TIMESTAMP,
        processed_by TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS outreach_drafts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER NOT NULL,
        draft_text TEXT NOT NULL DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS email_send_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        draft_id INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER DEFAULT 1,
        error_category TEXT,
        sent_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS agent_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT NOT NULL,
        action TEXT NOT NULL,
        detail TEXT,
        result TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS token_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT,
        action TEXT,
        tokens INTEGER DEFAULT 0,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS system_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level TEXT DEFAULT 'info',
        source TEXT,
        message TEXT,
        resolved INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        agent TEXT,
        title TEXT,
        error_type TEXT,
        alert_type TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS error_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT NOT NULL,
        action TEXT NOT NULL,
        error_type TEXT NOT NULL,
        message TEXT NOT NULL,
        detail TEXT,
        context TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolved INTEGER DEFAULT 0,
        resolved_at TIMESTAMP,
        resolved_by TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS api_usage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT NOT NULL,
        provider TEXT NOT NULL,
        endpoint TEXT,
        status TEXT NOT NULL,
        error_type TEXT,
        detail TEXT,
        called_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS agent_runs (
        run_id TEXT PRIMARY KEY,
        agent TEXT NOT NULL,
        started_at INTEGER NOT NULL,
        ended_at INTEGER,
        status TEXT NOT NULL DEFAULT 'running'
    )""")
    add_column_if_missing(conn, "leads", "website TEXT")
    add_column_if_missing(conn, "leads", "website_norm TEXT")
    add_column_if_missing(conn, "leads", "email TEXT")
    add_column_if_missing(conn, "leads", "email_status TEXT DEFAULT 'unknown'")
    add_column_if_missing(conn, "leads", "email_source TEXT")
    add_column_if_missing(conn, "leads", "bounce_count INTEGER DEFAULT 0")
    add_column_if_missing(conn, "leads", "follow_up_count INTEGER DEFAULT 0")
    add_column_if_missing(conn, "leads", "follow_up_due_at TIMESTAMP")
    add_column_if_missing(conn, "leads", "no_reply_since TIMESTAMP")
    add_column_if_missing(conn, "leads", "last_outreach_at TIMESTAMP")
    add_column_if_missing(conn, "leads", "manual_override INTEGER DEFAULT 0")
    add_column_if_missing(conn, "leads", "updated_at TIMESTAMP")
    add_column_if_missing(conn, "content_queue", "lead_id INTEGER")
    add_column_if_missing(conn, "content_queue", "processed_at TIMESTAMP")
    add_column_if_missing(conn, "content_queue", "processed_by TEXT")
    add_column_if_missing(conn, "api_usage_log", "service TEXT")
    add_column_if_missing(conn, "api_usage_log", "usage_date TEXT")
    add_column_if_missing(conn, "api_usage_log", "call_count INTEGER DEFAULT 0")
    add_column_if_missing(conn, "api_usage_log", "quota_limit INTEGER DEFAULT 0")
    add_column_if_missing(conn, "api_usage_log", "paused INTEGER DEFAULT 0")
    add_column_if_missing(conn, "system_alerts", "agent TEXT")
    add_column_if_missing(conn, "system_alerts", "title TEXT")
    add_column_if_missing(conn, "system_alerts", "error_type TEXT")
    add_column_if_missing(conn, "error_log", "created_at TIMESTAMP")
    add_column_if_missing(conn, "error_log", "resolved INTEGER DEFAULT 0")
    add_column_if_missing(conn, "error_log", "resolved_at TIMESTAMP")
    add_column_if_missing(conn, "error_log", "resolved_by TEXT")
    add_column_if_missing(conn, "error_log", "context TEXT")
    add_column_if_missing(conn, "token_log", "agent TEXT")
    add_column_if_missing(conn, "token_log", "action TEXT")
    conn.execute("UPDATE outreach_drafts SET status=lower(COALESCE(status,'')) WHERE status IS NOT NULL")
    conn.execute("UPDATE email_send_log SET status=lower(COALESCE(status,'')) WHERE status IS NOT NULL")
    conn.execute(
        """
        DELETE FROM outreach_drafts
        WHERE lower(COALESCE(status,'')) IN ('pending','approved')
          AND id NOT IN (
              SELECT MAX(id)
              FROM outreach_drafts
              WHERE lower(COALESCE(status,'')) IN ('pending','approved')
              GROUP BY lead_id
          )
        """
    )
    conn.execute(
        """
        DELETE FROM email_send_log
        WHERE draft_id IS NOT NULL
          AND id NOT IN (
              SELECT MAX(id)
              FROM email_send_log
              WHERE draft_id IS NOT NULL
              GROUP BY draft_id, status
          )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_website_norm ON leads(website_norm) WHERE website_norm IS NOT NULL"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_content_queue_lead ON content_queue(lead_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_content_queue_status ON content_queue(status)")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_drafts_active_lead "
        "ON outreach_drafts(lead_id) WHERE lower(COALESCE(status,'')) IN ('pending','approved')"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_token_log_agent_action_day ON token_log(agent, action, logged_at)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_email_send_log_draft_status "
        "ON email_send_log(draft_id, status) WHERE draft_id IS NOT NULL"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_api_usage_daily_counter "
        "ON api_usage_log(service, usage_date, endpoint) "
        "WHERE service IS NOT NULL AND usage_date IS NOT NULL AND endpoint IS NOT NULL"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_time ON agent_runs(agent, started_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_system_alerts_agent_title_day ON system_alerts(agent, title, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_error_log_resolved_ts ON error_log(resolved, ts)")
    conn.commit()


def log_error(
    conn: sqlite3.Connection,
    agent: str,
    action: str,
    error_type: str,
    message: str,
    detail: str = "",
) -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    ts = now_utc().replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT INTO error_log (agent, action, error_type, message, detail, ts, created_at, resolved)
        VALUES (?,?,?,?,?,?,?,0)
        """,
        (agent, action, safe_type, message, detail[:500], ts, ts),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail[:300], f"{safe_type}: {message[:300]}", ts),
    )


def transition_allowed(current_status: str, next_status: str, manual_override: bool = False) -> bool:
    if manual_override:
        return True
    return next_status in STATUS_TRANSITIONS.get(current_status, set())


def get_db_conn() -> sqlite3.Connection:
    global _SCHEMA_READY
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    if not _SCHEMA_READY:
        with _SCHEMA_INIT_LOCK:
            if not _SCHEMA_READY:
                ensure_operational_schema(conn)
                _SCHEMA_READY = True
    return conn


def query_scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    row = conn.execute(sql, params).fetchone()
    if not row:
        return 0
    value = row[0]
    return 0 if value is None else value


def now_utc() -> dt.datetime:
    tz_ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    return dt.datetime.now(tz=tz_ist)


def format_uptime() -> str:
    delta = now_utc() - SERVER_STARTED_AT
    total_seconds = int(delta.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}h {minutes}m"


def is_error_text(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(k in lowered for k in ("error", "failed", "fail", "exception", "timeout"))


def safe_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        logger.warning("safe_float parse failed for value=%r", value, exc_info=exc)
        return 0.0


def deterministic_run_id(agent: str, started_at: int, attempt: int = 0) -> str:
    safe_agent = re.sub(r"[^a-z0-9_]+", "_", str(agent or "saturn").strip().lower()).strip("_") or "saturn"
    base = f"run_{safe_agent}_{int(started_at)}"
    return base if attempt <= 0 else f"{base}_{attempt + 1}"


def compute_summary(conn: sqlite3.Connection) -> dict:
    revenue_lifetime = safe_float(
        query_scalar(conn, 'SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE status="paid"')
    )
    revenue_monthly = safe_float(
        query_scalar(
            conn,
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM revenue
            WHERE status = "paid"
              AND strftime('%Y-%m', COALESCE(paid_date, invoice_date, created_at)) = strftime('%Y-%m', 'now')
            """,
        )
    )
    leads_today = int(query_scalar(conn, "SELECT COUNT(*) FROM leads WHERE date(created_at)=date('now', '+5 hours', '+30 minutes')"))
    leads_total = int(query_scalar(conn, "SELECT COUNT(*) FROM leads"))
    tasks_pending = int(query_scalar(conn, "SELECT COUNT(*) FROM tasks WHERE status='pending'"))
    emails_today = int(
        query_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM agent_log
            WHERE date(ts)=date('now', '+5 hours', '+30 minutes')
              AND lower(agent)='echo'
              AND (lower(action) LIKE '%email%' OR lower(action) LIKE '%outreach%' OR lower(action) LIKE '%draft%')
            """,
        )
    )
    follow_ups_pending = int(
        query_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM leads
            WHERE status='contacted'
              AND COALESCE(follow_up_count,0) < 2
              AND datetime(COALESCE(last_contact, created_at)) <= datetime('now', '+5 hours', '+30 minutes', '-3 day')
            """,
        )
    )
    token_usage_today = int(
        query_scalar(conn, "SELECT COALESCE(SUM(tokens),0) FROM token_log WHERE date(logged_at)=date('now', '+5 hours', '+30 minutes')")
    )
    api_calls_today = int(
        query_scalar(conn, "SELECT COUNT(*) FROM api_usage_log WHERE date(called_at)=date('now', '+5 hours', '+30 minutes')")
    )
    hunter_api_calls_today = int(
        query_scalar(
            conn,
            "SELECT COUNT(*) FROM api_usage_log WHERE date(called_at)=date('now', '+5 hours', '+30 minutes') AND lower(agent)='hunter'",
        )
    )
    hunter_api_limit = int(os.environ.get("SATURN_HUNTER_API_DAILY_LIMIT", "100"))
    hunter_api_threshold = int(
        hunter_api_limit * float(os.environ.get("SATURN_HUNTER_API_STOP_THRESHOLD", "0.9"))
    )
    top_actions_rows = conn.execute(
        """
        SELECT lower(COALESCE(agent, 'saturn')) AS agent,
               COALESCE(action, 'general') AS action,
               SUM(tokens) AS token_total
        FROM token_log
        WHERE date(logged_at)=date('now', '+5 hours', '+30 minutes')
        GROUP BY lower(COALESCE(agent, 'saturn')), COALESCE(action, 'general')
        ORDER BY token_total DESC
        LIMIT 5
        """
    ).fetchall()
    token_top_actions = [
        {"agent": row["agent"], "action": row["action"], "tokens": int(row["token_total"] or 0)}
        for row in top_actions_rows
    ]
    api_quota_used = round((token_usage_today / TOKEN_QUOTA_DAILY) * 100, 1) if TOKEN_QUOTA_DAILY else 0
    agent_statuses = compute_agent_status(conn)
    agents_total = len(agent_statuses)
    agents_online = len([agent for agent in agent_statuses if agent.get("status") != "error"])
    revenue_pending = safe_float(
        query_scalar(conn, 'SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE status="pending"')
    )

    return {
        "revenueLifetime": revenue_lifetime,
        "revenueTotal": revenue_lifetime,
        "revenueMonthly": revenue_monthly,
        "revenuePending": revenue_pending,
        "leadsToday": leads_today,
        "leadsTotal": leads_total,
        "tasksPending": tasks_pending,
        "emailsToday": emails_today,
        "followUpsPending": follow_ups_pending,
        "agentsOnline": agents_online,
        "agentsTotal": agents_total,
        "apiQuotaUsed": api_quota_used,
        "tokenUsageToday": token_usage_today,
        "tokenQuotaTotal": TOKEN_QUOTA_DAILY,
        "tokenWarningThreshold": int(os.environ.get("SATURN_TOKEN_WARNING_THRESHOLD", "400000")),
        "tokenTopActions": token_top_actions,
        "apiCallsToday": api_calls_today,
        "hunterApiCallsToday": hunter_api_calls_today,
        "hunterApiLimit": hunter_api_limit,
        "hunterPaused": hunter_api_calls_today >= hunter_api_threshold,
    }


def fetch_latest_actions(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT lower(agent) AS agent, action, detail, result, ts
        FROM agent_log
        ORDER BY ts DESC
        LIMIT 300
        """
    ).fetchall()
    latest = {}
    for row in rows:
        agent = row["agent"]
        if agent in AGENT_CONFIG and agent not in latest:
            latest[agent] = row
    return latest


def _parse_agent_timestamp(value: str | None) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in (text.replace("Z", ""), text):
        try:
            return dt.datetime.fromisoformat(candidate)
        except ValueError as exc:
            logger.debug("agent timestamp parse failed for %r: %s", candidate, exc)
    return None


def compute_agent_status(conn: sqlite3.Connection) -> list[dict]:
    latest = fetch_latest_actions(conn)
    today_counts_rows = conn.execute(
        """
        SELECT lower(agent) AS agent,
               COUNT(*) AS task_count
        FROM agent_log
        WHERE date(ts)=date('now', '+5 hours', '+30 minutes')
        GROUP BY lower(agent)
        """
    ).fetchall()
    unresolved_error_rows = conn.execute(
        """
        SELECT lower(agent) AS agent,
               COUNT(*) AS error_count,
               MAX(COALESCE(created_at, ts)) AS last_error_at
        FROM error_log
        WHERE COALESCE(resolved, 0)=0
        GROUP BY lower(agent)
        """
    ).fetchall()
    latest_success_rows = conn.execute(
        """
        SELECT lower(agent) AS agent, MAX(ts) AS last_success_at
        FROM agent_log
        WHERE lower(result) IN ('success', 'handled')
        GROUP BY lower(agent)
        """
    ).fetchall()
    latest_success_by_agent = {
        row["agent"]: _parse_agent_timestamp(row["last_success_at"])
        for row in latest_success_rows
        if row["agent"] in AGENT_CONFIG
    }
    recent_window = dt.timedelta(hours=6)
    now = dt.datetime.now()
    unresolved_error_counts = {}
    for row in unresolved_error_rows:
        agent = row["agent"]
        if agent not in AGENT_CONFIG:
            continue
        error_at = _parse_agent_timestamp(row["last_error_at"])
        latest_success_at = latest_success_by_agent.get(agent)
        is_recent = bool(error_at and (now - error_at) <= recent_window)
        is_newer_than_success = bool(error_at and (latest_success_at is None or error_at >= latest_success_at))
        unresolved_error_counts[agent] = int(row["error_count"] or 0) if (is_recent and is_newer_than_success) else 0
    today_counts = {
        row["agent"]: {
            "tasksToday": int(row["task_count"] or 0),
            "errorCount": int(unresolved_error_counts.get(row["agent"], 0)),
        }
        for row in today_counts_rows
        if row["agent"] in AGENT_CONFIG
    }

    approvals_pending = int(
        query_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM outreach_drafts
            WHERE lower(status)='pending'
            """,
        )
    )
    leads_new = int(query_scalar(conn, "SELECT COUNT(*) FROM leads WHERE status='new'"))
    tasks_pending = int(query_scalar(conn, "SELECT COUNT(*) FROM tasks WHERE status='pending'"))
    alerts_open = int(query_scalar(conn, "SELECT COUNT(*) FROM system_alerts WHERE resolved=0"))
    token_rows = conn.execute(
        """
        SELECT lower(COALESCE(agent, 'saturn')) AS agent, SUM(tokens) AS token_total
        FROM token_log
        WHERE date(logged_at)=date('now', '+5 hours', '+30 minutes')
        GROUP BY lower(COALESCE(agent, 'saturn'))
        """
    ).fetchall()
    token_by_agent = {
        row["agent"]: int(row["token_total"] or 0)
        for row in token_rows
        if row["agent"] in AGENT_CONFIG
    }

    queue_map = {
        "saturn": approvals_pending,
        "hunter": leads_new,
        "echo": approvals_pending,
        "forge": tasks_pending,
        "sentinel": alerts_open,
        "pulse": tasks_pending,
    }

    agents = []

    for agent_id, cfg in AGENT_CONFIG.items():
        counts = today_counts.get(
            agent_id,
            {
                "tasksToday": 0,
                "errorCount": int(unresolved_error_counts.get(agent_id, 0)),
            },
        )
        last_row = latest.get(agent_id)
        queue_count = queue_map.get(agent_id, 0)

        last_action = "No activity yet"
        current_task = "Idle"

        if last_row:
            action = str(last_row["action"] or "task")
            detail = str(last_row["detail"] or "").strip()
            action_readable = action.replace("_", " ").strip().title()
            last_action = f"{action_readable}{': ' + detail if detail else ''}"
            if queue_count > 0 or counts["errorCount"] > 0:
                current_task = detail or action_readable

        if counts["errorCount"] > 0:
            status = "error"
        elif approvals_pending > 0 and agent_id in ("saturn", "echo"):
            status = "waiting_approval"
        elif queue_count > 0:
            status = "working"
        else:
            status = "idle"

        tokens_today = int(token_by_agent.get(agent_id, 0))

        agents.append(
            {
                "id": agent_id,
                "name": cfg["name"],
                "role": cfg["role"],
                "status": status,
                "currentTask": current_task,
                "lastAction": last_action,
                "tokensToday": int(tokens_today),
                "tasksToday": int(counts["tasksToday"]),
                "errorCount": int(counts["errorCount"]),
                "queueCount": int(queue_count),
            }
        )

    sort_order = {k: i for i, k in enumerate(("saturn", "hunter", "echo", "forge", "sentinel", "pulse"))}
    agents.sort(key=lambda item: sort_order.get(item["id"], 99))
    return agents


def check_n8n_status() -> str:
    try:
        with urlopen("http://127.0.0.1:5678/healthz", timeout=2):
            return "online"
    except (URLError, TimeoutError) as exc:
        logger.warning("n8n health probe failed", exc_info=exc)
        return "offline"


def latest_error_message(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT source, message
        FROM system_alerts
        WHERE lower(level) IN ('error', 'critical')
          AND COALESCE(resolved, 0)=0
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row:
        return f"{row['source']}: {row['message']}"

    row = conn.execute(
        """
        SELECT agent, action, error_type, message
        FROM error_log
        WHERE COALESCE(resolved, 0)=0
        ORDER BY COALESCE(created_at, ts) DESC
        LIMIT 1
        """
    ).fetchone()
    if row:
        return f"{row['agent']} {row['action']}: {row['error_type']}: {row['message']}"
    return "None"


_tool_registry: ToolRegistry | None = None


def get_tool_registry() -> ToolRegistry:
    global _tool_registry
    if _tool_registry is None:
        _tool_registry = ToolRegistry(MCP_SERVER_PATH)
    return _tool_registry


def check_openclaw_status() -> str:
    for url in ("http://127.0.0.1:18789/health", "http://127.0.0.1:18789"):
        try:
            with urlopen(url, timeout=2):
                return "online"
        except Exception as exc:
            logger.warning("openclaw health probe failed for %s", url, exc_info=exc)
            continue
    return "offline"


def count_active_saturn_timers() -> int:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "list-timers", "--all"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        lines = [line for line in result.stdout.splitlines() if "saturn-" in line and ".timer" in line]
        return len(lines)
    except Exception as exc:
        logger.warning("count_active_saturn_timers failed", exc_info=exc)
        return 0


def build_tool_error(
    tool: str,
    error_type: str,
    message: str,
    return_code: int | None = None,
    stdout_text: str = "",
    stderr_text: str = "",
) -> dict:
    err = {"type": error_type, "message": message}
    if return_code is not None:
        err["return_code"] = int(return_code)
    if stdout_text:
        err["stdout"] = stdout_text[:2000]
    if stderr_text:
        err["stderr"] = stderr_text[:2000]
    return {"ok": False, "tool": tool, "error": err}


def _normalize_tool_args(tool: str, args: dict) -> dict:
    normalized = dict(args or {})
    if tool == "add_task" and "title" not in normalized and "task" in normalized:
        normalized["title"] = normalized.pop("task")
    return normalized


def _agent_for_tool(tool: str) -> str:
    tool_lc = str(tool or "").strip().lower()
    prefix = tool_lc.split("_", 1)[0]
    if prefix in AGENT_CONFIG:
        return prefix
    if tool_lc in {
        "add_lead",
        "check_lead_exists",
    }:
        return "hunter"
    if tool_lc in {
        "trigger_echo_draft",
        "send_outreach",
        "send_outreach_email",
        "record_email_bounce",
        "list_due_followups",
        "send_follow_up",
        "process_approval_command",
    }:
        return "echo"
    if tool_lc == "create_project":
        return "forge"
    return "saturn"


def _tool_run_status(payload: dict) -> str:
    result = payload.get("result_json")
    if result is None:
        result = payload.get("result")
    if isinstance(result, dict):
        status = str(result.get("status") or "").strip().lower()
        if status:
            return status
    return "success"


def _execute_tool_with_run_log(tool: str, args: dict) -> tuple[dict, int]:
    payload = _normalize_tool_args(tool, args)
    agent = _agent_for_tool(tool)
    started_at = int(now_utc().timestamp())
    requested_run_id = str(payload.get("run_id") or "").strip()
    run_id = requested_run_id or ""

    try:
        for attempt in range(3):
            if not run_id:
                run_id = deterministic_run_id(agent, started_at, attempt)
            try:
                with get_db_conn() as conn:
                    conn.execute(
                        """
                        INSERT INTO agent_runs (run_id, agent, started_at, ended_at, status)
                        VALUES (?, ?, ?, NULL, 'running')
                        """,
                        (run_id, agent, started_at),
                    )
                    conn.commit()
                break
            except sqlite3.IntegrityError as exc:
                logger.warning(
                    "agent run log collision for tool=%s agent=%s run_id=%s attempt=%s",
                    tool,
                    agent,
                    run_id,
                    attempt + 1,
                    exc_info=exc,
                )
                if attempt == 2:
                    run_id = ""
                else:
                    run_id = ""
                    continue
    except Exception as exc:
        logger.warning("agent run log initialization failed for tool %s", tool, exc_info=exc)
        run_id = ""

    try:
        registry = get_tool_registry()
        result = registry.execute(tool, payload)
        if run_id:
            with get_db_conn() as conn:
                conn.execute(
                    "UPDATE agent_runs SET ended_at=?, status=? WHERE run_id=?",
                    (int(now_utc().timestamp()), _tool_run_status(result), run_id),
                )
                conn.commit()
        return result, 200
    except ToolNotFoundError as exc:
        logger.warning("tool not found: %s", tool, exc_info=exc)
        try:
            with get_db_conn() as conn:
                if run_id:
                    conn.execute(
                        "UPDATE agent_runs SET ended_at=?, status=? WHERE run_id=?",
                        (int(now_utc().timestamp()), exc.error_type, run_id),
                    )
                log_error(conn, agent, tool, "LOGIC_ERROR", exc.message, json.dumps(payload, default=str)[:500])
                conn.commit()
        except sqlite3.Error as log_exc:
            logger.warning("tool-not-found logging failed for %s", tool, exc_info=log_exc)
        return build_tool_error(tool, exc.error_type, exc.message), exc.status_code
    except ToolExecutionError as exc:
        logger.warning("tool execution error for %s", tool, exc_info=exc)
        try:
            with get_db_conn() as conn:
                if run_id:
                    conn.execute(
                        "UPDATE agent_runs SET ended_at=?, status=? WHERE run_id=?",
                        (int(now_utc().timestamp()), exc.error_type, run_id),
                    )
                mapped_type = "LOGIC_ERROR" if exc.error_type == "invalid_args" else "API_ERROR"
                log_error(conn, agent, tool, mapped_type, exc.message, json.dumps(payload, default=str)[:500])
                conn.commit()
        except sqlite3.Error as log_exc:
            logger.warning("tool-execution logging failed for %s", tool, exc_info=log_exc)
        return build_tool_error(tool, exc.error_type, exc.message), exc.status_code
    except Exception as exc:
        logger.warning("tool execution failed for %s", tool, exc_info=exc)
        try:
            with get_db_conn() as conn:
                if run_id:
                    conn.execute(
                        "UPDATE agent_runs SET ended_at=?, status=? WHERE run_id=?",
                        (int(now_utc().timestamp()), "execution_error", run_id),
                    )
                log_error(conn, agent, tool, "API_ERROR", str(exc)[:300], json.dumps(payload, default=str)[:500])
                conn.commit()
        except sqlite3.Error as log_exc:
            logger.warning("unexpected tool failure logging failed for %s", tool, exc_info=log_exc)
        return build_tool_error(tool, "execution_error", str(exc)[:300]), 500


def run_mcporter_tool(tool: str, args: dict) -> tuple[dict, int]:
    return _execute_tool_with_run_log(tool, args)


def _tool_result_message(payload: dict) -> str:
    raw_result = payload.get("raw_result")
    if isinstance(raw_result, str):
        return raw_result

    result = payload.get("result_json")
    if result is None:
        result = payload.get("result")

    if isinstance(result, dict):
        if str(result.get("status") or "").strip().lower() == "rate_limited":
            return RATE_LIMIT_USER_MESSAGE
        for key in ("message", "result", "reason", "status"):
            value = result.get(key)
            if value:
                return str(value)
        return json.dumps(result)
    if isinstance(result, list):
        return json.dumps(result)
    return str(result or "")


# --- CORS for local API clients ---
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin", "")
    if origin in UI_ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,OPTIONS"
    return response


@app.route("/api/saturn/<path:_path>", methods=["OPTIONS"])
def saturn_options(_path: str):
    return ("", 204)


# --- Legacy endpoint kept for compatibility ---
@app.route("/api/dashboard_data", methods=["GET"])
def get_dashboard_data():
    with get_db_conn() as conn:
        summary = compute_summary(conn)
        pipeline_count = int(
            query_scalar(conn, "SELECT COUNT(*) FROM leads WHERE status IN ('new','contacted')")
        )
        pipeline_value = safe_float(
            query_scalar(
                conn,
                "SELECT COALESCE(SUM(value_estimate), 0) FROM leads WHERE status IN ('new','contacted')",
            )
        )
        pending_tasks = int(query_scalar(conn, "SELECT COUNT(*) FROM tasks WHERE status='pending'"))

    return jsonify(
        {
            "provider": "Saturn Commander",
            "financials": {
                "total_earned": summary["revenueLifetime"],
                "pending": 0,
            },
            "pipeline": {"count": pipeline_count, "value": pipeline_value},
            "tasks": {"pending_count": pending_tasks},
        }
    )


@app.route("/api/saturn/summary", methods=["GET"])
def saturn_summary():
    with get_db_conn() as conn:
        return jsonify(compute_summary(conn))


@app.route("/api/saturn/agents/status", methods=["GET"])
def saturn_agents_status():
    with get_db_conn() as conn:
        return jsonify(compute_agent_status(conn))


@app.route("/api/saturn/pipeline", methods=["GET"])
def saturn_pipeline():
    bucket = {stage: [] for stage in PIPELINE_STAGES}
    with get_db_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, name, company, contact, status, value_estimate, notes, created_at
            FROM leads
            ORDER BY created_at DESC
            LIMIT 500
            """
        ).fetchall()

    for row in rows:
        status = str(row["status"] or "new").lower()
        if status not in bucket:
            status = "new"
        bucket[status].append(
            {
                "id": int(row["id"]),
                "name": row["name"] or f"Lead #{row['id']}",
                "company": row["company"] or "Unknown Company",
                "contact": row["contact"] or "",
                "value": safe_float(row["value_estimate"]),
                "notes": row["notes"] or "",
                "createdAt": row["created_at"] or "",
                "status": status,
            }
        )

    return jsonify(bucket)


@app.route("/api/saturn/leads/<int:lead_id>/status", methods=["PATCH"])
def saturn_update_lead_status(lead_id: int):
    payload = request.get_json(silent=True) or {}
    next_status = str(payload.get("status", "")).lower().strip()
    manual_override = bool(payload.get("manualOverride", False))

    if next_status not in PIPELINE_STAGES:
        return jsonify({"error": "Invalid status"}), 400

    with get_db_conn() as conn:
        lead = conn.execute(
            "SELECT status, manual_override FROM leads WHERE id=?",
            (lead_id,),
        ).fetchone()
        if not lead:
            return jsonify({"error": "Lead not found"}), 404

        current_status = str(lead["status"] or "new").lower()
        has_override = bool(lead["manual_override"]) or manual_override
        if current_status != next_status and not transition_allowed(current_status, next_status, has_override):
            log_error(
                conn,
                "saturn",
                "lead_status_update",
                "LOGIC_ERROR",
                "Invalid lead status transition",
                f"{lead_id}: {current_status}->{next_status}",
            )
            conn.commit()
            return (
                jsonify(
                    {
                        "error": "Invalid transition",
                        "currentStatus": current_status,
                        "requestedStatus": next_status,
                        "allowed": sorted(list(STATUS_TRANSITIONS.get(current_status, set()))),
                    }
                ),
                409,
            )

        now = now_utc().replace(microsecond=0).isoformat()
        if next_status == "contacted":
            follow_due = (now_utc() + dt.timedelta(days=3)).replace(microsecond=0).isoformat()
            cur = conn.execute(
                """
                UPDATE leads
                SET status=?,
                    last_contact=?,
                    no_reply_since=COALESCE(no_reply_since, ?),
                    follow_up_due_at=COALESCE(follow_up_due_at, ?),
                    updated_at=?,
                    manual_override=?
                WHERE id=?
                """,
                (next_status, now, now, follow_due, now, int(has_override), lead_id),
            )
        elif next_status == "lost":
            cur = conn.execute(
                """
                UPDATE leads
                SET status=?, follow_up_due_at=NULL, updated_at=?, manual_override=?
                WHERE id=?
                """,
                (next_status, now, int(has_override), lead_id),
            )
        else:
            cur = conn.execute(
                """
                UPDATE leads
                SET status=?, updated_at=?, manual_override=?
                WHERE id=?
                """,
                (next_status, now, int(has_override), lead_id),
            )
        conn.execute(
            "INSERT INTO agent_log (agent, action, detail, result) VALUES (?,?,?,?)",
            ("Saturn", "lead_status_update", f"{lead_id}: {current_status}->{next_status}", "success"),
        )
        conn.commit()
    return jsonify({"ok": True, "id": lead_id, "status": next_status})


@app.route("/api/saturn/approvals", methods=["GET"])
def saturn_approvals():
    status_filter = str(request.args.get("status", "pending")).lower().strip()
    with get_db_conn() as conn:
        if status_filter == "pending":
            rows = conn.execute(
                """
                SELECT od.id,
                       od.status,
                       od.created_at,
                       od.lead_id,
                       od.draft_text,
                       COALESCE(l.name, '') AS lead_name,
                       COALESCE(l.company, '') AS lead_company
                FROM outreach_drafts od
                LEFT JOIN leads l ON l.id = od.lead_id
                WHERE lower(od.status)='pending'
                ORDER BY od.created_at DESC, od.id DESC
                LIMIT 100
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT od.id,
                       od.status,
                       od.created_at,
                       od.lead_id,
                       od.draft_text,
                       COALESCE(l.name, '') AS lead_name,
                       COALESCE(l.company, '') AS lead_company
                FROM outreach_drafts od
                LEFT JOIN leads l ON l.id = od.lead_id
                WHERE lower(od.status)=?
                ORDER BY od.created_at DESC, od.id DESC
                LIMIT 100
                """,
                (status_filter,),
            ).fetchall()

    items = []
    for row in rows:
        preview = (row["draft_text"] or "").strip()
        if len(preview) > 100:
            preview = preview[:100]
        items.append(
            {
                "id": int(row["id"]),
                "agent": "echo",
                "leadName": row["lead_name"] or f"Lead #{row['lead_id']}",
                "company": row["lead_company"] or "Unknown Company",
                "type": "outreach",
                "preview": preview,
                "status": row["status"] or "pending",
                "createdAt": row["created_at"] or "",
                "leadId": row["lead_id"],
            }
        )

    return jsonify(items)


def quarantine_invalid_pending_approvals(
    conn: sqlite3.Connection,
    pending_statuses: tuple[str, ...],
) -> int:
    """Audit function - no longer used. Approval validation moved to outreach_drafts table."""
    return 0


def validate_approval_item(conn: sqlite3.Connection, item_id: int):
    row = conn.execute(
        "SELECT id, status, lead_id FROM outreach_drafts WHERE id=?",
        (item_id,),
    ).fetchone()
    if not row:
        return None, ("Approval item not found", 404)
    status = str(row["status"] or "").lower()
    if status in {"sent", "rejected"}:
        log_error(conn, "echo", "approval_validate", "LOGIC_ERROR", "Draft already processed", str(item_id))
        return None, ("Draft already processed", 409)
    lead_id = row["lead_id"]
    if not lead_id:
        log_error(conn, "echo", "approval_validate", "DB_ERROR", "Draft missing linked lead", str(item_id))
        return None, ("Draft linked lead missing", 409)
    lead = conn.execute("SELECT id FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not lead:
        log_error(conn, "echo", "approval_validate", "DB_ERROR", "Linked lead does not exist", str(lead_id))
        return None, ("Linked lead not found", 409)
    return row, None


@app.route("/api/saturn/approvals/<int:item_id>/approve", methods=["POST"])
def saturn_approval_approve(item_id: int):
    """Approve a draft and send email via MCP tool. Delegates to POST /api/saturn/approve."""
    with get_db_conn() as conn:
        _, err = validate_approval_item(conn, item_id)
        if err:
            msg, code = err
            conn.commit()
            return jsonify({"error": msg}), code

    # Delegate to saturn_approve which calls process_approval_command MCP tool
    return saturn_approve_internal(item_id, "approve", "")


@app.route("/api/saturn/approvals/<int:item_id>/reject", methods=["POST"])
def saturn_approval_reject(item_id: int):
    """Reject a draft. Delegates to saturn_approve_internal."""
    with get_db_conn() as conn:
        _, err = validate_approval_item(conn, item_id)
        if err:
            msg, code = err
            conn.commit()
            return jsonify({"error": msg}), code

    return saturn_approve_internal(item_id, "reject", "")


@app.route("/api/saturn/approvals/<int:item_id>", methods=["PATCH"])
def saturn_approval_edit(item_id: int):
    """Edit a draft. Delegates to saturn_approve_internal."""
    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text", "")).strip()
    if not text:
        return jsonify({"error": "text is required"}), 400

    with get_db_conn() as conn:
        _, err = validate_approval_item(conn, item_id)
        if err:
            msg, code = err
            conn.commit()
            return jsonify({"error": msg}), code

    return saturn_approve_internal(item_id, "edit", text)





def saturn_approve_internal(draft_id: int, action: str, new_text: str = ""):
    """Internal helper that delegates approval to process_approval_command MCP tool.
    
    This ensures all approval operations go through a single standardized path:
    outreach_drafts → MCP process_approval_command → send_outreach_email → leads + email_send_log
    """
    try:
        if action not in {"approve", "reject", "edit"}:
            return jsonify({"status": "failed", "message": "action must be approve|reject|edit"}), 400
        if action == "edit" and not new_text:
            return jsonify({"status": "failed", "message": "new_text is required for edit"}), 400

        command = f"/{action} {draft_id}"
        payload, status_code = _execute_tool_with_run_log(
            "process_approval_command",
            {"command": command, "text": new_text},
        )
        if status_code != 200:
            return jsonify({"status": "failed", "message": _tool_result_message(payload)}), status_code
        msg = _tool_result_message(payload)
        lower_msg = msg.lower()
        status = "ok"
        if "rate limit" in lower_msg:
            status = "rate_limited"
        if "failed" in lower_msg or "not found" in lower_msg or "invalid" in lower_msg:
            status = "failed"

        return jsonify({"status": status, "message": msg})
    except Exception as e:
        try:
            with sqlite3.connect(str(DB_PATH), timeout=10) as conn:
                log_error(conn, "echo", "approve_endpoint", "API_ERROR", "MCP approval command failed", str(e))
                conn.commit()
        except Exception as log_exc:
            logger.warning("approve_endpoint DB logging failed", exc_info=log_exc)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/saturn/approve", methods=["POST"])
def saturn_approve():
    """HTTP endpoint for draft approvals. Validates and delegates to MCP tool."""
    payload = request.get_json(silent=True) or {}
    draft_id = payload.get("draft_id")
    action = str(payload.get("action", "")).strip().lower()
    new_text = str(payload.get("new_text", "")).strip()

    try:
        draft_id = int(draft_id)
    except (TypeError, ValueError):
        return jsonify({"status": "failed", "message": "draft_id must be an integer"}), 400

    return saturn_approve_internal(draft_id, action, new_text)


@app.route("/api/tools/call", methods=["POST"])
def api_tools_call():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return (
            jsonify(
                build_tool_error(
                    "",
                    "invalid_request",
                    "Request body must be valid JSON object",
                )
            ),
            400,
        )

    tool = str(payload.get("tool", "")).strip()
    args = payload.get("args")
    if args is None:
        args = payload.get("params", {})

    if not tool:
        return jsonify(build_tool_error("", "invalid_request", "'tool' is required")), 400
    if not re.fullmatch(r"[A-Za-z0-9_]+", tool):
        return jsonify(build_tool_error(tool, "invalid_request", "Invalid tool name")), 400
    if args is None:
        args = {}
    if not isinstance(args, dict):
        return jsonify(build_tool_error(tool, "invalid_request", "'args'/'params' must be an object")), 400

    result, status_code = run_mcporter_tool(tool, args)
    return jsonify(result), status_code


@app.route("/api/tools", methods=["GET"])
def api_tools_list():
    try:
        return jsonify({"ok": True, "tools": get_tool_registry().list_tools()})
    except Exception as exc:
        return jsonify(build_tool_error("tools", "execution_error", str(exc)[:300])), 500


@app.route("/api/saturn/revenue", methods=["GET"])
def saturn_revenue():
    with get_db_conn() as conn:
        total_paid = safe_float(
            query_scalar(conn, 'SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE status="paid"')
        )
        this_month = safe_float(
            query_scalar(
                conn,
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM revenue
                WHERE status='paid'
                  AND strftime('%Y-%m', COALESCE(paid_date, invoice_date, created_at)) = strftime('%Y-%m', 'now')
                """,
            )
        )
        pending = safe_float(
            query_scalar(conn, 'SELECT COALESCE(SUM(amount), 0) FROM revenue WHERE status="pending"')
        )
    target = 20000.0
    progress_pct = round((total_paid / target) * 100, 2) if target else 0.0
    return jsonify(
        {
            "total_paid": total_paid,
            "this_month": this_month,
            "pending": pending,
            "target": int(target),
            "progress_pct": max(0.0, min(100.0, progress_pct)),
        }
    )


@app.route("/api/saturn/quota", methods=["GET"])
def saturn_quota():
    today = now_utc().date().isoformat()
    defaults = {
        "serpapi": int(os.environ.get("SATURN_HUNTER_API_DAILY_LIMIT", "100")),
        "smtp": int(os.environ.get("SATURN_EMAIL_DAILY_LIMIT", "10")),
        "gemini": int(os.environ.get("SATURN_GEMINI_API_DAILY_LIMIT", "1000")),
    }
    out = {}
    with get_db_conn() as conn:
        for service in ("serpapi", "smtp", "gemini"):
            row = conn.execute(
                """
                SELECT COALESCE(call_count,0) AS calls,
                       COALESCE(quota_limit,0) AS quota_limit,
                       COALESCE(paused,0) AS paused
                FROM api_usage_log
                WHERE service=? AND usage_date=? AND endpoint='daily_counter'
                ORDER BY id DESC
                LIMIT 1
                """,
                (service, today),
            ).fetchone()
            out[service] = {
                "calls": int(row["calls"] or 0) if row else 0,
                "limit": int(row["quota_limit"] or 0) if row and int(row["quota_limit"] or 0) > 0 else defaults[service],
                "paused": int(row["paused"] or 0) if row else 0,
            }
    return jsonify(out)


@app.route("/api/saturn/health/full", methods=["GET"])
def saturn_health_full():
    n8n = check_n8n_status()
    openclaw = check_openclaw_status()
    ram_pct = 0.0
    try:
        import psutil  # type: ignore

        ram_pct = float(psutil.virtual_memory().percent)
    except Exception as exc:
        logger.warning("psutil memory probe failed", exc_info=exc)
        ram_pct = 0.0

    _, _, free = shutil.disk_usage("/")
    disk_free_gb = round(free / (1024 ** 3), 2)
    timers_active = count_active_saturn_timers()

    with get_db_conn() as conn:
        db_tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        db_table_names = [row["name"] for row in db_tables]
        last_morning_plan = conn.execute(
            """
            SELECT ts FROM agent_log
            WHERE lower(action)='morning_plan'
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
        last_report = conn.execute(
            """
            SELECT ts FROM agent_log
            WHERE lower(action) LIKE '%report%'
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()

    return jsonify(
        {
            "status": "ok",
            "n8n": n8n,
            "openclaw": openclaw,
            "db_tables": db_table_names,
            "ram_pct": ram_pct,
            "disk_free_gb": disk_free_gb,
            "timers_active": timers_active,
            "last_morning_plan": last_morning_plan["ts"] if last_morning_plan else "",
            "last_report": last_report["ts"] if last_report else "",
            "email_reader_last_run": "",
        }
    )


@app.route("/healthz", methods=["GET"])
def saturn_healthz():
    return jsonify(
        {
            "status": "ok",
            "service": "saturn-api",
            "uptime": format_uptime(),
        }
    )


@app.route("/api/saturn/activity", methods=["GET"])
def saturn_activity():
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(limit or 50, 200))
    agent = str(request.args.get("agent", "all")).lower().strip()

    with get_db_conn() as conn:
        if agent != "all" and agent in AGENT_CONFIG:
            rows = conn.execute(
                """
                SELECT id, lower(agent) AS agent, action, detail, result, ts
                FROM agent_log
                WHERE lower(agent)=?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (agent, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, lower(agent) AS agent, action, detail, result, ts
                FROM agent_log
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    activity = []
    for row in rows:
        result = str(row["result"] or "")
        if is_error_text(result):
            status = "error"
        elif result:
            status = "success"
        else:
            status = "working"

        action = str(row["action"] or "task").replace("_", " ").strip().title()
        detail = str(row["detail"] or "").strip()
        activity.append(
            {
                "id": int(row["id"]),
                "agent": row["agent"] if row["agent"] in AGENT_CONFIG else "saturn",
                "action": f"{action}{': ' + detail if detail else ''}",
                "status": status,
                "result": result,
                "ts": row["ts"] or "",
            }
        )

    return jsonify(activity)


@app.route("/api/saturn/system-health", methods=["GET"])
def saturn_system_health():
    n8n_status = check_n8n_status()

    db_status = "online"
    try:
        with get_db_conn() as conn:
            conn.execute("SELECT 1").fetchone()
            token_usage_today = int(
                query_scalar(
                    conn,
                    "SELECT COALESCE(SUM(tokens),0) FROM token_log WHERE date(logged_at)=date('now', '+5 hours', '+30 minutes')",
                )
            )
            restart_count = int(
                query_scalar(
                    conn,
                    """
                    SELECT COUNT(*)
                    FROM system_alerts
                    WHERE lower(source) LIKE '%restart%'
                       OR lower(message) LIKE '%restart%'
                    """,
                )
            )
            last_error = latest_error_message(conn)
    except sqlite3.Error as exc:
        logger.warning("system health DB probe failed", exc_info=exc)
        db_status = "offline"
        token_usage_today = 0
        restart_count = 0
        last_error = "Database unavailable"

    api_quota_used = round((token_usage_today / TOKEN_QUOTA_DAILY) * 100, 1) if TOKEN_QUOTA_DAILY else 0
    api_quota_remaining = max(0, round(100 - api_quota_used, 1))

    db_size_mb = 0.0
    if DB_PATH.exists():
        db_size_mb = round(DB_PATH.stat().st_size / (1024 * 1024), 2)

    return jsonify(
        {
            "status": "ok",
            "n8n": n8n_status,
            "db": db_status,
            "apiQuotaRemaining": api_quota_remaining,
            "quotaRemaining": api_quota_remaining,
            "dbSizeMb": db_size_mb,
            "lastError": last_error,
            "restartCount": restart_count,
            "uptime": format_uptime(),
        }
    )


if __name__ == "__main__":
    # Runs on localhost, port 8787. Accessible only from this machine.
    app.run(host="127.0.0.1", port=8787, debug=False)
