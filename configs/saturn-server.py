import asyncio
import base64 as _base64
import datetime
import json
import os
import queue
import re
import shutil
import sqlite3
import subprocess
import subprocess as _subprocess
import tempfile as _tempfile
import threading
import time
import time as _time
from email.message import EmailMessage
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from mcp.server.fastmcp import FastMCP

import sys as _sys
import pathlib as _pathlib
_SATURN_ROOT = str(_pathlib.Path(__file__).resolve().parent.parent)
if _SATURN_ROOT not in _sys.path:
    _sys.path.insert(0, _SATURN_ROOT)

_LLM_QUEUE = queue.Queue()
_WORKER_STARTED = False
_BUCKET_TOKENS = 5
_BUCKET_CAPACITY = 5
_BUCKET_REFILL_RATE = 1.5
_LAST_REFILL = time.time()
_BUCKET_LOCK = threading.Lock()

try:
    from backend.tools.web_search import search as web_search_tool
    from backend.tools.email_sender import send_email as email_send_tool
    TOOLS_AVAILABLE = True
except ImportError as e:
    import sys as _sys2
    print(f"TOOLS_IMPORT_ERROR: {e}", file=_sys2.stderr)
    TOOLS_AVAILABLE = False

try:
    from backend.tools.linkedin_search import search_prospects as _linkedin_search
    _linkedin_available = True
except Exception:
    _linkedin_available = False

try:
    from backend.tools.notion_sync import NotionSync, NotionAPIError, get_sync
    _notion = get_sync()
except Exception as _ne:
    import logging
    logging.warning(f"[Saturn] Notion sync unavailable: {_ne}")
    _notion = None

try:
    from backend.tools.skill_n8n import (
        n8n_health as _n8n_health,
        n8n_list as _n8n_list,
        n8n_run as _n8n_run,
        n8n_activate as _n8n_activate,
        n8n_deploy as _n8n_deploy,
        n8n_delete as _n8n_delete,
        n8n_build_simple as _n8n_build,
    )
    _n8n_ok = True
except Exception:
    _n8n_ok = False

try:
    from backend.tools.skill_email_writer import (
        outreach_prompt as _ew_outreach,
        followup_prompt as _ew_followup,
        reply_prompt as _ew_reply,
        validate as _ew_validate,
    )
    _email_writer_ok = True
except Exception:
    _email_writer_ok = False

try:
    from backend.tools.skill_self_heal import (
        full_report as _sh_report,
        auto_heal as _sh_heal,
        restart_api as _sh_restart,
    )
    _self_heal_ok = True
except Exception:
    _self_heal_ok = False

try:
    from backend.tools.skill_linkedin import (
        connection_request_prompt as _li_conn,
        inmail_prompt as _li_inmail,
        score_prompt as _li_score,
        parse_score as _li_parse,
    )
    _linkedin_ok = True
except Exception:
    _linkedin_ok = False

try:
    from backend.tools.skill_cost_monitor import (
        today_usage as _cm_today,
        monthly_usage as _cm_monthly,
    )
    _cost_monitor_ok = True
except Exception:
    _cost_monitor_ok = False

# Self-contained BASE_PATH + write guard for runtime deployment.
BASE_PATH = Path(
    os.environ.get("SATURN_BASE_PATH", str(Path.home() / "Workspace" / "Saturn"))
).expanduser().resolve()
DATABASE_DIR = BASE_PATH / "database"
LOGS_DIR = BASE_PATH / "logs"
DB_PATH = DATABASE_DIR / "saturn.db"
SECURITY_LOG = LOGS_DIR / "security.log"
SKILLS_DIR = BASE_PATH / "skills" / "n8n"


def read_skill(skill_name: str) -> str:
    path = SKILLS_DIR / skill_name / 'SKILL.md'
    try:
        return path.read_text() if path.exists() else ''
    except Exception:
        return ''


# Preload Forge skills at startup
FORGE_WORKFLOW_PATTERNS = read_skill('n8n-workflow-patterns')
FORGE_VALIDATION_RULES = read_skill('n8n-validation-expert')
FORGE_NODE_CONFIG = read_skill('n8n-node-configuration')


def _within_base(path: Path) -> bool:
    base = BASE_PATH.resolve()
    target = path.resolve()
    return target == base or base in target.parents


def _log_violation(target: Path, purpose: str) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.datetime.utcnow().isoformat()}Z DENY purpose={purpose} path={target}\n"
    with SECURITY_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line)


def enforce_write_path(path: str | Path, purpose: str = "write") -> Path:
    target = Path(path).expanduser().resolve()
    if _within_base(target):
        return target
    _log_violation(target, purpose)
    raise PermissionError(f"Write blocked outside BASE_PATH: {target}")

mcp = FastMCP("saturn")
server = mcp

VOICE_ALERT_PYTHON = os.environ.get(
    "SATURN_VOICE_PYTHON", str(Path.home() / "mcp-env/bin/python3")
)
VOICE_ALERT_SCRIPT = os.environ.get(
    "SATURN_VOICE_SCRIPT", str(BASE_PATH / "backend/voice_alert.py")
)
TELEGRAM_ENV_FILE = os.environ.get(
    "SATURN_TELEGRAM_ENV",
    str(Path.home() / ".config/openclaw-secrets/telegram.env"),
)
_VOICE_SENT_TODAY: set[str] = set()

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
    "contacted": {"qualified"},
    "qualified": {"proposal"},
    "proposal": {"won", "lost"},
    "won": set(),
    "lost": set(),
}
MAX_DAILY_LEADS = 10
MAX_DAILY_EMAIL_SEND = 10
MAX_EMAIL_RETRY = 1
MAX_HUNTER_PAGES = 1
RESULTS_PER_PAGE = 10
HUNTER_API_DAILY_LIMIT = int(os.environ.get("SATURN_HUNTER_API_DAILY_LIMIT", "100"))
HUNTER_API_STOP_THRESHOLD = float(os.environ.get("SATURN_HUNTER_API_STOP_THRESHOLD", "0.9"))
TOKEN_DAILY_LIMIT = int(os.environ.get("SATURN_TOKEN_DAILY_LIMIT", "500000"))
TOKEN_WARNING_THRESHOLD = int(os.environ.get("SATURN_TOKEN_WARNING_THRESHOLD", "400000"))
TOKEN_AGENT_DAILY_ALERT_LIMIT = int(os.environ.get("SATURN_AGENT_TOKEN_ALERT_LIMIT", "50000"))
GEMINI_API_DAILY_LIMIT = int(os.environ.get("SATURN_GEMINI_API_DAILY_LIMIT", "1000"))
_SCHEMA_INIT_LOCK = threading.Lock()
_SCHEMA_READY = False


# ── LLM CALL WRAPPER ─────────────────────────────────────────────────────────
import time as _time

# Use model names confirmed available from: genai.list_models()
# Update this list based on Step 1.3 output — fastest/cheapest first
_LLM_FALLBACK_MODELS = [
    "models/gemini-2.5-flash",
    "models/gemini-2.0-flash",
    "models/gemini-2.0-flash-lite",
]

# Wire fallback models into the central queue at import time
try:
    from backend.modules.llm_queue import set_fallback_models, set_min_interval

    set_fallback_models(_LLM_FALLBACK_MODELS)
    _interval = float(os.environ.get("SATURN_LLM_INTERVAL", "1.5"))
    set_min_interval(_interval)
except Exception:
    pass  # fail-open: call_llm still works via direct fallback

_AGENT_SYSTEM_PROMPTS = {
    "saturn": "You are Saturn, an AI operations orchestrator managing an automation agency.",
    "forge": "You are Forge, a senior automation architect that designs robust automation workflows.",
    "echo": "You are Echo, a professional outreach copywriter writing natural human emails.",
    "pulse": "You are Pulse, an analytics assistant generating structured operational reports.",
    "hunter": "You are Hunter, a lead discovery specialist extracting and scoring potential clients.",
    "sentinel": "You are Sentinel, a monitoring AI ensuring systems are healthy and secure.",
}


def _response_text_or_empty(response: object) -> str:
    try:
        text = getattr(response, "text", "")
        if text:
            return str(text).strip()
    except Exception:
        pass
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None) or []
        fragments: list[str] = []
        for part in parts:
            value = getattr(part, "text", None)
            if value:
                fragments.append(str(value))
        if fragments:
            return "".join(fragments).strip()
    return ""


def _consume_token() -> bool:
    global _BUCKET_TOKENS, _LAST_REFILL

    with _BUCKET_LOCK:
        now = time.time()
        refill = (now - _LAST_REFILL) * _BUCKET_REFILL_RATE
        _BUCKET_TOKENS = min(_BUCKET_CAPACITY, _BUCKET_TOKENS + refill)
        _LAST_REFILL = now

        if _BUCKET_TOKENS < 1:
            return False

        _BUCKET_TOKENS -= 1
        return True


def _llm_worker() -> None:
    while True:
        item = _LLM_QUEUE.get()
        fn, args, result_queue = item

        while not _consume_token():
            time.sleep(0.5)

        try:
            result = fn(*args)
            result_queue.put(result)
        except Exception as exc:
            result_queue.put(exc)

        _LLM_QUEUE.task_done()


def _start_llm_worker() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return

    t = threading.Thread(target=_llm_worker, daemon=True)
    t.start()
    _WORKER_STARTED = True


def call_llm(prompt: str, system: str = "", max_tokens: int = 1000,
             agent: str = "saturn") -> str:
    """
    Central LLM call - routes through the shared rate limiter.
    All burst protection, retry, and fallback handled in llm_queue.py.
    """
    try:
        from backend.modules.llm_queue import call_llm_queued
        return call_llm_queued(prompt, system=system, max_tokens=max_tokens)
    except Exception as e:
        raise RuntimeError(f"[Saturn:{agent}] LLM call failed: {e}") from e


def utc_now() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def add_column_if_missing(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name not in table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def ensure_operational_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS token_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tokens INTEGER NOT NULL,
        model TEXT,
        agent TEXT,
        action TEXT,
        logged_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS token_usage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT NOT NULL,
        action TEXT NOT NULL,
        tokens_used INTEGER NOT NULL,
        log_date TEXT NOT NULL,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    conn.execute("""CREATE TABLE IF NOT EXISTS error_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT NOT NULL,
        action TEXT NOT NULL,
        error_type TEXT NOT NULL,
        message TEXT NOT NULL,
        detail TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        priority TEXT DEFAULT 'normal',
        created_at TEXT,
        updated_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS work_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry TEXT NOT NULL,
        category TEXT DEFAULT 'general',
        logged_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        company TEXT,
        contact TEXT,
        source TEXT,
        status TEXT DEFAULT 'new',
        value_estimate REAL,
        notes TEXT,
        last_contact TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS content_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_type TEXT,
        title TEXT,
        body TEXT,
        platform TEXT,
        status TEXT DEFAULT 'draft',
        scheduled_for TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS outreach_drafts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER NOT NULL,
        draft_text TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS email_send_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        draft_id INTEGER,
        status TEXT NOT NULL,
        attempt_count INTEGER DEFAULT 1,
        error_category TEXT,
        sent_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS system_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent TEXT,
        title TEXT,
        level TEXT,
        source TEXT,
        message TEXT,
        resolved INTEGER DEFAULT 0,
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

    add_column_if_missing(conn, "token_log", "agent TEXT")
    add_column_if_missing(conn, "token_log", "action TEXT")
    add_column_if_missing(conn, "system_alerts", "agent TEXT")
    add_column_if_missing(conn, "system_alerts", "title TEXT")
    add_column_if_missing(conn, "system_alerts", "error_type TEXT")
    add_column_if_missing(conn, "system_alerts", "alert_type TEXT")
    add_column_if_missing(conn, "api_usage_log", "service TEXT")
    add_column_if_missing(conn, "api_usage_log", "usage_date TEXT")
    add_column_if_missing(conn, "api_usage_log", "call_date TEXT")
    add_column_if_missing(conn, "api_usage_log", "call_count INTEGER DEFAULT 0")
    add_column_if_missing(conn, "api_usage_log", "quota_limit INTEGER DEFAULT 0")
    add_column_if_missing(conn, "api_usage_log", "paused INTEGER DEFAULT 0")
    conn.execute(
        """
        UPDATE api_usage_log
        SET
            call_date = COALESCE(NULLIF(call_date, ''), NULLIF(usage_date, ''), date(called_at), date('now')),
            usage_date = COALESCE(NULLIF(usage_date, ''), NULLIF(call_date, ''), date(called_at), date('now'))
        WHERE call_date IS NULL OR call_date='' OR usage_date IS NULL OR usage_date=''
        """
    )
    add_column_if_missing(conn, "outreach_drafts", "processed_at TIMESTAMP")
    add_column_if_missing(conn, "email_send_log", "lead_id INTEGER")
    add_column_if_missing(conn, "email_send_log", "draft_id INTEGER")
    add_column_if_missing(conn, "email_send_log", "status TEXT")
    add_column_if_missing(conn, "email_send_log", "attempt_count INTEGER DEFAULT 1")
    add_column_if_missing(conn, "email_send_log", "error_category TEXT")
    add_column_if_missing(conn, "email_send_log", "sent_at TIMESTAMP")

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_website_norm ON leads(website_norm) WHERE website_norm IS NOT NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_status_followup ON leads(status, follow_up_due_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_content_queue_status ON content_queue(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_content_queue_lead ON content_queue(lead_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_outreach_drafts_lead ON outreach_drafts(lead_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_send_log_day ON email_send_log(status, sent_at)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_api_usage_day ON api_usage_log(agent, provider, called_at)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_api_usage_daily_counter ON api_usage_log(service, usage_date, endpoint) "
        "WHERE endpoint='daily_counter'"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_error_log_ts ON error_log(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_token_log_day ON token_log(logged_at)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_token_log_agent_action_day ON token_log(agent, action, logged_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_system_alerts_agent_title_day ON system_alerts(agent, title, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_token_usage_agent_day ON token_usage_log(agent, log_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_service_day ON api_usage_log(service, usage_date, paused)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_service_call_day ON api_usage_log(service, call_date, paused)")
    conn.commit()


def db_conn():
    global _SCHEMA_READY
    db_path = enforce_write_path(DB_PATH, "sqlite-db-write")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
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


def db():
    return db_conn()


def run_async_tool(coro) -> None:
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        asyncio.run(coro)
    except Exception:
        pass


def normalize_website(website: str | None) -> str | None:
    if not website:
        return None
    candidate = website.strip().lower()
    if not candidate:
        return None
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def extract_email_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0).lower() if match else None


def resolve_email(primary_email: str, notes: str, website_norm: str | None) -> tuple[str, str, str]:
    direct = extract_email_from_text(primary_email)
    if direct:
        return direct, "found", "direct_input"

    from_notes = extract_email_from_text(notes)
    if from_notes:
        return from_notes, "found", "snippet_regex"

    if website_norm:
        return f"info@{website_norm}", "fallback", "domain_default"

    return "", "missing", "none"


def extract_contact_page_mailto(
    website_norm: str | None, conn: sqlite3.Connection | None = None
) -> tuple[str | None, str | None]:
    if not website_norm:
        return None, None
    last_error: str | None = None
    candidates = [
        f"https://{website_norm}/contact",
        f"https://{website_norm}/contact-us",
        f"https://{website_norm}",
        f"http://{website_norm}/contact",
        f"http://{website_norm}",
    ]
    for candidate in candidates:
        try:
            req = Request(candidate, headers={"User-Agent": "SATURN-Hunter/1.0"})
            with urlopen(req, timeout=10) as response:
                body = response.read(200000).decode("utf-8", errors="ignore")
            match = re.search(
                r"mailto:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
                body,
                flags=re.IGNORECASE,
            )
            if match:
                return match.group(1).lower(), None
        except HTTPError as exc:
            if exc.code in (401, 403):
                return None, "AUTH_ERROR"
            if exc.code == 429:
                return None, "RATE_LIMIT"
            last_error = "API_ERROR"
            continue
        except TimeoutError:
            return None, "NETWORK_ERROR"
        except URLError as exc:
            text = str(exc).lower()
            if "timed out" in text or "timeout" in text:
                return None, "NETWORK_ERROR"
            last_error = "NETWORK_ERROR"
            continue
        except Exception as exc:
            if conn is not None and last_error != "API_ERROR":
                log_error(
                    conn,
                    "hunter",
                    "email_extraction",
                    "API_ERROR",
                    "Contact page fallback failed",
                    f"{candidate} :: {str(exc)[:300]}",
                )
            last_error = "API_ERROR"
            continue
    return None, last_error


def resolve_hunter_email(
    conn: sqlite3.Connection,
    primary_email: str,
    notes: str,
    website_norm: str | None,
) -> tuple[str | None, str, str]:
    direct = extract_email_from_text(primary_email)
    if direct:
        return direct, "found", "api_response"

    from_notes = extract_email_from_text(notes)
    if from_notes:
        return from_notes, "found", "api_response"

    fallback_email, fallback_error = extract_contact_page_mailto(website_norm, conn)
    if fallback_error:
        log_error(
            conn,
            "hunter",
            "email_extraction",
            fallback_error,
            "Contact page email fallback failed",
            website_norm or "",
        )
    if fallback_email:
        return fallback_email, "found", "contact_page_mailto"
    return None, "not_found", "none"


def build_echo_draft_text(lead_name: str, lead_company: str) -> str:
    first_name = (str(lead_name or "").strip().split(" ")[0] or "there")
    company = str(lead_company or "").strip()
    return (
        f"Hi {first_name}, I saw you work with {company} and thought I'd reach out. "
        f"Companies in your space often face challenges with manual processes. "
        f"I build automations to solve exactly that, saving valuable time."
    )


def log_error(
    conn: sqlite3.Connection,
    agent: str,
    action: str,
    error_type: str,
    message: str,
    detail: str = "",
) -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        (agent, action, safe_type, message, detail, utc_now()),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail[:300], f"{safe_type}: {message[:300]}", utc_now()),
    )


def log_agent(
    conn: sqlite3.Connection,
    agent: str,
    action: str,
    detail: str = "",
    result: str = "success",
) -> None:
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail, result, utc_now()),
    )


def log_api_call(
    conn: sqlite3.Connection,
    agent: str,
    provider: str,
    endpoint: str,
    status: str,
    error_type: str = "",
    detail: str = "",
) -> None:
    safe_service = (provider or agent or "unknown").strip().lower()
    quota_limit = service_quota_limit(safe_service)
    now = utc_now()
    conn.execute(
        """
        INSERT INTO api_usage_log
        (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_date, call_count, quota_limit, paused)
        VALUES ('system', ?, 'daily_counter', 'success', '', '', ?, ?, DATE('now','localtime'), DATE('now','localtime'), 1, ?, 0)
        ON CONFLICT(service, usage_date, endpoint) DO UPDATE SET
            call_count=COALESCE(api_usage_log.call_count,0)+1,
            called_at=excluded.called_at,
            call_date=excluded.call_date,
            status=excluded.status,
            error_type=excluded.error_type,
            detail=excluded.detail,
            quota_limit=CASE
                WHEN COALESCE(api_usage_log.quota_limit,0) > 0 THEN api_usage_log.quota_limit
                ELSE excluded.quota_limit
            END
        """,
        (safe_service, now, safe_service, quota_limit),
    )
    conn.execute(
        """
        INSERT INTO api_usage_log
        (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_date, call_count, quota_limit, paused)
        VALUES (?,?,?,?,?,?,?, ?, DATE('now','localtime'), DATE('now','localtime'), 1, ?, 0)
        ON CONFLICT(service, usage_date, endpoint) DO UPDATE SET
            call_count=COALESCE(api_usage_log.call_count,0)+1,
            called_at=excluded.called_at,
            call_date=excluded.call_date,
            status=excluded.status,
            error_type=excluded.error_type,
            detail=excluded.detail
        """,
        (agent, provider, endpoint, status, error_type, detail, now, safe_service, quota_limit),
    )


def telegram_env() -> dict[str, str]:
    env_vars: dict[str, str] = {}
    try:
        with open(TELEGRAM_ENV_FILE, encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    env_vars[key] = value
    except FileNotFoundError:
        return {}
    return env_vars


def _normalize_alert_identity(agent: str, title: str) -> tuple[str, str]:
    safe_agent = str(agent or "system").strip()[:120] or "system"
    safe_title = str(title or "Untitled Alert").strip()[:300] or "Untitled Alert"
    return safe_agent, safe_title


def _sarvam_tts(text: str) -> str:
    """
    Call Sarvam AI TTS with the best available natural Indian female voice.
    Returns wav path on success, empty string on failure.
    Fail-open: never raises, always returns a path or empty string.
    """
    import requests as _req

    api_key = os.environ.get("SARVAM_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        r = _req.post(
            "https://api.sarvam.ai/text-to-speech",
            headers={
                "api-subscription-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "inputs": [text[:500]],
                "target_language_code": "en-IN",
                "speaker": "rupali",
                "pace": 1.0,
                "speech_sample_rate": 22050,
                "enable_preprocessing": True,
                "model": "bulbul:v3",
            },
            timeout=10,
        )
        if r.status_code != 200:
            return ""
        audio_b64 = r.json().get("audios", [""])[0]
        if not audio_b64:
            return ""
        audio_bytes = _base64.b64decode(audio_b64)
        with _tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as fh:
            fh.write(audio_bytes)
            tmp_path = fh.name
        return tmp_path
    except Exception:
        return ""


def _system_tts(text: str) -> str:
    """
    System TTS fallback. Uses the local OpenClaw/Linux voice path and softens
    the output so it sounds less robotic than raw espeak defaults.
    Returns wav path on success, empty string on failure.
    """
    try:
        cleaned = " ".join(str(text or "").replace("\n", " ").split())[:320]
        if not cleaned:
            return ""

        with _tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as raw_fh:
            raw_path = raw_fh.name
        result = _subprocess.run(
            [
                "espeak",
                "-v",
                "en-us+f3",
                "-s",
                "142",
                "-p",
                "46",
                "-a",
                "170",
                "-g",
                "8",
                "-w",
                raw_path,
                cleaned,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return ""

        ffmpeg_bin = shutil.which("ffmpeg")
        if not ffmpeg_bin:
            return raw_path

        with _tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as soft_fh:
            soft_path = soft_fh.name
        ffmpeg_result = _subprocess.run(
            [
                ffmpeg_bin,
                "-y",
                "-i",
                raw_path,
                "-ac",
                "1",
                "-ar",
                "22050",
                "-af",
                "highpass=f=110,lowpass=f=3800,volume=1.8,alimiter=limit=0.92",
                soft_path,
            ],
            capture_output=True,
            text=True,
        )
        if ffmpeg_result.returncode == 0:
            try:
                os.unlink(raw_path)
            except OSError:
                pass
            return soft_path
        return raw_path
    except Exception:
        return ""


def _speak(text: str) -> str:
    """
    Generate speech using Sarvam Meera (primary) or system TTS (fallback).
    Returns wav file path on success, empty string on failure.
    """
    sarvam_audio = _sarvam_tts(text)
    if sarvam_audio:
        return sarvam_audio
    return _system_tts(text)


def _send_telegram_voice_note(audio_file: str) -> bool:
    """Send Telegram voice note upload. Returns True on success."""
    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    chat_id = os.environ.get("GROUP_ID", os.environ.get("CHAT_ID", "")).strip()
    thread_id = os.environ.get("THREAD_ALERTS", "").strip()
    if not bot_token or not chat_id or not audio_file:
        return False
    try:
        cmd = [
            "curl",
            "-s",
            "-X",
            "POST",
            f"https://api.telegram.org/bot{bot_token}/sendVoice",
            "-F",
            f"chat_id={chat_id}",
            "-F",
            f"voice=@{audio_file}",
        ]
        if thread_id:
            cmd.extend(["-F", f"message_thread_id={thread_id}"])
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0 and '"ok":true' in (result.stdout or "")
    except Exception:
        return False


_TELEGRAM_LAST_SENT = 0.0
_TELEGRAM_MIN_INTERVAL = 1.1
_TELEGRAM_SENT_TODAY: set[str] = set()


def _telegram_rate_limit() -> None:
    global _TELEGRAM_LAST_SENT
    elapsed = _time.time() - _TELEGRAM_LAST_SENT
    if elapsed < _TELEGRAM_MIN_INTERVAL:
        _time.sleep(_TELEGRAM_MIN_INTERVAL - elapsed)
    _TELEGRAM_LAST_SENT = _time.time()


def _telegram_dedup_key(message: str) -> str:
    import hashlib

    return datetime.date.today().isoformat() + ":" + hashlib.md5(message.encode()).hexdigest()[:12]


def send_telegram_message(message: str, thread_key: str = "THREAD_ALERTS", alert_type: str = "") -> bool | dict[str, str]:
    _telegram_rate_limit()
    dedup_key = _telegram_dedup_key(message)
    if dedup_key in _TELEGRAM_SENT_TODAY:
        return {"status": "skipped", "reason": "duplicate_today"}
    _TELEGRAM_SENT_TODAY.add(dedup_key)
    safe_alert_type = (alert_type or "").strip().lower()
    if safe_alert_type in {"daily_report", "hourly_check"}:
        conn = db_conn()
        try:
            exists = conn.execute(
                """
                SELECT id FROM system_alerts
                WHERE alert_type=? AND date(created_at)=date('now')
                LIMIT 1
                """,
                (safe_alert_type,),
            ).fetchone()
            if exists:
                return True
        finally:
            conn.close()

    env_vars = telegram_env()
    bot_token = env_vars.get("BOT_TOKEN")
    group_id = env_vars.get("GROUP_ID")
    thread_id = env_vars.get(thread_key) or env_vars.get("THREAD_LEADS")
    if not all([bot_token, group_id, thread_id]):
        return False
    result = subprocess.run(
        [
            "curl",
            "-s",
            "-X",
            "POST",
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            "-d",
            f"chat_id={group_id}",
            "-d",
            f"message_thread_id={thread_id}",
            "-d",
            f"text={message}",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    sent = result.returncode == 0
    if sent and safe_alert_type in {"daily_report", "hourly_check"}:
        conn = db_conn()
        try:
            agent, title = _normalize_alert_identity("telegram-report", safe_alert_type)
            conn.execute(
                """
                INSERT INTO system_alerts (agent, title, level, source, message, error_type, alert_type, resolved, created_at)
                VALUES (?,?,?,?,?,?,?,0,?)
                """,
                (agent, title, "info", "telegram-report", message, "", safe_alert_type, utc_now()),
            )
            conn.commit()
        finally:
            conn.close()
    return sent


def create_system_alert_once(
    conn: sqlite3.Connection,
    level: str,
    source: str,
    error_type: str,
    message: str,
) -> bool:
    agent, title = _normalize_alert_identity(source, message)
    existing = conn.execute(
        """
        SELECT id FROM system_alerts WHERE agent=? AND title=? AND DATE(created_at)=DATE('now', 'localtime')
        """,
        (agent, title),
    ).fetchone()
    if existing:
        return False
    cursor = conn.execute(
        """
        INSERT INTO system_alerts (agent, title, level, source, message, error_type, resolved, created_at)
        VALUES (?,?,?,?,?,?,0,?)
        """,
        (agent, title, level, source, message, error_type, utc_now()),
    )
    alert_id = int(cursor.lastrowid)
    try:
        alert_dict = {
            "id": alert_id,
            "agent": agent,
            "title": title,
            "level": level,
            "source": source,
            "message": message,
            "error_type": error_type,
            "resolved": 0,
            "created_at": utc_now(),
        }
        run_async_tool(notion_sync_alert(json.dumps(alert_dict)))
    except Exception:
        pass
    return True


def service_quota_limit(service: str) -> int:
    service_lc = (service or "").strip().lower()
    if service_lc in {"hunter", "serpapi"}:
        return MAX_DAILY_LEADS
    if service_lc == "smtp":
        return MAX_DAILY_EMAIL_SEND
    if service_lc == "gemini":
        return GEMINI_API_DAILY_LIMIT
    return 0


def ensure_service_daily_counter_row(conn: sqlite3.Connection, service: str) -> None:
    service_lc = (service or "").strip().lower()
    if not service_lc:
        return
    now = utc_now()
    quota_limit = service_quota_limit(service_lc)
    conn.execute(
        """
        INSERT INTO api_usage_log
        (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_date, call_count, quota_limit, paused)
        VALUES ('system', ?, 'daily_counter', 'success', '', '', ?, ?, DATE('now','localtime'), DATE('now','localtime'), 0, ?, 0)
        ON CONFLICT(service, usage_date, endpoint) DO UPDATE SET
            called_at=excluded.called_at,
            call_date=excluded.call_date,
            status=excluded.status,
            error_type=excluded.error_type,
            detail=excluded.detail,
            quota_limit=CASE
                WHEN COALESCE(api_usage_log.quota_limit,0) > 0 THEN api_usage_log.quota_limit
                ELSE excluded.quota_limit
            END
        """,
        (service_lc, now, service_lc, quota_limit),
    )
    if quota_limit > 0:
        conn.execute(
            """
            UPDATE api_usage_log
            SET quota_limit=?
            WHERE service=? AND call_date=DATE('now','localtime') AND endpoint='daily_counter' AND (quota_limit IS NULL OR quota_limit<=0)
            """,
            (quota_limit, service_lc),
        )


def service_is_paused(conn: sqlite3.Connection, service: str) -> bool:
    service_lc = (service or "").strip().lower()
    ensure_service_daily_counter_row(conn, service_lc)
    row = conn.execute(
        """
        SELECT COALESCE(paused,0)
        FROM api_usage_log
        WHERE service=? AND call_date=date('now') AND endpoint='daily_counter'
        LIMIT 1
        """,
        (service_lc,),
    ).fetchone()
    return bool(row and int(row[0] or 0) == 1)


def hunter_api_calls_today(conn: sqlite3.Connection, provider: str = "serpapi") -> int:
    ensure_service_daily_counter_row(conn, "hunter")
    daily_counter = conn.execute(
        """
        SELECT call_count
        FROM api_usage_log
        WHERE lower(service)='hunter' AND call_date=date('now') AND endpoint='daily_counter'
        LIMIT 1
        """
    ).fetchone()
    if daily_counter and daily_counter[0] is not None:
        return int(daily_counter[0] or 0)

    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM api_usage_log
        WHERE lower(agent)='hunter'
          AND lower(provider)=lower(?)
          AND lower(status)='success'
          AND date(called_at)=date('now')
          AND (endpoint IS NULL OR endpoint!='daily_counter')
        """,
        (provider,),
    ).fetchone()
    return int(row[0] or 0)


def hunter_quota_blocked(conn: sqlite3.Connection, provider: str = "serpapi") -> bool:
    calls = hunter_api_calls_today(conn, provider)
    return calls >= MAX_DAILY_LEADS


def hunter_increment_daily_usage(conn: sqlite3.Connection) -> None:
    ensure_service_daily_counter_row(conn, "hunter")
    now = utc_now()
    conn.execute(
        """
        UPDATE api_usage_log
        SET call_count=COALESCE(call_count,0)+1, called_at=?
        WHERE service='hunter' AND call_date=date('now') AND endpoint='daily_counter'
        """,
        (now,),
    )


def validate_lead_transition(
    current: str,
    requested: str,
    follow_up_count: int,
    manual_override: bool,
) -> tuple[bool, str]:
    current_status = (current or "").strip().lower()
    requested_status = (requested or "").strip().lower()
    if manual_override:
        return True, "ok"
    if current_status == requested_status:
        return True, "ok"
    if current_status == "contacted" and requested_status == "lost":
        if int(follow_up_count or 0) >= 2:
            return True, "ok"
        return False, "contacted_to_lost_requires_followup_count_2"
    allowed = {
        "new": {"contacted"},
        "contacted": {"qualified"},
        "qualified": {"proposal"},
        "proposal": {"won", "lost"},
        "won": set(),
        "lost": set(),
    }
    if requested_status in allowed.get(current_status, set()):
        return True, "ok"
    return False, "invalid_transition"


def get_lead_status(conn: sqlite3.Connection, lead_id: int) -> str:
    row = conn.execute("SELECT status FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not row:
        return "unknown"
    status = str(row["status"] or "").strip().lower()
    return status or "unknown"

@mcp.tool()
def ping() -> str:
    """Health check"""
    return f"Saturn MCP online — {datetime.datetime.utcnow().isoformat()}Z"

@mcp.tool()
def add_task(title: str, priority: str = "normal") -> str:
    """Add a task. Priority: low | normal | high"""
    now = datetime.datetime.utcnow().isoformat()
    conn = db()
    try:
        cursor = conn.execute(
            "INSERT INTO tasks (title, priority, created_at, updated_at) VALUES (?,?,?,?)",
            (title, priority, now, now)
        )
        task_id = int(cursor.lastrowid)
        conn.commit()
        if _notion:
            try:
                _notion.sync_task({
                    "saturn_id": f"task_{int(time.time())}",
                    "name": title,
                    "priority": priority,
                    "status": "Todo",
                    "agent": "Saturn",
                })
            except Exception as _e:
                pass
        return f"Task added: [{priority}] {title}"
    finally:
        conn.close()

@mcp.tool()
def list_tasks(status: str = "pending") -> str:
    """List tasks. status: pending | done | all"""
    conn = db()
    try:
        if status == "all":
            rows = conn.execute("SELECT id, title, status, priority, created_at FROM tasks ORDER BY created_at DESC").fetchall()
        else:
            rows = conn.execute("SELECT id, title, status, priority, created_at FROM tasks WHERE status=? ORDER BY created_at DESC", (status,)).fetchall()
        if not rows:
            return f"No {status} tasks."
        return "\n".join([f"[{r[0]}] [{r[3]}] {r[1]} — {r[2]} ({r[4][:10]})" for r in rows])
    finally:
        conn.close()

@mcp.tool()
def complete_task(task_id: int) -> str:
    """Mark task complete by ID"""
    now = datetime.datetime.utcnow().isoformat()
    conn = db()
    try:
        conn.execute("UPDATE tasks SET status='done', updated_at=? WHERE id=?", (now, task_id))
        task = conn.execute(
            "SELECT id, title, status, priority, created_at, updated_at FROM tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        conn.commit()
        try:
            if task:
                task_dict = {
                    "id": int(task["id"]),
                    "title": task["title"],
                    "status": task["status"],
                    "priority": task["priority"],
                    "created_at": task["created_at"],
                    "updated_at": task["updated_at"],
                }
                run_async_tool(notion_create_task(json.dumps(task_dict)))
        except Exception:
            pass
        return f"Task {task_id} marked complete."
    finally:
        conn.close()

@mcp.tool()
def log_work(entry: str, category: str = "general") -> str:
    """Log work. category: general | dev | ops | content | meeting"""
    now = datetime.datetime.utcnow().isoformat()
    conn = db()
    try:
        conn.execute("INSERT INTO work_log (entry, category, logged_at) VALUES (?,?,?)", (entry, category, now))
        conn.commit()
        return f"Logged [{category}]: {entry}"
    finally:
        conn.close()

@mcp.tool()
def daily_report() -> str:
    """Generate today's work report"""
    # Guard: only run once per calendar day.
    # daily_plans.plan_date is UNIQUE, so we use agent_log for the marker to
    # avoid colliding with the actual saved daily plan for the same day.
    import datetime as _dt

    _today_str = _dt.date.today().isoformat()
    _conn_guard = db()
    try:
        _conn_guard.execute("BEGIN IMMEDIATE")
        _existing = _conn_guard.execute(
            "SELECT id FROM agent_log WHERE action=? AND detail=? LIMIT 1",
            ("daily_report_guard", _today_str),
        ).fetchone()
        if _existing:
            _conn_guard.commit()
            return {
                "status": "skipped",
                "reason": "daily_report_already_ran_today",
                "date": _today_str,
            }
        _conn_guard.execute(
            "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
            ("Pulse", "daily_report_guard", _today_str, "running", utc_now()),
        )
        _conn_guard.commit()
    except Exception:
        try:
            _conn_guard.rollback()
        except Exception:
            pass
    finally:
        _conn_guard.close()

    conn = db()
    try:
        today = _today_str
        tasks = conn.execute("SELECT title, status, priority FROM tasks WHERE date(created_at)=?", (today,)).fetchall()
        logs = conn.execute("SELECT entry, category, logged_at FROM work_log WHERE date(logged_at)=? ORDER BY logged_at", (today,)).fetchall()
        done = [t for t in tasks if t[1] == "done"]
        pending = [t for t in tasks if t[1] == "pending"]
        r = f"=== Saturn Daily Report: {today} ===\n"
        r += f"\nCOMPLETED ({len(done)}):\n" + "\n".join([f"  ✓ [{t[2]}] {t[0]}" for t in done]) if done else "\nCOMPLETED: None"
        r += f"\n\nPENDING ({len(pending)}):\n" + "\n".join([f"  ○ [{t[2]}] {t[0]}" for t in pending]) if pending else "\n\nPENDING: None"
        r += f"\n\nWORK LOG ({len(logs)}):\n" + "\n".join([f"  {l[2][11:16]} [{l[1]}] {l[0]}" for l in logs]) if logs else "\n\nWORK LOG: Empty"
        if _notion:
            try:
                report = _notion.progress_report()
                _notion.notion_update_hq_status(
                    date=datetime.datetime.now().strftime("%d %b %Y"),
                    leads_total=report.get("leads", {}).get("total", 0),
                    revenue_earned=report.get("revenue", {}).get("total_paid", 0.0),
                    pending_approvals=report.get("outreach", {}).get("pending_approval", 0),
                )
                telegram_report = _notion.format_progress_report(report)
            except Exception as _e:
                pass
        return r
    finally:
        conn.close()

@mcp.tool()
def trigger_n8n(workflow_name: str) -> str:
    """Trigger n8n workflow. Available: daily_summary | task_sync"""
    import httpx
    webhooks = {
        "daily_summary": "http://127.0.0.1:5678/webhook/daily-summary",
        "task_sync": "http://127.0.0.1:5678/webhook/task-sync",
    }
    url = webhooks.get(workflow_name)
    if not url:
        return f"Unknown workflow: {workflow_name}. Available: {list(webhooks.keys())}"
    try:
        r = httpx.post(url, timeout=10)
        return f"n8n [{r.status_code}]: {r.text[:200]}"
    except Exception as e:
        conn = db_conn()
        log_error(conn, "sentinel", "trigger_n8n", "NETWORK_ERROR", "n8n trigger failed", str(e)[:500])
        conn.commit()
        conn.close()
        return f"n8n trigger failed: {str(e)}"


@mcp.tool()
def web_search(query: str, max_results: int = 10) -> str:
    """Run web search through SATURN unified tool layer."""
    conn = db_conn()
    try:
        if not TOOLS_AVAILABLE:
            log_error(conn, "hunter", "web_search", "LOGIC_ERROR", "Unified tools unavailable", "")
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "tools_unavailable"})

        rows = web_search_tool(query=query, max_results=max_results)
        log_agent(conn, "Hunter", "web_search", f"query={query} results={len(rows)}", "success")
        conn.commit()
        return json.dumps(rows)
    except Exception as exc:
        log_error(conn, "hunter", "web_search", "API_ERROR", "web_search tool failed", str(exc)[:500])
        conn.commit()
        return json.dumps({"status": "failed", "error_type": "API_ERROR"})
    finally:
        conn.close()


@mcp.tool()
def forge_list_workflows() -> dict:
    """List all n8n workflows with id, name, active status."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    return _n8n_list()


@mcp.tool()
def forge_run_workflow(workflow_id: str, payload: str = "{}") -> dict:
    """Run an n8n workflow by ID. payload is a JSON string."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    try:
        data = json.loads(payload)
    except Exception:
        data = {}
    return _n8n_run(workflow_id, data)


@mcp.tool()
def forge_deploy_workflow(workflow_json: str, activate: bool = False) -> dict:
    """Deploy a new n8n workflow from JSON string. Returns workflow_id."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    try:
        wf = json.loads(workflow_json)
    except Exception as e:
        return {"status": "error", "reason": f"invalid JSON: {e}"}
    return _n8n_deploy(wf, activate)


@mcp.tool()
def forge_build_webhook_workflow(
    name: str,
    webhook_path: str,
    target_url: str,
    method: str = "POST",
) -> dict:
    """Build a webhook→HTTP workflow JSON. Use forge_deploy_workflow to deploy it."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    return _n8n_build(name, webhook_path, target_url, method)


@mcp.tool()
def forge_activate_workflow(workflow_id: str, active: bool = True) -> dict:
    """Activate or deactivate an n8n workflow."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    return _n8n_activate(workflow_id, active)


@mcp.tool()
def forge_n8n_health() -> dict:
    """Check if n8n is reachable."""
    if not _n8n_ok:
        return {"status": "error", "reason": "n8n skill not loaded"}
    return _n8n_health()


@mcp.tool()
def echo_write_outreach(
    contact_name: str,
    company: str,
    pain_point: str,
    service: str,
    result_example: str = "",
    sender_name: str = "Navin",
) -> dict:
    """Generate a human cold outreach email. Returns subject and body."""
    if not _email_writer_ok:
        return {"status": "error", "reason": "email writer not loaded"}
    try:
        prompt = f"""
You are Echo, a human sales email writer.

Write a cold outreach email.

Recipient: {contact_name}
Company: {company}

Problem:
{pain_point}

Service:
{service}

Rules:

1. Write 4–6 sentences.
2. Natural human tone.
3. No marketing jargon.
4. First line must mention their company.
5. Include a clear benefit.
6. Final line asks for a quick reply.

Format EXACTLY like:

Subject: <short subject line>

<email body paragraph>

Do not output explanations.
Only the email.
"""
        subject = "Quick question"
        body = ""
        raw = ""
        for attempt in range(3):
            raw = call_llm(prompt, max_tokens=350, agent="echo")
            text = raw.strip()
            subject = "Quick question"
            if text.lower().startswith("subject:"):
                first_line, *rest = text.split("\n", 1)
                subject = first_line.replace("Subject:", "").strip()
                body = rest[0].strip() if rest else ""
            else:
                parts = text.split("\n", 1)
                subject = parts[0].strip() if parts else subject
                body = parts[1].strip() if len(parts) > 1 else text
            if len(body) < 20:
                body = text
            if len(body.split()) >= 30:
                break
            if attempt == 2:
                body = raw
        if len(body.split()) < 30:
            subject = f"Quick question about {company}"
            body = (
                f"{contact_name} — noticed {company} is likely handling more inbound lead volume as it grows. "
                f"When lead qualification stays manual, it usually eats up hours every week and slows down follow-up for the strongest prospects. "
                f"We build small AI automation systems that score, route, and prioritize leads automatically so your team can focus on the conversations most likely to close. "
                f"That usually means faster response times, cleaner handoffs, and less time spent sorting low-intent inquiries. "
                f"Would you be open to a quick reply to see if this could fit your process?"
            )
        if len(body.split()) < 30:
            raise ValueError("email too short")
        v = _ew_validate(body)
        return {"status": "success", "subject": subject, "body": body, "validation": v}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def echo_write_followup(
    contact_name: str,
    original_summary: str,
    followup_number: int = 1,
    sender_name: str = "Navin",
) -> dict:
    """Generate follow-up email. followup_number must be 1 or 2."""
    if followup_number > 2:
        return {"status": "error", "reason": "max 2 follow-ups allowed"}
    if not _email_writer_ok:
        return {"status": "error", "reason": "email writer not loaded"}
    try:
        prompt = _ew_followup(sender_name, contact_name, original_summary, followup_number)
        body = call_llm(prompt, max_tokens=150)
        return {"status": "success", "body": body.strip(), "followup_number": followup_number}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def echo_write_reply(
    contact_name: str,
    their_message: str,
    context: str = "",
    sender_name: str = "Navin",
) -> dict:
    """Generate a reply to a lead's message."""
    if not _email_writer_ok:
        return {"status": "error", "reason": "email writer not loaded"}
    try:
        prompt = _ew_reply(sender_name, contact_name, their_message, context)
        body = call_llm(prompt, max_tokens=200)
        return {"status": "success", "body": body.strip()}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def sentinel_health_report() -> dict:
    """Full system health: API, n8n, SQLite, disk, timers."""
    if not _self_heal_ok:
        return {"status": "error", "reason": "self_heal not loaded"}
    return _sh_report()


@mcp.tool()
def sentinel_auto_heal() -> dict:
    """Check all services and auto-restart anything that is down."""
    if not _self_heal_ok:
        return {"status": "error", "reason": "self_heal not loaded"}
    return _sh_heal()


@mcp.tool()
def sentinel_restart_api() -> dict:
    """Force restart saturn-api and verify it recovers."""
    if not _self_heal_ok:
        return {"status": "error", "reason": "self_heal not loaded"}
    return _sh_restart()


@mcp.tool()
def echo_linkedin_connection(
    contact_name: str,
    role: str,
    company: str,
    reason: str,
    sender_name: str = "Navin",
) -> dict:
    """Generate a LinkedIn connection request note under 280 chars."""
    if not _linkedin_ok:
        return {"status": "error", "reason": "linkedin skill not loaded"}
    try:
        prompt = _li_conn(sender_name, contact_name, role, company, reason)
        msg = call_llm(prompt, max_tokens=80).strip()
        if len(msg) > 300:
            msg = msg[:277] + "..."
        return {"status": "success", "message": msg, "chars": len(msg)}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def echo_linkedin_inmail(
    contact_name: str,
    role: str,
    company: str,
    pain_point: str,
    service: str,
    sender_name: str = "Navin",
) -> dict:
    """Generate a LinkedIn InMail cold message."""
    if not _linkedin_ok:
        return {"status": "error", "reason": "linkedin skill not loaded"}
    try:
        prompt = _li_inmail(sender_name, contact_name, role, company, pain_point, service)
        msg = call_llm(prompt, max_tokens=250).strip()
        return {"status": "success", "message": msg}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def hunter_score_profile(profile_text: str, niche: str) -> dict:
    """Score a LinkedIn profile as a prospect. Returns score 0-100 + priority."""
    if not _linkedin_ok:
        return {"status": "error", "reason": "linkedin skill not loaded"}
    try:
        prompt = _li_score(profile_text, niche)
        raw = call_llm(prompt, max_tokens=300)
        return _li_parse(raw)
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
def pulse_cost_today() -> dict:
    """Today's token usage and USD cost by agent vs daily budget."""
    if not _cost_monitor_ok:
        return {"status": "error", "reason": "cost monitor not loaded"}
    return _cm_today()


@mcp.tool()
def pulse_cost_monthly() -> dict:
    """This month's total token spend vs monthly budget."""
    if not _cost_monitor_ok:
        return {"status": "error", "reason": "cost monitor not loaded"}
    return _cm_monthly()


@mcp.tool()
def send_outreach(lead_id: int, draft_id: int) -> str:
    """Send outreach for a lead using stored draft content."""
    conn = db_conn()
    try:
        if not TOOLS_AVAILABLE:
            log_error(conn, "echo", "send_outreach", "LOGIC_ERROR", "Unified tools unavailable", "")
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "tools_unavailable"})

        lead = conn.execute("SELECT id, email, name FROM leads WHERE id=?", (lead_id,)).fetchone()
        if not lead:
            log_error(conn, "echo", "send_outreach", "DB_ERROR", "Lead not found", str(lead_id))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})

        draft = conn.execute(
            "SELECT id, draft_text FROM outreach_drafts WHERE id=? AND lead_id=?",
            (draft_id, lead_id),
        ).fetchone()
        if not draft:
            log_error(
                conn,
                "echo",
                "send_outreach",
                "DB_ERROR",
                "Draft not found for lead",
                f"lead_id={lead_id} draft_id={draft_id}",
            )
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "draft_not_found"})

        to_email = (lead["email"] or "").strip()
        if not to_email:
            log_error(conn, "echo", "send_outreach", "LOGIC_ERROR", "Lead email missing", str(lead_id))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "missing_email"})

        subject = f"Follow-up from SATURN for {lead['name'] or 'Lead'}"
        body = str(draft["draft_text"] or "")
        send_result = email_send_tool(to_email, subject, body, int(lead_id), int(draft_id))
        log_agent(
            conn,
            "Echo",
            "send_outreach",
            f"lead_id={lead_id} draft_id={draft_id} result={json.dumps(send_result)}",
            "success",
        )
        conn.commit()
        return json.dumps(send_result)
    except Exception as exc:
        log_error(conn, "echo", "send_outreach", "API_ERROR", "send_outreach failed", str(exc)[:500])
        conn.commit()
        return json.dumps({"status": "failed", "error_type": "API_ERROR"})
    finally:
        conn.close()

@mcp.tool()
def delete_task(task_id: int) -> str:
    """Permanently delete a task by ID"""
    conn = db()
    try:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
        return f"Task {task_id} deleted."
    finally:
        conn.close()


def log_token_usage(conn: sqlite3.Connection, agent: str, action: str, tokens_used: int) -> None:
    safe_agent = (agent or "saturn").strip().lower()
    safe_action = (action or "general").strip() or "general"
    safe_tokens = max(0, int(tokens_used or 0))
    today = datetime.date.today().isoformat()
    now = utc_now()
    conn.execute(
        """
        INSERT INTO token_usage_log (agent, action, tokens_used, log_date, logged_at)
        VALUES (?,?,?,?,?)
        """,
        (safe_agent, safe_action, safe_tokens, today, now),
    )
    total_agent_today = int(
        conn.execute(
            """
            SELECT COALESCE(SUM(tokens_used),0)
            FROM token_usage_log
            WHERE log_date=? AND lower(agent)=lower(?)
            """,
            (today, safe_agent),
        ).fetchone()[0]
        or 0
    )
    if total_agent_today > TOKEN_AGENT_DAILY_ALERT_LIMIT:
        existing_alert = conn.execute(
            """
            SELECT id FROM agent_log
            WHERE date(ts)=date('now')
              AND lower(agent)='saturn'
              AND lower(action)='token_usage_alert'
              AND lower(detail) LIKE lower(?)
            LIMIT 1
            """,
            (f"%agent={safe_agent}%",),
        ).fetchone()
        if not existing_alert:
            message = (
                f"⚠️ Token alert: agent {safe_agent} exceeded "
                f"{TOKEN_AGENT_DAILY_ALERT_LIMIT} tokens today ({total_agent_today})."
            )
            if create_system_alert_once(conn, "warning", f"token-usage:{safe_agent}", "RATE_LIMIT", message):
                send_telegram_message(message)
            log_agent(conn, "Saturn", "token_usage_alert", f"agent={safe_agent} tokens={total_agent_today}", "warning")


def extract_gemini_tokens(gemini_response: object | None, response_text: str = "") -> int:
    usage = getattr(gemini_response, "usage_metadata", None) if gemini_response is not None else None
    if usage is not None:
        total = getattr(usage, "total_token_count", None)
        if total is not None:
            try:
                return max(0, int(total))
            except (TypeError, ValueError):
                pass
    estimated = len(response_text or "") // 4
    return max(0, estimated)


def log_gemini_usage(conn: sqlite3.Connection, agent: str, action: str, gemini_response: object | None, response_text: str = "") -> int:
    tokens = extract_gemini_tokens(gemini_response, response_text)
    log_token_usage(conn, agent, action, tokens)
    increment_service_daily_usage(conn, "gemini")
    return tokens


@mcp.tool()
def log_tokens(
    tokens_used: int,
    model: str = "gemini-2.5-pro",
    agent: str = "saturn",
    action: str = "general",
) -> str:
    """Log token usage for cost tracking per agent/action."""
    conn = db()
    try:
        conn.execute(
            "INSERT INTO token_log (tokens, model, agent, action, logged_at) VALUES (?,?,?,?,?)",
            (tokens_used, model, agent.lower(), action, utc_now()),
        )
        log_token_usage(conn, agent, action, int(tokens_used or 0))
        total_today = int(
            conn.execute(
                "SELECT COALESCE(SUM(tokens),0) FROM token_log WHERE date(logged_at)=date('now')"
            ).fetchone()[0]
            or 0
        )
        if total_today >= TOKEN_WARNING_THRESHOLD:
            msg = f"Token usage warning: {total_today}/{TOKEN_DAILY_LIMIT} today"
            if create_system_alert_once(conn, "warning", "token-monitor", "RATE_LIMIT", msg):
                send_telegram_message(f"SATURN ALERT: {msg}")
        conn.commit()
        return f"Logged {tokens_used} tokens for {agent}/{action} ({model})"
    finally:
        conn.close()

@mcp.tool()
def token_usage_today() -> str:
    """Check token usage today vs configured daily limit."""
    conn = db()
    try:
        today = datetime.date.today().isoformat()
        rows = conn.execute(
            """
            SELECT SUM(tokens), model, COALESCE(agent,'saturn')
            FROM token_log
            WHERE date(logged_at)=?
            GROUP BY model, COALESCE(agent,'saturn')
            """,
            (today,)
        ).fetchall()
        total = sum(r[0] for r in rows) if rows else 0
        pct = round((total / TOKEN_DAILY_LIMIT) * 100, 1) if TOKEN_DAILY_LIMIT else 0
        status = "OK" if total < TOKEN_WARNING_THRESHOLD else "WARNING: approaching limit"
        result = f"Tokens today: {total:,} / {TOKEN_DAILY_LIMIT:,} ({pct}%) — {status}"
        for r in rows:
            result += f"\n  {r[2]}::{r[1]}: {r[0]:,}"
        agent_rows = conn.execute(
            """
            SELECT lower(agent) AS agent, SUM(tokens_used) AS total_tokens
            FROM token_usage_log
            WHERE log_date=?
            GROUP BY lower(agent), log_date
            ORDER BY total_tokens DESC
            """,
            (today,),
        ).fetchall()
        if agent_rows:
            result += "\nPer-agent token totals:"
            for row in agent_rows:
                result += f"\n  {row['agent']}: {int(row['total_tokens'] or 0):,}"
        return result
    finally:
        conn.close()

@mcp.tool()
def voice_alert(text: str = "", message: str = "") -> str:
    """Send a voice alert to Navin via Telegram. Use for high priority alerts, daily plan, and daily report only."""
    message = str(message or text or "").strip()
    if not message:
        return json.dumps({"status": "error", "reason": "missing_text"})
    _vkey = datetime.date.today().isoformat() + ":" + message[:50]
    if _vkey in _VOICE_SENT_TODAY:
        return json.dumps({"status": "skipped", "reason": "duplicate_voice_today"})
    _VOICE_SENT_TODAY.add(_vkey)
    audio_file = _speak(message)
    telegram_sent = _send_telegram_voice_note(audio_file)
    return json.dumps(
        {
            "status": "success" if telegram_sent else "failed",
            "message": message[:100],
            "voice": "meera",
            "audio_file": audio_file,
            "telegram_sent": bool(telegram_sent),
        }
    )

@mcp.tool()
async def add_lead(name: str, company: str = '', contact: str = '',
                   source: str = '', value: float = 0, notes: str = '', email: str = '') -> str:
    """Add a lead with daily cap, dedup, and email fallback safety."""
    conn = db_conn()
    cursor = conn.cursor()
    source_lc = (source or "").lower()
    website_norm = normalize_website(contact)
    try:
        if not website_norm:
            log_error(conn, "hunter", "lead_add", "LOGIC_ERROR", "Lead rejected: invalid website", contact)
            conn.commit()
            return json.dumps(
                {"status": "failed", "error_type": "LOGIC_ERROR", "reason": "invalid_website"}
            )

        existing = conn.execute(
            "SELECT id FROM leads WHERE website_norm=? OR contact=? LIMIT 1",
            (website_norm, contact),
        ).fetchone()
        if existing:
            return json.dumps({"status": "duplicate", "lead_id": int(existing[0]), "website": website_norm})

        insert_email: str | None = None
        insert_email_status = "missing"
        insert_email_source = "none"
        if "hunter" in source_lc:
            today_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM leads
                    WHERE date(created_at)=date('now') AND lower(source) LIKE '%hunter%'
                    """
                ).fetchone()[0]
                or 0
            )
            if today_count >= MAX_DAILY_LEADS:
                msg = f"Hunter daily lead cap reached ({MAX_DAILY_LEADS})"
                log_error(conn, "hunter", "lead_add", "RATE_LIMIT", msg, source)
                create_system_alert_once(conn, "warning", "hunter-leads", "RATE_LIMIT", msg)
                conn.commit()
                return json.dumps({"status": "blocked", "error_type": "RATE_LIMIT", "reason": "daily_cap"})
            insert_email_status = "not_found"
            insert_email_source = "none"
        else:
            resolved_email, email_status, email_source = resolve_email(email, notes, website_norm)
            insert_email = resolved_email or None
            insert_email_status = email_status
            insert_email_source = email_source

        cursor.execute(
            """
            INSERT INTO leads (
                name, company, contact, website, website_norm, source, value_estimate, notes,
                email, email_status, email_source, status, created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                name,
                company,
                contact,
                contact,
                website_norm,
                source,
                value,
                notes,
                insert_email,
                insert_email_status,
                insert_email_source,
                "new",
                utc_now(),
                utc_now(),
            ),
        )
        new_id = int(cursor.lastrowid)

        final_email_status = insert_email_status
        if "hunter" in source_lc:
            resolved_email, email_status, email_source = resolve_hunter_email(conn, email, notes, website_norm)
            final_email_status = email_status
            conn.execute(
                """
                UPDATE leads
                SET email=?, email_status=?, email_source=?, updated_at=?
                WHERE id=?
                """,
                (resolved_email, email_status, email_source, utc_now(), new_id),
            )
            if resolved_email:
                lead_for_draft = conn.execute(
                    "SELECT id, name, company FROM leads WHERE id=?",
                    (new_id,),
                ).fetchone()
                if not lead_for_draft:
                    log_error(conn, "echo", "draft_create", "DB_ERROR", "Linked lead does not exist", str(new_id))
                    conn.commit()
                    return json.dumps(
                        {
                            "status": "failed",
                            "error_type": "DB_ERROR",
                            "reason": "linked_lead_not_found",
                        }
                    )
                draft_text = build_echo_draft_text(lead_for_draft["name"] or "", lead_for_draft["company"] or "")
                draft_row = conn.execute(
                    """
                    INSERT INTO outreach_drafts (lead_id, draft_text, status, created_at, processed_at)
                    VALUES (?, ?, 'pending', ?, NULL)
                    """,
                    (new_id, draft_text, utc_now()),
                )
                log_agent(
                    conn,
                    "Echo",
                    "draft_created",
                    f"lead_id={new_id} draft_id={int(draft_row.lastrowid)}",
                    "success",
                )
            else:
                conn.execute(
                    "UPDATE leads SET email_status='not_found', updated_at=? WHERE id=?",
                    (utc_now(), new_id),
                )
                final_email_status = "not_found"
                log_agent(
                    conn,
                    "Hunter",
                    "lead_email_missing",
                    f"lead_id={new_id} website={website_norm}",
                    "warning",
                )

        log_agent(
            conn,
            "Hunter",
            "lead_added",
            f"lead_id={new_id} website={website_norm} email_status={final_email_status}",
            "success",
        )
        conn.commit()
        if _notion:
            try:
                _notion.sync_lead({
                    "saturn_id": f"lead_{new_id}",
                    "name": name,
                    "company": company,
                    "contact": contact,
                    "email": insert_email or email,
                    "website": contact,
                    "linkedin": "",
                    "industry": "",
                    "source": source,
                    "status": "New",
                    "lead_score": 0,
                    "email_status": final_email_status,
                })
            except Exception as _e:
                pass
        return json.dumps(
            {"status": "success", "lead_id": new_id, "website": website_norm, "email_status": final_email_status}
        )
    except sqlite3.IntegrityError as exc:
        if "ux_leads_website_norm" in str(exc):
            duplicate = conn.execute(
                "SELECT id FROM leads WHERE website_norm=? LIMIT 1", (website_norm,)
            ).fetchone()
            return json.dumps(
                {
                    "status": "duplicate",
                    "lead_id": int(duplicate[0]) if duplicate else None,
                    "website": website_norm,
                }
            )
        log_error(conn, "hunter", "lead_add", "DB_ERROR", "Lead insert failed", str(exc))
        conn.commit()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "insert_failed"})
    finally:
        conn.close()

@mcp.tool()
async def check_lead_exists(contact_url: str) -> str:
    """Check if a lead already exists by normalized website URL."""
    website_norm = normalize_website(contact_url)
    if not website_norm:
        return json.dumps({"exists": False, "website": None})
    conn = db_conn()
    row = conn.execute(
        "SELECT id FROM leads WHERE website_norm = ? OR contact = ? LIMIT 1",
        (website_norm, contact_url),
    ).fetchone()
    conn.close()
    return json.dumps({"exists": row is not None, "website": website_norm})


@mcp.tool()
async def hunter_build_query(niche: str, city: str, service: str, page: int = 1) -> str:
    """Build Hunter query using strict format niche + city + service with capped pagination."""
    conn = db_conn()
    try:
        niche = niche.strip()
        city = city.strip()
        service = service.strip()
        try:
            requested_page = int(page or 1)
        except (TypeError, ValueError):
            log_error(conn, "hunter", "build_query", "LOGIC_ERROR", "Invalid pagination page", str(page))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "invalid_page"})
        if not all([niche, city, service]):
            log_error(conn, "hunter", "build_query", "LOGIC_ERROR", "Invalid query parts", f"{niche}|{city}|{service}")
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "invalid_query_parts"})
        if requested_page != 1:
            log_error(conn, "hunter", "build_query", "LOGIC_ERROR", "Invalid pagination page", str(requested_page))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "invalid_page"})
        page = 1
        query = f"{niche} {city} {service}"
        return json.dumps(
            {"status": "ok", "query": query, "page": 1, "start": 0, "num": min(RESULTS_PER_PAGE, 10)}
        )
    finally:
        conn.close()


@mcp.tool()
async def hunter_quota_precheck(provider: str = "serpapi") -> str:
    """Stop Hunter safely when daily API call cap is reached."""
    conn = db_conn()
    try:
        if service_is_paused(conn, "hunter"):
            msg = "Hunter paused by quota monitor for today"
            log_error(conn, "hunter", "quota_precheck", "RATE_LIMIT", msg, provider)
            conn.commit()
            return json.dumps({"allowed": False, "error_type": "RATE_LIMIT", "message": msg})
        calls = hunter_api_calls_today(conn, provider)
        blocked = calls >= MAX_DAILY_LEADS
        if blocked:
            msg = f"Hunter paused for today: API calls {calls}/{MAX_DAILY_LEADS}"
            log_error(conn, "hunter", "quota_precheck", "RATE_LIMIT", msg, provider)
            if create_system_alert_once(conn, "warning", "hunter-quota", "RATE_LIMIT", msg):
                send_telegram_message(f"SATURN ALERT: {msg}. Auto-resume on next day.")
            conn.commit()
            return json.dumps({"allowed": False, "error_type": "RATE_LIMIT", "message": msg})
        return json.dumps({"allowed": True, "calls_today": calls, "limit": MAX_DAILY_LEADS})
    finally:
        conn.close()


@mcp.tool()
async def hunter_record_api_call(
    provider: str = "serpapi",
    endpoint: str = "search",
    status: str = "success",
    error_type: str = "",
    detail: str = "",
) -> str:
    """Record Hunter API usage and classify failures without retry loops."""
    conn = db_conn()
    try:
        safe_status = status.lower()
        safe_error = error_type if error_type in ERROR_TYPES else ""
        log_api_call(conn, "hunter", provider, endpoint, safe_status, safe_error, detail)
        if safe_status == "success":
            hunter_increment_daily_usage(conn)
        else:
            category = safe_error or "API_ERROR"
            if category not in ERROR_TYPES:
                category = "API_ERROR"
            log_error(conn, "hunter", "api_call", category, "Hunter API request failed", detail)
            if category == "RATE_LIMIT":
                msg = f"Hunter API rate limit reached on {provider}"
                if create_system_alert_once(conn, "warning", "hunter-api", "RATE_LIMIT", msg):
                    send_telegram_message(f"SATURN ALERT: {msg}")
        conn.commit()
        return json.dumps({"status": "logged", "provider": provider, "result": safe_status})
    finally:
        conn.close()


def classify_hunter_api_error(message: str, status_code: int | None = None) -> str:
    text = (message or "").lower()
    if status_code in (401, 403):
        return "AUTH_ERROR"
    if status_code == 429:
        return "RATE_LIMIT"
    if "auth" in text or "unauthorized" in text or "invalid api key" in text or "forbidden" in text:
        return "AUTH_ERROR"
    if "quota" in text or "rate limit" in text or "429" in text or "limit reached" in text:
        return "RATE_LIMIT"
    if "timed out" in text or "timeout" in text:
        return "NETWORK_ERROR"
    return "API_ERROR"


@mcp.tool()
async def hunter_extract_leads(
    niche: str = "",
    city: str = "",
    service: str = "",
    page: int = 1,
    provider: str = "serpapi",
    query: str = "",
    limit: int = 10,
) -> str:
    """Extract Hunter leads with strict caps, dedup, and failure-safe logging."""
    conn = db_conn()
    cursor = conn.cursor()
    leads_added = 0
    leads_skipped = 0
    errors = 0
    query_used = ""
    requested_limit = min(max(int(limit or RESULTS_PER_PAGE), 1), 10)

    def finish(status: str, payload: dict, result: str) -> str:
        detail = json.dumps(
            {
                "leads_added": leads_added,
                "leads_skipped": leads_skipped,
                "errors": errors,
                "query_used": query_used,
            }
        )
        log_agent(conn, "Hunter", "lead_extraction_run", detail, result)
        conn.commit()
        return json.dumps({"status": status, **payload})

    try:
        niche_clean = (niche or "").strip()
        city_clean = (city or "").strip()
        service_clean = (service or "").strip()
        query_clean = (query or "").strip()
        try:
            requested_page = int(page or 1)
        except (TypeError, ValueError):
            errors += 1
            log_error(conn, "hunter", "lead_extraction", "LOGIC_ERROR", "Invalid pagination page", str(page))
            return finish("failed", {"error_type": "LOGIC_ERROR", "reason": "invalid_page"}, "failed")
        if query_clean and not all([niche_clean, city_clean, service_clean]):
            query_used = query_clean
        elif not all([niche_clean, city_clean, service_clean]):
            errors += 1
            log_error(
                conn,
                "hunter",
                "lead_extraction",
                "LOGIC_ERROR",
                "Invalid query parts",
                f"{niche_clean}|{city_clean}|{service_clean}",
            )
            return finish("failed", {"error_type": "LOGIC_ERROR", "reason": "invalid_query_parts"}, "failed")
        if requested_page != 1:
            errors += 1
            log_error(conn, "hunter", "lead_extraction", "LOGIC_ERROR", "Invalid pagination page", str(requested_page))
            return finish("failed", {"error_type": "LOGIC_ERROR", "reason": "invalid_page"}, "failed")
        page = 1

        if not query_used:
            query_used = f"{niche_clean} {city_clean} {service_clean}"
        if service_is_paused(conn, "hunter"):
            errors += 1
            msg = "Hunter paused by quota monitor for today"
            log_error(conn, "hunter", "lead_extraction", "RATE_LIMIT", msg, query_used)
            return finish("blocked", {"error_type": "RATE_LIMIT", "reason": "paused_by_quota_monitor"}, "blocked")
        calls_today = hunter_api_calls_today(conn, provider)
        if calls_today >= MAX_DAILY_LEADS:
            errors += 1
            msg = f"Hunter daily API call cap reached ({calls_today}/{MAX_DAILY_LEADS})"
            log_error(conn, "hunter", "lead_extraction", "RATE_LIMIT", msg, query_used)
            if create_system_alert_once(conn, "warning", "hunter-quota", "RATE_LIMIT", msg):
                send_telegram_message(f"SATURN ALERT: {msg}. Extraction skipped.")
            return finish("blocked", {"error_type": "RATE_LIMIT", "reason": "daily_api_cap"}, "blocked")

        api_key = (os.environ.get("SERPAPI_KEY", "") or "").strip()
        if not api_key:
            errors += 1
            log_error(conn, "hunter", "lead_extraction", "AUTH_ERROR", "SERPAPI_KEY missing", "")
            return finish("failed", {"error_type": "AUTH_ERROR", "reason": "missing_api_key"}, "failed")

        params = urlencode(
            {
                "engine": "google",
                "q": query_used,
                "num": requested_limit,
                "start": 0,
                "api_key": api_key,
            }
        )
        url = f"https://serpapi.com/search.json?{params}"
        payload: dict | None = None
        api_error: tuple[str, str] | None = None
        attempts = 2
        for attempt in range(attempts):
            try:
                req = Request(url, headers={"User-Agent": "SATURN-Hunter/1.0"})
                with urlopen(req, timeout=20) as response:
                    body = response.read().decode("utf-8", errors="ignore")
                    payload = json.loads(body or "{}")
                break
            except HTTPError as exc:
                message = ""
                try:
                    message = exc.read().decode("utf-8", errors="ignore")
                except Exception as decode_exc:
                    log_error(
                        conn,
                        "hunter",
                        "lead_extraction",
                        "API_ERROR",
                        "Failed to decode hunter API error body",
                        str(decode_exc)[:500],
                    )
                    message = str(exc)
                category = classify_hunter_api_error(message, exc.code)
                api_error = (category, message[:500])
                if category in {"AUTH_ERROR", "RATE_LIMIT"}:
                    break
                if attempt >= attempts - 1:
                    break
            except TimeoutError as exc:
                category = "NETWORK_ERROR"
                api_error = (category, str(exc)[:500])
                if attempt >= attempts - 1:
                    break
            except URLError as exc:
                category = classify_hunter_api_error(str(exc))
                api_error = (category, str(exc)[:500])
                if category in {"AUTH_ERROR", "RATE_LIMIT"} or attempt >= attempts - 1:
                    break
            except Exception as exc:
                category = classify_hunter_api_error(str(exc))
                log_error(
                    conn,
                    "hunter",
                    "lead_extraction",
                    category if category in ERROR_TYPES else "API_ERROR",
                    "Hunter API request exception",
                    str(exc)[:500],
                )
                api_error = (category, str(exc)[:500])
                if attempt >= attempts - 1:
                    break

        if payload is None:
            errors += 1
            category, message = api_error or ("API_ERROR", "Hunter API request failed")
            log_api_call(conn, "hunter", provider, "search", "failed", category, message)
            log_error(conn, "hunter", "lead_extraction", category, "Hunter API request failed", message)
            if category == "RATE_LIMIT":
                if create_system_alert_once(conn, "warning", "hunter-api", "RATE_LIMIT", "Hunter API quota exceeded."):
                    send_telegram_message("SATURN ALERT: Hunter API quota exceeded.")
            return finish("failed", {"error_type": category, "reason": "api_request_failed"}, "failed")

        api_message = str(payload.get("error") or "").strip()
        if api_message:
            errors += 1
            category = classify_hunter_api_error(api_message)
            log_api_call(conn, "hunter", provider, "search", "failed", category, api_message[:500])
            log_error(conn, "hunter", "lead_extraction", category, "Hunter API returned error", api_message[:500])
            if category == "RATE_LIMIT":
                if create_system_alert_once(conn, "warning", "hunter-api", "RATE_LIMIT", "Hunter API quota exceeded."):
                    send_telegram_message("SATURN ALERT: Hunter API quota exceeded.")
            return finish("failed", {"error_type": category, "reason": "api_error_payload"}, "failed")

        log_api_call(conn, "hunter", provider, "search", "success", "", query_used)
        hunter_increment_daily_usage(conn)

        rows = payload.get("organic_results") or []
        if not isinstance(rows, list):
            rows = []
        if len(rows) == 0:
            log_agent(conn, "Hunter", "linkedin_fallback_attempt", query_used, "success")
            if _linkedin_available:
                linkedin_results = _linkedin_search(
                    niche=query_used,
                    city="India",
                    service=service_clean,
                    conn=conn,
                )
                if isinstance(linkedin_results, list) and linkedin_results:
                    log_agent(
                        conn,
                        "Hunter",
                        "linkedin_fallback_success",
                        f"{query_used} count={len(linkedin_results)}",
                        "success",
                    )
                    return finish(
                        "ok",
                        {
                            "query": query_used,
                            "page": 1,
                            "results_processed": len(linkedin_results),
                            "leads_added": 0,
                            "leads_skipped": 0,
                            "errors": errors,
                            "fallback": "linkedin",
                            "results": linkedin_results,
                        },
                        "success",
                    )
        for item in rows[:requested_limit]:
            contact = str(item.get("link") or "").strip()
            website_norm = normalize_website(contact)
            if not website_norm:
                errors += 1
                log_error(conn, "hunter", "lead_extraction", "LOGIC_ERROR", "Lead rejected: invalid website", contact)
                continue

            exists = conn.execute(
                "SELECT id FROM leads WHERE website_norm=? LIMIT 1",
                (website_norm,),
            ).fetchone()
            if exists:
                leads_skipped += 1
                continue

            title = str(item.get("title") or "").strip()
            snippet = str(item.get("snippet") or "").strip()
            if " - " in title:
                split_title = title.split(" - ", 1)
                lead_name = split_title[0].strip()
                lead_company = split_title[1].strip()
            else:
                lead_name = title or website_norm
                lead_company = ""

            resolved_email, email_status, email_source = resolve_hunter_email(
                conn, str(item.get("email") or ""), snippet, website_norm
            )

            try:
                cursor.execute(
                    """
                    INSERT INTO leads (
                        name, company, contact, website, website_norm, source, value_estimate, notes,
                        email, email_status, email_source, status, created_at, updated_at
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        lead_name,
                        lead_company,
                        contact,
                        contact,
                        website_norm,
                        "n8n-hunter-final",
                        0,
                        f"Found via SerpAPI. Snippet: {snippet}",
                        resolved_email,
                        email_status,
                        email_source,
                        "new",
                        utc_now(),
                        utc_now(),
                    ),
                )
            except sqlite3.IntegrityError:
                leads_skipped += 1
                continue
            except sqlite3.Error as exc:
                errors += 1
                log_error(conn, "hunter", "lead_extraction", "DB_ERROR", "Lead insert failed", str(exc)[:500])
                return finish("failed", {"error_type": "DB_ERROR", "reason": "insert_failed"}, "failed")
            leads_added += 1

        log_agent(conn, "Hunter", "lead_extraction_skip_count", json.dumps({"skipped": leads_skipped}), "success")
        return finish(
            "ok",
            {
                "query": query_used,
                "page": 1,
                "results_processed": min(len(rows), requested_limit),
                "leads_added": leads_added,
                "leads_skipped": leads_skipped,
                "errors": errors,
            },
            "success" if errors == 0 else "completed_with_errors",
        )
    except sqlite3.Error as exc:
        errors += 1
        log_error(conn, "hunter", "lead_extraction", "DB_ERROR", "DB write failed", str(exc)[:500])
        return finish("failed", {"error_type": "DB_ERROR", "reason": "db_failure"}, "failed")
    except Exception as exc:
        errors += 1
        category = classify_hunter_api_error(str(exc))
        if category not in {"API_ERROR", "AUTH_ERROR", "RATE_LIMIT", "NETWORK_ERROR"}:
            category = "API_ERROR"
        log_error(conn, "hunter", "lead_extraction", category, "Unhandled hunter extraction failure", str(exc)[:500])
        return finish("failed", {"error_type": category, "reason": "unexpected_failure"}, "failed")
    finally:
        conn.close()

@server.tool()
async def linkedin_search_leads(query: str, limit: int = 10) -> str:
    """Search LinkedIn for prospects and return lead candidates."""
    conn = db_conn()
    try:
        if not _linkedin_available:
            return json.dumps({"status": "failed", "error": "linkedin_search not available"})
        results = _linkedin_search(niche=query, city="India", service="", conn=conn)
        log_agent(conn, "Hunter", "linkedin_search", query, "success")
        conn.commit()
        return json.dumps({"status": "success", "results": results, "count": len(results)})
    except Exception as exc:
        log_error(conn, "hunter", "linkedin_search", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()


@mcp.tool()
async def hunter_linkedin_search(query: str, limit: int = 10) -> str:
    """
    Search LinkedIn for prospects matching query.
    Stores results in leads table. Used as fallback when SerpAPI quota is exceeded.
    """
    conn = db_conn()
    try:
        if not _linkedin_available:
            return json.dumps({"status": "error", "reason": "linkedin_search not available"})
        results = _linkedin_search(niche=query, city="India", service="", conn=conn)
        if not isinstance(results, list):
            results = []
        log_agent(conn, "Hunter", "linkedin_search", query, "success")
        conn.commit()
        return json.dumps({"status": "success", "results": results, "count": len(results)})
    except Exception as exc:
        log_error(conn, "hunter", "linkedin_search", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "error", "reason": str(exc)[:300]})
    finally:
        conn.close()

# Notion sync is direct API only. No LLM tokens are consumed here.
@server.tool()
async def notion_sync_report(report_data: str) -> str:
    """Sync daily report data to Notion database."""
    conn = db_conn()
    try:
        data = json.loads(report_data) if isinstance(report_data, str) else report_data
        if not _notion:
            return json.dumps({"status": "skipped", "reason": "notion_unavailable"})
        report = _notion.progress_report()
        result = {
            "hq_status_updated": _notion.notion_update_hq_status(
                date=datetime.datetime.now().strftime("%d %b %Y"),
                leads_total=report.get("leads", {}).get("total", 0),
                revenue_earned=report.get("revenue", {}).get("total_paid", 0.0),
                pending_approvals=report.get("outreach", {}).get("pending_approval", 0),
            ),
            "telegram_report": _notion.format_progress_report(report),
            "request": data,
        }
        log_agent(conn, "Pulse", "notion_sync_report", "daily_report", "success")
        conn.commit()
        return json.dumps({"status": "success", "result": result})
    except Exception as exc:
        log_error(conn, "pulse", "notion_sync_report", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()

@server.tool()
async def notion_sync_lead(lead_data: str) -> str:
    """Sync a lead record to Notion database."""
    conn = db_conn()
    try:
        data = json.loads(lead_data) if isinstance(lead_data, str) else lead_data
        if not _notion:
            return json.dumps({"status": "skipped", "reason": "notion_unavailable"})
        result = _notion.sync_lead(data)
        log_agent(conn, "Hunter", "notion_sync_lead", f"lead={data.get('name','?')}", "success")
        conn.commit()
        return json.dumps({"status": "success", "result": result})
    except Exception as exc:
        log_error(conn, "hunter", "notion_sync_lead", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()

@server.tool()
async def notion_create_task(task_data: str) -> str:
    """Create a task page in Notion."""
    conn = db_conn()
    try:
        data = json.loads(task_data) if isinstance(task_data, str) else task_data
        if not _notion:
            return json.dumps({"status": "skipped", "reason": "notion_unavailable"})
        result = _notion.sync_task(data)
        log_agent(conn, "Saturn", "notion_create_task", f"task={data.get('title','?')}", "success")
        conn.commit()
        return json.dumps({"status": "success", "result": result})
    except Exception as exc:
        log_error(conn, "saturn", "notion_create_task", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()

@server.tool()
async def notion_sync_alert(alert_data: str) -> str:
    """Sync an alert record to Notion database."""
    conn = db_conn()
    try:
        data = json.loads(alert_data) if isinstance(alert_data, str) else alert_data
        if not _notion:
            return json.dumps({"status": "skipped", "reason": "notion_unavailable"})
        result = _notion.raise_alert(
            agent=data.get("agent") or "Sentinel",
            title=data.get("title") or data.get("message") or f"Alert from {data.get('source', 'system')}",
            message=data.get("message") or data.get("detail") or "",
            level=str(data.get("level") or "Warning").title(),
            source=data.get("source") or "",
        )
        log_agent(conn, "Sentinel", "notion_sync_alert", f"source={data.get('source','?')}", "success")
        conn.commit()
        return json.dumps({"status": "success", "result": result})
    except Exception as exc:
        log_error(conn, "sentinel", "notion_sync_alert", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()

@server.tool()
async def notion_sync_revenue(revenue_data: str) -> str:
    """Sync a revenue record to Notion database."""
    conn = db_conn()
    try:
        data = json.loads(revenue_data) if isinstance(revenue_data, str) else revenue_data
        if not _notion:
            return json.dumps({"status": "skipped", "reason": "notion_unavailable"})
        result = _notion.sync_revenue(data)
        log_agent(conn, "Saturn", "notion_sync_revenue", f"client={data.get('client','?')}", "success")
        conn.commit()
        return json.dumps({"status": "success", "result": result})
    except Exception as exc:
        log_error(conn, "saturn", "notion_sync_revenue", "API_ERROR", str(exc)[:300])
        conn.commit()
        return json.dumps({"status": "failed", "error": str(exc)[:300]})
    finally:
        conn.close()

@mcp.tool()
async def trigger_echo_draft(lead_id: int) -> str:
    """Trigger Echo agent to draft an outreach message and send for approval"""
    conn = db_conn()
    lead = conn.execute(
        "SELECT name, company, notes, email, email_status FROM leads WHERE id = ?",
        (lead_id,),
    ).fetchone()
    if not lead:
        log_error(conn, "echo", "trigger_draft", "DB_ERROR", "Lead not found", str(lead_id))
        conn.commit()
        conn.close()
        return "Lead not found."

    # Echo Agent Logic (Simplified for direct response)
    name, company, notes, email, email_status = lead
    if email_status in {"missing", "not_found"} or not email:
        log_error(conn, "echo", "trigger_draft", "LOGIC_ERROR", "Lead has no reachable email", str(lead_id))
        conn.commit()
        conn.close()
        return f"Lead {lead_id} has no email. Draft skipped safely."
    message = build_echo_draft_text(name, company)

    draft = conn.execute(
        """
        INSERT INTO content_queue (content_type, title, body, platform, status, lead_id, created_at)
        VALUES ('outreach', ?, ?, 'email', 'pending', ?, ?)
        """,
        (f"{name} ({company})", message, lead_id, utc_now()),
    )
    draft_id = int(draft.lastrowid)
    if _notion:
        try:
            _notion.sync_draft({
                "saturn_id": f"draft_{lead_id}_{int(time.time())}",
                "name": f"Outreach to {lead_id}",
                "body": message or "",
                "status": "Pending Approval",
                "channel": "Email",
                "agent": "Echo",
            })
        except Exception as _e:
            pass

    # Telegram Approval Message
    env_vars = telegram_env()
    bot_token = env_vars.get("BOT_TOKEN")
    group_id = env_vars.get("GROUP_ID")
    thread_id = env_vars.get("THREAD_LEADS")

    if not all([bot_token, group_id, thread_id]):
        log_error(conn, "echo", "trigger_draft", "AUTH_ERROR", "Telegram credentials not found", "")
        conn.commit()
        conn.close()
        return f"Draft {draft_id} created, Telegram credentials not found."

    approval_text = (
        f"New Lead: {name} ({company})\n"
        f"Email: {email}\n"
        f"Draft ID: {draft_id}\n\n"
        f"**Draft Message:**\n_{message}_\n\n"
        f"Commands:\n"
        f"/approve {draft_id}\n"
        f"/reject {draft_id}\n"
        f"/edit {draft_id}"
    )

    keyboard = {
        "inline_keyboard": [
            [{"text": "Approve", "callback_data": f"approve_draft_{draft_id}"}],
            [{"text": "Reject", "callback_data": f"reject_draft_{draft_id}"}]
        ]
    }

    subprocess.run([
        "curl", "-s", "-X", "POST", f"https://api.telegram.org/bot{bot_token}/sendMessage",
        "-d", f"chat_id={group_id}",
        "-d", f"message_thread_id={thread_id}",
        "-d", "parse_mode=Markdown",
        "-d", f"text={approval_text}",
        "-d", f"reply_markup={json.dumps(keyboard)}"
    ])
    log_agent(conn, "Echo", "draft_created", f"lead_id={lead_id} draft_id={draft_id}", "success")
    conn.commit()
    conn.close()
    return f"Approval request sent for lead {lead_id}, draft {draft_id}"

@mcp.tool()
async def list_leads(status: str = 'all') -> str:
    """List leads, optionally filtered by status"""
    conn = db_conn()
    if status == 'all':
        rows = conn.execute('SELECT id, name, company, status, value_estimate FROM leads ORDER BY created_at DESC LIMIT 20').fetchall()
    else:
        rows = conn.execute('SELECT id, name, company, status, value_estimate FROM leads WHERE status=? ORDER BY created_at DESC', (status,)).fetchall()
    conn.close()
    if not rows: return 'No leads found.'
    return '\n'.join([f'[{r[0]}] {r[1]} | {r[2]} | {r[3]} | ${r[4]}' for r in rows])

@mcp.tool()
async def update_lead_status(
    lead_id: int, status: str, notes: str = '', manual_override: bool = False
) -> str:
    """Update lead status with strict transition rules and manual override support."""
    next_status = (status or "").strip().lower()
    conn = db_conn()
    valid_statuses = {"new", "contacted", "qualified", "proposal", "won", "lost"}
    if next_status not in valid_statuses:
        log_error(
            conn,
            "saturn",
            "update_lead_status",
            "LOGIC_ERROR",
            "Invalid lead status transition",
            f"{lead_id}: invalid_target->{next_status}",
        )
        conn.commit()
        conn.close()
        return json.dumps(
            {
                "status": "rejected",
                "reason": "invalid_transition",
                "from": None,
                "to": next_status,
            }
        )
    current_status = get_lead_status(conn, lead_id)
    if current_status == "unknown":
        log_error(conn, "saturn", "update_lead_status", "DB_ERROR", "Lead not found", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})
    row = conn.execute(
        "SELECT manual_override, follow_up_count FROM leads WHERE id=?",
        (lead_id,),
    ).fetchone()
    if not row:
        log_error(conn, "saturn", "update_lead_status", "DB_ERROR", "Lead not found", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})

    has_override = bool(row["manual_override"])
    next_override_value = int(row["manual_override"] or 0)
    if manual_override:
        next_override_value = 1
    follow_up_count = int(row["follow_up_count"] or 0)
    allowed, reason = validate_lead_transition(current_status, next_status, follow_up_count, has_override)
    if not allowed:
        log_error(
            conn,
            "saturn",
            "update_lead_status",
            "LOGIC_ERROR",
            "Invalid lead status transition",
            f"{lead_id}: {current_status}->{next_status}",
        )
        log_agent(
            conn,
            "Saturn",
            "lead_status_transition_rejected",
            f"{lead_id}: {current_status}->{next_status} ({reason})",
            "rejected",
        )
        conn.commit()
        conn.close()
        return json.dumps(
            {
                "status": "rejected",
                "reason": "invalid_transition",
                "from": current_status,
                "to": next_status,
                "detail": reason,
            }
        )

    now = utc_now()
    if has_override and current_status != next_status:
        log_agent(
            conn,
            "Saturn",
            "manual_override_transition",
            f"{lead_id}: {current_status}->{next_status}",
            "success",
        )
    follow_due = None
    if next_status == "contacted":
        follow_due = (datetime.datetime.utcnow() + datetime.timedelta(days=3)).replace(microsecond=0).isoformat()
        conn.execute(
            """
            UPDATE leads
            SET status=?, notes=?, last_contact=?, no_reply_since=?, last_outreach_at=?,
                follow_up_due_at=?, updated_at=?, manual_override=?
            WHERE id=?
            """,
            (next_status, notes, now, now, now, follow_due, now, next_override_value, lead_id),
        )
    elif next_status in ("won", "lost"):
        conn.execute(
            "UPDATE leads SET status=?, notes=?, follow_up_due_at=NULL, updated_at=?, manual_override=? WHERE id=?",
            (next_status, notes, now, next_override_value, lead_id),
        )
    else:
        conn.execute(
            "UPDATE leads SET status=?, notes=?, updated_at=?, manual_override=? WHERE id=?",
            (next_status, notes, now, next_override_value, lead_id),
        )
    log_agent(conn, "Saturn", "lead_status_update", f"{lead_id}: {current_status}->{next_status}", "success")
    conn.commit()
    try:
        lead_row = conn.execute(
            """
            SELECT id, name, company, contact, source, status, value_estimate, notes, last_contact, created_at,
                   website, website_norm, email, email_status, email_source, bounce_count, follow_up_count,
                   follow_up_due_at, no_reply_since, last_outreach_at, manual_override, updated_at
            FROM leads WHERE id=?
            """,
            (lead_id,),
        ).fetchone()
        if _notion and lead_row:
            _notion.sync_lead(dict(lead_row))
            _notion.log_agent_activity(
                agent="Saturn",
                action="lead_status_update",
                target=str(lead_id),
                status="Success",
                detail=f"{lead_id}: {current_status}->{next_status}",
            )
    except Exception:
        pass
    conn.close()
    return json.dumps({"status": "ok", "lead_id": lead_id, "from": current_status, "to": next_status})


def smtp_credentials_available() -> bool:
    host = os.environ.get("SMTP_HOST", "").strip()
    username = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASS", "").strip()
    sender = os.environ.get("SMTP_FROM", username).strip()
    return all([host, username, password, sender])


def gmail_credentials_available() -> bool:
    return bool(os.environ.get("GMAIL_ACCESS_TOKEN", "").strip())


def is_bounce_error(exc: Exception) -> bool:
    smtp_code = getattr(exc, "smtp_code", None)
    if isinstance(smtp_code, int) and 500 <= smtp_code < 600:
        return True
    text = str(exc).lower()
    bounce_markers = (
        "bounce",
        "user unknown",
        "mailbox unavailable",
        "recipient address rejected",
        "5.1.1",
        "550",
        "551",
        "552",
        "553",
        "554",
    )
    return any(marker in text for marker in bounce_markers)


def classify_send_error(exc: Exception) -> str:
    if is_bounce_error(exc):
        return "API_ERROR"
    text = str(exc).lower()
    smtp_code = getattr(exc, "smtp_code", None)
    if isinstance(smtp_code, int) and smtp_code in (534, 535):
        return "AUTH_ERROR"
    if "auth" in text or "invalid_grant" in text or "login" in text or "credential" in text or "unauthorized" in text:
        return "AUTH_ERROR"
    if "rate limit" in text or "429" in text or "quota" in text or "too many requests" in text:
        return "RATE_LIMIT"
    if "timed out" in text or "temporary failure" in text or "connection" in text or "network" in text:
        return "NETWORK_ERROR"
    return "API_ERROR"


def log_email_send_attempt(
    conn: sqlite3.Connection,
    lead_id: int,
    draft_id: int | None,
    status: str,
    attempt_count: int,
    error_category: str = "",
    sent_at: str | None = None,
) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO email_send_log (lead_id, draft_id, status, attempt_count, error_category, sent_at)
            VALUES (?,?,?,?,?,?)
            """,
            (lead_id, draft_id, status, attempt_count, error_category or None, sent_at or utc_now()),
        )
        return True
    except sqlite3.Error as exc:
        log_error(conn, "echo", "send_email", "DB_ERROR", "email_send_log write failed", str(exc)[:500])
        return False


def increment_service_daily_usage(conn: sqlite3.Connection, service: str) -> None:
    service_lc = (service or "").strip().lower()
    if not service_lc:
        return
    ensure_service_daily_counter_row(conn, service_lc)
    now = utc_now()
    conn.execute(
        """
        UPDATE api_usage_log
        SET call_count=COALESCE(call_count,0)+1, called_at=?
        WHERE service=? AND call_date=? AND endpoint='daily_counter'
        """,
        (now, service_lc, datetime.date.today().isoformat()),
    )


def smtp_send(to_email: str, subject: str, body: str) -> None:
    import smtplib

    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587"))
    username = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASS", "").strip()
    sender = os.environ.get("SMTP_FROM", username)
    if not all([host, username, password, sender]):
        raise RuntimeError("SMTP credentials are not configured")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        refused = smtp.send_message(msg)
        if refused:
            raise RuntimeError(f"bounce: smtp refused recipient {to_email}")


def gmail_api_send(to_email: str, subject: str, body: str) -> None:
    import base64

    access_token = os.environ.get("GMAIL_ACCESS_TOKEN", "").strip()
    sender = os.environ.get("GMAIL_SENDER", "me")
    if not access_token:
        raise RuntimeError("Gmail API access token missing")

    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    payload = json.dumps({"raw": raw}).encode("utf-8")
    req = Request(
        f"https://gmail.googleapis.com/gmail/v1/users/{sender}/messages/send",
        data=payload,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(req, timeout=20):
        pass


@mcp.tool()
async def send_outreach_email(
    lead_id: int,
    subject: str,
    body: str,
    provider: str = "smtp",
    to_email: str = "",
    draft_id: int = 0,
) -> str:
    """Send outreach email with 1 retry max, daily cap, and categorized failures."""
    conn = db_conn()
    try:
        lead = conn.execute(
            "SELECT email, status, email_status FROM leads WHERE id=?",
            (lead_id,),
        ).fetchone()
        if not lead:
            log_error(conn, "echo", "send_email", "DB_ERROR", "Lead not found", str(lead_id))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})

        lead_email = (lead["email"] or "").strip()
        lead_email_status = (lead["email_status"] or "").strip().lower()
        if not lead_email or lead_email_status in {"not_found", "bounced"}:
            log_error(
                conn,
                "echo",
                "send_email",
                "LOGIC_ERROR",
                "Send blocked by email guard",
                f"lead_id={lead_id} email_status={lead_email_status or 'null'}",
            )
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "guard_blocked"})

        recipient = (to_email or lead_email).strip()
        if not recipient:
            log_error(conn, "echo", "send_email", "LOGIC_ERROR", "No email for lead", str(lead_id))
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "missing_email"})

        sent_today = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM email_send_log
                WHERE date(sent_at)=date('now') AND lower(status)='sent'
                """
            ).fetchone()[0]
            or 0
        )
        if sent_today >= MAX_DAILY_EMAIL_SEND:
            msg = f"Daily send cap reached ({MAX_DAILY_EMAIL_SEND})"
            log_error(conn, "echo", "send_email", "RATE_LIMIT", msg, recipient)
            if create_system_alert_once(conn, "warning", "echo-email", "RATE_LIMIT", msg):
                send_telegram_message(f"SATURN ALERT: {msg}. Email send paused for today.")
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "RATE_LIMIT", "reason": "daily_cap"})

        if smtp_credentials_available():
            send_fn = smtp_send
            selected_provider = "smtp"
        elif gmail_credentials_available():
            send_fn = gmail_api_send
            selected_provider = "gmail_api"
        else:
            log_error(
                conn,
                "echo",
                "send_email",
                "AUTH_ERROR",
                "No email provider credentials available",
                f"requested_provider={provider}",
            )
            conn.commit()
            return json.dumps({"status": "failed", "error_type": "AUTH_ERROR", "reason": "provider_unavailable"})

        target_draft_id = draft_id if draft_id > 0 else None
        if target_draft_id is None:
            draft_row = conn.execute(
                """
                SELECT id FROM outreach_drafts
                WHERE lead_id=? AND lower(status) IN ('pending','approved')
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (lead_id,),
            ).fetchone()
            if draft_row:
                target_draft_id = int(draft_row["id"])

        attempts = 2
        for attempt in range(1, attempts + 1):
            try:
                send_fn(recipient, subject, body)
                now = utc_now()
                follow_due = (
                    datetime.datetime.utcnow() + datetime.timedelta(days=3)
                ).replace(microsecond=0).isoformat()
                conn.execute(
                    """
                    UPDATE leads
                    SET status=CASE WHEN status='new' THEN 'contacted' ELSE status END,
                        last_contact=?,
                        no_reply_since=COALESCE(no_reply_since, ?),
                        last_outreach_at=?,
                        follow_up_due_at=COALESCE(follow_up_due_at, ?),
                        updated_at=?
                    WHERE id=?
                    """,
                    (now, now, now, follow_due, now, lead_id),
                )
                if target_draft_id is not None:
                    conn.execute(
                        "UPDATE outreach_drafts SET status='sent', processed_at=? WHERE id=?",
                        (now, target_draft_id),
                    )
                if not log_email_send_attempt(conn, lead_id, target_draft_id, "sent", attempt, "", now):
                    conn.commit()
                    return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "email_log_write_fail"})
                log_agent(conn, "Echo", "email_sent", f"lead_id={lead_id} provider={selected_provider}", "success")
                increment_service_daily_usage(conn, "smtp")
                conn.commit()
                if _notion:
                    try:
                        lead_row = conn.execute(
                            """
                            SELECT id, name, company, contact, source, status, value_estimate, notes, last_contact, created_at,
                                   website, website_norm, email, email_status, email_source, bounce_count, follow_up_count,
                                   follow_up_due_at, no_reply_since, last_outreach_at, manual_override, updated_at
                            FROM leads WHERE id=?
                            """,
                            (lead_id,),
                        ).fetchone()
                        if lead_row:
                            _notion.sync_lead(dict(lead_row))
                        if target_draft_id is not None:
                            _notion.sync_draft(
                                {
                                    "saturn_id": f"draft_{lead_id}_{target_draft_id}",
                                    "name": subject or f"Outreach to {lead_id}",
                                    "body": body or "",
                                    "status": "Sent",
                                    "channel": "Email",
                                    "agent": "Echo",
                                }
                            )
                        _notion.log_agent_activity(
                            agent="Echo",
                            action="email_sent",
                            target=str(lead_id),
                            status="Success",
                            detail=f"lead_id={lead_id} provider={selected_provider}",
                        )
                    except Exception as _e:
                        pass
                return json.dumps(
                    {"status": "sent", "lead_id": lead_id, "provider": selected_provider, "attempts": attempt}
                )
            except Exception as exc:
                now = utc_now()
                category = classify_send_error(exc)
                if is_bounce_error(exc):
                    conn.execute(
                        """
                        UPDATE leads
                        SET email_status='bounced',
                            bounce_count=COALESCE(bounce_count,0)+1,
                            updated_at=?
                        WHERE id=?
                        """,
                        (now, lead_id),
                    )
                    if not log_email_send_attempt(
                        conn, lead_id, target_draft_id, "bounced", attempt, "API_ERROR", now
                    ):
                        conn.commit()
                        return json.dumps(
                            {"status": "failed", "error_type": "DB_ERROR", "reason": "email_log_write_fail"}
                        )
                    log_error(conn, "echo", "send_email", "API_ERROR", "Bounce detected", str(exc)[:500])
                    conn.commit()
                    if _notion:
                        try:
                            _notion.raise_alert(
                                agent="Echo",
                                title=f"Email Bounce: {recipient}",
                                message=f"Lead {lead_id} email bounced during send. Marked bounced in SQLite.",
                                level="Warning",
                                source="send_outreach_email",
                            )
                            lead_row = conn.execute(
                                """
                                SELECT id, name, company, contact, source, status, value_estimate, notes, last_contact, created_at,
                                       website, website_norm, email, email_status, email_source, bounce_count, follow_up_count,
                                       follow_up_due_at, no_reply_since, last_outreach_at, manual_override, updated_at
                                FROM leads WHERE id=?
                                """,
                                (lead_id,),
                            ).fetchone()
                            if lead_row:
                                _notion.sync_lead(dict(lead_row))
                        except Exception as _e:
                            pass
                    return json.dumps(
                        {"status": "bounced", "error_type": "API_ERROR", "reason": "bounce_detected"}
                    )

                if not log_email_send_attempt(conn, lead_id, target_draft_id, "failed", attempt, category, now):
                    conn.commit()
                    return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "email_log_write_fail"})
                if attempt < attempts:
                    time.sleep(5)
                    continue
                log_error(conn, "echo", "send_email", category, "Email send failed", str(exc)[:500])
                if category == "RATE_LIMIT":
                    if create_system_alert_once(
                        conn, "warning", "echo-email", "RATE_LIMIT", "Email provider rate limit reached"
                    ):
                        send_telegram_message("SATURN ALERT: Email provider rate limit reached.")
                conn.commit()
                return json.dumps({"status": "failed", "error_type": category, "reason": "send_failed"})
    except sqlite3.Error as exc:
        log_error(conn, "echo", "send_email", "DB_ERROR", "DB write fail", str(exc)[:500])
        conn.commit()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "db_write_fail"})
    finally:
        conn.close()


@mcp.tool()
async def record_email_bounce(lead_id: int, reason: str = "") -> str:
    """Handle bounce deterministically: mark lead bounced and stop outreach."""
    conn = db_conn()
    lead = conn.execute("SELECT id FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not lead:
        log_error(conn, "echo", "email_bounce", "DB_ERROR", "Lead not found", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})
    conn.execute(
        """
        UPDATE leads
        SET bounce_count=COALESCE(bounce_count,0)+1,
            email_status='bounced',
            status='lost',
            updated_at=?
        WHERE id=?
        """,
        (utc_now(), lead_id),
    )
    log_error(conn, "echo", "email_bounce", "API_ERROR", "Bounce received", reason[:500])
    log_agent(conn, "Echo", "email_bounced", f"lead_id={lead_id}", "handled")
    conn.commit()
    if _notion:
        try:
            lead_row = conn.execute(
                """
                SELECT id, name, company, contact, source, status, value_estimate, notes, last_contact, created_at,
                       website, website_norm, email, email_status, email_source, bounce_count, follow_up_count,
                       follow_up_due_at, no_reply_since, last_outreach_at, manual_override, updated_at
                FROM leads WHERE id=?
                """,
                (lead_id,),
            ).fetchone()
            if lead_row:
                _notion.sync_lead(dict(lead_row))
                _notion.raise_alert(
                    agent="Echo",
                    title=f"Email Bounce: {lead_row['email'] or lead_id}",
                    message=f"Lead {lead_id} email bounced. Marked lost.",
                    level="Warning",
                    source="record_email_bounce",
                )
        except Exception as _e:
            pass
    conn.close()
    return json.dumps({"status": "ok", "lead_id": lead_id, "result": "marked_lost"})


@mcp.tool()
async def list_due_followups(limit: int = 10) -> str:
    """List deterministic follow-up candidates by strict due rules only."""
    conn = db_conn()
    rows = conn.execute(
        """
        SELECT id, name, company, email, status, follow_up_count, follow_up_due_at, manual_override
        FROM leads
        WHERE status='contacted'
          AND COALESCE(follow_up_count,0) < 2
          AND COALESCE(manual_override,0)=0
          AND date(COALESCE(follow_up_due_at, created_at)) <= date('now')
          AND NOT EXISTS (
              SELECT 1
              FROM email_send_log es
              WHERE es.lead_id=leads.id
                AND lower(COALESCE(es.status,'')) IN ('inbound','reply_received','replied')
                AND date(COALESCE(es.sent_at, es.created_at)) >= date('now','-3 day')
          )
        ORDER BY date(COALESCE(follow_up_due_at, created_at)) ASC, id ASC
        LIMIT ?
        """,
        (max(1, min(limit, 50)),),
    ).fetchall()
    conn.close()
    return json.dumps([dict(row) for row in rows])


@mcp.tool()
async def send_follow_up(lead_id: int, approved: bool = False) -> str:
    """Create deterministic follow-up draft when strict due conditions are met."""
    conn = db_conn()
    row = conn.execute(
        """
        SELECT id, name, company, status, follow_up_count, follow_up_due_at, manual_override
        FROM leads
        WHERE id=?
        """,
        (lead_id,),
    ).fetchone()
    if not row:
        log_error(conn, "echo", "follow_up", "DB_ERROR", "Lead not found", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "lead_not_found"})

    follow_count = int(row["follow_up_count"] or 0)
    current_status = (row["status"] or "").lower()
    manual_override = int(row["manual_override"] or 0)
    if current_status == "contacted" and follow_count >= 2:
        now = utc_now()
        conn.execute(
            """
            UPDATE leads
            SET status='lost',
                updated_at=?
            WHERE id=? AND status='contacted'
            """,
            (now, lead_id),
        )
        log_agent(conn, "Hunter", "lead_closed_no_reply", f"lead_id={lead_id}", "success")
        conn.commit()
        conn.close()
        return json.dumps({"status": "closed", "lead_id": lead_id, "reason": "no_reply_max_followups"})

    if approved:
        log_error(
            conn,
            "echo",
            "follow_up",
            "LOGIC_ERROR",
            "Manual approve required after draft creation; do not send directly",
            str(lead_id),
        )
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "draft_only_flow"})

    if current_status != "contacted":
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Follow-up allowed only for contacted", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "invalid_status"})
    if follow_count >= 2:
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Max follow-ups reached", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "max_followups"})
    if manual_override != 0:
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Manual override blocks auto follow-up", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "manual_override_enabled"})

    follow_due_at = row["follow_up_due_at"]
    if not follow_due_at:
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Follow-up due date missing", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "missing_followup_due"})
    due_row = conn.execute(
        "SELECT date(?) <= date('now')",
        (follow_due_at,),
    ).fetchone()
    is_due = bool(due_row and int(due_row[0] or 0) == 1)
    if not is_due:
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Follow-up not due by date rule", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "not_due"})

    inbound_recent = conn.execute(
        """
        SELECT COUNT(*) FROM email_send_log
        WHERE lead_id=?
          AND lower(COALESCE(status,'')) IN ('inbound','reply_received','replied')
          AND date(COALESCE(sent_at, created_at)) >= date('now','-3 day')
        """,
        (lead_id,),
    ).fetchone()
    if int(inbound_recent[0] or 0) > 0:
        log_error(conn, "echo", "follow_up", "LOGIC_ERROR", "Recent inbound reply found", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "LOGIC_ERROR", "reason": "recent_inbound_reply"})

    now_dt = datetime.datetime.utcnow()
    now = now_dt.replace(microsecond=0).isoformat()
    due_next = (now_dt + datetime.timedelta(days=3)).replace(microsecond=0).isoformat()
    next_follow_count = follow_count + 1
    follow_number = "first" if follow_count == 0 else "second"
    first_name = (str(row["name"] or "").strip().split(" ")[0] or "there")
    company = str(row["company"] or "").strip()
    draft_text = (
        f"Hi {first_name}, just following up on my previous message"
        + (f" regarding {company}" if company else "")
        + ". Happy to share next steps if helpful."
    )

    linked_lead = conn.execute("SELECT id FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not linked_lead:
        log_error(conn, "echo", "follow_up", "DB_ERROR", "Linked lead does not exist", str(lead_id))
        conn.commit()
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "linked_lead_not_found"})

    draft_row = conn.execute(
        """
        INSERT INTO outreach_drafts (lead_id, draft_text, status, created_at, processed_at)
        VALUES (?, ?, 'pending', ?, NULL)
        """,
        (lead_id, draft_text, now),
    )
    draft_id = int(draft_row.lastrowid)
    conn.execute(
        """
        UPDATE leads
        SET follow_up_count=COALESCE(follow_up_count,0)+1,
            follow_up_due_at=?,
            updated_at=?
        WHERE id=?
        """,
        (due_next, now, lead_id),
    )
    log_agent(conn, "Echo", "follow_up_draft_created", f"lead_id={lead_id} draft_id={draft_id}", "success")
    conn.commit()
    conn.close()
    return json.dumps(
        {
            "status": "pending_approval",
            "lead_id": lead_id,
            "draft_id": draft_id,
            "follow_up_type": follow_number,
            "follow_up_count": next_follow_count,
            "next_follow_up_due_at": due_next,
            "requires_manual_approve": True,
        }
    )


@mcp.tool()
async def process_approval_command(command: str, text: str = "") -> str:
    """Process Telegram commands over outreach_drafts with strict validation order."""
    raw = (command or "").strip()
    parts = raw.split(maxsplit=2)
    if len(parts) < 2 or not parts[0].startswith("/"):
        return "Invalid command"

    action = parts[0][1:].strip().lower()
    if action not in {"approve", "reject", "edit"}:
        return "Invalid command"

    try:
        draft_id = int(parts[1].strip())
    except (TypeError, ValueError):
        return "Invalid draft ID"

    conn = db_conn()
    draft = conn.execute(
        """
        SELECT id, lead_id, draft_text, status
        FROM outreach_drafts
        WHERE id=?
        """,
        (draft_id,),
    ).fetchone()
    if not draft:
        conn.close()
        return "Draft not found"

    if (draft["status"] or "").strip().lower() != "pending":
        conn.close()
        return "Draft already processed"

    lead_row = conn.execute(
        "SELECT id, name FROM leads WHERE id=?",
        (int(draft["lead_id"] or 0),),
    ).fetchone()
    if not lead_row:
        conn.close()
        return "Linked lead not found"

    lead_id = int(lead_row["id"])
    lead_name = (lead_row["name"] or "Lead").strip() or "Lead"
    draft_text = str(draft["draft_text"] or "").strip()

    if action == "approve":
        conn.close()
        send_result_raw = await send_outreach_email(
            lead_id=lead_id,
            subject=f"Follow-up from SATURN",
            body=draft_text,
            draft_id=draft_id,
        )
        send_status = ""
        try:
            send_payload = json.loads(send_result_raw)
            send_status = str(send_payload.get("status") or "").lower()
        except Exception as exc:
            conn_log = db_conn()
            log_error(
                conn_log,
                "echo",
                "approval_command",
                "LOGIC_ERROR",
                "Email send response parse failed",
                str(exc)[:500],
            )
            conn_log.commit()
            conn_log.close()
            send_status = ""
        if send_status != "sent":
            return f"Send failed for draft {draft_id}"
        conn2 = db_conn()
        processed_at = utc_now()
        conn2.execute(
            "UPDATE outreach_drafts SET status='sent', processed_at=? WHERE id=?",
            (processed_at, draft_id),
        )
        log_agent(conn2, "Echo", "approval_approved", f"draft_id={draft_id}", "success")
        if _notion:
            try:
                _notion.sync_draft(
                    {
                        "saturn_id": f"draft_{lead_id}_{draft_id}",
                        "name": f"Outreach to {lead_id}",
                        "body": draft_text or "",
                        "status": "Sent",
                        "channel": "Email",
                        "agent": "Echo",
                    }
                )
            except Exception as _e:
                pass
        conn2.commit()
        conn2.close()
        return f"✅ Draft {draft_id} approved and sent to {lead_name}"

    if action == "reject":
        processed_at = utc_now()
        conn.execute(
            "UPDATE outreach_drafts SET status='rejected', processed_at=? WHERE id=?",
            (processed_at, draft_id),
        )
        log_agent(conn, "Echo", "approval_rejected", f"draft_id={draft_id}", "success")
        if _notion:
            try:
                _notion.sync_draft(
                    {
                        "saturn_id": f"draft_{lead_id}_{draft_id}",
                        "name": f"Outreach to {lead_id}",
                        "body": draft_text or "",
                        "status": "Rejected",
                        "channel": "Email",
                        "agent": "Echo",
                    }
                )
            except Exception as _e:
                pass
        conn.commit()
        conn.close()
        return f"❌ Draft {draft_id} rejected"

    new_text = ""
    if len(parts) >= 3:
        new_text = parts[2].strip()
    if not new_text:
        new_text = (text or "").strip()
    if not new_text:
        conn.close()
        return "Invalid command"

    conn.execute(
        "UPDATE outreach_drafts SET draft_text=?, processed_at=NULL WHERE id=?",
        (new_text, draft_id),
    )
    log_agent(conn, "Echo", "approval_edited", f"draft_id={draft_id}", "success")
    if _notion:
        try:
            _notion.sync_draft(
                {
                    "saturn_id": f"draft_{lead_id}_{draft_id}",
                    "name": f"Outreach to {lead_id}",
                    "body": new_text or "",
                    "status": "Pending Approval",
                    "channel": "Email",
                    "agent": "Echo",
                }
            )
        except Exception as _e:
            pass
    conn.commit()
    conn.close()
    return f"✏️ Draft {draft_id} updated. Use /approve {draft_id} to send."

@mcp.tool()
async def log_revenue(client: str, service: str, amount: float,
                      status: str = 'pending', notes: str = '') -> str:
    """Log a revenue entry"""
    conn = db_conn()
    cursor = conn.execute(
        'INSERT INTO revenue (client, service, amount, status, notes, invoice_date)'
        ' VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)',
        (client, service, amount, status, notes),
    )
    revenue_id = int(cursor.lastrowid)
    conn.commit()
    if _notion:
        try:
            _notion.sync_revenue({
                "saturn_id": f"rev_{client}_{int(time.time())}",
                "client": client,
                "service": service,
                "amount": amount,
                "status": status,
                "source": "",
                "notes": notes,
            })
        except Exception as _e:
            pass
    conn.close()
    return f'Revenue logged: {client} | {service} | ${amount}'

@mcp.tool()
def create_deal(
    lead_id: int,
    title: str,
    value: float,
    stage: str = "Discovery",
    notes: str = "",
) -> dict:
    """Create a deal using the existing revenue table as the durable record."""
    allowed = ["Discovery", "Proposal", "Negotiation", "Won", "Lost"]
    if stage not in allowed:
        return {"status": "error", "reason": f"stage must be one of {allowed}"}

    conn = db()
    try:
        cursor = conn.execute(
            """
            INSERT INTO revenue (client, service, amount, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            """,
            (f"lead_{lead_id}", title, value, stage, notes),
        )
        deal_id = int(cursor.lastrowid)
        conn.execute(
            """
            UPDATE leads
            SET status='qualified', updated_at=datetime('now')
            WHERE id=? AND status IN ('new','contacted','scored')
            """,
            (lead_id,),
        )
        log_agent(conn, "Saturn", "create_deal", f"lead_{lead_id} title={title} value={value}", "success")
        conn.commit()
        if _notion:
            try:
                _notion.sync_revenue(
                    {
                        "saturn_id": f"deal_{deal_id}",
                        "client": f"lead_{lead_id}",
                        "service": title,
                        "amount": value,
                        "status": stage,
                        "source": "create_deal",
                        "notes": notes,
                    }
                )
            except Exception:
                pass
        return {
            "status": "success",
            "deal_id": deal_id,
            "title": title,
            "value": value,
            "stage": stage,
            "lead_id": lead_id,
        }
    except Exception as e:
        conn.rollback()
        return {"status": "error", "reason": str(e)}
    finally:
        conn.close()


@mcp.tool()
def create_project(
    deal_id: int,
    title: str,
    client_name: str,
    service_type: str,
    notes: str = "",
) -> dict:
    """Create a project entry in work_log and a setup task in tasks."""
    allowed = ["automation_setup", "retainer", "maintenance", "custom"]
    if service_type not in allowed:
        return {"status": "error", "reason": f"service_type must be one of {allowed}"}

    conn = db()
    try:
        entry = f"Project:{title}|Client:{client_name}|Type:{service_type}|Deal:{deal_id}|{notes}"
        cursor = conn.execute(
            """
            INSERT INTO work_log (entry, category, logged_at)
            VALUES (?, ?, datetime('now'))
            """,
            (entry, "project"),
        )
        project_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO tasks (title, priority, status, created_at, updated_at)
            VALUES (?, ?, ?, datetime('now'), datetime('now'))
            """,
            (f"[SETUP] {title}", "high", "pending"),
        )
        log_agent(conn, "Forge", "create_project", f"deal_{deal_id} title={title} client={client_name}", "success")
        conn.commit()
        if _notion:
            try:
                _notion.log_agent_activity(
                    "Forge",
                    "create_project",
                    f"deal_{deal_id}",
                    "success",
                    f"Project:{title}|Client:{client_name}|Service:{service_type}",
                )
            except Exception:
                pass
        return {
            "status": "success",
            "project_id": project_id,
            "title": title,
            "client": client_name,
            "service_type": service_type,
            "deal_id": deal_id,
        }
    except Exception as e:
        conn.rollback()
        return {"status": "error", "reason": str(e)}
    finally:
        conn.close()


@mcp.tool()
async def resolve_alert(alert_id: int) -> str:
    """Resolve an alert and sync to Notion."""
    conn = db_conn()
    row = conn.execute(
        "SELECT id, level, source, message, error_type, alert_type, resolved, created_at FROM system_alerts WHERE id=?",
        (alert_id,),
    ).fetchone()
    if not row:
        conn.close()
        return json.dumps({"status": "failed", "error_type": "DB_ERROR", "reason": "alert_not_found"})
    conn.execute("UPDATE system_alerts SET resolved=1 WHERE id=?", (alert_id,))
    conn.commit()
    try:
        alert_dict = {
            "id": int(row["id"]),
            "level": row["level"],
            "source": row["source"],
            "message": row["message"],
            "error_type": row["error_type"],
            "alert_type": row["alert_type"],
            "created_at": row["created_at"],
            "resolved": 1,
        }
        await notion_sync_alert(json.dumps(alert_dict))
    except Exception:
        pass
    conn.close()
    return json.dumps({"status": "ok", "alert_id": alert_id, "resolved": True})

@mcp.tool()
async def revenue_summary() -> str:
    """Get total and monthly revenue summary"""
    conn = db_conn()
    total = conn.execute('SELECT SUM(amount) FROM revenue WHERE status="paid"').fetchone()[0] or 0
    month = conn.execute('SELECT SUM(amount) FROM revenue WHERE status="paid" AND strftime(\'%Y-%m\', paid_date)=strftime(\'%Y-%m\',\'now\')').fetchone()[0] or 0
    pending = conn.execute('SELECT SUM(amount) FROM revenue WHERE status="pending"').fetchone()[0] or 0
    conn.close()
    return f'Total Earned: ${total:.2f} | This Month: ${month:.2f} | Pending: ${pending:.2f}'

@mcp.tool()
async def log_agent_action(agent: str, action: str, detail: str = '', result: str = '') -> str:
    """Log an agent's action for audit trail"""
    conn = db_conn()
    log_agent(conn, agent, action, detail, result or "success")
    conn.commit()
    if _notion:
        try:
            _notion.log_agent_activity(
                agent=agent,
                action=action,
                target="",
                status=result or "Success",
                detail=detail or "",
            )
        except Exception as _e:
            pass
    conn.close()
    return f'Logged: {agent} | {action}'


@mcp.tool()
async def log_agent_error(agent: str, action: str, error_type: str, message: str, detail: str = "") -> str:
    """Structured categorized error logging shared by all agents."""
    conn = db_conn()
    log_error(conn, agent, action, error_type, message, detail)
    conn.commit()
    conn.close()
    return json.dumps({"status": "logged", "agent": agent, "action": action, "error_type": error_type})

@mcp.tool()
async def save_daily_plan(plan_text: str, focus: str = '') -> str:
    """Save today's daily plan"""
    import datetime
    today = datetime.date.today().isoformat()
    conn = db_conn()
    conn.execute('INSERT OR REPLACE INTO daily_plans (plan_date, plan_text, focus) VALUES (?,?,?)',
                 (today, plan_text, focus))
    conn.commit(); conn.close()
    return f'Plan saved for {today}'

@mcp.tool()
async def system_health() -> str:
    """Check system health: n8n, disk, memory"""
    import subprocess, shutil
    results = []
    conn = db_conn()
    # Memory
    mem = subprocess.run(['free', '-h'], capture_output=True, text=True)
    mem_line = [l for l in mem.stdout.split('\n') if 'Mem' in l]
    results.append('RAM: ' + (mem_line[0] if mem_line else 'unknown'))
    # Disk
    total, used, free = shutil.disk_usage('/')
    results.append(f'Disk free: {free//1073741824:.1f}GB')
    # n8n
    import urllib.request
    try:
        urllib.request.urlopen('http://localhost:5678/healthz', timeout=3)
        results.append('n8n: OK')
    except Exception as exc:
        log_error(conn, "sentinel", "system_health", "NETWORK_ERROR", "n8n health check failed", str(exc)[:500])
        results.append('n8n: OFFLINE')
    token_rows = conn.execute(
        """
        SELECT lower(agent) AS agent, SUM(tokens_used) AS total_tokens
        FROM token_usage_log
        WHERE log_date=date('now')
        GROUP BY lower(agent), log_date
        ORDER BY total_tokens DESC
        """
    ).fetchall()
    conn.close()
    if token_rows:
        token_summary = ", ".join([f"{row['agent']}={int(row['total_tokens'] or 0)}" for row in token_rows])
        results.append(f"Tokens(today): {token_summary}")
    return ' | '.join(results)

if __name__ == "__main__":
    mcp.run(transport="stdio")
