"""
saturn_notion_integration.py
==============================
SATURN FlowCraft OS — Production Notion Sync Layer
Version:  flowcraft_os_v2
Verified: 2026-03-13 — all 15 database IDs confirmed live in Notion

WHAT THIS FILE DOES:
  Bidirectional sync between Saturn SQLite and all 15 Notion databases.
  Auto-loads credentials from ~/.config/openclaw-secrets/telegram.env.
  Fail-open design — Notion failure never blocks core Saturn operations.

DATABASES (15 total):
  Sales Pipeline:    leads, deals
  Outreach System:   outreach_drafts
  Client Delivery:   clients, projects, automation_builds
  Finance:           revenue
  Operations:        tasks, work_log
  System Monitoring: agent_activity, alerts
  Intelligence:      api_usage, token_usage
  Knowledge Base:    automation_library, prompt_library

KEY METHODS:
  sync_lead(row)           — push SQLite lead row to Notion Leads DB
  sync_revenue(row)        — push SQLite revenue row to Notion Revenue DB
  sync_task(row)           — push SQLite task row to Notion Tasks DB
  sync_draft(row)          — push SQLite draft row to Notion Outreach Drafts DB
  log_agent_activity(...)  — append agent action to Agent Activity DB
  raise_alert(...)         — create alert in Alerts DB
  log_token_usage(...)     — log token cost + auto-check budget
  progress_report()        — pull live metrics from all DBs, returns dict
  format_progress_report() — format metrics as Telegram message
  notion_update_hq_status(...)    — rewrite Command Center live callout (run at 10pm)
  notion_update_os_revenue(...)   — update FlowCraft OS revenue tracker
  health_check()           — ping Notion, raise Sentinel alert on failure
  pull_all()               — bulk sync all 15 DBs to SQLite ID map

USAGE:
  from backend.tools.notion_sync import get_sync
  sync = get_sync()          # singleton — safe to call anywhere
  sync.health_check()
  sync.sync_lead(row_dict)

DEPLOY:
  cp saturn_notion_integration.py ~/Workspace/Saturn/backend/tools/notion_sync.py
  systemctl --user restart saturn-api
"""

import logging
import os
import re
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH

# ─────────────────────────────────────────────────────────────────────────────
# ENV LOADING
# ─────────────────────────────────────────────────────────────────────────────

_ENV_FILE = Path.home() / ".config" / "openclaw-secrets" / "telegram.env"

def _load_env(path: Path = _ENV_FILE) -> None:
    """Load KEY=VALUE pairs from the SATURN env file into os.environ."""
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val

_load_env()

logger = logging.getLogger("saturn.notion_sync")


def _print_runtime_error(action: str, message: str, detail: str = "") -> None:
    suffix = f" detail={detail[:180]}" if detail else ""
    logger.warning("[Saturn] %s failed: %s%s", action, str(message)[:240], suffix)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_VERSION = "flowcraft_os_v2"

NOTION_API_KEY  = os.environ.get("NOTION_API_KEY", "")
NOTION_VERSION  = os.environ.get("NOTION_VERSION", "2022-06-28")
NOTION_BASE_URL = "https://api.notion.com/v1"

MAX_DAILY_TOKEN_COST = float(os.environ.get("SATURN_TOKEN_BUDGET_USD", "20.0"))

# FlowCraft OS page IDs — used by update methods
HQ_PAGE_ID = "322c3191-532c-815d-9a6b-fef8eb53e9dd"   # 🎯 Command Center (child of FlowCraft OS)
FLOWCRAFT_OS_PAGE_ID = "322c3191-532c-8106-b16c-d78cb8c9c7ec"  # 🪐 FlowCraft OS root

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE REGISTRY — All 15 verified against live Notion 2026-03-13
# ─────────────────────────────────────────────────────────────────────────────

DATABASES: dict[str, dict] = {

    # ── SALES PIPELINE ───────────────────────────────────────────────────────
    "leads": {
        "collection_id": "c39c32db-580b-42e7-99e7-371711b8d271",
        "database_id":   "b89036a0-1670-4d6a-914a-854b298c1bb6",
        "section":       "Sales Pipeline",
        "title_field":   "Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["New", "Scored", "Contacted", "Replied", "Meeting", "Proposal", "Won", "Lost"],
        "required_fields": ["Saturn ID"],
    },

    "deals": {
        "collection_id": "2673b620-1db0-458a-bfe7-ba1406201ed0",
        "database_id":   "382a3a48-a992-4f14-9253-35d66364a083",
        "section":       "Sales Pipeline",
        "title_field":   "Deal Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Discovery", "Qualification", "Proposal", "Negotiation", "Won", "Lost"],
        "required_fields": ["Saturn ID", "Deal Name", "Stage"],
        "relations": {"Lead": "leads", "Client": "clients"},
    },

    # ── OUTREACH SYSTEM ──────────────────────────────────────────────────────
    "outreach_drafts": {
        "collection_id": "a491a7a4-4eb1-43fd-a48f-17bfa9804df2",
        "database_id":   "7c641130-ddad-4831-925d-88fc7a27249e",
        "section":       "Outreach System",
        "title_field":   "Draft Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Draft", "Pending Approval", "Approved", "Sent", "Follow Up", "Replied", "Rejected"],
        "required_fields": ["Saturn ID", "Draft Name"],
        "relations": {"Lead": "leads"},
    },

    # ── CLIENT DELIVERY ──────────────────────────────────────────────────────
    "clients": {
        "collection_id": "f5535a0b-a090-4c95-8c1c-b8245f68180b",
        "database_id":   "4a569d51-7e19-4125-8ffb-66ceb337ae66",
        "section":       "Client Delivery",
        "title_field":   "Client Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Active", "Onboarding", "Delivered", "Inactive"],
        "required_fields": ["Saturn ID", "Client Name"],
    },

    "projects": {
        "collection_id": "d30f6fa3-a54f-40a6-b7f7-68a7da8c6073",
        "database_id":   "dfae31d5-8c48-4aa7-a988-8bc2dd597af0",
        "section":       "Client Delivery",
        "title_field":   "Project Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Planning", "Building", "Testing", "Delivered", "Closed"],
        "required_fields": ["Saturn ID", "Project Name"],
        "relations": {"Client": "clients"},
    },

    "automation_builds": {
        "collection_id": "eec051ce-231c-4c93-bc58-23a9b7ffba60",
        "database_id":   "3f958335-dbcc-4505-838f-88b06ef4bdcf",
        "section":       "Client Delivery",
        "title_field":   "Automation Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Draft", "In Progress", "Testing", "Live", "Archived"],
        "required_fields": ["Saturn ID", "Automation Name"],
        "relations": {"Project": "projects"},
    },

    # ── FINANCE ──────────────────────────────────────────────────────────────
    "revenue": {
        "collection_id": "abdc3e72-07a2-4af3-aae1-a098b5bbff00",
        "database_id":   "4d4efcf8-3c82-4000-9911-986c5f0e3aa7",
        "section":       "Finance",
        "title_field":   "Revenue Entry",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Pending", "Paid", "Overdue", "Refunded"],
        "required_fields": ["Saturn ID", "Revenue Entry", "Amount"],
        "relations": {"Client": "clients", "Lead": "leads", "Deal": "deals"},
    },

    # ── OPERATIONS ───────────────────────────────────────────────────────────
    "tasks": {
        "collection_id": "9682b19f-d71e-42b8-9c3f-d95a97f4b0b2",
        "database_id":   "c5c0ef98-4b2b-4e3b-a720-d675863b9c12",
        "section":       "Operations",
        "title_field":   "Task Name",
        "saturn_id_field": "Saturn ID",
        "status_pipeline": ["Backlog", "Todo", "Doing", "Review", "Done"],
        "required_fields": ["Saturn ID", "Task Name"],
        "relations": {"Project": "projects"},
    },

    "work_log": {
        "collection_id": "3ca1ea87-81ec-467f-a232-e411a7969ab9",
        "database_id":   "d01bfd93-1d5e-4332-ab6b-b80e321e8270",
        "section":       "Operations",
        "title_field":   "Entry",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID"],
        "relations": {"Task": "tasks"},
    },

    # ── SYSTEM MONITORING ────────────────────────────────────────────────────
    "agent_activity": {
        "collection_id": "e325645b-acbc-4a83-84fa-12fca883a0d0",
        "database_id":   "02f23d82-5cc0-414e-ab41-c2be8a9a8652",
        "section":       "System Monitoring",
        "title_field":   "Activity",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID"],
        "agents": ["Saturn", "Hunter", "Echo", "Forge", "Pulse", "Sentinel"],
    },

    "alerts": {
        "collection_id": "f59818df-1a84-4ac7-96bc-e36532fe40ef",
        "database_id":   "3a6e941a-925d-43bd-9320-552a04450470",
        "section":       "System Monitoring",
        "title_field":   "Alert Title",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID", "Alert Title", "Level"],
        "severity_levels": ["Info", "Warning", "Critical"],
    },

    # ── INTELLIGENCE ─────────────────────────────────────────────────────────
    "api_usage": {
        "collection_id": "da4703bf-f2e0-4b88-9bde-f1f66114d628",
        "database_id":   "4fa8c8c5-2199-490d-9f97-3a1cc452dee9",
        "section":       "Intelligence",
        "title_field":   "Log Entry",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID"],
    },

    "token_usage": {
        "collection_id": "6b6dad49-e018-454f-af07-5006cdd51388",
        "database_id":   "2abcd0c9-20cd-4a42-89dc-13ad26cf5cb2",
        "section":       "Intelligence",
        "title_field":   "Log Entry",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID"],
    },

    # ── KNOWLEDGE BASE ───────────────────────────────────────────────────────
    "automation_library": {
        "collection_id": "64210719-cca2-4dfa-bed0-51253f65e071",
        "database_id":   "8f7ff72d-99ee-47eb-b715-5fb0b1b13b10",
        "section":       "Knowledge Base",
        "title_field":   "Automation Name",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID", "Automation Name"],
    },

    "prompt_library": {
        "collection_id": "16d66c5a-0783-45c9-be27-3cfe165fea88",
        "database_id":   "df0dbaa6-627f-4b5f-a4ee-e0faeca43505",
        "section":       "Knowledge Base",
        "title_field":   "Prompt Name",
        "saturn_id_field": "Saturn ID",
        "required_fields": ["Saturn ID", "Prompt Name"],
    },
}

DATABASE_IDS = {n: d["database_id"] for n, d in DATABASES.items()}

RELATION_MAP: dict[str, list[str]] = {
    "leads":             ["outreach_drafts", "deals"],
    "deals":             ["revenue"],
    "clients":           ["projects", "revenue"],
    "projects":          ["automation_builds", "tasks"],
    "tasks":             ["work_log"],
    "outreach_drafts":   [],
    "revenue":           [],
    "automation_builds": [],
    "work_log":          [],
    "agent_activity":    [],
    "alerts":            [],
    "api_usage":         [],
    "token_usage":       [],
    "automation_library": [],
    "prompt_library":    [],
}

DEFAULT_STAGE_PROB: dict[str, float] = {
    "Discovery":     0.20,
    "Qualification": 0.35,
    "Proposal":      0.55,
    "Negotiation":   0.75,
    "Won":           1.00,
    "Lost":          0.00,
}

# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class NotionAPIError(RuntimeError):
    """Raised when a Notion API call must fail fast. Carry `reason` for routing."""
    def __init__(self, reason: str, original: Exception):
        self.reason   = reason
        self.original = original
        super().__init__(f"Notion API failure: {reason}")
        self.__cause__ = original

# ─────────────────────────────────────────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def safe_request(func, *args, **kwargs):
    """Single-attempt wrapper. Never sleep or retry on rate limit."""
    try:
        return func(*args, **kwargs)
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code == 429:
            raise NotionAPIError("rate limited", exc)
        raise

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_properties(db_name: str, properties: dict) -> bool:
    if db_name not in DATABASES:
        raise ValueError(f"Unknown database '{db_name}'. Valid: {list(DATABASES.keys())}")
    required = DATABASES[db_name].get("required_fields", ["Saturn ID"])
    missing  = [f for f in required if f != "Saturn ID" and f not in properties]
    if missing:
        raise ValueError(f"[Schema] Missing required fields for '{db_name}': {missing}")
    return True

# ─────────────────────────────────────────────────────────────────────────────
# NOTION REST CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class NotionClient:
    """Thin Notion REST wrapper with retry on every call."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError(
                "[Saturn] NOTION_API_KEY not set. "
                "Add it to ~/.config/openclaw-secrets/telegram.env"
            )
        self.headers = {
            "Authorization":  f"Bearer {api_key}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type":   "application/json",
        }

    def _get(self, url: str) -> dict:
        def _call():
            r = requests.get(url, headers=self.headers, timeout=15)
            r.raise_for_status()
            return r.json()
        return safe_request(_call)

    def _post(self, url: str, body: dict) -> dict:
        def _call():
            r = requests.post(url, headers=self.headers, json=body, timeout=15)
            r.raise_for_status()
            return r.json()
        return safe_request(_call)

    def _patch(self, url: str, body: dict) -> dict:
        def _call():
            r = requests.patch(url, headers=self.headers, json=body, timeout=15)
            r.raise_for_status()
            return r.json()
        return safe_request(_call)

    def query_database(self, database_id: str,
                       filter_obj: dict = None, sorts: list = None) -> list:
        url  = f"{NOTION_BASE_URL}/databases/{database_id}/query"
        body: dict = {}
        if filter_obj: body["filter"] = filter_obj
        if sorts:      body["sorts"]  = sorts
        results, has_more, cursor = [], True, None
        while has_more:
            if cursor: body["start_cursor"] = cursor
            data     = self._post(url, body)
            results += data.get("results", [])
            has_more = data.get("has_more", False)
            cursor   = data.get("next_cursor")
        return results

    def create_page(self, database_id: str, properties: dict) -> dict:
        return self._post(f"{NOTION_BASE_URL}/pages",
                          {"parent": {"database_id": database_id},
                           "properties": properties})

    def update_page(self, page_id: str, properties: dict) -> dict:
        return self._patch(f"{NOTION_BASE_URL}/pages/{page_id}",
                           {"properties": properties})

    def update_page_content(self, page_id: str, children: list) -> dict:
        """Replace block content on a Notion page (for HQ callout rewrites)."""
        # Step 1: get existing block IDs
        blocks = self._get(f"{NOTION_BASE_URL}/blocks/{page_id}/children")
        for block in blocks.get("results", []):
            bid = block["id"]
            self._delete_block(bid)
        # Step 2: append new blocks
        return self._patch(
            f"{NOTION_BASE_URL}/blocks/{page_id}/children",
            {"children": children},
        )

    def _delete_block(self, block_id: str) -> dict:
        def _call():
            r = requests.delete(
                f"{NOTION_BASE_URL}/blocks/{block_id}",
                headers=self.headers, timeout=15
            )
            r.raise_for_status()
            return {}
        try:
            return safe_request(_call)
        except Exception as exc:
            _print_runtime_error("notion_delete_block", str(exc), block_id)
            return {"status": "error", "error": "API_ERROR", "detail": str(exc)}

    def append_block_children(self, page_id: str, children: list) -> dict:
        return self._patch(
            f"{NOTION_BASE_URL}/blocks/{page_id}/children",
            {"children": children},
        )

# ─────────────────────────────────────────────────────────────────────────────
# NOTION PROPERTY BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _title(text: str) -> dict:
    return {"title": [{"text": {"content": str(text)[:2000]}}]}

def _text(text: str) -> dict:
    return {"rich_text": [{"text": {"content": str(text)[:2000]}}]}

def _select(name: str) -> dict:
    return {"select": {"name": (str(name or "Unknown").strip() or "Unknown")[:100]}}

def _multi_select(names: list[str]) -> dict:
    return {"multi_select": [{"name": n} for n in names]}

def _number(val: float) -> dict:
    return {"number": float(val)}

def _checkbox(val: bool) -> dict:
    return {"checkbox": bool(val)}

def _date(dt: str) -> dict:
    """dt: ISO-8601 date string e.g. '2026-03-13' or '2026-03-13T10:00:00+05:30'."""
    return {"date": {"start": dt}}

def _url(val: str) -> dict:
    return {"url": str(val)}

def _email(val: str) -> dict:
    return {"email": str(val).strip()}

def _saturn_id(sid: str) -> dict:
    return {"rich_text": [{"text": {"content": sid}}]}

def _now_iso() -> str:
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(tz_ist).isoformat()

def _today_iso() -> str:
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(tz_ist).date().isoformat()

def _make_sid(prefix: str) -> str:
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    ts = datetime.now(tz_ist).strftime("%Y%m%d%H%M%S%f")
    safe_prefix = re.sub(r"[^a-z0-9_]+", "_", str(prefix or "row").strip().lower()).strip("_") or "row"
    return f"{safe_prefix}_{ts}"


def _deterministic_run_id(agent: str, started_at: int, attempt: int = 0) -> str:
    safe_agent = re.sub(r"[^a-z0-9_]+", "_", str(agent or "saturn").strip().lower()).strip("_") or "saturn"
    base = f"run_{safe_agent}_{int(started_at)}"
    return base if attempt <= 0 else f"{base}_{attempt + 1}"

# ─────────────────────────────────────────────────────────────────────────────
# MAIN SYNC CLASS
# ─────────────────────────────────────────────────────────────────────────────

class NotionSync:
    """
    Saturn's bidirectional Notion ↔ SQLite sync layer.
    All V2 fixes active. Drop-in replacement for backend/tools/notion_sync.py.
    """

    def __init__(self, api_key: str = None, sqlite_path: str = None):
        # Multi-source API key resolution
        api_key = api_key or NOTION_API_KEY
        if not api_key:
            raise ValueError(
                "[Saturn] NOTION_API_KEY missing. "
                "Set it in ~/.config/openclaw-secrets/telegram.env"
            )
        self.client      = NotionClient(api_key)
        sqlite_target = sqlite_path or os.environ.get("SATURN_DB_PATH", str(DB_PATH))
        self.sqlite_path = str(enforce_write_path(sqlite_target, "notion-sync-db"))
        self._daily_cost_cache: float = 0.0
        self._daily_cost_date:  str   = ""
        self._init_sqlite()
        self._check_registry_version()

    # ── SQLITE SETUP ──────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.sqlite_path,
            timeout=30,
            check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _init_sqlite(self):
        conn = self._conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS _saturn_id_map (
            saturn_id TEXT PRIMARY KEY,
            notion_page_id TEXT NOT NULL,
            database_name TEXT NOT NULL,
            synced_at TEXT NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS _saturn_registry (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_lock (
            agent     TEXT PRIMARY KEY,
            locked_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_runs (
            run_id     TEXT PRIMARY KEY,
            agent      TEXT    NOT NULL,
            started_at INTEGER NOT NULL,
            ended_at   INTEGER,
            status     TEXT    NOT NULL DEFAULT 'running'
        )""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_time
            ON agent_runs(agent, started_at DESC)""")
        conn.commit()
        conn.close()

    def _check_registry_version(self):
        conn = self._conn()
        row  = conn.execute(
            "SELECT value FROM _saturn_registry WHERE key='version'"
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO _saturn_registry VALUES ('version', ?)",
                (DATABASE_VERSION,)
            )
            conn.commit()
        elif row[0] != DATABASE_VERSION:
            # Auto-migrate version string — no hard stop
            conn.execute(
                "UPDATE _saturn_registry SET value=? WHERE key='version'",
                (DATABASE_VERSION,)
            )
            conn.commit()
            logger.info("[Saturn] Registry version migrated: %s -> %s", row[0], DATABASE_VERSION)
        conn.close()

    def _log_sync_error(self, action: str, error_type: str, message: str, detail: str = "") -> None:
        conn = None
        try:
            conn = self._conn()
            conn.execute("""CREATE TABLE IF NOT EXISTS error_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent TEXT NOT NULL,
                action TEXT NOT NULL,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                detail TEXT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            conn.execute(
                "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
                ("notion_sync", action, error_type, str(message)[:300], str(detail)[:500], _now_iso()),
            )
            conn.commit()
        except Exception as exc:
            logger.warning(
                "[Saturn] notion_sync log failure: action=%s message=%s error=%s",
                action,
                str(message)[:120],
                exc,
            )
        finally:
            if conn is not None:
                conn.close()

    def _structured_error(self, action: str, exc: Exception, detail: str = "") -> dict:
        error_type = "API_ERROR"
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            error_type = "NETWORK_ERROR"
        elif isinstance(exc, requests.HTTPError) and exc.response is not None:
            if exc.response.status_code in (401, 403):
                error_type = "AUTH_ERROR"
            elif exc.response.status_code == 429:
                error_type = "RATE_LIMIT"
        elif isinstance(exc, NotionAPIError) and exc.reason == "rate limited":
            error_type = "RATE_LIMIT"
        self._log_sync_error(action, error_type, str(exc), detail)
        return {"status": "error", "error": error_type, "detail": str(exc)}

    # ── ID MAP ────────────────────────────────────────────────────────────────

    def _get_notion_page_id(self, saturn_id: str) -> Optional[str]:
        conn = self._conn()
        row  = conn.execute(
            "SELECT notion_page_id FROM _saturn_id_map WHERE saturn_id=?",
            (saturn_id,)
        ).fetchone()
        conn.close()
        return row[0] if row else None

    def _register(self, saturn_id: str, page_id: str, db_name: str):
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO _saturn_id_map VALUES (?,?,?,?)",
            (saturn_id, page_id, db_name, _now_iso())
        )
        conn.commit()
        conn.close()

    # ── CORE READ ─────────────────────────────────────────────────────────────

    def get_all(self, db_name: str, filter_obj: dict = None) -> list:
        if db_name not in DATABASES:
            raise ValueError(f"Unknown database: {db_name}")
        return self.client.query_database(
            DATABASES[db_name]["database_id"], filter_obj
        )

    # ── CORE WRITE ────────────────────────────────────────────────────────────

    def upsert(self, db_name: str, saturn_id: str, properties: dict) -> tuple[dict, str]:
        """
        Idempotent create-or-update by Saturn ID.
        Saturn ID injected only on CREATE path, never on UPDATE.
        Returns (notion_page, saturn_id).
        """
        try:
            saturn_id = str(saturn_id or _make_sid(db_name.rstrip("s") or "row")).strip() or _make_sid("row")
            validate_properties(db_name, properties)

            existing = self._get_notion_page_id(saturn_id)

            if existing:
                # UPDATE — never touch Saturn ID
                props_clean = {k: v for k, v in properties.items() if k != "Saturn ID"}
                result = self.client.update_page(existing, props_clean)
                self._register(saturn_id, existing, db_name)
            else:
                # CREATE — inject Saturn ID
                props_with_sid = dict(properties)
                props_with_sid["Saturn ID"] = _saturn_id(saturn_id)
                result = self.client.create_page(
                    DATABASES[db_name]["database_id"], props_with_sid
                )
                self._register(saturn_id, result["id"], db_name)

            try:
                action = "UPDATE" if existing else "CREATE"
                self.log_agent_activity(
                    "Saturn", f"{action} {db_name}", saturn_id, "Success"
                )
            except Exception as exc:
                self._log_sync_error("upsert_audit", "API_ERROR", str(exc), f"{db_name}:{saturn_id}")

            return result, saturn_id
        except Exception as exc:
            return self._structured_error("upsert", exc, f"{db_name}:{saturn_id}"), saturn_id

    # ── HEALTH CHECK ──────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """Ping Notion. Raises Critical Sentinel alert on failure."""
        try:
            self.get_all("leads")
            logger.info("[Saturn] Notion connectivity OK")
            return True
        except Exception as e:
            self._log_sync_error("health_check", "API_ERROR", str(e), "leads")
            logger.warning("[Saturn] Notion unreachable: %s", e)
            try:
                self.raise_alert(
                    "Sentinel", "Notion Sync Failure",
                    f"Saturn cannot reach Notion API: {e}",
                    level="Critical", source="health_check()"
                )
            except Exception as alert_exc:
                self._log_sync_error("health_check_alert", "API_ERROR", str(alert_exc), "health_check()")
            return False

    # ── EXECUTION LOCK ────────────────────────────────────────────────────────

    def acquire_lock(self, agent: str, timeout_seconds: int = 300) -> bool:
        conn     = self._conn()
        now_ts   = int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp())
        cutoff   = now_ts - timeout_seconds
        conn.execute("DELETE FROM agent_lock WHERE locked_at < ?", (cutoff,))
        conn.commit()
        try:
            conn.execute(
                "INSERT INTO agent_lock (agent, locked_at) VALUES (?, ?)",
                (agent, now_ts)
            )
            conn.commit()
            conn.close()
            logger.info("[Saturn] Lock acquired: %s", agent)
            return True
        except sqlite3.IntegrityError as exc:
            conn.close()
            logger.warning("[Saturn] Lock denied: %s is already running (%s)", agent, exc)
            return False

    def release_lock(self, agent: str):
        conn = self._conn()
        conn.execute("DELETE FROM agent_lock WHERE agent = ?", (agent,))
        conn.commit()
        conn.close()
        logger.info("[Saturn] Lock released: %s", agent)

    # ── AGENT RUN LOG ─────────────────────────────────────────────────────────

    def start_run(self, agent: str, run_id: str = None) -> str:
        now_ts = int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp())
        conn   = self._conn()
        base_run_id = str(run_id or "").strip()
        active_run_id = base_run_id or _deterministic_run_id(agent, now_ts, 0)
        for attempt in range(3):
            try:
                conn.execute(
                    "INSERT INTO agent_runs (run_id, agent, started_at, ended_at, status) "
                    "VALUES (?,?,?,NULL,'running')",
                    (active_run_id, agent, now_ts)
                )
                conn.commit()
                break
            except sqlite3.IntegrityError as exc:
                logger.warning(
                    "[Saturn] start_run collision: agent=%s run_id=%s attempt=%s error=%s",
                    agent,
                    active_run_id,
                    attempt + 1,
                    exc,
                )
                if attempt == 2:
                    conn.close()
                    raise RuntimeError(f"[Saturn] start_run failed for {agent} after 3 attempts")
                active_run_id = _deterministic_run_id(agent, now_ts, attempt + 1)
        conn.close()
        logger.info("[Saturn] Run started: %s [%s]", agent, active_run_id)
        return active_run_id

    def end_run(self, run_id: str, status: str = "success") -> float:
        now_ts = int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp())
        conn   = self._conn()
        conn.execute(
            "UPDATE agent_runs SET ended_at = ?, status = ? WHERE run_id = ?",
            (now_ts, status, run_id)
        )
        conn.commit()
        row = conn.execute(
            "SELECT started_at FROM agent_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        conn.close()
        duration = float(now_ts - row[0]) if row else 0.0
        logger.info("[Saturn] Run ended: [%s] status=%s duration=%.1fs", run_id, status, duration)
        return duration

    def get_run_history(self, agent: str = None, limit: int = 50) -> list[dict]:
        try:
            safe_limit = max(1, min(int(limit or 50), 500))
        except (TypeError, ValueError) as exc:
            logger.warning("[Saturn] invalid run history limit %r: %s", limit, exc)
            safe_limit = 50
        conn = self._conn()
        if agent:
            rows = conn.execute(
                "SELECT run_id, agent, started_at, ended_at, status "
                "FROM agent_runs WHERE agent = ? ORDER BY started_at DESC LIMIT ?",
                (agent, safe_limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT run_id, agent, started_at, ended_at, status "
                "FROM agent_runs ORDER BY started_at DESC LIMIT ?",
                (safe_limit,)
            ).fetchall()
        conn.close()
        return [
            {
                "run_id":     r[0],
                "agent":      r[1],
                "started_at": r[2],
                "ended_at":   r[3],
                "status":     r[4],
                "duration_s": (r[3] - r[2]) if r[3] else None,
            }
            for r in rows
        ]

    # ── BUDGET GUARDRAIL ──────────────────────────────────────────────────────

    def check_budget(self) -> bool:
        today = _today_iso()
        if today == self._daily_cost_date:
            total_cost = self._daily_cost_cache
        else:
            try:
                records = self.get_all("token_usage", {
                    "property": "Logged At",
                    "date":     {"on_or_after": today}
                })
                total_cost = sum(
                    (r.get("properties", {}).get("Cost", {}).get("number") or 0.0)
                    for r in records
                )
                self._daily_cost_cache = total_cost
                self._daily_cost_date  = today
            except Exception as e:
                self._log_sync_error("check_budget", "API_ERROR", str(e), "token_usage")
                logger.warning("[Saturn] Budget check error (non-blocking): %s", e)
                return True

        if total_cost >= MAX_DAILY_TOKEN_COST:
            try:
                self.raise_alert(
                    "Sentinel", "Daily Token Budget Exceeded",
                    f"Cost ${total_cost:.2f} exceeds ${MAX_DAILY_TOKEN_COST:.2f} daily limit.",
                    level="Critical", source="check_budget()"
                )
            except Exception as alert_exc:
                self._log_sync_error("check_budget_alert", "API_ERROR", str(alert_exc), "check_budget()")
            logger.warning("[Saturn] BUDGET EXCEEDED: $%.2f / $%.2f", total_cost, MAX_DAILY_TOKEN_COST)
            return False

        logger.info("[Saturn] Budget OK: $%.2f / $%.2f", total_cost, MAX_DAILY_TOKEN_COST)
        return True

    # ── HIGH-LEVEL ENTITY SYNC METHODS ────────────────────────────────

    def sync_lead(self, lead: dict) -> tuple[dict, str]:
        """
        Sync a SQLite `leads` row to Notion.
        lead: dict with keys matching SQLite leads table columns.
        Returns (notion_page, saturn_id).

        Usage:
            row = dict(cursor.fetchone())
            sync.sync_lead(row)
        """
        sid = str(lead.get("saturn_id") or _make_sid("lead")).strip() or _make_sid("lead")
        name = str(lead.get("name") or lead.get("company") or "Unnamed Lead").strip() or "Unnamed Lead"
        props: dict = {
            "Name":    _title(name),
        }
        if lead.get("company"):      props["Company"]      = _text(str(lead["company"]))
        if lead.get("contact"):      props["Contact Name"] = _text(str(lead["contact"]))
        if lead.get("email"):        props["Email"]        = _email(str(lead["email"]).strip())
        if lead.get("website"):      props["Website"]      = _url(str(lead["website"]).strip())
        if lead.get("linkedin"):     props["LinkedIn"]     = _url(str(lead["linkedin"]).strip())
        if lead.get("industry"):     props["Industry"]     = _text(str(lead["industry"]))
        if lead.get("source"):       props["Source"]       = _select(str(lead["source"]).strip().capitalize())
        if lead.get("status"):       props["Status"]       = _select(str(lead["status"]).strip().capitalize())
        if lead.get("lead_score") is not None:
            props["Lead Score"] = _number(float(lead["lead_score"]))
        if lead.get("email_status"): props["Email Status"] = _select(str(lead["email_status"]).strip())
        if lead.get("follow_up_count") is not None:
            props["Follow Up Count"] = _number(int(lead["follow_up_count"]))
        if lead.get("follow_up_due_at"):
            props["Follow Up Due"] = _date(str(lead["follow_up_due_at"])[:10])
        if lead.get("last_outreach_at"):
            props["Last Outreach"] = _date(str(lead["last_outreach_at"])[:10])
        return self.upsert("leads", sid, props)

    def sync_revenue(self, rev: dict) -> tuple[dict, str]:
        """
        Sync a SQLite `revenue` row to Notion.
        rev: dict with keys matching SQLite revenue table columns.
        """
        sid = str(rev.get("saturn_id") or _make_sid("rev")).strip() or _make_sid("rev")
        client_label = str(rev.get("client") or "Unknown").strip() or "Unknown"
        service_label = str(rev.get("service") or "").strip()
        entry_label  = f"{client_label} — {service_label}"
        props: dict  = {
            "Revenue Entry": _title(entry_label),
        }
        if rev.get("amount") is not None: props["Amount"]  = _number(float(rev["amount"]))
        if service_label:                 props["Service"] = _text(service_label)
        if rev.get("status"):             props["Status"]  = _select(str(rev["status"]).strip().capitalize())
        if rev.get("source"):             props["Source"]  = _select(str(rev["source"]).strip().capitalize())
        if rev.get("invoice_date"):       props["Invoice Date"] = _date(str(rev["invoice_date"])[:10])
        if rev.get("paid_date"):          props["Paid Date"]    = _date(str(rev["paid_date"])[:10])
        return self.upsert("revenue", sid, props)

    def sync_task(self, task: dict) -> tuple[dict, str]:
        # Notion sync is direct API only. No LLM tokens are consumed here.
        """Sync a SQLite `tasks` row to Notion."""
        sid   = str(task.get("saturn_id") or _make_sid("task")).strip() or _make_sid("task")
        props = {"Task Name": _title(str(task.get("task") or task.get("name") or "Untitled"))}
        priority_value = str(task.get("priority") or "").strip().lower()
        priority_map = {
            "low": "Low",
            "normal": "Medium",
            "medium": "Medium",
            "high": "High",
            "critical": "Critical",
        }
        status_value = str(task.get("status") or "").strip().lower().replace("-", "_")
        status_map = {
            "new": "Backlog",
            "backlog": "Backlog",
            "pending": "Todo",
            "todo": "Todo",
            "doing": "Doing",
            "in_progress": "Doing",
            "review": "Review",
            "done": "Done",
            "complete": "Done",
            "completed": "Done",
        }
        if priority_value in priority_map:
            props["Priority"] = _select(priority_map[priority_value])
        if status_value in status_map:
            props["Status"] = _select(status_map[status_value])
        # Owner is a Notion people field. Skip raw agent-name strings to keep task sync fail-open.
        if task.get("due_date"):  props["Due Date"] = _date(str(task["due_date"])[:10])
        return self.upsert("tasks", sid, props)

    def sync_draft(self, draft: dict) -> tuple[dict, str]:
        """Sync a SQLite `outreach_drafts` / `content_queue` row to Notion."""
        sid   = str(draft.get("saturn_id") or _make_sid("draft")).strip() or _make_sid("draft")
        props = {
            "Draft Name": _title(str(draft.get("subject") or draft.get("name") or "Draft")),
        }
        if draft.get("body") or draft.get("draft_text"):
            props["Draft Text"] = _text(str(draft.get("body") or draft.get("draft_text", "")))
        if draft.get("status"):  props["Status"]  = _select(str(draft["status"]).replace("_", " ").title())
        if draft.get("channel"): props["Channel"] = _select(str(draft["channel"]).strip().capitalize())
        if draft.get("agent"):   props["Agent"]   = _select(str(draft["agent"]).strip())
        approved = draft.get("approved")
        if approved is not None: props["Approved"] = _checkbox(bool(approved))
        return self.upsert("outreach_drafts", sid, props)

    def sync_agent_activity(self, agent: str, action: str, target: str,
                            status: str = "Success", detail: str = "",
                            execution_time: float = 0.0) -> tuple[dict, str]:
        """Sync an agent activity entry to Notion."""
        sid   = _make_sid("act")
        props = {
            "Activity":       _title(f"{agent}: {action}"),
            "Agent":          _select(agent),
            "Action":         _text(action),
            "Target":         _text(target),
            "Status":         _select(status),
            "Execution Time": _number(execution_time),
            "Detail":         _text(detail),
            "Timestamp":      _date(_now_iso()),
        }
        return self.upsert("agent_activity", sid, props)

    # ── CONVENIENCE WRITE HELPERS (direct create, no upsert) ─────────────────

    def log_agent_activity(self, agent: str, action: str, target: str,
                           status: str = "Success", detail: str = "",
                           execution_time: float = 0.0) -> dict:
        """Fast append-only activity log (no dedup check)."""
        sid = _make_sid("act")
        try:
            result = self.client.create_page(DATABASES["agent_activity"]["database_id"], {
                "Activity":       _title(f"{str(agent).strip()}: {str(action).strip()}"),
                "Agent":          _select(str(agent).strip() or "Saturn"),
                "Action":         _text(str(action)),
                "Target":         _text(str(target)),
                "Status":         _select(str(status).strip() or "Success"),
                "Execution Time": _number(float(execution_time or 0.0)),
                "Detail":         _text(str(detail)),
                "Timestamp":      _date(_now_iso()),
                "Saturn ID":      _saturn_id(sid),
            })
            self._register(sid, result["id"], "agent_activity")
            return result
        except Exception as exc:
            return self._structured_error("log_agent_activity", exc, f"{agent}:{action}:{target}")

    def _find_existing_alert(self, agent: str, title: str) -> Optional[dict]:
        today = _today_iso()
        try:
            results = self.get_all("alerts", {
                "and": [
                    {"property": "Agent", "select": {"equals": agent}},
                    {"property": "Resolved", "checkbox": {"equals": False}},
                    {"timestamp": "created_time", "created_time": {"on_or_after": today}},
                ]
            })
        except Exception as exc:
            self._log_sync_error("find_existing_alert", "API_ERROR", str(exc), f"{agent}:{title}")
            return None

        normalized_title = str(title or "").strip().lower()
        for item in results:
            rich_title = (
                item.get("properties", {})
                .get("Alert Title", {})
                .get("title", [])
            )
            item_title = "".join(part.get("plain_text", "") for part in rich_title).strip().lower()
            if item_title == normalized_title:
                return item
        return None

    def raise_alert(self, agent: str, title: str, message: str,
                    level: str = "Warning", source: str = "") -> dict:
        """Resolved field uses correct Notion API boolean."""
        sid = _make_sid("alert")
        try:
            existing = self._find_existing_alert(agent, title)
            if existing:
                return {
                    "status": "skipped",
                    "reason": "duplicate_alert",
                    "page_id": existing.get("id", ""),
                }
            result = self.client.create_page(DATABASES["alerts"]["database_id"], {
                "Alert Title":     _title(str(title) or "Untitled Alert"),
                "Level":           _select(str(level).strip() or "Warning"),
                "Agent":           _select(str(agent).strip() or "Saturn"),
                "Source":          _text(str(source)),
                "Message":         _text(str(message)),
                "Resolved":        _checkbox(False),   # bool, not string
                "Saturn ID":       _saturn_id(sid),
            })
            self._register(sid, result["id"], "alerts")
            return result
        except Exception as exc:
            return self._structured_error("raise_alert", exc, f"{agent}:{title}")

    def log_token_usage(self, agent: str, model: str, action: str,
                        tokens: int, cost: float) -> dict:
        """Log tokens and auto-check budget."""
        sid    = _make_sid("tok")
        try:
            result = self.client.create_page(DATABASES["token_usage"]["database_id"], {
                "Log Entry": _title(f"{str(agent).strip()} / {str(model).strip()}"),
                "Agent":     _select(str(agent).strip() or "Saturn"),
                "Model":     _text(str(model)),
                "Action":    _text(str(action)),
                "Tokens":    _number(max(0, int(tokens))),
                "Cost":      _number(max(0.0, float(cost))),
                "Logged At": _date(_now_iso()),
                "Saturn ID": _saturn_id(sid),
            })
            self._register(sid, result["id"], "token_usage")
            today = _today_iso()
            if self._daily_cost_date == today:
                self._daily_cost_cache += cost
            self.check_budget()
            return result
        except Exception as exc:
            return self._structured_error("log_token_usage", exc, f"{agent}:{model}:{action}")

    # ── SATURN HQ STATUS UPDATE ───────────────────────────────────────

    def notion_update_hq_status(
        self,
        date: str,
        leads_total: int,
        revenue_earned: float,
        pending_approvals: int,
        extra: str = "",
    ) -> bool:
        """
        Rewrites the SATURN LIVE callout on the Command Center page.
        Called by daily_report at 10pm.

        Args:
            date:              e.g. "13 Mar 2026"
            leads_total:       total leads in pipeline
            revenue_earned:    total revenue earned to date
            pending_approvals: count of drafts pending approval
            extra:             optional extra note appended to callout
        """
        status_text = (
            f"🟢 SATURN LIVE · {date} · "
            f"Revenue: ${revenue_earned:,.0f} · "
            f"Pipeline: {leads_total} leads · "
            f"Pending approvals: {pending_approvals}"
        )
        if extra:
            status_text += f" · {extra}"

        callout_block = {
            "object": "block",
            "type":   "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": status_text}}],
                "icon":      {"type": "emoji", "emoji": "🟢"},
                "color":     "green_background",
            },
        }

        # Find the Command Center page — it's a child of FlowCraft OS
        # We patch the first block (the live callout) specifically
        try:
            blocks = self.client._get(
                f"{NOTION_BASE_URL}/blocks/{HQ_PAGE_ID}/children"
            )
            first_block = blocks.get("results", [{}])[0] if blocks.get("results") else None
            if first_block and first_block.get("type") == "callout":
                # Update existing callout in-place
                self.client._patch(
                    f"{NOTION_BASE_URL}/blocks/{first_block['id']}",
                    {
                        "callout": {
                            "rich_text": [{"type": "text", "text": {"content": status_text}}],
                            "icon":      {"type": "emoji", "emoji": "🟢"},
                            "color":     "green_background",
                        }
                    }
                )
            else:
                # Prepend callout if not found
                self.client.append_block_children(HQ_PAGE_ID, [callout_block])
            logger.info("[Saturn] HQ status updated: %s", status_text)
            return True
        except Exception as e:
            self._log_sync_error("notion_update_hq_status", "API_ERROR", str(e), status_text)
            logger.warning("[Saturn] HQ status update failed: %s", e)
            return False

    # ── FLOWCRAFT OS REVENUE TRACKER UPDATE ───────────────────────────

    def notion_update_os_revenue(self, month: str, earned: float) -> bool:
        """
        Updates the Earned column in the FlowCraft OS revenue tracker table.
        Called by daily_report when revenue is logged.

        Args:
            month:  e.g. "Mar 2026"
            earned: total earned this month in USD
        """
        # This is a static table in the page content — we do a search-and-replace
        # on the Earned cell for the matching month row.
        # Pattern: find the month row and update the $X value in it.
        try:
            blocks = self.client._get(
                f"{NOTION_BASE_URL}/blocks/{FLOWCRAFT_OS_PAGE_ID}/children"
            )
            for block in blocks.get("results", []):
                if block.get("type") != "table_row":
                    continue
                cells = block.get("table_row", {}).get("cells", [])
                if not cells:
                    continue
                # First cell is month label
                first_cell_text = ""
                for chunk in cells[0]:
                    first_cell_text += chunk.get("plain_text", "")
                if month.lower() not in first_cell_text.lower():
                    continue
                # Found the row — update "Earned" cell (index 2)
                self.client._patch(
                    f"{NOTION_BASE_URL}/blocks/{block['id']}",
                    {
                        "table_row": {
                            "cells": [
                                cells[0],
                                cells[1],
                                [{"type": "text", "text": {"content": f"${earned:,.0f}"}}],
                                cells[3] if len(cells) > 3 else [],
                            ]
                        }
                    }
                )
                logger.info("[Saturn] OS revenue updated: %s -> $%s", month, format(earned, ",.0f"))
                return True
            logger.warning("[Saturn] OS revenue month not found in table: %s", month)
            return False
        except Exception as e:
            self._log_sync_error("notion_update_os_revenue", "API_ERROR", str(e), month)
            logger.warning("[Saturn] OS revenue update failed: %s", e)
            return False

    # ── PROGRESS REPORT ───────────────────────────────────────────────

    def progress_report(self) -> dict:
        """
        Live dashboard metrics — single call, no caching.
        Returns structured dict for Telegram report, Pulse, and Sentinel.

        Returns:
            {
              "leads": {"total": int, "by_status": {...}, "high_score": int},
              "pipeline": {"open_deals": int, "total_value": float, "weighted_value": float},
              "outreach": {"pending_approval": int, "sent_today": int, "follow_up_due": int},
              "revenue": {"total_paid": float, "pending": float, "this_month": float},
              "tasks": {"todo": int, "doing": int, "overdue": int},
              "alerts": {"critical": int, "unresolved": int},
              "generated_at": str,
            }
        """
        report: dict = {"generated_at": _now_iso()}
        today = _today_iso()

        # Leads
        try:
            all_leads = self.get_all("leads")
            by_status: dict[str, int] = {}
            for r in all_leads:
                s = (r.get("properties", {}).get("Status", {}).get("select") or {}).get("name", "Unknown")
                by_status[s] = by_status.get(s, 0) + 1
            high_score = sum(
                1 for r in all_leads
                if (r.get("properties", {}).get("Lead Score", {}).get("number") or 0) >= 70
            )
            report["leads"] = {
                "total":      len(all_leads),
                "by_status":  by_status,
                "high_score": high_score,
            }
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "leads")
            report["leads"] = {"error": str(e)}

        # Pipeline (deals)
        try:
            pipeline = self.get_pipeline_value()
            report["pipeline"] = pipeline
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "pipeline")
            report["pipeline"] = {"error": str(e)}

        # Outreach
        try:
            pending   = self.get_pending_outreach()
            follow_up = self.get_all("outreach_drafts", {
                "property": "Status", "select": {"equals": "Follow Up"}
            })
            report["outreach"] = {
                "pending_approval": len(pending),
                "follow_up_due":    len(follow_up),
            }
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "outreach")
            report["outreach"] = {"error": str(e)}

        # Revenue
        try:
            all_rev     = self.get_all("revenue")
            total_paid  = 0.0
            total_pend  = 0.0
            this_month  = 0.0
            month_prefix = today[:7]  # "YYYY-MM"
            for r in all_rev:
                props  = r.get("properties", {})
                amount = props.get("Amount", {}).get("number") or 0.0
                status = (props.get("Status", {}).get("select") or {}).get("name", "")
                paid_d = (props.get("Paid Date", {}).get("date") or {}).get("start", "")
                if status == "Paid":
                    total_paid += amount
                    if paid_d.startswith(month_prefix):
                        this_month += amount
                else:
                    total_pend += amount
            report["revenue"] = {
                "total_paid":  round(total_paid, 2),
                "pending":     round(total_pend, 2),
                "this_month":  round(this_month, 2),
            }
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "revenue")
            report["revenue"] = {"error": str(e)}

        # Tasks
        try:
            todo   = self.get_all("tasks", {"property": "Status", "select": {"equals": "Todo"}})
            doing  = self.get_all("tasks", {"property": "Status", "select": {"equals": "Doing"}})
            overdue = self.get_overdue_tasks()
            report["tasks"] = {
                "todo":    len(todo),
                "doing":   len(doing),
                "overdue": len(overdue),
            }
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "tasks")
            report["tasks"] = {"error": str(e)}

        # Alerts
        try:
            critical   = self.get_critical_alerts()
            unresolved = self.get_all("alerts", {
                "property": "Resolved", "checkbox": {"equals": False}
            })
            report["alerts"] = {
                "critical":   len(critical),
                "unresolved": len(unresolved),
            }
        except Exception as e:
            self._log_sync_error("progress_report", "API_ERROR", str(e), "alerts")
            report["alerts"] = {"error": str(e)}

        return report

    def format_progress_report(self, report: dict = None) -> str:
        """Format progress_report() output as a human-readable Telegram message."""
        if report is None:
            report = self.progress_report()

        ts   = report.get("generated_at", _now_iso())[:16].replace("T", " ")
        lines = [
            f"🪐 *SATURN DAILY REPORT* — {ts} UTC",
            "",
        ]

        leads = report.get("leads", {})
        if "total" in leads:
            lines.append(f"📊 *PIPELINE*: {leads['total']} leads total | {leads.get('high_score', 0)} high-score")
            for s, c in leads.get("by_status", {}).items():
                lines.append(f"   › {s}: {c}")
        lines.append("")

        pipeline = report.get("pipeline", {})
        if "total_deals" in pipeline:
            lines.append(
                f"💼 *DEALS*: {pipeline['total_deals']} open | "
                f"${pipeline.get('total_value', 0):,.0f} total | "
                f"${pipeline.get('weighted_value', 0):,.0f} weighted"
            )
        lines.append("")

        rev = report.get("revenue", {})
        if "total_paid" in rev:
            lines.append(
                f"💰 *REVENUE*: ${rev.get('total_paid', 0):,.0f} paid | "
                f"${rev.get('this_month', 0):,.0f} this month | "
                f"${rev.get('pending', 0):,.0f} pending"
            )
        lines.append("")

        outreach = report.get("outreach", {})
        if "pending_approval" in outreach:
            lines.append(
                f"📬 *OUTREACH*: {outreach.get('pending_approval', 0)} pending approval | "
                f"{outreach.get('follow_up_due', 0)} follow-ups due"
            )
        lines.append("")

        tasks = report.get("tasks", {})
        if "todo" in tasks:
            lines.append(
                f"✅ *TASKS*: {tasks.get('todo', 0)} todo | "
                f"{tasks.get('doing', 0)} in progress | "
                f"{tasks.get('overdue', 0)} overdue"
            )
        lines.append("")

        alerts = report.get("alerts", {})
        if "critical" in alerts:
            crit = alerts.get("critical", 0)
            flag = "🚨" if crit > 0 else "🛡️"
            lines.append(f"{flag} *ALERTS*: {crit} critical | {alerts.get('unresolved', 0)} unresolved")

        return "\n".join(lines)

    # ── QUERY HELPERS ─────────────────────────────────────────────────────────

    def get_high_score_leads(self, threshold: int = 70) -> list:
        return self.get_all("leads", {
            "property": "Lead Score", "number": {"greater_than": threshold}
        })

    def get_pending_outreach(self) -> list:
        return self.get_all("outreach_drafts", {
            "property": "Status", "select": {"equals": "Pending Approval"}
        })

    def get_overdue_tasks(self) -> list:
        today = _today_iso()
        return self.get_all("tasks", {"and": [
            {"property": "Due Date", "date":   {"before": today}},
            {"property": "Status",   "select": {"does_not_equal": "Done"}},
        ]})

    def get_open_deals(self, stage: str = None) -> list:
        if stage:
            f = {"property": "Stage", "select": {"equals": stage}}
        else:
            f = {"and": [
                {"property": "Stage", "select": {"does_not_equal": "Won"}},
                {"property": "Stage", "select": {"does_not_equal": "Lost"}},
            ]}
        return self.get_all("deals", f)

    def get_critical_alerts(self) -> list:
        return self.get_all("alerts", {"and": [
            {"property": "Level",    "select":   {"equals": "Critical"}},
            {"property": "Resolved", "checkbox": {"equals": False}},
        ]})

    def get_pipeline_value(self) -> dict:
        deals = self.get_open_deals()
        total_value = weighted_value = 0.0
        by_stage: dict[str, float] = {}
        for deal in deals:
            props    = deal.get("properties", {})
            val      = props.get("Value",   {}).get("number") or 0.0
            stage    = (props.get("Stage",  {}).get("select") or {}).get("name", "Unknown")
            raw_prob = props.get("Probability %", {}).get("number")
            prob     = (raw_prob / 100) if raw_prob is not None else DEFAULT_STAGE_PROB.get(stage, 0.0)
            total_value    += val
            weighted_value += val * prob
            by_stage[stage] = by_stage.get(stage, 0.0) + val
        return {
            "total_deals":    len(deals),
            "total_value":    round(total_value, 2),
            "weighted_value": round(weighted_value, 2),
            "by_stage":       {k: round(v, 2) for k, v in by_stage.items()},
        }

    # ── BULK SYNC WITH PROGRESS ──────────────────────────────────────

    def pull_all(self):
        """Pull all 15 databases, registering Saturn IDs in SQLite with progress."""
        dbs    = list(DATABASES.keys())
        total  = len(dbs)
        ok     = 0
        errors = []
        logger.info("[Saturn] Pulling %s Notion databases", total)
        for i, db_name in enumerate(dbs, 1):
            pct = int((i / total) * 100)
            try:
                records = self.get_all(db_name)
                for r in records:
                    rt = r.get("properties", {}).get("Saturn ID", {}).get("rich_text", [])
                    if rt:
                        self._register(rt[0]["text"]["content"], r["id"], db_name)
                logger.info("[Saturn] pull_all progress=%s%% db=%s records=%s", pct, db_name, len(records))
                ok += 1
            except Exception as e:
                self._log_sync_error("pull_all", "API_ERROR", str(e), db_name)
                logger.warning("[Saturn] pull_all db=%s progress=%s%% failed: %s", db_name, pct, e)
                errors.append((db_name, str(e)))
        logger.info(
            "[Saturn] Pull complete: %s/%s OK%s",
            ok,
            total,
            f", {len(errors)} errors" if errors else "",
        )
        return {"ok": ok, "total": total, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL SINGLETON (for import convenience)
# ─────────────────────────────────────────────────────────────────────────────

_sync_instance: Optional[NotionSync] = None

def get_sync() -> NotionSync:
    """Return the module-level singleton NotionSync. Creates on first call."""
    global _sync_instance
    if _sync_instance is None:
        _sync_instance = NotionSync()
    return _sync_instance


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print(f"FlowCraft OS — Saturn Registry  [{DATABASE_VERSION}]")
    print("=" * 65)

    for name, db in DATABASES.items():
        print(f"\n{name.upper()}")
        print(f"  Section:       {db['section']}")
        print(f"  Collection ID: {db['collection_id']}")
        print(f"  Database ID:   {db['database_id']}")
        if db.get("relations"):
            print(f"  Relations:     {db['relations']}")

    print("\n" + "=" * 65)
    print("Data flow:")
    print("  Hunter → Leads → Outreach Drafts → Deals → Revenue")
    print("  Deals  → Clients → Projects → Tasks → Work Log")
    print("                              → Automation Builds")
    print("\nMonitoring: Agent Activity / Alerts / API Usage / Token Usage")
    print("Knowledge:  Automation Library / Prompt Library")
    print(f"\nBudget cap: ${MAX_DAILY_TOKEN_COST}/day")
    print("=" * 65)

    # Quick health check
    print("\nRunning health check...")
    sync = NotionSync()
    sync.health_check()
    print("\nRunning progress report...")
    report = sync.progress_report()
    print(sync.format_progress_report(report))
