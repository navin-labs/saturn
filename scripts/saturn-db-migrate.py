import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH, ensure_structure

ensure_structure()
db_path = enforce_write_path(DB_PATH, "sqlite-db-write")
conn = sqlite3.connect(str(db_path))
c = conn.cursor()


def table_columns(cursor: sqlite3.Cursor, table: str) -> set[str]:
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def add_column_if_missing(cursor: sqlite3.Cursor, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name not in table_columns(cursor, table):
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")

# Agent activity log
c.execute(
    """CREATE TABLE IF NOT EXISTS agent_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT,
    result TEXT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

# Lead pipeline
c.execute(
    """CREATE TABLE IF NOT EXISTS leads (
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
)"""
)

add_column_if_missing(c, "leads", "website TEXT")
add_column_if_missing(c, "leads", "website_norm TEXT")
add_column_if_missing(c, "leads", "email TEXT")
add_column_if_missing(c, "leads", "email_status TEXT DEFAULT 'unknown'")
add_column_if_missing(c, "leads", "email_source TEXT")
add_column_if_missing(c, "leads", "bounce_count INTEGER DEFAULT 0")
add_column_if_missing(c, "leads", "follow_up_count INTEGER DEFAULT 0")
add_column_if_missing(c, "leads", "follow_up_due_at TIMESTAMP")
add_column_if_missing(c, "leads", "no_reply_since TIMESTAMP")
add_column_if_missing(c, "leads", "last_outreach_at TIMESTAMP")
add_column_if_missing(c, "leads", "manual_override INTEGER DEFAULT 0")
add_column_if_missing(c, "leads", "updated_at TIMESTAMP")

# Revenue tracking
c.execute(
    """CREATE TABLE IF NOT EXISTS revenue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client TEXT,
    service TEXT,
    amount REAL,
    status TEXT DEFAULT 'pending',
    invoice_date TIMESTAMP,
    paid_date TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

# Daily plans
c.execute(
    """CREATE TABLE IF NOT EXISTS daily_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_date DATE UNIQUE,
    plan_text TEXT,
    focus TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

# Hourly check logs
c.execute(
    """CREATE TABLE IF NOT EXISTS hourly_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    summary TEXT,
    open_tasks INTEGER,
    completed_today INTEGER
)"""
)

# Content queue
c.execute(
    """CREATE TABLE IF NOT EXISTS content_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type TEXT,
    title TEXT,
    body TEXT,
    platform TEXT,
    status TEXT DEFAULT 'draft',
    scheduled_for TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

add_column_if_missing(c, "content_queue", "lead_id INTEGER")
add_column_if_missing(c, "content_queue", "processed_at TIMESTAMP")
add_column_if_missing(c, "content_queue", "processed_by TEXT")

# System alerts
c.execute(
    """CREATE TABLE IF NOT EXISTS system_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT,
    source TEXT,
    message TEXT,
    resolved INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

add_column_if_missing(c, "system_alerts", "error_type TEXT")

c.execute(
    """CREATE TABLE IF NOT EXISTS token_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tokens INTEGER NOT NULL,
    model TEXT,
    agent TEXT,
    action TEXT,
    logged_at TEXT
)"""
)

add_column_if_missing(c, "token_log", "agent TEXT")
add_column_if_missing(c, "token_log", "action TEXT")

c.execute(
    """CREATE TABLE IF NOT EXISTS api_usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    provider TEXT NOT NULL,
    endpoint TEXT,
    status TEXT NOT NULL,
    error_type TEXT,
    detail TEXT,
    called_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

c.execute(
    """CREATE TABLE IF NOT EXISTS error_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    action TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    detail TEXT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
)

c.execute(
    """CREATE TABLE IF NOT EXISTS agent_runs (
    run_id TEXT PRIMARY KEY,
    agent TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    status TEXT NOT NULL DEFAULT 'running'
)"""
)

c.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_website_norm ON leads(website_norm) WHERE website_norm IS NOT NULL"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_leads_status_followup ON leads(status, follow_up_due_at)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_content_queue_status ON content_queue(status)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_content_queue_lead ON content_queue(lead_id)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_api_usage_day ON api_usage_log(agent, provider, called_at)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_token_log_day ON token_log(logged_at)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_token_log_agent_action_day ON token_log(agent, action, logged_at)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_error_log_ts ON error_log(ts)"
)
c.execute(
    "CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_time ON agent_runs(agent, started_at DESC)"
)

conn.commit()
conn.close()
print("Migration complete. Tables created.")
