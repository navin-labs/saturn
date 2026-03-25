from __future__ import annotations

import datetime as dt
import os
import smtplib
import sqlite3
import time
from email.message import EmailMessage
from pathlib import Path

DB_PATH = Path("/home/navin/Workspace/Saturn/database/saturn.db")
MAX_SENDS_PER_DAY = 10
MAX_RETRY = 1
ERROR_TYPES = {"API_ERROR", "AUTH_ERROR", "RATE_LIMIT", "NETWORK_ERROR", "DB_ERROR", "LOGIC_ERROR"}


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def log_agent(conn: sqlite3.Connection, agent: str, action: str, detail: str, result: str) -> None:
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail[:500], result[:200], utc_now()),
    )


def log_error(conn: sqlite3.Connection, action: str, error_type: str, message: str, detail: str = "") -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    now = utc_now()
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        ("Echo", action, safe_type, message[:300], detail[:500], now),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("Echo", action, detail[:500], safe_type, now),
    )


def daily_send_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM email_send_log
        WHERE date(sent_at)=date('now') AND lower(status)='sent'
        """
    ).fetchone()
    return int(row["total"] or 0)


def log_send_attempt(
    conn: sqlite3.Connection,
    lead_id: int,
    draft_id: int,
    status: str,
    attempt_count: int,
    error_category: str,
) -> None:
    conn.execute(
        """
        INSERT INTO email_send_log (lead_id, draft_id, status, attempt_count, error_category, sent_at)
        VALUES (?,?,?,?,?,?)
        """,
        (lead_id, draft_id if draft_id > 0 else None, status, attempt_count, error_category or None, utc_now()),
    )


def is_bounce_error(exc: Exception) -> bool:
    smtp_code = getattr(exc, "smtp_code", None)
    if isinstance(smtp_code, int) and 500 <= smtp_code < 600:
        return True
    text = str(exc).lower()
    return any(marker in text for marker in ("550", "551", "552", "553", "554", "user unknown", "mailbox"))


def classify_error(exc: Exception) -> str:
    if is_bounce_error(exc):
        return "API_ERROR"
    smtp_code = getattr(exc, "smtp_code", None)
    text = str(exc).lower()
    if isinstance(smtp_code, int) and smtp_code in (534, 535):
        return "AUTH_ERROR"
    if "auth" in text or "login" in text or "credential" in text or "password" in text:
        return "AUTH_ERROR"
    if "rate" in text or "quota" in text or "too many" in text or "429" in text:
        return "RATE_LIMIT"
    if "timeout" in text or "network" in text or "connection" in text:
        return "NETWORK_ERROR"
    return "API_ERROR"


def smtp_send(to_email: str, subject: str, body: str) -> None:
    username = (os.environ.get("GMAIL_USER", "") or "").strip()
    password = (os.environ.get("GMAIL_APP_PASSWORD", "") or "").strip()
    if not username or not password:
        raise RuntimeError("GMAIL_USER or GMAIL_APP_PASSWORD not configured")

    msg = EmailMessage()
    msg["From"] = username
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        refused = smtp.send_message(msg)
        if refused:
            raise RuntimeError(f"SMTP refused recipient: {to_email}")


def send_email(to, subject, body, lead_id, draft_id) -> dict:
    conn = db_conn()
    lead_id = int(lead_id or 0)
    draft_id = int(draft_id or 0)
    try:
        to_email = (to or "").strip()
        if not to_email:
            log_error(conn, "send_email", "LOGIC_ERROR", "recipient email missing", f"lead_id={lead_id}")
            conn.commit()
            return {"status": "failed", "error_type": "LOGIC_ERROR"}

        if daily_send_count(conn) >= MAX_SENDS_PER_DAY:
            log_error(conn, "send_email", "RATE_LIMIT", "daily email send cap reached", f"limit={MAX_SENDS_PER_DAY}")
            log_agent(conn, "Echo", "send_email_blocked", "daily cap reached", "warning")
            conn.commit()
            return {"status": "failed", "error_type": "RATE_LIMIT"}

        username = (os.environ.get("GMAIL_USER", "") or "").strip()
        password = (os.environ.get("GMAIL_APP_PASSWORD", "") or "").strip()
        if not username or not password:
            log_error(conn, "send_email", "AUTH_ERROR", "gmail smtp credentials missing", "")
            conn.commit()
            return {"status": "failed", "error_type": "AUTH_ERROR"}

        attempts = MAX_RETRY + 1
        last_error_type = "API_ERROR"
        for attempt in range(1, attempts + 1):
            try:
                smtp_send(to_email, str(subject or ""), str(body or ""))
                log_send_attempt(conn, lead_id, draft_id, "sent", attempt, "")
                if draft_id > 0:
                    conn.execute(
                        "UPDATE outreach_drafts SET status='sent', processed_at=? WHERE id=?",
                        (utc_now(), draft_id),
                    )
                log_agent(
                    conn,
                    "Echo",
                    "email_sent",
                    f"lead_id={lead_id} draft_id={draft_id} attempt={attempt}",
                    "success",
                )
                conn.commit()
                return {"status": "sent", "error_type": ""}
            except Exception as exc:
                last_error_type = classify_error(exc)
                bounce = is_bounce_error(exc)
                status = "bounced" if bounce else "failed"
                log_send_attempt(conn, lead_id, draft_id, status, attempt, last_error_type)
                if bounce and lead_id > 0:
                    conn.execute(
                        "UPDATE leads SET email_status='bounced', updated_at=? WHERE id=?",
                        (utc_now(), lead_id),
                    )
                    log_error(conn, "send_email", "API_ERROR", "smtp bounce detected", str(exc))
                    conn.commit()
                    return {"status": "bounced", "error_type": "API_ERROR"}

                if attempt <= MAX_RETRY:
                    time.sleep(5)
                    continue

                log_error(conn, "send_email", last_error_type, "email send failed", str(exc))
                conn.commit()
                return {"status": "failed", "error_type": last_error_type}

        conn.commit()
        return {"status": "failed", "error_type": last_error_type}
    except sqlite3.Error as exc:
        try:
            log_error(conn, "send_email", "DB_ERROR", "database write failure", str(exc))
            conn.commit()
        except sqlite3.Error:
            pass
        return {"status": "failed", "error_type": "DB_ERROR"}
    finally:
        conn.close()
