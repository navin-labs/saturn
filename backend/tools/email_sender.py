from __future__ import annotations

import datetime as dt
import logging
import os
import re
import smtplib
import sqlite3
from email.message import EmailMessage

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH

logger = logging.getLogger("saturn.email_sender")
MAX_SENDS_PER_DAY = 10
MAX_RETRY = 1
RATE_LIMIT_MESSAGE = "Rate limit reached. Try again in a few minutes."
RATE_LIMIT_RETRY_AFTER = 60
ERROR_TYPES = {
    "API_ERROR",
    "AUTH_ERROR",
    "RATE_LIMIT",
    "NETWORK_ERROR",
    "DB_ERROR",
    "LOGIC_ERROR",
    "SMTP_FAILURE",
    "SMTP_NOT_CONFIGURED",
}
EMAIL_SIGNATURE = "\n".join(
    [
        "Best regards,",
        "Navin Rana",
        "AI Automation Engineer",
        "FlowCraft Automations",
        "theautomationguy.navin@gmail.com",
    ]
)
MIN_EMAIL_BODY_WORDS = 5
EMAIL_BODY_MIN_WORDS = 80
EMAIL_BODY_MAX_WORDS = 120
DISALLOWED_EMAIL_PHRASES = (
    "hope you're doing well",
    "i hope you're doing well",
    "quick thought",
    "just checking",
    "i came across",
    "we specialize",
    "your company",
    "your name",
    "company name",
    "decision maker",
)
EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
SUBJECT_LINE_RE = re.compile(r"(?im)^subject:\s*")


def _ist_now() -> dt.datetime:
    return dt.datetime.now(
        tz=dt.timezone(dt.timedelta(hours=5, minutes=30))
    )


def _missing_smtp_config_fields() -> list[str]:
    required = ("SMTP_HOST", "SMTP_USER", "SMTP_PASS")
    return [key for key in required if not (os.environ.get(key, "") or "").strip()]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            status TEXT DEFAULT 'new',
            email_status TEXT DEFAULT 'unknown',
            bounce_count INTEGER DEFAULT 0,
            last_contact TIMESTAMP,
            follow_up_due_at TIMESTAMP,
            no_reply_since TIMESTAMP,
            last_outreach_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS outreach_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            draft_text TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS email_send_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            draft_id INTEGER,
            status TEXT NOT NULL,
            attempt_count INTEGER DEFAULT 1,
            error_category TEXT,
            sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS error_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            action TEXT NOT NULL,
            error_type TEXT NOT NULL,
            message TEXT NOT NULL,
            detail TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            action TEXT NOT NULL,
            detail TEXT,
            result TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _add_column_if_missing(conn, "leads", "email TEXT")
    _add_column_if_missing(conn, "leads", "status TEXT DEFAULT 'new'")
    _add_column_if_missing(conn, "leads", "email_status TEXT DEFAULT 'unknown'")
    _add_column_if_missing(conn, "leads", "bounce_count INTEGER DEFAULT 0")
    _add_column_if_missing(conn, "leads", "last_contact TIMESTAMP")
    _add_column_if_missing(conn, "leads", "follow_up_due_at TIMESTAMP")
    _add_column_if_missing(conn, "leads", "no_reply_since TIMESTAMP")
    _add_column_if_missing(conn, "leads", "last_outreach_at TIMESTAMP")
    _add_column_if_missing(conn, "leads", "updated_at TIMESTAMP")
    _add_column_if_missing(conn, "outreach_drafts", "processed_at TIMESTAMP")
    _add_column_if_missing(conn, "email_send_log", "lead_id INTEGER")
    _add_column_if_missing(conn, "email_send_log", "draft_id INTEGER")
    _add_column_if_missing(conn, "email_send_log", "status TEXT")
    _add_column_if_missing(conn, "email_send_log", "attempt_count INTEGER DEFAULT 1")
    _add_column_if_missing(conn, "email_send_log", "error_category TEXT")
    _add_column_if_missing(conn, "email_send_log", "sent_at TIMESTAMP")
    conn.execute("UPDATE email_send_log SET status=lower(COALESCE(status,'')) WHERE status IS NOT NULL")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_send_log_day ON email_send_log(status, sent_at)")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_email_send_log_draft_status "
        "ON email_send_log(draft_id, status) WHERE draft_id IS NOT NULL"
    )
    conn.commit()


def db_conn() -> sqlite3.Connection:
    db_path = enforce_write_path(DB_PATH, "email-sender-db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    return conn


def log_agent(conn: sqlite3.Connection, agent: str, action: str, detail: str, result: str) -> None:
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail[:500], result[:200], _ist_now().replace(microsecond=0).isoformat()),
    )


def log_error(conn: sqlite3.Connection, action: str, error_type: str, message: str, detail: str = "") -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    now = _ist_now().replace(microsecond=0).isoformat()
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
        WHERE date(sent_at)=date('now', '+5 hours', '+30 minutes') AND lower(status)='sent'
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
    existing = conn.execute(
        """
        SELECT id
        FROM email_send_log
        WHERE lead_id=?
          AND COALESCE(draft_id, -1)=COALESCE(?, -1)
          AND lower(status)=lower(?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (lead_id, draft_id if draft_id > 0 else None, status),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO email_send_log (lead_id, draft_id, status, attempt_count, error_category, sent_at)
        VALUES (?,?,?,?,?,?)
        """,
        (
            lead_id,
            draft_id if draft_id > 0 else None,
            status,
            attempt_count,
            error_category or None,
            _ist_now().replace(microsecond=0).isoformat(),
        ),
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


def _sanitize_subject(subject: str, fallback: str = "") -> str:
    raw = str(subject or "").replace("\r", "\n")
    first_line = raw.split("\n", 1)[0].strip()
    if first_line.startswith("__SUBJECT__:"):
        first_line = first_line.replace("__SUBJECT__:", "", 1).strip()
    elif first_line.lower().startswith("subject:"):
        first_line = first_line[len("subject:"):].strip()
    return re.sub(r"\s+", " ", first_line).strip() or re.sub(r"\s+", " ", str(fallback or "")).strip()


def _strip_signature(body: str) -> str:
    text = str(body or "").strip()
    for marker in ("\n\nBest regards,\n", "\n\nBest,\n", "\n\nRegards,\n"):
        if marker in text:
            return text.split(marker, 1)[0].rstrip()
    return text


def _ensure_signature(body: str) -> str:
    base = _strip_signature(body)
    if not base:
        return ""
    return f"{base}\n\n{EMAIL_SIGNATURE}".strip()


def _body_word_count(body: str) -> int:
    normalized = _strip_signature(str(body or "")).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return 0
    return len(re.sub(r"\s+", " ", normalized).split())


def _body_structure_error(body: str, subject: str = "") -> str:
    normalized = _strip_signature(str(body or "")).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return "body_missing"
    lower_body = normalized.lower()
    if any(token in lower_body for token in ("[", "]", "<", ">", "your company", "your name", "company name", "decision maker")):
        return "placeholder_content"
    if any(phrase in lower_body for phrase in DISALLOWED_EMAIL_PHRASES):
        return "generic_phrase"
    sanitized_subject = _sanitize_subject(subject).lower()
    if sanitized_subject and sanitized_subject in lower_body:
        return "body_repeats_subject"
    paragraphs = [block.strip() for block in normalized.split("\n\n") if block.strip()]
    if len(paragraphs) != 4:
        return "paragraph_count_invalid"
    if not paragraphs[0].startswith("Hi "):
        return "greeting_missing"
    if any(len(paragraph.split()) < 5 for paragraph in paragraphs[1:]):
        return "paragraph_too_short"
    word_count = _body_word_count(normalized)
    if word_count < EMAIL_BODY_MIN_WORDS or word_count > EMAIL_BODY_MAX_WORDS:
        return "word_count_invalid"
    return ""


def _normalize_payload(subject: str, body: str) -> tuple[str, str, str]:
    raw_body = str(body or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    subject_line = _sanitize_subject(subject)
    if raw_body.startswith("__SUBJECT__:"):
        first_line, _, remainder = raw_body.partition("\n")
        subject_line = _sanitize_subject(first_line, subject_line)
        raw_body = remainder.strip()
    elif raw_body.lower().startswith("subject:"):
        lines = raw_body.splitlines()
        subject_line = _sanitize_subject(lines[0], subject_line)
        raw_body = "\n".join(lines[1:]).strip()
    raw_body = re.sub(r"\n{3,}", "\n\n", raw_body)
    raw_body = _strip_signature(raw_body)
    if "__SUBJECT__" in raw_body or SUBJECT_LINE_RE.search(raw_body):
        return "", "", "body_contains_subject_marker"
    if not subject_line:
        return "", "", "subject_missing"
    if not raw_body:
        return "", "", "body_missing"
    if len(raw_body.split()) < MIN_EMAIL_BODY_WORDS:
        return "", "", "body_too_short"
    normalized_body = _ensure_signature(raw_body)
    structure_error = _body_structure_error(normalized_body, subject_line)
    if structure_error:
        return "", "", structure_error
    return subject_line, normalized_body, ""


def smtp_send(to_email: str, subject: str, body: str) -> None:
    missing = _missing_smtp_config_fields()
    if missing:
        raise RuntimeError(f"SMTP_NOT_CONFIGURED:{','.join(missing)}")

    host = (os.environ.get("SMTP_HOST", "") or "").strip()
    try:
        port = int(os.environ.get("SMTP_PORT", "587"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("SMTP_PORT is invalid") from exc
    username = (os.environ.get("SMTP_USER", "") or "").strip()
    password = (os.environ.get("SMTP_PASS", "") or "").strip()
    sender = (os.environ.get("SMTP_FROM", username) or "").strip() or username
    recipient = (to_email or "").strip()
    normalized_subject, normalized_body, payload_error = _normalize_payload(subject, body)
    if not recipient:
        raise RuntimeError("recipient email missing")
    if payload_error:
        raise RuntimeError(f"invalid_email_payload:{payload_error}")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = normalized_subject
    msg.set_content(normalized_body)

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        refused = smtp.send_message(msg)
        if refused:
            raise RuntimeError(f"SMTP refused recipient: {to_email}")


def send_email(to, subject, body, lead_id, draft_id) -> dict:
    conn: sqlite3.Connection | None = None
    try:
        conn = db_conn()
        lead_id = int(lead_id or 0)
        draft_id = int(draft_id or 0)
        missing_smtp = _missing_smtp_config_fields()
        if missing_smtp:
            log_error(
                conn,
                "send_email",
                "SMTP_NOT_CONFIGURED",
                "smtp configuration missing",
                ",".join(missing_smtp),
            )
            conn.commit()
            return {"status": "error", "type": "SMTP_NOT_CONFIGURED"}
        to_email = (to or "").strip()
        if not to_email:
            log_error(conn, "send_email", "LOGIC_ERROR", "recipient email missing", f"lead_id={lead_id}")
            conn.commit()
            return {"status": "failed", "error_type": "LOGIC_ERROR"}
        if not EMAIL_PATTERN.fullmatch(to_email):
            log_error(conn, "send_email", "LOGIC_ERROR", "recipient email invalid", to_email)
            conn.commit()
            return {"status": "failed", "error_type": "LOGIC_ERROR"}

        normalized_subject, normalized_body, payload_error = _normalize_payload(str(subject or ""), str(body or ""))
        if payload_error:
            log_error(conn, "send_email", "LOGIC_ERROR", "email payload invalid", payload_error)
            conn.commit()
            return {"status": "failed", "error_type": "LOGIC_ERROR"}

        if daily_send_count(conn) >= MAX_SENDS_PER_DAY:
            log_error(conn, "send_email", "RATE_LIMIT", "daily email send cap reached", f"limit={MAX_SENDS_PER_DAY}")
            log_agent(conn, "Echo", "send_email_blocked", "daily cap reached", "warning")
            conn.commit()
            return {"status": "failed", "error_type": "RATE_LIMIT"}

        attempts = 1
        last_error_type = "API_ERROR"
        for attempt in range(1, attempts + 1):
            try:
                smtp_send(to_email, normalized_subject, normalized_body)
                sent_at = _ist_now().replace(microsecond=0).isoformat()
                log_send_attempt(conn, lead_id, draft_id, "sent", attempt, "")
                if draft_id > 0:
                    conn.execute(
                        "UPDATE outreach_drafts SET status='sent', processed_at=? WHERE id=?",
                        (sent_at, draft_id),
                    )
                if lead_id > 0:
                    follow_due = (_ist_now() + dt.timedelta(days=3)).replace(microsecond=0).isoformat()
                    conn.execute(
                        """
                        UPDATE leads
                        SET status=CASE WHEN lower(COALESCE(status,''))='new' THEN 'contacted' ELSE status END,
                            last_contact=?,
                            no_reply_since=COALESCE(no_reply_since, ?),
                            last_outreach_at=?,
                            follow_up_due_at=COALESCE(follow_up_due_at, ?),
                            updated_at=?
                        WHERE id=?
                        """,
                        (sent_at, sent_at, sent_at, follow_due, sent_at, lead_id),
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
                    now = _ist_now().replace(microsecond=0).isoformat()
                    conn.execute(
                        """
                        UPDATE leads
                        SET email_status='bounced',
                            bounce_count=COALESCE(bounce_count, 0)+1,
                            updated_at=?
                        WHERE id=?
                        """,
                        (now, lead_id),
                    )
                    log_error(conn, "send_email", "API_ERROR", "smtp bounce detected", str(exc))
                    conn.commit()
                    return {"status": "bounced", "error_type": "API_ERROR"}

                if last_error_type == "RATE_LIMIT":
                    if draft_id > 0:
                        conn.execute(
                            "UPDATE outreach_drafts SET status='pending', processed_at=NULL WHERE id=? AND lower(status)!='sent'",
                            (draft_id,),
                        )
                    log_error(
                        conn,
                        "rate_limit",
                        "RATE_LIMIT",
                        str(exc),
                        f"lead_id={lead_id} draft_id={draft_id}",
                    )
                    conn.commit()
                    return {
                        "status": "rate_limited",
                        "message": RATE_LIMIT_MESSAGE,
                        "retry_after": RATE_LIMIT_RETRY_AFTER,
                    }

                if draft_id > 0:
                    conn.execute(
                        "UPDATE outreach_drafts SET status='pending', processed_at=NULL WHERE id=? AND lower(status)!='sent'",
                        (draft_id,),
                    )
                log_error(conn, "send_email", "SMTP_FAILURE", "email send failed", str(exc))
                conn.commit()
                return {"status": "error", "type": "SMTP_FAILURE"}

        conn.commit()
        return {"status": "failed", "error_type": last_error_type}
    except sqlite3.Error as exc:
        if conn is not None:
            log_error(conn, "send_email", "DB_ERROR", "database write failure", str(exc))
            conn.commit()
        return {"status": "failed", "error_type": "DB_ERROR"}
    except Exception as exc:
        if conn is not None:
            try:
                log_error(conn, "send_email", classify_error(exc), "unexpected send failure", str(exc))
                conn.commit()
            except sqlite3.Error as log_exc:
                logger.warning("[Saturn] email_sender unexpected send failure logging failed: %s", log_exc)
        return {"status": "failed", "error_type": classify_error(exc)}
    finally:
        if conn is not None:
            conn.close()
