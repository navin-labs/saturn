from __future__ import annotations

import datetime as dt
import email
import imaplib
import json
import logging
import os
import re
import sqlite3
from email.header import decode_header
from email.utils import parseaddr
from pathlib import Path

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH


def _load_saturn_env() -> None:
    p = Path.home() / ".config" / "openclaw-secrets" / "telegram.env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())


_load_saturn_env()

logger = logging.getLogger("saturn.email_reader")

ERROR_TYPES = {"API_ERROR", "AUTH_ERROR", "RATE_LIMIT", "NETWORK_ERROR", "DB_ERROR", "LOGIC_ERROR"}
INTERESTED_TERMS = ("call", "meeting", "schedule", "interested", "tell me more", "sounds good", "yes")
NOT_INTERESTED_TERMS = ("not interested", "unsubscribe", "remove", "stop", "no thanks")
OOO_SUBJECT_TERMS = ("out of office", "away", "vacation", "auto-reply")


def _ist_now() -> dt.datetime:
    return dt.datetime.now(
        tz=dt.timezone(dt.timedelta(hours=5, minutes=30))
    )


def _missing_gmail_config_fields() -> list[str]:
    required = ("GMAIL_USER", "GMAIL_APP_PASSWORD")
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
            updated_at TIMESTAMP
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
    _add_column_if_missing(conn, "leads", "updated_at TIMESTAMP")
    _add_column_if_missing(conn, "email_send_log", "lead_id INTEGER")
    _add_column_if_missing(conn, "email_send_log", "draft_id INTEGER")
    _add_column_if_missing(conn, "email_send_log", "status TEXT")
    _add_column_if_missing(conn, "email_send_log", "attempt_count INTEGER DEFAULT 1")
    _add_column_if_missing(conn, "email_send_log", "error_category TEXT")
    _add_column_if_missing(conn, "email_send_log", "sent_at TIMESTAMP")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_send_log_day ON email_send_log(status, sent_at)")
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
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_email_send_log_draft_status "
        "ON email_send_log(draft_id, status) WHERE draft_id IS NOT NULL"
    )
    conn.commit()


def db_conn() -> sqlite3.Connection:
    db_path = enforce_write_path(DB_PATH, "email-reader-db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    return conn


def log_agent(conn: sqlite3.Connection, action: str, detail: str, result: str) -> None:
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("Hunter", action, detail[:500], result[:200], _ist_now().replace(microsecond=0).isoformat()),
    )


def log_error(conn: sqlite3.Connection, action: str, error_type: str, message: str, detail: str = "") -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    now = _ist_now().replace(microsecond=0).isoformat()
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        ("Hunter", action, safe_type, message[:300], detail[:500], now),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("Hunter", action, detail[:500], safe_type, now),
    )


def decode_value(value: str | None) -> str:
    if not value:
        return ""
    decoded = decode_header(value)
    parts = []
    for chunk, enc in decoded:
        if isinstance(chunk, bytes):
            try:
                parts.append(chunk.decode(enc or "utf-8", errors="ignore"))
            except Exception as exc:
                logger.warning(
                    "[Saturn] email header decode fallback used: encoding=%s error=%s",
                    enc or "utf-8",
                    exc,
                )
                parts.append(chunk.decode("utf-8", errors="ignore"))
        else:
            parts.append(chunk)
    return "".join(parts).strip()


def extract_text_body(msg: email.message.Message) -> str:
    if msg.is_multipart():
        plain_parts: list[str] = []
        html_parts: list[str] = []
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            disposition = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="ignore")
            if content_type == "text/plain":
                plain_parts.append(text)
            elif content_type == "text/html":
                html_parts.append(text)
        if plain_parts:
            return "\n".join(plain_parts).strip()
        if html_parts:
            html = "\n".join(html_parts)
            text = re.sub(r"<[^>]+>", " ", html)
            return re.sub(r"\s+", " ", text).strip()
        return ""

    payload = msg.get_payload(decode=True) or b""
    charset = msg.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="ignore").strip()


def classify_reply(subject: str, body: str = "") -> str:
    subject_lc = subject.lower()
    body_lc = body.lower()
    combined_lc = f"{subject_lc} {body_lc}".strip()

    if any(term in subject_lc for term in OOO_SUBJECT_TERMS):
        return "out_of_office"
    if any(term in combined_lc for term in NOT_INTERESTED_TERMS):
        return "not_interested"
    if any(term in combined_lc for term in INTERESTED_TERMS):
        return "interested"
    return "unknown"


def read_replies(max_emails: int = 20) -> list[dict] | dict:
    conn: sqlite3.Connection | None = None
    results: list[dict] = []
    imap: imaplib.IMAP4_SSL | None = None
    try:
        conn = db_conn()
        missing_gmail = _missing_gmail_config_fields()
        if missing_gmail:
            log_error(
                conn,
                "read_replies",
                "AUTH_ERROR",
                "gmail credentials missing",
                ",".join(missing_gmail),
            )
            conn.commit()
            return {"status": "error", "type": "GMAIL_NOT_CONFIGURED"}

        _u = os.environ.get("GMAIL_USER", "")
        _p = os.environ.get("GMAIL_APP_PASSWORD", "")

        username = (_u or "").strip()
        password = (_p or "").strip()
        if not username or not password:
            log_error(conn, "read_replies", "AUTH_ERROR", "gmail credentials missing", "")
            conn.commit()
            return {"status": "error", "type": "GMAIL_NOT_CONFIGURED"}

        imap = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        imap.login(username, password)
        status, _ = imap.select("INBOX")
        if status != "OK":
            log_error(conn, "read_replies", "API_ERROR", "failed to select INBOX", status)
            conn.commit()
            return []

        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            log_error(conn, "read_replies", "API_ERROR", "failed to search unseen emails", status)
            conn.commit()
            return []

        ids = data[0].split() if data and data[0] else []
        if not ids:
            log_agent(conn, "reply_classified", "no unseen inbox replies", "success")
            conn.commit()
            return []

        try:
            max_emails = max(1, min(int(max_emails or 20), 100))
        except (TypeError, ValueError):
            log_error(conn, "read_replies", "LOGIC_ERROR", "invalid max_emails value", str(max_emails))
            conn.commit()
            return {"status": "error", "type": "INVALID_MAX_EMAILS"}
        for msg_id in ids[-max_emails:]:
            try:
                fetch_status, msg_data = imap.fetch(msg_id, "(RFC822)")
                if fetch_status != "OK" or not msg_data:
                    log_error(conn, "read_replies", "API_ERROR", "failed to fetch email", str(msg_id))
                    continue

                raw_payload = None
                for part in msg_data:
                    if isinstance(part, tuple) and len(part) > 1:
                        raw_payload = part[1]
                        break
                if not raw_payload:
                    log_error(conn, "read_replies", "API_ERROR", "email payload missing", str(msg_id))
                    continue

                message = email.message_from_bytes(raw_payload)
                sender_email = parseaddr(message.get("From", ""))[1].strip().lower()
                sender = sender_email
                subject = decode_value(message.get("Subject"))
                body = extract_text_body(message)
                date_header = decode_value(message.get("Date"))

                lead_id = None
                if sender_email:
                    row = conn.execute(
                        "SELECT id FROM leads WHERE lower(trim(email))=? ORDER BY id ASC LIMIT 1",
                        (sender_email,),
                    ).fetchone()
                    if row:
                        lead_id = int(row[0])
                    else:
                        log_error(conn, "read_replies", "LOGIC_ERROR", "unlinked reply sender", sender_email)
                else:
                    log_error(conn, "read_replies", "LOGIC_ERROR", "reply sender missing", str(msg_id))

                classification = classify_reply(subject, body)
                if lead_id is not None:
                    reply_now = _ist_now().replace(microsecond=0).isoformat()
                    reply_logged = conn.execute(
                        """
                        SELECT id
                        FROM email_send_log
                        WHERE lead_id=?
                          AND status='reply_received'
                          AND sent_at >= datetime('now','+5 hours','+30 minutes','-1 day')
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (lead_id,),
                    ).fetchone()
                    if not reply_logged:
                        conn.execute(
                            """
                            INSERT INTO email_send_log (lead_id, draft_id, status, attempt_count, error_category, sent_at)
                            VALUES (?, NULL, 'reply_received', 1, NULL, ?)
                            """,
                            (lead_id, reply_now),
                        )
                    current = conn.execute(
                        "SELECT status FROM leads WHERE id=?", (lead_id,)
                    ).fetchone()
                    current_status = (current[0] or "").lower() if current else ""
                    if classification == "interested" and current_status == "contacted":
                        conn.execute(
                            "UPDATE leads SET status='qualified', updated_at=? WHERE id=?",
                            (reply_now, lead_id),
                        )
                    elif classification == "not_interested" and current_status == "contacted":
                        conn.execute(
                            "UPDATE leads SET status='lost', updated_at=? WHERE id=?",
                            (reply_now, lead_id),
                        )

                log_agent(
                    conn,
                    "reply_classified",
                    json.dumps(
                        {
                            "sender": sender,
                            "subject": subject,
                            "classification": classification,
                            "lead_id": lead_id,
                            "date": date_header,
                        }
                    ),
                    "success",
                )
                results.append(
                    {
                        "sender": sender_email,
                        "subject": subject,
                        "classification": classification,
                        "lead_id": lead_id,
                    }
                )
            except Exception as exc:
                log_error(conn, "read_replies", "API_ERROR", "email processing failure", f"msg_id={msg_id} error={str(exc)[:300]}")
                continue

        conn.commit()
        return results
    except imaplib.IMAP4.error as exc:
        try:
            log_error(conn, "read_replies", "AUTH_ERROR", "imap authentication failure", str(exc))
            conn.commit()
        except (AttributeError, sqlite3.Error) as log_exc:
            logger.warning("[Saturn] email_reader auth failure logging failed: %s", log_exc)
        return {"status": "error", "type": "IMAP_AUTH_FAILURE"}
    except sqlite3.Error as exc:
        try:
            log_error(conn, "read_replies", "DB_ERROR", "database failure", str(exc))
            conn.commit()
        except (AttributeError, sqlite3.Error) as log_exc:
            logger.warning("[Saturn] email_reader DB failure logging failed: %s", log_exc)
        return {"status": "error", "type": "DB_ERROR"}
    except Exception as exc:
        try:
            log_error(conn, "read_replies", "NETWORK_ERROR", "imap read failure", str(exc))
            conn.commit()
        except (AttributeError, sqlite3.Error) as log_exc:
            logger.warning("[Saturn] email_reader runtime failure logging failed: %s", log_exc)
        return {"status": "error", "type": "IMAP_READ_FAILURE"}
    finally:
        if imap is not None:
            try:
                imap.close()
            except Exception as exc:
                logger.warning("[Saturn] email_reader IMAP close failed: %s", exc)
            try:
                imap.logout()
            except Exception as exc:
                logger.warning("[Saturn] email_reader IMAP logout failed: %s", exc)
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    out = read_replies(max_emails=20)
    if isinstance(out, dict):
        print(json.dumps(out, ensure_ascii=True))
    else:
        print(json.dumps({"processed": len(out), "replies": out}, ensure_ascii=True))
