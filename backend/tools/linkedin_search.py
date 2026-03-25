from __future__ import annotations

import datetime as dt
import os
import random
import sqlite3
import time
from pathlib import Path

import httpx
try:
    from linkedin_api import Linkedin
except Exception:
    Linkedin = None

DB_PATH = Path("/home/navin/Workspace/Saturn/database/saturn.db")
ERROR_TYPES = {"API_ERROR", "AUTH_ERROR", "RATE_LIMIT", "NETWORK_ERROR", "DB_ERROR", "LOGIC_ERROR"}
DAILY_LIMIT = 20


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def row_get(row: sqlite3.Row | tuple | None, key: str, index: int, default: int = 0) -> int:
    if row is None:
        return default
    try:
        if isinstance(row, sqlite3.Row):
            return int(row[key] or default)
        return int(row[index] or default)
    except Exception:
        return default


def log_agent(conn: sqlite3.Connection, action: str, detail: str, result: str) -> None:
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("Hunter", action, detail[:500], result[:200], utc_now()),
    )


def log_error(conn: sqlite3.Connection, action: str, error_type: str, message: str, detail: str = "") -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    now = utc_now()
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        ("Hunter", action, safe_type, message[:300], detail[:500], now),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("Hunter", action, detail[:500], safe_type, now),
    )


def ensure_counter(conn: sqlite3.Connection) -> sqlite3.Row | tuple:
    today = dt.date.today().isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO api_usage_log
            (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_count, quota_limit, paused)
        VALUES
            ('hunter', 'linkedin', 'daily_counter', 'success', '', '', ?, 'linkedin', ?, 0, ?, 0)
        """,
        (utc_now(), today, DAILY_LIMIT),
    )
    row = conn.execute(
        """
        SELECT id, COALESCE(call_count,0) AS call_count, COALESCE(paused,0) AS paused
        FROM api_usage_log
        WHERE service='linkedin' AND usage_date=? AND endpoint='daily_counter'
        LIMIT 1
        """,
        (today,),
    ).fetchone()
    if row is None:
        raise sqlite3.DatabaseError("missing linkedin daily counter row")
    return row


def increment_counter(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE api_usage_log
        SET call_count=COALESCE(call_count,0)+1, called_at=?
        WHERE service='linkedin' AND usage_date=? AND endpoint='daily_counter'
        """,
        (utc_now(), dt.date.today().isoformat()),
    )


def pause_linkedin(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE api_usage_log
        SET paused=1, called_at=?
        WHERE service='linkedin' AND usage_date=? AND endpoint='daily_counter'
        """,
        (utc_now(), dt.date.today().isoformat()),
    )


def _is_auth_error(message: str) -> bool:
    text = message.lower()
    return any(
        term in text
        for term in (
            "unauthorized",
            "401",
            "403",
            "checkpoint",
            "invalid credentials",
            "bad_username_or_password",
            "password",
            "auth",
        )
    )


def _is_rate_limited(message: str) -> bool:
    text = message.lower()
    return any(term in text for term in ("429", "rate", "too many requests", "quota", "limit"))


def _clean_name_from_title(title: str) -> str:
    title = (title or "").replace("| LinkedIn", "").strip()
    for sep in (" - ", " | ", " — "):
        if sep in title:
            return title.split(sep, 1)[0].strip()
    return title


def _company_from_snippet(snippet: str) -> str:
    text = (snippet or "").strip()
    for sep in (".", "|", " - "):
        if sep in text:
            return text.split(sep, 1)[0].strip()
    return text[:120].strip()


def _serpapi_search(query: str, conn: sqlite3.Connection) -> list[dict]:
    api_key = (os.environ.get("SERPAPI_KEY", "") or "").strip()
    if not api_key:
        log_error(conn, "linkedin_search", "AUTH_ERROR", "SERPAPI_KEY missing", "")
        return []

    params = {
        "q": query,
        "api_key": api_key,
        "num": 10,
        "hl": "en",
    }

    attempted_call = False
    try:
        time.sleep(random.uniform(1, 3))
        attempted_call = True
        response = httpx.get("https://serpapi.com/search.json", params=params, timeout=10.0)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        if attempted_call:
            increment_counter(conn)
        category = "AUTH_ERROR" if _is_auth_error(str(exc)) else "RATE_LIMIT" if _is_rate_limited(str(exc)) else "NETWORK_ERROR"
        log_error(conn, "linkedin_search", category, "SerpAPI linkedin search failed", str(exc))
        if category == "RATE_LIMIT":
            pause_linkedin(conn)
        return []

    increment_counter(conn)
    rows = payload.get("organic_results") or []
    if not isinstance(rows, list):
        rows = []

    prospects: list[dict] = []
    for row in rows[:10]:
        link = str(row.get("link") or "").strip()
        if "linkedin.com/in/" not in link:
            continue
        title = str(row.get("title") or "").strip()
        snippet = str(row.get("snippet") or "").strip()
        prospects.append(
            {
                "name": _clean_name_from_title(title),
                "company": _company_from_snippet(snippet),
                "profile_url": link,
                "source": "linkedin_serp",
            }
        )

    log_agent(conn, "linkedin_search_serpapi", f"query={query} results={len(prospects)}", "success")
    return prospects


def _linkedin_api_search(niche: str, city: str, conn: sqlite3.Connection) -> list[dict]:
    if Linkedin is None:
        log_error(conn, "linkedin_search", "AUTH_ERROR", "linkedin_api package unavailable", "")
        return []
    linkedin_email = (os.environ.get("LINKEDIN_EMAIL", "") or "").strip()
    linkedin_password = (os.environ.get("LINKEDIN_PASSWORD", "") or "").strip()
    if not linkedin_email or not linkedin_password:
        return []

    attempted_call = False
    try:
        time.sleep(random.uniform(1, 3))
        attempted_call = True
        api = Linkedin(linkedin_email, linkedin_password)
        results = api.search_people(keywords=f"{niche} {city}".strip(), limit=10) or []
    except Exception as exc:
        if attempted_call:
            increment_counter(conn)
        log_error(conn, "linkedin_search", "AUTH_ERROR", "linkedin-api auth/search failed", str(exc))
        return []

    increment_counter(conn)
    prospects: list[dict] = []
    for row in results[:10]:
        first = str(row.get("firstName") or "").strip()
        last = str(row.get("lastName") or "").strip()
        name = f"{first} {last}".strip() or str(row.get("title") or "").strip()
        company = str(row.get("companyName") or row.get("company") or "").strip()
        public_id = str(row.get("public_id") or row.get("publicIdentifier") or "").strip()
        profile_url = f"https://www.linkedin.com/in/{public_id}" if public_id else ""
        prospects.append(
            {
                "name": name,
                "company": company,
                "profile_url": profile_url,
                "source": "linkedin_api",
            }
        )
    log_agent(conn, "linkedin_search_api", f"results={len(prospects)}", "success")
    return prospects


def search_prospects(
    niche: str,
    city: str,
    service: str,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    own_conn = conn is None
    conn = conn or db_conn()
    try:
        counter = ensure_counter(conn)
        if row_get(counter, "paused", 2, 0) == 1:
            log_agent(conn, "linkedin_search", "service paused", "warning")
            conn.commit()
            return []

        if row_get(counter, "call_count", 1, 0) >= DAILY_LIMIT:
            pause_linkedin(conn)
            log_error(conn, "linkedin_search", "RATE_LIMIT", "daily linkedin action limit reached", str(DAILY_LIMIT))
            conn.commit()
            return []

        query = f'site:linkedin.com/in "{(niche or "").strip()}" "{(city or "").strip()}" {(service or "").strip()}'.strip()
        prospects = _serpapi_search(query, conn)

        if prospects:
            conn.commit()
            return prospects

        # Keep direct linkedin-api login path available, but only after SerpAPI returns 0 results.
        fallback = _linkedin_api_search((niche or "").strip(), (city or "").strip(), conn)
        conn.commit()
        return fallback
    except sqlite3.Error as exc:
        try:
            log_error(conn, "linkedin_search", "DB_ERROR", "database write failure", str(exc))
            conn.commit()
        except sqlite3.Error:
            pass
        return []
    finally:
        if own_conn:
            conn.close()
