from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3
from urllib.parse import urlparse

import httpx

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH

logger = logging.getLogger("saturn.web_search")
ERROR_TYPES = {"API_ERROR", "AUTH_ERROR", "RATE_LIMIT", "NETWORK_ERROR", "DB_ERROR", "LOGIC_ERROR"}


def utc_now() -> str:
    tz_ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    return dt.datetime.now(tz=tz_ist).replace(microsecond=0).isoformat()


def db_conn() -> sqlite3.Connection:
    db_path = enforce_write_path(DB_PATH, "web-search-db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
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


def log_error(
    conn: sqlite3.Connection,
    agent: str,
    action: str,
    error_type: str,
    message: str,
    detail: str = "",
) -> None:
    safe_type = error_type if error_type in ERROR_TYPES else "LOGIC_ERROR"
    now = utc_now()
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        (agent, action, safe_type, message[:300], detail[:500], now),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        (agent, action, detail[:500], safe_type, now),
    )


def ensure_service_counter(conn: sqlite3.Connection, service: str, provider: str) -> sqlite3.Row:
    today = dt.datetime.now(
        tz=dt.timezone(dt.timedelta(hours=5, minutes=30))
    ).date().isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO api_usage_log
            (agent, provider, endpoint, status, error_type, detail, called_at, service, usage_date, call_count, quota_limit, paused)
        VALUES
            ('saturn', ?, 'daily_counter', 'success', '', '', ?, ?, ?, 0, 0, 0)
        """,
        (provider, utc_now(), service, today),
    )
    row = conn.execute(
        """
        SELECT id, COALESCE(paused,0) AS paused, COALESCE(call_count,0) AS call_count
        FROM api_usage_log
        WHERE service=? AND usage_date=? AND endpoint='daily_counter'
        ORDER BY id DESC
        LIMIT 1
        """,
        (service, today),
    ).fetchone()
    if row is None:
        raise sqlite3.DatabaseError("failed to initialize api_usage_log daily_counter row")
    return row


def row_get(row: sqlite3.Row | tuple, key: str, index: int, default: int = 0) -> int:
    if row is None:
        return default
    try:
        if isinstance(row, sqlite3.Row):
            return int(row[key] or default)
        return int(row[index] or default)
    except Exception as exc:
        logger.warning("web_search row_get failed", exc_info=exc)
        return default


def increment_service_counter(conn: sqlite3.Connection, service: str) -> None:
    conn.execute(
        """
        UPDATE api_usage_log
        SET call_count=COALESCE(call_count,0)+1, called_at=?
        WHERE service=? AND usage_date=? AND endpoint='daily_counter'
        """,
        (
            utc_now(),
            service,
            dt.datetime.now(
                tz=dt.timezone(dt.timedelta(hours=5, minutes=30))
            ).date().isoformat(),
        ),
    )


def search(query: str, max_results: int = 10, conn: sqlite3.Connection | None = None) -> list[dict]:
    query = (query or "").strip()
    if not query:
        return []

    own_conn = conn is None
    conn = conn or db_conn()
    try:
        counter = ensure_service_counter(conn, "serpapi", "serpapi")
        if row_get(counter, "paused", 1, 0) == 1:
            log_agent(conn, "Hunter", "web_search_paused", "service=serpapi daily pause active", "warning")
            conn.commit()
            return []

        api_key = (os.environ.get("SERPAPI_KEY", "") or "").strip()
        if not api_key:
            log_error(conn, "Hunter", "web_search", "AUTH_ERROR", "SERPAPI_KEY not set", "")
            conn.commit()
            return []

        max_results = max(1, min(int(max_results or 10), 20))
        params = {
            "engine": "google",
            "q": query,
            "num": str(max_results),
            "api_key": api_key,
        }

        attempted_call = False
        try:
            attempted_call = True
            response = httpx.get("https://serpapi.com/search.json", params=params, timeout=10.0)
            response.raise_for_status()
            payload = response.json()
            increment_service_counter(conn, "serpapi")
        except httpx.TimeoutException as exc:
            if attempted_call:
                increment_service_counter(conn, "serpapi")
            log_error(conn, "Hunter", "web_search", "NETWORK_ERROR", "SerpAPI timeout", str(exc))
            conn.commit()
            return []
        except httpx.HTTPError as exc:
            if attempted_call:
                increment_service_counter(conn, "serpapi")
            err_type = "RATE_LIMIT" if "429" in str(exc) else "API_ERROR"
            log_error(conn, "Hunter", "web_search", err_type, "SerpAPI request failed", str(exc))
            conn.commit()
            return []
        except Exception as exc:
            if attempted_call:
                increment_service_counter(conn, "serpapi")
            log_error(conn, "Hunter", "web_search", "API_ERROR", "Unexpected SerpAPI failure", str(exc))
            conn.commit()
            return []

        rows = payload.get("organic_results") or []
        if not isinstance(rows, list):
            rows = []

        results: list[dict] = []
        for row in rows[:max_results]:
            link = str(row.get("link") or "").strip()
            parsed = urlparse(link)
            results.append(
                {
                    "title": str(row.get("title") or "").strip(),
                    "link": link,
                    "snippet": str(row.get("snippet") or "").strip(),
                    "domain": (parsed.netloc or "").lower(),
                }
            )

        log_agent(conn, "Hunter", "web_search", f"query={query} results={len(results)}", "success")
        conn.commit()
        return results
    except sqlite3.Error as exc:
        try:
            log_error(conn, "Hunter", "web_search", "DB_ERROR", "database write failure", str(exc))
            conn.commit()
        except sqlite3.Error as log_exc:
            logger.warning("web_search DB error logging failed", exc_info=log_exc)
        return []
    finally:
        if own_conn:
            conn.close()
