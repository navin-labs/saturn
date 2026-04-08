"""
Saturn LLM Queue - Central rate limiter for all Gemini API calls.

Architecture:
  - Token bucket: max 1 call per MIN_INTERVAL seconds (smooths burst)
  - Threading lock: thread-safe across Flask threads and timer processes
  - Immediate graceful return on 429 / quota
  - No retry loops on rate limit
  - Persistent=false on timers prevents catch-up runs (set separately)

Based on: token bucket algorithm used in production LLM systems.
Reference: rateLLMiter (llmonpy/ratellmiter), smooths requests over the minute
           rather than bursting at start.
"""
import datetime
import logging
import os
import sqlite3
import threading
import time

from backend.path_guard import enforce_write_path
from configs.paths import DB_PATH

logger = logging.getLogger("saturn.llm_queue")

# -- RATE LIMIT CONFIG -------------------------------------------------------
# Gemini 2.5 Flash: 1000 RPM = ~16/sec. We use 1.5s interval = ~40 RPM.
# This leaves enormous headroom while preventing burst from simultaneous timers.
_MIN_INTERVAL = 1.5
_RATE_LIMIT_MESSAGE = "Rate limit reached. Try again in a few minutes."
_RATE_LIMIT_RETRY_AFTER = 60

# -- RATE LIMIT STATE (module-level, shared across all callers) --------------
_last_call_time = 0.0
_call_lock = threading.Lock()
_token_log_lock = threading.Lock()

# -- MODEL FALLBACK CHAIN ----------------------------------------------------
# Format depends on SDK - set at runtime from saturn-server.py
_FALLBACK_MODELS = [
    "models/gemini-2.5-flash",
    "models/gemini-2.0-flash",
    "models/gemini-2.0-flash-lite",
]


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
        CREATE TABLE IF NOT EXISTS token_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            action TEXT NOT NULL,
            tokens_used INTEGER NOT NULL,
            log_date TEXT NOT NULL,
            logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS api_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL DEFAULT 'system',
            provider TEXT NOT NULL DEFAULT 'gemini',
            endpoint TEXT,
            status TEXT NOT NULL DEFAULT 'success',
            error_type TEXT,
            detail TEXT,
            called_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            service TEXT,
            usage_date TEXT,
            call_date TEXT,
            call_count INTEGER DEFAULT 0,
            quota_limit INTEGER DEFAULT 0,
            paused INTEGER DEFAULT 0
        )
        """
    )
    _add_column_if_missing(conn, "token_usage_log", "logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    _add_column_if_missing(conn, "api_usage_log", "service TEXT")
    _add_column_if_missing(conn, "api_usage_log", "usage_date TEXT")
    _add_column_if_missing(conn, "api_usage_log", "call_date TEXT")
    _add_column_if_missing(conn, "api_usage_log", "call_count INTEGER DEFAULT 0")
    _add_column_if_missing(conn, "api_usage_log", "quota_limit INTEGER DEFAULT 0")
    _add_column_if_missing(conn, "api_usage_log", "paused INTEGER DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_token_usage_agent_day ON token_usage_log(agent, log_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_service_day ON api_usage_log(service, usage_date, paused)")
    conn.commit()


def _response_text_or_empty(response: object) -> str:
    try:
        text = getattr(response, "text", "")
        if text:
            return str(text).strip()
    except Exception as exc:
        logger.debug("[LLM] response.text access failed: %s", exc)
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None) or []
        fragments = []
        for part in parts:
            value = getattr(part, "text", None)
            if value:
                fragments.append(str(value))
        if fragments:
            return "".join(fragments).strip()
    return ""


def _smooth_rate_limit() -> None:
    """
    Token bucket: block until MIN_INTERVAL has passed since last call.
    Thread-safe. Smooths burst into an evenly-spaced queue.
    """
    global _last_call_time
    with _call_lock:
        now = time.monotonic()
        gap = now - _last_call_time
        if gap < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - gap)
        _last_call_time = time.monotonic()


def _is_rate_limit_error(err: Exception) -> bool:
    """Detect 429 / quota errors across both old and new Gemini SDKs."""
    msg = str(err).lower()
    return any(
        token in msg
        for token in [
            "429",
            "quota",
            "rate_limit",
            "resource_exhausted",
            "too_many_requests",
            "ratequota",
            "exhausted",
            "rate limit",
            "ratelimit",
        ]
    )


def _db_conn() -> sqlite3.Connection:
    db_path = enforce_write_path(DB_PATH, "sqlite-db-write")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=10000")
    _ensure_schema(conn)
    return conn


def _today_ist() -> str:
    tz_ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    return datetime.datetime.now(tz=tz_ist).date().isoformat()


def _agent_tokens_today(conn: sqlite3.Connection, agent: str) -> int:
    safe_agent = (agent or "saturn").strip().lower() or "saturn"
    today = _today_ist()
    row = conn.execute(
        """
        SELECT COALESCE(SUM(tokens_used), 0)
        FROM token_usage_log
        WHERE log_date=? AND lower(agent)=lower(?)
        """,
        (today, safe_agent),
    ).fetchone()
    return int(row[0] or 0)


def _extract_token_count(response: object) -> int:
    usage = getattr(response, "usage_metadata", None) if response is not None else None
    if usage is None:
        return 0

    total = getattr(usage, "total_token_count", None)
    if total is not None:
        try:
            return max(0, int(total))
        except (TypeError, ValueError) as exc:
            logger.debug("[LLM] invalid total_token_count: %s", exc)

    parts = []
    for attr in ("prompt_token_count", "candidates_token_count", "cached_content_token_count"):
        value = getattr(usage, attr, None)
        if value is None:
            continue
        try:
            parts.append(max(0, int(value)))
        except (TypeError, ValueError) as exc:
            logger.debug("[LLM] invalid %s token count: %s", attr, exc)
    return sum(parts)


def _log_token_usage(conn: sqlite3.Connection, agent: str, action: str, tokens_used: int) -> None:
    safe_agent = (agent or "saturn").strip().lower() or "saturn"
    safe_action = (action or "llm_call").strip() or "llm_call"
    safe_tokens = max(0, int(tokens_used or 0))
    today = _today_ist()
    conn.execute(
        """
        INSERT INTO token_usage_log (agent, action, tokens_used, log_date)
        VALUES (?,?,?,?)
        """,
        (safe_agent, safe_action, safe_tokens, today),
    )


def _log_system_error(
    conn: sqlite3.Connection,
    action: str,
    error_type: str,
    message: str,
    detail: str = "",
) -> None:
    now = datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    ).replace(microsecond=0).isoformat()
    conn.execute(
        "INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)",
        ("system", action, error_type, str(message)[:300], str(detail)[:500], now),
    )
    conn.execute(
        "INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)",
        ("system", action, str(detail)[:500], error_type, now),
    )


def _rate_limited_response(service: str, agent: str, detail: str = "") -> dict:
    return {
        "status": "rate_limited",
        "message": _RATE_LIMIT_MESSAGE,
        "retry_after": _RATE_LIMIT_RETRY_AFTER,
        "service": service,
        "agent": agent,
        "detail": str(detail)[:500],
    }


def call_llm(
    prompt: str,
    system: str = "",
    max_tokens: int = 1000,
    model_override: str = "",
    agent: str = "saturn",
    action: str = "llm_call",
) -> str | dict:
    """
    Central LLM call with:
    - Shared rate limit (token bucket, smooths burst)
    - Immediate graceful return on 429 / quota
    - Model fallback chain for non-rate-limit provider errors

    This is the ONLY function that should call the Gemini API.
    All MCP tools must route through this function.
    """
    safe_agent = (agent or "saturn").strip().lower() or "saturn"
    safe_action = (action or "llm_call").strip() or "llm_call"
    normalized_prompt = str(prompt or "").strip()
    if not normalized_prompt:
        raise ValueError("LLM prompt is empty")
    if max_tokens <= 0:
        raise ValueError("max_tokens must be greater than zero")
    api_key = (os.environ.get("GOOGLE_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is not configured")

    import google.generativeai as genai

    genai.configure(api_key=api_key)
    full_prompt = f"{system}\n\n{normalized_prompt}" if system else normalized_prompt

    model_chain = [model_override] + _FALLBACK_MODELS if model_override else list(_FALLBACK_MODELS)
    seen = set()
    model_chain = [model for model in model_chain if not (model in seen or seen.add(model))]

    last_error = None
    try:
        with _db_conn() as conn:
            if _agent_tokens_today(conn, safe_agent) >= 50000:
                logger.warning("[LLM] token quota blocked for agent=%s", safe_agent)
                _log_system_error(
                    conn,
                    "rate_limit",
                    "RATE_LIMIT",
                    "daily token limit reached",
                    f"service=llm agent={safe_agent} action={safe_action}",
                )
                conn.commit()
                return _rate_limited_response("llm", safe_agent, "daily token limit reached")
    except sqlite3.Error as err:
        logger.warning("[LLM] quota precheck unavailable: %s", err)

    for model_name in model_chain:
        try:
            _smooth_rate_limit()
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(
                full_prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_tokens,
                    temperature=0.0,
                ),
            )
            text = _response_text_or_empty(response)
            if not text:
                raise ValueError(f"{model_name} returned empty response")
            try:
                from configs.saturn_server import increment_service_daily_usage

                with _db_conn() as conn:
                    _log_token_usage(conn, safe_agent, safe_action, _extract_token_count(response))
                    increment_service_daily_usage(conn, "gemini")
                    conn.commit()
            except Exception as usage_err:
                logger.warning("[LLM] usage logging failed after successful response: %s", usage_err)
            return text
        except Exception as err:
            last_error = err
            if _is_rate_limit_error(err):
                logger.warning("[LLM] rate limited on %s: %s", model_name, err)
                try:
                    with _db_conn() as conn:
                        _log_system_error(
                            conn,
                            "rate_limit",
                            "RATE_LIMIT",
                            str(err),
                            f"service=llm agent={safe_agent} action={safe_action} model={model_name}",
                        )
                        conn.commit()
                except sqlite3.Error as log_err:
                    logger.warning("[LLM] rate limit logging failed: %s", log_err)
                return _rate_limited_response("llm", safe_agent, str(err))
            logger.warning("[LLM] %s error (non-rate-limit): %s", model_name, err)
            continue

    raise RuntimeError(
        f"[Saturn] All LLM models exhausted. Last error: {last_error}"
    )


def call_llm_queued(
    prompt: str,
    system: str = "",
    max_tokens: int = 1000,
    model_override: str = "",
    agent: str = "saturn",
    action: str = "llm_call",
) -> str | dict:
    return call_llm(
        prompt,
        system=system,
        max_tokens=max_tokens,
        model_override=model_override,
        agent=agent,
        action=action,
    )


def set_fallback_models(models: list) -> None:
    """Allow saturn-server.py to set the model list at startup."""
    global _FALLBACK_MODELS
    cleaned = [str(model).strip() for model in (models or []) if str(model).strip()]
    if cleaned:
        _FALLBACK_MODELS = cleaned


def set_min_interval(seconds: float) -> None:
    """Allow overriding the minimum interval (e.g. from env var)."""
    global _MIN_INTERVAL
    try:
        parsed = float(seconds)
    except (TypeError, ValueError) as exc:
        logger.warning("[LLM] invalid min interval %r: %s", seconds, exc)
        return
    if parsed > 0:
        _MIN_INTERVAL = parsed
