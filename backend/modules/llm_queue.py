"""
Saturn LLM Queue - Central rate limiter for all Gemini API calls.

Architecture:
  - Token bucket: max 1 call per MIN_INTERVAL seconds (smooths burst)
  - Threading lock: thread-safe across Flask threads and timer processes
  - Exponential backoff with jitter on 429
  - Model fallback chain on sustained rate limit
  - Persistent=false on timers prevents catch-up runs (set separately)

Based on: token bucket algorithm used in production LLM systems.
Reference: rateLLMiter (llmonpy/ratellmiter), smooths requests over the minute
           rather than bursting at start.
"""
import datetime
import logging
import os
import random
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
_MAX_RETRIES = 5
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 60.0

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
            time.sleep(_MIN_INTERVAL - gap + random.uniform(0, 0.1))
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
    conn = sqlite3.connect(str(enforce_write_path(DB_PATH, "sqlite-db-write")), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=10000")
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
        except (TypeError, ValueError):
            pass

    parts = []
    for attr in ("prompt_token_count", "candidates_token_count", "cached_content_token_count"):
        value = getattr(usage, attr, None)
        if value is None:
            continue
        try:
            parts.append(max(0, int(value)))
        except (TypeError, ValueError):
            continue
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
    - Per-model retry with exponential backoff + jitter
    - Model fallback chain on sustained 429

    This is the ONLY function that should call the Gemini API.
    All MCP tools must route through this function.
    """
    import google.generativeai as genai

    safe_agent = (agent or "saturn").strip().lower() or "saturn"
    safe_action = (action or "llm_call").strip() or "llm_call"

    genai.configure(api_key=os.environ.get("GOOGLE_API_KEY", ""))
    full_prompt = f"{system}\n\n{prompt}" if system else prompt

    model_chain = [model_override] + _FALLBACK_MODELS if model_override else list(_FALLBACK_MODELS)
    seen = set()
    model_chain = [model for model in model_chain if not (model in seen or seen.add(model))]

    last_error = None
    with _db_conn() as conn:
        if _agent_tokens_today(conn, safe_agent) >= 50000:
            return {
                "status": "quota_blocked",
                "service": "llm",
                "agent": safe_agent,
                "detail": "daily token limit reached",
            }

    for model_name in model_chain:
        backoff = _BASE_BACKOFF
        for attempt in range(_MAX_RETRIES):
            try:
                _smooth_rate_limit()
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(
                    full_prompt,
                    generation_config=genai.types.GenerationConfig(
                        max_output_tokens=max_tokens,
                        temperature=0.7,
                    ),
                )
                text = _response_text_or_empty(response)
                if not text:
                    raise ValueError(f"{model_name} returned empty response")
                with _db_conn() as conn:
                    _log_token_usage(conn, safe_agent, safe_action, _extract_token_count(response))
                    conn.commit()
                return text
            except Exception as err:
                last_error = err
                if _is_rate_limit_error(err):
                    jitter = random.uniform(0, backoff * 0.3)
                    sleep_t = min(backoff + jitter, _MAX_BACKOFF)
                    logger.warning(
                        "[LLM] 429 on %s attempt %s. Sleeping %.1fs",
                        model_name,
                        attempt + 1,
                        sleep_t,
                    )
                    time.sleep(sleep_t)
                    backoff = min(backoff * 2, _MAX_BACKOFF)
                    continue
                logger.warning("[LLM] %s error (non-429): %s", model_name, err)
                break

    raise RuntimeError(
        f"[Saturn] All LLM models exhausted after {_MAX_RETRIES} attempts. Last error: {last_error}"
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
    _FALLBACK_MODELS = models


def set_min_interval(seconds: float) -> None:
    """Allow overriding the minimum interval (e.g. from env var)."""
    global _MIN_INTERVAL
    _MIN_INTERVAL = seconds
