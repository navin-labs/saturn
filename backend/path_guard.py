from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from configs.paths import BASE_PATH, LOGS_DIR, SECURITY_LOG, ensure_structure


def _within_base(path: Path) -> bool:
    base = BASE_PATH.resolve()
    target = path.resolve()
    return target == base or base in target.parents


def _log_violation(target: Path, purpose: str) -> None:
    ensure_structure()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now(timezone.utc).isoformat()}Z DENY purpose={purpose} path={target}\n"
    with SECURITY_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line)


def enforce_write_path(path: str | Path, purpose: str = "write") -> Path:
    target = Path(path).expanduser().resolve()
    if _within_base(target):
        return target
    _log_violation(target, purpose)
    raise PermissionError(f"Write blocked outside BASE_PATH: {target}")
