from __future__ import annotations

import os
from pathlib import Path

BASE_PATH = Path(os.environ.get("SATURN_BASE_PATH", str(Path.home() / "Workspace" / "Saturn"))).expanduser().resolve()
BACKEND_DIR = BASE_PATH / "backend"
DATABASE_DIR = BASE_PATH / "database"
LOGS_DIR = BASE_PATH / "logs"
SCRIPTS_DIR = BASE_PATH / "scripts"
CONFIGS_DIR = BASE_PATH / "configs"
DOCS_DIR = BASE_PATH / "docs"
SKILLS_DIR = BASE_PATH / "skills"
CONTROL_DIR = BASE_PATH / ".saturn"
DB_PATH = DATABASE_DIR / "saturn.db"
SECURITY_LOG = LOGS_DIR / "security.log"

REQUIRED_DIRS = [
    BASE_PATH,
    BACKEND_DIR,
    DATABASE_DIR,
    LOGS_DIR,
    SCRIPTS_DIR,
    CONFIGS_DIR,
    DOCS_DIR,
    SKILLS_DIR,
    CONTROL_DIR,
]


def ensure_structure() -> None:
    for p in REQUIRED_DIRS:
        p.mkdir(parents=True, exist_ok=True)
