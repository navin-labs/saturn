from __future__ import annotations

import os
from pathlib import Path

BASE_PATH = Path(os.environ.get("SATURN_BASE_PATH", str(Path.home() / "Workspace" / "Saturn"))).expanduser().resolve()
BACKEND_DIR = BASE_PATH / "backend"
AGENTS_DIR = BASE_PATH / "agents"
UI_DIR = BASE_PATH / "ui"
DATABASE_DIR = BASE_PATH / "database"
LOGS_DIR = BASE_PATH / "logs"
SCRIPTS_DIR = BASE_PATH / "scripts"
CONFIGS_DIR = BASE_PATH / "configs"
DOCS_DIR = BASE_PATH / "docs"
ASSETS_DIR = BASE_PATH / "assets"
BACKUPS_DIR = BASE_PATH / "backups"
TEMP_DIR = BASE_PATH / "temp"
ARCHIVE_DIR = BASE_PATH / "archive"
DB_PATH = DATABASE_DIR / "saturn.db"
MIGRATION_LOG = LOGS_DIR / "migration.log"
SECURITY_LOG = LOGS_DIR / "security.log"

REQUIRED_DIRS = [
    BASE_PATH,
    BACKEND_DIR,
    AGENTS_DIR,
    UI_DIR,
    DATABASE_DIR,
    LOGS_DIR,
    SCRIPTS_DIR,
    CONFIGS_DIR,
    DOCS_DIR,
    ASSETS_DIR,
    BACKUPS_DIR,
    TEMP_DIR,
    ARCHIVE_DIR,
    ASSETS_DIR / "images",
    ASSETS_DIR / "videos",
    ASSETS_DIR / "media",
]


def ensure_structure() -> None:
    for p in REQUIRED_DIRS:
        p.mkdir(parents=True, exist_ok=True)
