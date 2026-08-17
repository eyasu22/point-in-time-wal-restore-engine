from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

from app.persistence.schema import SCHEMA_SQL


def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    if db_path is None or db_path == ":memory:":
        uri = f"file:pitr-{uuid.uuid4().hex}?mode=memory&cache=shared"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
