import sqlite3
from pathlib import Path
from typing import Any

APP_CONFIG_TABLE = "app_config"


def connect(db_path: str | Path) -> sqlite3.Connection:
    """
    Connect to the SQLite database with basic hardening to avoid lock issues.
    """
    db_file = Path(db_path)
    if not db_file.is_absolute():
        db_file = Path.cwd() / db_file
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {APP_CONFIG_TABLE} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def get_config(conn: sqlite3.Connection, key: str) -> str | None:
    _ensure_table(conn)
    row = conn.execute(
        f"SELECT value FROM {APP_CONFIG_TABLE} WHERE key = ?",
        (key,),
    ).fetchone()
    return row[0] if row else None


def set_config(conn: sqlite3.Connection, key: str, value: Any) -> None:
    _ensure_table(conn)
    conn.execute(
        f"""
        INSERT INTO {APP_CONFIG_TABLE} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value=excluded.value
        """,
        (key, str(value)),
    )
    conn.commit()


def get_bool(conn: sqlite3.Connection, key: str) -> bool | None:
    value = get_config(conn, key)
    if value is None:
        return None
    return str(value) == "1"


def set_bool(conn: sqlite3.Connection, key: str, value: bool) -> None:
    set_config(conn, key, "1" if value else "0")


def get_float(conn: sqlite3.Connection, key: str) -> float | None:
    value = get_config(conn, key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def set_float(conn: sqlite3.Connection, key: str, value: float) -> None:
    set_config(conn, key, str(value))
