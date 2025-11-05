import sqlite3
from utils import ensure_app_dir, DB_PATH

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2
DEFAULT_CMD_TIMEOUT = 60
DEFAULT_STUCK_AFTER = 120  # seconds


def get_conn():
    ensure_app_dir()
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            next_run_at TEXT,
            last_error TEXT,
            processing_started_at TEXT
        )"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS control (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY,
            pid INTEGER,
            last_seen TEXT NOT NULL
        )"""
    )

    # defaults
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('max_retries', ?)", (str(DEFAULT_MAX_RETRIES),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('backoff_base', ?)", (str(DEFAULT_BACKOFF_BASE),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('cmd_timeout', ?)", (str(DEFAULT_CMD_TIMEOUT),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('stuck_after', ?)", (str(DEFAULT_STUCK_AFTER),))

    conn.commit()
    conn.close()
