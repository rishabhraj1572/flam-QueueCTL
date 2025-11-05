from db import get_conn


def get_config(key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    return row["value"] if row else default


def set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def get_all_config():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM config")
    return {r["key"]: r["value"] for r in cur.fetchall()}
