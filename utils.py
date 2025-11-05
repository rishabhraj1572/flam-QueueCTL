import os
from pathlib import Path
from datetime import datetime, timezone

APP_DIR = os.path.join(Path.home(), ".queuectl")
DB_PATH = os.path.join(APP_DIR, "queue.db")


def ensure_app_dir():
    os.makedirs(APP_DIR, exist_ok=True)


def utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def now_utc():
    return datetime.now(timezone.utc)


def log(msg: str, **extra):
    payload = {"ts": utcnow_iso(), "msg": msg}
    payload.update(extra)
    print(payload, flush=True)
