from datetime import timedelta
from utils import utcnow_iso, now_utc, log
from db import get_conn, DEFAULT_BACKOFF_BASE, DEFAULT_STUCK_AFTER
from config import get_config


def enqueue_job(job_dict):
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()

    job_id = job_dict["id"]
    command = job_dict["command"]
    max_retries = int(job_dict.get("max_retries") or get_config("max_retries", 3))

    cur.execute(
        """INSERT INTO jobs
        (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, last_error, processing_started_at)
        VALUES (?, ?, 'pending', 0, ?, ?, ?, NULL, NULL, NULL)
        """,
        (job_id, command, max_retries, now, now),
    )
    conn.commit()
    conn.close()


def list_jobs(state=None):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at")
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def job_counts():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    return {r["state"]: r["cnt"] for r in rows}


def fetch_job_for_processing():
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    cur.execute(
        """UPDATE jobs
        SET state='processing',
            updated_at=?,
            processing_started_at=?
        WHERE id = (
            SELECT id FROM jobs
            WHERE state='pending' AND (next_run_at IS NULL OR next_run_at <= ?)
            ORDER BY created_at
            LIMIT 1
        )
        RETURNING id, command, attempts, max_retries
        """,
        (now, now, now),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def mark_job_completed(job_id):
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    cur.execute(
        """UPDATE jobs
        SET state='completed',
            updated_at=?,
            last_error=NULL,
            processing_started_at=NULL
        WHERE id=?
        """,
        (now, job_id),
    )
    conn.commit()
    conn.close()


def mark_job_failed(job_id, attempts, max_retries, error_msg):
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    backoff_base = int(get_config("backoff_base", DEFAULT_BACKOFF_BASE))

    if attempts < max_retries:
        delay = backoff_base ** attempts
        next_run = (now_utc() + timedelta(seconds=delay)).replace(microsecond=0).isoformat()
        cur.execute(
            """UPDATE jobs
            SET state='pending',
                attempts=?,
                updated_at=?,
                next_run_at=?,
                last_error=?,
                processing_started_at=NULL
            WHERE id=?
            """,
            (attempts, now, next_run, error_msg, job_id),
        )
    else:
        cur.execute(
            """UPDATE jobs
            SET state='dead',
                attempts=?,
                updated_at=?,
                next_run_at=NULL,
                last_error=?,
                processing_started_at=NULL
            WHERE id=?
            """,
            (attempts, now, error_msg, job_id),
        )

    conn.commit()
    conn.close()


def list_dlq():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at DESC")
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def retry_dlq_job(job_id):
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    cur.execute(
        """UPDATE jobs
        SET state='pending',
            attempts=0,
            updated_at=?,
            next_run_at=NULL,
            last_error=NULL,
            processing_started_at=NULL
        WHERE id=? AND state='dead'
        """,
        (now, job_id),
    )
    changed = cur.rowcount
    conn.commit()
    conn.close()
    return changed > 0


def set_control_flag(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO control(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )
    conn.commit()
    conn.close()


def get_control_flag(key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM control WHERE key=?", (key,))
    row = cur.fetchone()
    return row["value"] if row else None


def upsert_worker(worker_id, pid):
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    cur.execute(
        """INSERT INTO workers(worker_id, pid, last_seen)
        VALUES(?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET pid=excluded.pid, last_seen=excluded.last_seen
        """,
        (worker_id, pid, now),
    )
    conn.commit()
    conn.close()


def list_workers():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM workers")
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def requeue_stuck_jobs():
    stuck_after = int(get_config("stuck_after", DEFAULT_STUCK_AFTER))
    cutoff = (now_utc() - timedelta(seconds=stuck_after)).replace(microsecond=0).isoformat()
    conn = get_conn()
    cur = conn.cursor()
    now = utcnow_iso()
    cur.execute(
        """UPDATE jobs
        SET state='pending',
            updated_at=?,
            processing_started_at=NULL,
            last_error='requeued_by_reaper'
        WHERE state='processing'
          AND processing_started_at IS NOT NULL
          AND processing_started_at < ?
        """,
        (now, cutoff),
    )
    count = cur.rowcount
    conn.commit()
    conn.close()
    if count:
        log("reaper_requeued", count=count)
