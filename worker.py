import os
import time
import uuid
import signal
import subprocess

from config import get_config
from jobs import (
    fetch_job_for_processing,
    mark_job_completed,
    mark_job_failed,
    get_control_flag,
    upsert_worker,
    requeue_stuck_jobs,
)
from utils import log

stop_signaled = False


def _signal_handler(signum, frame):
    global stop_signaled
    stop_signaled = True
    log("worker_signal", signum=signum)


def run_worker(poll_interval=2):
    global stop_signaled
    worker_id = str(uuid.uuid4())
    pid = os.getpid()
    log("worker_started", worker_id=worker_id, pid=pid)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    while True:
        upsert_worker(worker_id, pid)

        if stop_signaled or get_control_flag("stop_workers") == "1":
            log("worker_stopping", worker_id=worker_id)
            break

        if int(time.time()) % 10 == 0:
            requeue_stuck_jobs()

        job = fetch_job_for_processing()
        if not job:
            time.sleep(poll_interval)
            continue

        job_id = job["id"]
        cmd = job["command"]
        attempts = job["attempts"]
        max_retries = job["max_retries"]

        log("job_start", worker_id=worker_id, job_id=job_id, cmd=cmd, attempts=attempts, max_retries=max_retries)

        cmd_timeout = int(get_config("cmd_timeout", 60))

        try:
            res = subprocess.run(cmd, shell=True, timeout=cmd_timeout)
            if res.returncode == 0:
                log("job_completed", worker_id=worker_id, job_id=job_id)
                mark_job_completed(job_id)
            else:
                attempts += 1
                err = f"exit_code={res.returncode}"
                log("job_failed", worker_id=worker_id, job_id=job_id, error=err, attempts=attempts)
                mark_job_failed(job_id, attempts, max_retries, err)
        except subprocess.TimeoutExpired:
            attempts += 1
            err = f"timeout_after_{cmd_timeout}s"
            log("job_failed_timeout", worker_id=worker_id, job_id=job_id, error=err, attempts=attempts)
            mark_job_failed(job_id, attempts, max_retries, err)
        except Exception as e:
            attempts += 1
            err = f"exception: {e}"
            log("job_failed_exception", worker_id=worker_id, job_id=job_id, error=err, attempts=attempts)
            mark_job_failed(job_id, attempts, max_retries, err)

        time.sleep(0.2)

    log("worker_exited", worker_id=worker_id)
