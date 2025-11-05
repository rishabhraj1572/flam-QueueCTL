#!/usr/bin/env python3
import argparse
import json
import sys
import multiprocessing

from db import init_db, get_conn
from jobs import (
    enqueue_job,
    list_jobs,
    job_counts,
    list_dlq,
    retry_dlq_job,
    set_control_flag,
    list_workers,
)
from config import get_config, set_config, get_all_config
from worker import run_worker


def normalize_key(k: str) -> str:
    # allow both max-retries and max_retries
    return k.replace("-", "_")


# ---------- command handlers ----------

def cmd_enqueue(args):
    try:
        job = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print("Invalid JSON:", e)
        sys.exit(1)
    if "id" not in job or "command" not in job:
        print("Job must contain 'id' and 'command'")
        sys.exit(1)
    enqueue_job(job)
    print(f"Enqueued job {job['id']}")


def cmd_status(args):
    counts = job_counts()
    workers = list_workers()
    print("Job states:")
    for st in ["pending", "processing", "completed", "dead"]:
        print(f"  {st}: {counts.get(st, 0)}")

    print("\nWorkers:")
    if not workers:
        print("  (none)")
    else:
        for w in workers:
            print(f"  {w['worker_id']} pid={w['pid']} last_seen={w['last_seen']}")


def cmd_list(args):
    jobs = list_jobs(args.state)
    for j in jobs:
        print(
            f"{j['id']}  {j['state']}  cmd={j['command']}  "
            f"attempts={j['attempts']}/{j['max_retries']}  "
            f"updated_at={j['updated_at']}  last_error={j['last_error']}"
        )


def cmd_dlq(args):
    if args.action == "list":
        dlq = list_dlq()
        if not dlq:
            print("DLQ empty")
            return
        for j in dlq:
            print(
                f"{j['id']} cmd={j['command']} attempts={j['attempts']} last_error={j['last_error']}"
            )
    elif args.action == "retry":
        ok = retry_dlq_job(args.job_id)
        print("OK" if ok else "Not found in DLQ")


def worker_process():
    # target for multiprocessing
    run_worker()


def cmd_worker(args):
    if args.action == "start":
        count = args.count or 1
        print(f"Starting {count} workers (Ctrl+C to stop here)...")
        procs = []
        for _ in range(count):
            p = multiprocessing.Process(target=worker_process)
            p.start()
            procs.append(p)
        try:
            for p in procs:
                p.join()
        except KeyboardInterrupt:
            set_control_flag("stop_workers", "1")

    elif args.action == "stop":
        # set global stop flag
        set_control_flag("stop_workers", "1")
        print("Stop flag set. Workers will exit gracefully.")

    elif args.action == "clear-stop":
        # remove global stop flag so new workers will run
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM control WHERE key='stop_workers'")
        conn.commit()
        conn.close()
        print("Cleared stop flag. You can start workers again.")


def cmd_config(args):
    if args.subaction == "get":
        if args.key:
            val = get_config(normalize_key(args.key), None)
            print(val if val is not None else "(not set)")
        else:
            cfg = get_all_config()
            for k, v in cfg.items():
                print(f"{k}={v}")
    elif args.subaction == "set":
        set_config(normalize_key(args.key), args.value)
        print(f"{normalize_key(args.key)} set to {args.value}")


# ---------- main / CLI ----------

def main():
    init_db()

    parser = argparse.ArgumentParser(
        prog="queuectl", description="CLI job queue system"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # enqueue
    p_enq = sub.add_parser("enqueue", help="enqueue a new job (JSON)")
    p_enq.add_argument(
        "job_json",
        help='e.g. \'{"id":"job1","command":"echo hi"}\'',
    )
    p_enq.set_defaults(func=cmd_enqueue)

    # status
    p_status = sub.add_parser("status", help="show job stats and workers")
    p_status.set_defaults(func=cmd_status)

    # list
    p_list = sub.add_parser("list", help="list jobs")
    p_list.add_argument("--state", help="filter by state")
    p_list.set_defaults(func=cmd_list)

    # dlq
    p_dlq = sub.add_parser("dlq", help="dead letter queue ops")
    p_dlq_sub = p_dlq.add_subparsers(dest="action", required=True)

    p_dlq_list = p_dlq_sub.add_parser("list", help="list DLQ jobs")
    p_dlq_list.set_defaults(func=cmd_dlq)

    p_dlq_retry = p_dlq_sub.add_parser("retry", help="retry DLQ job")
    p_dlq_retry.add_argument("job_id")
    p_dlq_retry.set_defaults(func=cmd_dlq)

    # worker
    p_worker = sub.add_parser("worker", help="start/stop/clear workers")
    p_worker_sub = p_worker.add_subparsers(dest="action", required=True)

    p_worker_start = p_worker_sub.add_parser("start", help="start worker(s)")
    p_worker_start.add_argument("--count", type=int, help="number of workers")
    p_worker_start.set_defaults(func=cmd_worker)

    p_worker_stop = p_worker_sub.add_parser("stop", help="set global stop flag")
    p_worker_stop.set_defaults(func=cmd_worker)

    p_worker_clear = p_worker_sub.add_parser(
        "clear-stop", help="clear global stop flag so new workers can run"
    )
    p_worker_clear.set_defaults(func=cmd_worker)

    # config
    p_cfg = sub.add_parser("config", help="manage config")
    p_cfg_sub = p_cfg.add_subparsers(dest="subaction", required=True)

    p_cfg_get = p_cfg_sub.add_parser("get", help="get config")
    p_cfg_get.add_argument("key", nargs="?")
    p_cfg_get.set_defaults(func=cmd_config)

    p_cfg_set = p_cfg_sub.add_parser("set", help="set config")
    p_cfg_set.add_argument("key")
    p_cfg_set.add_argument("value")
    p_cfg_set.set_defaults(func=cmd_config)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
