# QueueCTL

A CLI-based background job queue system in Python.  
It lets you enqueue shell commands as jobs, run one or more worker processes to execute them, retry failed jobs with exponential backoff, and move permanently failed jobs to a Dead Letter Queue (DLQ). All state is persisted in SQLite.

---

## 1. Setup Instructions — How to run locally

### Prerequisites
- Python **3.8+**
- macOS / Linux (Windows works but paths differ)

### Install / Run

```bash
git clone https://github.com/rishabhraj1572/flam-QueueCTL.git
cd queuectl
python3 queuectl.py --help
````

First run will create the SQLite DB at:

```text
~/.queuectl/queue.db
```

This is where jobs, config, workers, and control flags are stored.

---

## 2. Usage Examples — CLI commands with example outputs

### Enqueue a job

```bash
python3 queuectl.py enqueue '{"id":"job1","command":"echo Hello"}'
```

Output:

```text
Enqueued job job1
```

### Start workers

```bash
python3 queuectl.py worker clear-stop   # only needed if you stopped workers earlier
python3 queuectl.py worker start --count 2
```

This starts 2 worker processes in this terminal.

### Stop workers (gracefully)

```bash
python3 queuectl.py worker stop
```

This sets a stop flag — all running workers will finish current jobs and exit.

If you want to start again later, clear the flag:

```bash
python3 queuectl.py worker clear-stop
```

### Check status

```bash
python3 queuectl.py status
```

Example:

```text
Job states:
  pending: 1
  processing: 0
  completed: 3
  dead: 1

Workers:
  8793... pid=12345 last_seen=2025-11-05T12:57:55+00:00
```

### List jobs

```bash
python3 queuectl.py list
python3 queuectl.py list --state pending
```

Example:

```text
job1  completed  cmd=echo Hello  attempts=0/3  updated_at=...  last_error=None
job2  dead       cmd=exit 12     attempts=2/2  updated_at=...  last_error=exit_code=12
```

### DLQ

```bash
python3 queuectl.py dlq list
python3 queuectl.py dlq retry job2
```

### Config

```bash
python3 queuectl.py config get
python3 queuectl.py config set max-retries 5
python3 queuectl.py config set backoff_base 2
python3 queuectl.py config set cmd_timeout 60
```

We accept both `max-retries` and `max_retries`.

---

## 3. Architecture Overview — Job lifecycle, data persistence, worker logic

### Files

* **`queuectl.py`** — CLI entrypoint, all subcommands (`enqueue`, `worker`, `status`, `list`, `dlq`, `config`)
* **`db.py`** — SQLite init + connection, creates tables: `jobs`, `config`, `control`, `workers`
* **`config.py`** — get/set config in DB
* **`jobs.py`** — job lifecycle (enqueue, list, fetch for processing, complete, fail → backoff → DLQ), worker heartbeats, stop flag, stuck-job requeue
* **`worker.py`** — long-running worker loop: heartbeat, check stop flag, pick job, run command with timeout, update state
* **`utils.py`** — shared helpers (timestamps, app dir, logging)

### Job Lifecycle

1. **Enqueue**: job inserted as

   * `state = 'pending'`
   * `attempts = 0`
   * `max_retries` from job JSON or config
2. **Worker fetches**:

   * atomic SQL `UPDATE ... RETURNING ...`
   * job → `processing`
   * `processing_started_at` set
3. **Worker runs** the shell command (`subprocess.run(..., shell=True, timeout=cmd_timeout)`)
4. **Success (exit 0)** → job → `completed`
5. **Failure**:

   * attempts += 1
   * if attempts < max_retries → reschedule with exponential backoff

     * `next_run_at = now + (backoff_base ** attempts)`
     * job → `pending` again
   * else → job → `dead` (DLQ)
6. **Reaper**:

   * if a job is stuck in `processing` for longer than `stuck_after` seconds, it is requeued to `pending`
7. **Stop flag**:

   * `queuectl worker stop` sets `stop_workers=1` in DB
   * workers exit on next loop
   * `queuectl worker clear-stop` removes the flag so new workers can start

### Data Persistence

* Everything is in SQLite at `~/.queuectl/queue.db`
* We enable WAL mode for better concurrency
* Jobs survive restarts

---

## 4. Assumptions & Trade-offs

* **Single-node / single-DB**: designed for one machine with multiple worker processes. For multi-node you’d use Postgres/Redis and real distributed locks.
* **Trusted commands**: we run `shell=True` because the assignment examples use shell commands (`echo`, `sleep`). In production you’d restrict this.
* **Simple exponential backoff**: `delay = backoff_base ** attempts` (no cap). Enough for the assignment.
* **Graceful worker management via DB**: we didn’t send OS signals to other processes; we wrote a stop flag in DB so the CLI can stop workers from another terminal.
* **No priority/scheduling**: we always pick the oldest due `pending` job; can be extended.
* **Minimal logging**: printed dicts to stdout; easy to pipe into file or logging system.

---

## 5. Testing Instructions — How to verify functionality

### 5.1 Basic success flow

**Terminal 1**:

```bash
python3 queuectl.py worker clear-stop
python3 queuectl.py worker start --count 1
```

**Terminal 2**:

```bash
python3 queuectl.py enqueue '{"id":"ok1","command":"echo hi"}'
python3 queuectl.py list --state completed
```

You should see `ok1` as `completed`.

---

### 5.2 Failed job → retry → DLQ

```bash
python3 queuectl.py enqueue '{"id":"bad1","command":"exit 12","max_retries":2}'
```

Worker will:

* run it → fail
* retry with backoff
* after 2 attempts, move it to `dead`

Check:

```bash
python3 queuectl.py dlq list
```

You should see `bad1`.

---

### 5.3 Multiple workers, no overlap

```bash
python3 queuectl.py worker clear-stop
python3 queuectl.py worker start --count 3
python3 queuectl.py enqueue '{"id":"j1","command":"echo one"}'
python3 queuectl.py enqueue '{"id":"j2","command":"echo two"}'
python3 queuectl.py enqueue '{"id":"j3","command":"echo three"}'
```

Each job will be claimed by one worker, because we atomically update the job row to `processing`.

---

### 5.4 Persistence

1. Enqueue a job:

   ```bash
   python3 queuectl.py enqueue '{"id":"persist1","command":"echo saved"}'
   ```
2. Quit terminal / restart Python.
3. List again:

   ```bash
   python3 queuectl.py list --state pending
   ```

   → job is still there.

---

### 5.5 Stop / start cycle

```bash
python3 queuectl.py worker stop
python3 queuectl.py worker clear-stop
python3 queuectl.py worker start --count 2
```

This proves the control flag works and workers exit gracefully.

---
