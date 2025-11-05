# Architecture

- queuectl.py: CLI
- db.py: SQLite init and connection (WAL)
- config.py: config store
- jobs.py: job lifecycle, DLQ, worker metadata, reaper
- worker.py: long-running worker loop
- utils.py: paths, timestamps, logging
