import subprocess, json, time

print("Enqueueing success job...")
subprocess.run(["python3", "queuectl.py", "enqueue", json.dumps({"id": "t-ok", "command": "echo ok"})])

print("Enqueueing failure job...")
subprocess.run(["python3", "queuectl.py", "enqueue", json.dumps({"id": "t-bad", "command": "exit 12", "max_retries": 2})])

print("Now run `python3 queuectl.py worker start --count 1` in another terminal.")
time.sleep(1)
subprocess.run(["python3", "queuectl.py", "status"])
