#!/usr/bin/env python3
"""Crash-proof wrapper for the phase 1 RTMP worker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

from exit_codes import ProcessExitCode

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_SCRIPT = os.path.join(SCRIPT_DIR, "stream_worker.py")
OWNER_PID = int(os.environ.get("STREAM_OWNER_PID", "0") or "0")
try:
    OWNER_START_TICKS = int(os.environ.get("STREAM_OWNER_START_TICKS", "") or "0") or None
except ValueError:
    OWNER_START_TICKS = None
PID_FILE = os.path.join(SCRIPT_DIR, "stream_worker.pid")
PID_FILE_ROLE = "smartcam_stream_worker_wrapper"
MAX_CRASHES = 20
CRASH_RESET_SEC = 300


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _process_start_ticks(pid: int) -> int | None:
    try:
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
        fields_after_comm = stat.rsplit(") ", 1)[1].split()
        return int(fields_after_comm[19])
    except (OSError, IndexError, ValueError):
        return None


def _pid_payload(pid: int) -> dict:
    payload = {
        "pid": pid,
        "role": PID_FILE_ROLE,
        "script": os.path.abspath(__file__),
        "owner_pid": OWNER_PID,
    }
    if OWNER_START_TICKS is not None:
        payload["owner_start_ticks"] = OWNER_START_TICKS
    start_ticks = _process_start_ticks(pid)
    if start_ticks is not None:
        payload["start_ticks"] = start_ticks
    return payload


def _write_pid() -> None:
    tmp = f"{PID_FILE}.tmp"
    with open(tmp, "w") as f:
        json.dump(_pid_payload(os.getpid()), f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PID_FILE)


def _read_pid_file() -> dict | None:
    try:
        with open(PID_FILE) as f:
            raw = f.read().strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"pid": int(raw)}
        except ValueError:
            return None

    if isinstance(data, int) and not isinstance(data, bool):
        return {"pid": data}

    if not isinstance(data, dict):
        return None
    try:
        data["pid"] = int(data.get("pid", 0))
    except (TypeError, ValueError):
        return None
    return data


def _pid_file_matches_current_process() -> bool:
    data = _read_pid_file()
    if not data or data.get("pid") != os.getpid():
        return False

    if "start_ticks" not in data:
        return True
    start_ticks_raw = data["start_ticks"]
    if isinstance(start_ticks_raw, bool) or not isinstance(start_ticks_raw, (int, str)):
        return False
    try:
        start_ticks = int(start_ticks_raw)
    except (TypeError, ValueError):
        return False
    return start_ticks == _process_start_ticks(os.getpid())


def _cleanup_pid() -> None:
    try:
        if _pid_file_matches_current_process():
            os.unlink(PID_FILE)
    except OSError:
        pass


def _owner_alive() -> bool:
    if OWNER_PID <= 0:
        return True
    try:
        os.kill(OWNER_PID, 0)
    except OSError:
        return False
    if OWNER_START_TICKS is not None:
        return _process_start_ticks(OWNER_PID) == OWNER_START_TICKS
    return True


def _sleep_for_attempt(attempt: int) -> int:
    if attempt <= 3:
        return 2
    if attempt <= 8:
        return 5
    return 10


def _stop_child(proc: subprocess.Popen, timeout_sec: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
    except OSError:
        return

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.1)

    if proc.poll() is None:
        try:
            proc.kill()
        except OSError:
            return
        proc.wait()


def main() -> None:
    shutdown = False
    crash_count = 0
    stream_error_count = 0
    child: subprocess.Popen | None = None

    _write_pid()

    def _handler(_sig, _frame):
        nonlocal shutdown, child
        shutdown = True
        if child is not None:
            try:
                child.send_signal(signal.SIGTERM)
            except OSError:
                pass

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    try:
        while not shutdown:
            if not _owner_alive():
                break
            if crash_count >= MAX_CRASHES:
                print(f"[{_ts()}] worker crashed {crash_count} times — giving up")
                raise SystemExit(1)

            print(f"[{_ts()}] launching RTMP worker (crash count: {crash_count})")
            start = time.monotonic()
            worker_proc = subprocess.Popen([sys.executable, WORKER_SCRIPT], cwd=SCRIPT_DIR)
            child = worker_proc

            ret: int | None = None
            try:
                while worker_proc.poll() is None:
                    if not _owner_alive():
                        shutdown = True
                        _stop_child(worker_proc)
                        break
                    time.sleep(0.2)
                ret = worker_proc.poll()
            except KeyboardInterrupt:
                _stop_child(worker_proc)
                break
            finally:
                child = None

            run_duration = time.monotonic() - start
            if shutdown:
                break

            if ret is None:
                ret = 1

            if ret == 0:
                print(f"[{_ts()}] worker exited cleanly after {run_duration:.0f}s")
                break

            if not _owner_alive():
                break

            if ret == ProcessExitCode.STREAM_ERROR:
                stream_error_count += 1
                delay = _sleep_for_attempt(stream_error_count)
                print(
                    f"[{_ts()}] RTMP/connectivity error after {run_duration:.0f}s "
                    f"— retrying worker in {delay}s"
                )
            else:
                stream_error_count = 0
                if run_duration >= CRASH_RESET_SEC:
                    crash_count = 1
                else:
                    crash_count += 1
                delay = _sleep_for_attempt(crash_count)
                print(
                    f"[{_ts()}] worker exit code {ret} after {run_duration:.0f}s "
                    f"— retrying in {delay}s"
                )
            end = time.monotonic() + delay
            while time.monotonic() < end and not shutdown:
                if not _owner_alive():
                    shutdown = True
                    break
                time.sleep(0.2)
    finally:
        _cleanup_pid()


if __name__ == "__main__":
    main()
