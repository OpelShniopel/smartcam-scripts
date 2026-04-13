#!/usr/bin/env python3
"""Crash-proof wrapper for the phase 1 RTMP worker."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_SCRIPT = os.path.join(SCRIPT_DIR, "stream_worker.py")
OWNER_PID = int(os.environ.get("STREAM_OWNER_PID", "0") or "0")
PID_FILE = os.path.join(SCRIPT_DIR, "stream_worker.pid")
STREAM_ERROR_EXIT_CODE = 43
MAX_CRASHES = 20
CRASH_RESET_SEC = 300


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_pid() -> None:
    tmp = f"{PID_FILE}.tmp"
    with open(tmp, "w") as f:
        f.write(f"{os.getpid()}\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PID_FILE)


def _cleanup_pid() -> None:
    try:
        if os.path.exists(PID_FILE):
            os.unlink(PID_FILE)
    except OSError:
        pass


def _owner_alive() -> bool:
    if OWNER_PID <= 0:
        return True
    try:
        os.kill(OWNER_PID, 0)
        return True
    except OSError:
        return False


def _sleep_for_attempt(attempt: int) -> int:
    if attempt <= 3:
        return 2
    if attempt <= 8:
        return 5
    return 10


def main() -> None:
    shutdown = False
    crash_count = 0
    child: subprocess.Popen | None = None

    _write_pid()

    def _handler(_sig, _frame):
        nonlocal shutdown, child
        shutdown = True
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    try:
        while not shutdown:
            if not _owner_alive():
                shutdown = True
                break
            if crash_count >= MAX_CRASHES:
                print(f"[{_ts()}] worker crashed {crash_count} times — giving up")
                raise SystemExit(1)

            print(f"[{_ts()}] launching RTMP worker (crash count: {crash_count})")
            start = time.monotonic()
            child = subprocess.Popen([sys.executable, WORKER_SCRIPT], cwd=SCRIPT_DIR)

            try:
                ret = child.wait()
            except KeyboardInterrupt:
                shutdown = True
                if child.poll() is None:
                    child.send_signal(signal.SIGTERM)
                    child.wait(timeout=5)
                break
            finally:
                child = None

            run_duration = time.monotonic() - start
            if shutdown:
                break

            if ret == 0:
                print(f"[{_ts()}] worker exited cleanly after {run_duration:.0f}s")
                break

            if run_duration >= CRASH_RESET_SEC:
                crash_count = 1
            else:
                crash_count += 1

            if not _owner_alive():
                shutdown = True
                break

            delay = _sleep_for_attempt(crash_count)
            if ret == STREAM_ERROR_EXIT_CODE:
                print(
                    f"[{_ts()}] RTMP/connectivity error after {run_duration:.0f}s "
                    f"— retrying worker in {delay}s"
                )
            else:
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
