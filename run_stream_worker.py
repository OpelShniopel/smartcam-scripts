#!/usr/bin/env python3
"""Crash-proof wrapper for the RTMP worker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime

from exit_codes import ProcessExitCode
from runtime_paths import (
    SCRIPT_DIR,
    STREAM_WORKER_PID as PID_FILE,
    STREAM_WORKER_PID_ROLE as PID_FILE_ROLE,
    STREAM_WORKER_SCRIPT as WORKER_SCRIPT,
)

OWNER_PID = int(os.environ.get("STREAM_OWNER_PID", "0") or "0")
try:
    OWNER_START_TICKS = int(os.environ.get("STREAM_OWNER_START_TICKS", "") or "0") or None
except ValueError:
    OWNER_START_TICKS = None
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


@dataclass
class _SupervisorState:
    shutdown: bool = False
    crash_count: int = 0
    stream_error_count: int = 0
    child: subprocess.Popen | None = None


@dataclass
class _WorkerRunResult:
    ret: int | None
    run_duration: float


def _request_shutdown(state: _SupervisorState) -> None:
    state.shutdown = True
    if state.child is not None:
        try:
            state.child.send_signal(signal.SIGTERM)
        except OSError:
            pass


def _install_signal_handlers(state: _SupervisorState) -> None:
    def _handler(_sig, _frame):
        _request_shutdown(state)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def _ensure_can_launch_worker(state: _SupervisorState) -> None:
    if not _owner_alive():
        state.shutdown = True
        return
    if state.crash_count >= MAX_CRASHES:
        print(f"[{_ts()}] worker crashed {state.crash_count} times — giving up")
        raise SystemExit(1)


def _wait_for_worker_exit(worker_proc: subprocess.Popen, state: _SupervisorState) -> int | None:
    while worker_proc.poll() is None:
        if not _owner_alive():
            state.shutdown = True
            _stop_child(worker_proc)
            break
        time.sleep(0.2)
    return worker_proc.poll()


def _run_worker_once(state: _SupervisorState) -> _WorkerRunResult:
    print(f"[{_ts()}] launching RTMP worker (crash count: {state.crash_count})")
    start = time.monotonic()
    worker_proc = subprocess.Popen([sys.executable, WORKER_SCRIPT], cwd=SCRIPT_DIR)
    state.child = worker_proc

    ret: int | None = None
    try:
        ret = _wait_for_worker_exit(worker_proc, state)
    except KeyboardInterrupt:
        _stop_child(worker_proc)
        state.shutdown = True
    finally:
        state.child = None

    return _WorkerRunResult(ret=ret, run_duration=time.monotonic() - start)


def _handle_worker_exit(ret: int, run_duration: float, state: _SupervisorState) -> int | None:
    if ret == 0:
        print(f"[{_ts()}] worker exited cleanly after {run_duration:.0f}s")
        return None

    if not _owner_alive():
        state.shutdown = True
        return None

    if ret == ProcessExitCode.STREAM_ERROR:
        state.stream_error_count += 1
        delay = _sleep_for_attempt(state.stream_error_count)
        print(
            f"[{_ts()}] RTMP/connectivity error after {run_duration:.0f}s "
            f"— retrying worker in {delay}s"
        )
        return delay

    state.stream_error_count = 0
    if run_duration >= CRASH_RESET_SEC:
        state.crash_count = 1
    else:
        state.crash_count += 1
    delay = _sleep_for_attempt(state.crash_count)
    print(
        f"[{_ts()}] worker exit code {ret} after {run_duration:.0f}s "
        f"— retrying in {delay}s"
    )
    return delay


def _sleep_until_retry(delay: int, state: _SupervisorState) -> None:
    end = time.monotonic() + delay
    while time.monotonic() < end and not state.shutdown:
        if not _owner_alive():
            state.shutdown = True
            return
        time.sleep(0.2)


def main() -> None:
    state = _SupervisorState()
    _write_pid()
    _install_signal_handlers(state)

    try:
        while not state.shutdown:
            _ensure_can_launch_worker(state)
            if state.shutdown:
                break

            result = _run_worker_once(state)
            if state.shutdown:
                break

            ret = result.ret if result.ret is not None else 1
            delay = _handle_worker_exit(ret, result.run_duration, state)
            if delay is None:
                break

            _sleep_until_retry(delay, state)
    finally:
        _cleanup_pid()


if __name__ == "__main__":
    main()
