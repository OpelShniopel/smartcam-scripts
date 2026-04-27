#!/usr/bin/env python3
"""
DeepStream Basketball Detection - Crash-Proof Wrapper
======================================================
Spawns the pipeline as a subprocess so that segfaults (SIGSEGV)
don't kill the restart loop.

Exit code convention from pipeline:
  0                           - clean shutdown, do NOT restart
  ProcessExitCode.RESTART     - intentional restart, relaunch immediately
  ProcessExitCode.STREAM_ERROR - stream error path, restart without counting crash
  other                       - crash or error, relaunch after delay

Usage:  python3 run_pipeline.py
"""
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime

from exit_codes import ProcessExitCode

RESTART_DELAY_SEC = 2
MAX_CRASHES = 10  # stop looping if we crash this many times without a clean run
CRASH_RESET_SEC = 300  # reset crash counter if pipeline ran cleanly for this long

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "pipeline.py")


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class _SupervisorState:
    crash_count: int = 0
    shutdown: bool = False
    child: subprocess.Popen | None = None
    stop_signal = signal.SIGTERM


@dataclass
class _PipelineRunResult:
    ret: int | None
    run_duration: float


def _signal_child(child_proc: subprocess.Popen, sig: int) -> None:
    if child_proc.poll() is None:
        try:
            child_proc.send_signal(sig)
        except OSError:
            pass


def _stop_child_with_timeout(
        proc: subprocess.Popen,
        sig: int,
        timeout_sec: float = 5.0,
        timeout_message: str | None = None,
) -> None:
    _signal_child(proc, sig)
    try:
        proc.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        if timeout_message:
            print(timeout_message)
        proc.kill()
        proc.wait()


def _request_shutdown(state: _SupervisorState, sig: int) -> None:
    state.shutdown = True
    state.stop_signal = sig
    if state.child is not None:
        _signal_child(state.child, sig)


def _install_signal_handlers(state: _SupervisorState) -> None:
    def _handler(_sig, _frame):
        _request_shutdown(state, _sig)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def _enforce_crash_limit(state: _SupervisorState) -> None:
    if state.crash_count < MAX_CRASHES:
        return

    print(f"\n[{_ts()}] *** FATAL: pipeline crashed {state.crash_count} times — giving up ***")
    print(f"[{_ts()}] Check logs above. Fix the issue and restart manually.")
    raise SystemExit(1)


def _log_pipeline_launch(crash_count: int) -> None:
    print(f"\n[{_ts()}] {'=' * 55}")
    print(f"[{_ts()}]   LAUNCHING DEEPSTREAM PIPELINE (crash count: {crash_count})")
    print(f"[{_ts()}] {'=' * 55}")


def _run_pipeline_once(state: _SupervisorState, extra_args: list[str]) -> _PipelineRunResult:
    _log_pipeline_launch(state.crash_count)

    start_time = time.monotonic()
    proc = subprocess.Popen([sys.executable, SCRIPT_PATH] + extra_args)
    state.child = proc
    if state.shutdown:
        _signal_child(proc, state.stop_signal)

    ret: int | None = None
    try:
        ret = proc.wait()
    except KeyboardInterrupt:
        print(f"\n[{_ts()}] Interrupt received — stopping pipeline ...")
        _stop_child_with_timeout(
            proc,
            signal.SIGINT,
            timeout_message=f"[{_ts()}] Pipeline did not stop — killing ...",
        )
        state.shutdown = True
    finally:
        state.child = None

    return _PipelineRunResult(ret=ret, run_duration=time.monotonic() - start_time)


def _handle_pipeline_exit(result: _PipelineRunResult, state: _SupervisorState) -> bool:
    ret = result.ret
    if ret is None:
        return False

    run_duration = result.run_duration
    if ret == 0:
        print(f"\n[{_ts()}] *** Pipeline exited cleanly (ran {run_duration:.0f}s) — not restarting ***")
        return False

    if ret == ProcessExitCode.RESTART:
        print(f"\n[{_ts()}] *** Config change restart (ran {run_duration:.0f}s) ***")
        state.crash_count = 0
        return True

    if ret == ProcessExitCode.STREAM_ERROR:
        print(f"\n[{_ts()}] *** Stream error exit (ran {run_duration:.0f}s) — restarting ***")
        state.crash_count = 0
        return True

    state.crash_count += 1
    if run_duration >= CRASH_RESET_SEC:
        print(
            f"\n[{_ts()}] *** Pipeline ran {run_duration:.0f}s then crashed "
            f"(code {ret}) — crash counter reset ***"
        )
        state.crash_count = 1
    elif ret == -signal.SIGSEGV:
        print(
            f"\n[{_ts()}] *** SEGFAULT (signal 11) after {run_duration:.0f}s "
            f"— crash {state.crash_count}/{MAX_CRASHES} "
            f"— restarting in {RESTART_DELAY_SEC}s ***"
        )
    else:
        print(
            f"\n[{_ts()}] *** Exit code {ret} after {run_duration:.0f}s "
            f"— crash {state.crash_count}/{MAX_CRASHES} "
            f"— restarting in {RESTART_DELAY_SEC}s ***"
        )

    time.sleep(RESTART_DELAY_SEC)
    return True


def main() -> None:
    state = _SupervisorState()
    extra_args = sys.argv[1:]
    _install_signal_handlers(state)

    while not state.shutdown:
        _enforce_crash_limit(state)
        result = _run_pipeline_once(state, extra_args)
        if state.shutdown:
            break
        if not _handle_pipeline_exit(result, state):
            break

    print(f"\n[{_ts()}] Wrapper exiting.")


if __name__ == "__main__":
    main()
