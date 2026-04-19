#!/usr/bin/env python3
"""
DeepStream Basketball Detection - Crash-Proof Wrapper
======================================================
Spawns the pipeline as a subprocess so that segfaults (SIGSEGV)
don't kill the restart loop.

Exit code convention from pipeline:
  0                           - clean shutdown, do NOT restart
  ProcessExitCode.RESTART     - intentional restart, relaunch immediately
  ProcessExitCode.STREAM_ERROR - reserved stream error, restart without counting crash
  other                       - crash or error, relaunch after delay

Usage:  python3 run_pipeline.py [--no-stream]
"""
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

from exit_codes import ProcessExitCode

RESTART_DELAY_SEC = 2
MAX_CRASHES = 10  # stop looping if we crash this many times without a clean run
CRASH_RESET_SEC = 300  # reset crash counter if pipeline ran cleanly for this long

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "pipeline.py")


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    shutdown = False
    crash_count = 0
    extra_args = sys.argv[1:]

    def _handler(_sig, _frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    while not shutdown:
        if crash_count >= MAX_CRASHES:
            print(f"\n[{_ts()}] *** FATAL: pipeline crashed {crash_count} times — giving up ***")
            print(f"[{_ts()}] Check logs above. Fix the issue and restart manually.")
            sys.exit(1)

        print(f"\n[{_ts()}] {'=' * 55}")
        print(f"[{_ts()}]   LAUNCHING DEEPSTREAM PIPELINE (crash count: {crash_count})")
        print(f"[{_ts()}] {'=' * 55}")

        start_time = time.monotonic()
        proc = subprocess.Popen([sys.executable, SCRIPT_PATH] + extra_args)

        try:
            ret = proc.wait()
        except KeyboardInterrupt:
            print(f"\n[{_ts()}] Interrupt received — stopping pipeline ...")
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"[{_ts()}] Pipeline did not stop — killing ...")
                proc.kill()
                proc.wait()
            break

        run_duration = time.monotonic() - start_time

        if shutdown:
            # SIGTERM arrived while pipeline was running
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            break

        if ret == 0:
            print(f"\n[{_ts()}] *** Pipeline exited cleanly (ran {run_duration:.0f}s) — not restarting ***")
            break

        if ret == ProcessExitCode.RESTART:
            print(f"\n[{_ts()}] *** Config change restart (ran {run_duration:.0f}s) ***")
            # Reset crash count — this was an intentional restart, not a crash
            crash_count = 0
            continue

        if ret == ProcessExitCode.STREAM_ERROR:
            print(f"\n[{_ts()}] *** Reserved stream error exit (ran {run_duration:.0f}s) — restarting ***")
            # stream.conf already cleared by pipeline, reset crash count — not a crash
            crash_count = 0
            continue

        # Crashed or errored
        crash_count += 1
        if run_duration >= CRASH_RESET_SEC:
            # Ran long enough to be considered healthy before crashing — reset counter
            print(f"\n[{_ts()}] *** Pipeline ran {run_duration:.0f}s then crashed "
                  f"(code {ret}) — crash counter reset ***")
            crash_count = 1
        elif ret == -signal.SIGSEGV:
            print(f"\n[{_ts()}] *** SEGFAULT (signal 11) after {run_duration:.0f}s "
                  f"— crash {crash_count}/{MAX_CRASHES} "
                  f"— restarting in {RESTART_DELAY_SEC}s ***")
        else:
            print(f"\n[{_ts()}] *** Exit code {ret} after {run_duration:.0f}s "
                  f"— crash {crash_count}/{MAX_CRASHES} "
                  f"— restarting in {RESTART_DELAY_SEC}s ***")

        time.sleep(RESTART_DELAY_SEC)

    print(f"\n[{_ts()}] Wrapper exiting.")


if __name__ == "__main__":
    main()
