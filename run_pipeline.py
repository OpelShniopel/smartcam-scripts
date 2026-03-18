#!/usr/bin/env python3
"""
DeepStream Basketball Detection – Crash-Proof Wrapper
======================================================
Spawns the pipeline as a subprocess so that segfaults
(SIGSEGV) don't kill the restart loop.

Exit code convention from pipeline:
  0   — clean shutdown (SIGINT/SIGTERM from operator), do NOT restart
  42  — intentional restart (stream config changed), relaunch immediately
  other — crash or error, relaunch after delay

Usage:  python3 run_pipeline.py [--no-stream]
"""
import os
import signal
import subprocess
import sys
import time

RESTART_DELAY_SEC = 2
RESTART_EXIT_CODE = 42
SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "pipeline.py")


def main():
    shutdown = False

    def _handler(_sig, _frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    extra_args = [a for a in sys.argv[1:]]  # pass through e.g. --no-stream

    while not shutdown:
        print("=" * 60)
        print("  LAUNCHING DEEPSTREAM PIPELINE PROCESS")
        print("=" * 60)

        proc = subprocess.Popen([sys.executable, SCRIPT_PATH] + extra_args)

        try:
            ret = proc.wait()
        except KeyboardInterrupt:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)
            break

        if shutdown:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            break

        if ret == 0:
            print("\n*** Pipeline exited cleanly — not restarting ***")
            break
        elif ret == RESTART_EXIT_CODE:
            print("\n*** Pipeline requested restart (config change) ***")
            # no delay — restart immediately
            continue
        elif ret == -signal.SIGSEGV:
            print(f"\n*** Pipeline SEGFAULT (signal 11) — restarting in {RESTART_DELAY_SEC}s ***")
        else:
            print(f"\n*** Pipeline exited with code {ret} — restarting in {RESTART_DELAY_SEC}s ***")

        time.sleep(RESTART_DELAY_SEC)

    print("Exiting wrapper.")


if __name__ == "__main__":
    main()
