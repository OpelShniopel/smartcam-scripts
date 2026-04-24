"""Runtime file paths shared by the pipeline and stream worker processes."""

import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

STREAM_CONF = os.path.join(SCRIPT_DIR, "stream.conf")
SCORE_STATE_FILE = os.path.join(SCRIPT_DIR, "score_state.json")
STREAM_WORKER_CONFIG = os.path.join(SCRIPT_DIR, "stream_worker_config.json")
STREAM_WORKER_STATUS = os.path.join(SCRIPT_DIR, "stream_worker_status.json")
STREAM_WORKER_PID = os.path.join(SCRIPT_DIR, "stream_worker.pid")

STREAM_WORKER_SCRIPT = os.path.join(SCRIPT_DIR, "stream_worker.py")
STREAM_WORKER_WRAPPER = os.path.join(SCRIPT_DIR, "run_stream_worker.py")
STREAM_WORKER_PID_ROLE = "smartcam_stream_worker_wrapper"

SCOREBOARD_PNG = os.path.join(SCRIPT_DIR, "scoreboard.png")
TIMEOUT_BG_PNG = os.path.join(SCRIPT_DIR, "timeout_bg.png")
BLITZBALL_SCOREBOARD_PNG = os.path.join(SCRIPT_DIR, "blitzball_scoreboard.png")
BLITZBALL_ACTIVE_PNG = os.path.join(SCRIPT_DIR, "blitzball_blitz_active.png")

__all__ = [
    "BLITZBALL_ACTIVE_PNG",
    "BLITZBALL_SCOREBOARD_PNG",
    "SCOREBOARD_PNG",
    "TIMEOUT_BG_PNG",
    "SCORE_STATE_FILE",
    "SCRIPT_DIR",
    "STREAM_CONF",
    "STREAM_WORKER_CONFIG",
    "STREAM_WORKER_PID",
    "STREAM_WORKER_PID_ROLE",
    "STREAM_WORKER_SCRIPT",
    "STREAM_WORKER_STATUS",
    "STREAM_WORKER_WRAPPER",
]
