#!/usr/bin/env python3
"""
DeepStream Basketball Detection Pipeline
==========================================
Fixed and PTZ cameras with a switched clean RTSP stream and optional AI RTSP
streams via MediaMTX. Optional live-streaming to YouTube/Twitch/Kick is
handled by a separate RTMP worker with scoreboard overlay.

KNOWN ISSUES / HISTORY:
  - JetPack 6.2 + DeepStream 7.1 bug: cudaErrorIllegalAddress (700) in
    nvbufsurftransform_copy.cpp. Fix: copy-hw=2 on all nvvideoconvert elements.
  - link_filtered() on NVMM paths causes NULL caps assertions at runtime.
    Fix: use real capsfilter elements for every inline caps constraint.
  - nvv4l2decoder has a static src pad — do NOT use pad-added signal.
  - nvdsosd process-mode=1 requires NVMM RGBA input — always insert
    nvvideoconvert before nvosd to convert NV12->RGBA.
  - cairooverlay was too slow (CPU BGRA conversion every frame). Replaced with
    gdkpixbufoverlay (static PNG) + textoverlay x8 (dynamic text) in the RTMP
    worker branch.
  - stream_status race: Go bridge may not be connected when stream_status fires
    (Python restarts faster than Go's 2s reconnect delay). Fix: cache the
    stream_status result and replay it when Go connects in _handle_go_connection.

STREAM DESIGN:
  Fixed camera + PTZ camera:
    - program branch: switched 1080p clean feed → WebRTC tablet viewing + RTMP worker input
    - AI branch:      720p with bounding boxes when enabled → debug only

SERVICES (for Go backend):
  Unix socket  /tmp/smartcam.sock  — Go bridge, bidirectional newline-delimited JSON
    Python -> Go: {"type":"state", "streaming":bool, "stream_active_camera":"ptz",
                   "webrtc":{...}, "internal_streams":{...}}
                  {"type":"stream_status", "active":bool, "error":"..."}
                  {"type":"ack", "action":"...", "ok":bool}
                  {"type":"pong"}
    Go -> Python: {"type":"cmd", "action":"start_stream", "rtmp_url":"rtmp://..."}
                  {"type":"cmd", "action":"stop_stream"}
                  {"type":"cmd", "action":"set_config", "bitrateKbps":N}
                  {"type":"cmd", "action":"switch_cam", "camId":"fixed"|"ptz"}
                  {"type":"cmd", "action":"set_osd", "visible":bool}
                  {"type":"cmd", "action":"set_score", ...score fields...}
                  {"type":"ping"}

  Unix socket  /tmp/ptz-control.sock  — outbound only, newline-delimited JSON
    Python -> PTZ control: {"camera":"fixed","frame":N,"timestamp":T,"detections":[...]}

Class IDs (model v9):  0=RIM  1=BALL
"""

import json
import os
import queue
import signal
import socket
import socket as _socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst

import pyds

from camera_config import (
    CAMERA_DEVICE_ALIASES,
    FIXED_CAMERA,
    FIXED_CAMERA_DEVICE,
    PTZ_CAMERA,
    PTZ_CAMERA_DEVICE,
)
from exit_codes import ProcessExitCode
from gst_utils import force_key_unit
from runtime_paths import (
    SCOREBOARD_PNG,
    SCORE_STATE_FILE,
    SCRIPT_DIR,
    STREAM_CONF,
    STREAM_WORKER_CONFIG,
    STREAM_WORKER_PID,
    STREAM_WORKER_PID_ROLE,
    STREAM_WORKER_STATUS,
    STREAM_WORKER_WRAPPER,
)
from unix_socket_utils import create_unix_stream_server
from rtmp_elements import (
    foul_png_path,
    populate_timeout_texts,
    TIMEOUT_TEXT_KEYS,
    update_blitzball_end_stats,
    update_blitzball_overlay,
    update_milestone_overlays,
    update_quarter_overlay,
    update_score_clock_overlays,
)
from score_utils import default_score_state, truncate_team_name


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
def _get_local_ip() -> str:
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "localhost"


JETSON_HOST = os.environ.get("JETSON_HOST") or _get_local_ip()
GO_BRIDGE_SOCK = os.environ.get("SMARTCAM_SOCK", "/tmp/smartcam.sock")
PTZ_CONTROL_SOCK = "/tmp/ptz_control.sock"
PTZ_MANUAL_SOCK = "/tmp/ptz_manual.sock"

# Degrees per Go "step" — matches ESP32: STEP_SIZE_STEPS(62) / STEPS_PER_DEG(125)
PAN_DEG_PER_STEP = 0.5

_PTZ_CMD_TYPES = frozenset({
    "cmd.cam_pan_step",
    "cmd.cam_move_start",
    "cmd.cam_move_stop",
    "cmd.set_cam_mode",
    "cmd.cam_zoom_step",
    "cmd.cam_zoom_start",
    "cmd.cam_zoom_stop",
    "cmd.cam_focus_offset",
})

CLASS_ID_RIM = 0
CLASS_ID_BALL = 1
CLASS_NAMES = {CLASS_ID_RIM: "RIM", CLASS_ID_BALL: "BALL"}

PROBE_EVERY_N_FRAMES = 1
RAW_I420_CAPS = "video/x-raw,format=I420"

# ---------------------------------------------------------------------------
# Camera / AI feature flags
# ---------------------------------------------------------------------------
ENABLE_FIXED_CAMERA = True
ENABLE_PTZ_CAMERA = True

ENABLE_AI_GLOBAL = True
ENABLE_FIXED_CAMERA_AI = True
ENABLE_PTZ_CAMERA_AI = False
ENABLE_FIXED_CAMERA_AI_STREAM = True
ENABLE_PTZ_CAMERA_AI_STREAM = True

# Terminal FPS metrics. Disable the global flag to silence all main-pipeline
# FPS logs, or disable the AI flag to keep other future FPS metrics available.
ENABLE_TERMINAL_FPS_METRICS = True
ENABLE_AI_FPS_METRICS = True
TERMINAL_FPS_INTERVAL_SEC = 5

PIPELINE_CAMERA_LABEL_BY_STREAM_CAMERA = {
    FIXED_CAMERA: "CAM0",
    PTZ_CAMERA: "CAM2",
}
STREAM_CAMERA_BY_PIPELINE_LABEL = {
    "CAM0": FIXED_CAMERA,
    "CAM2": PTZ_CAMERA,
}


def _pipeline_camera_label(value) -> str:
    text = str(value or "").strip()
    stream_camera = CAMERA_DEVICE_ALIASES.get(text.lower())
    if stream_camera in PIPELINE_CAMERA_LABEL_BY_STREAM_CAMERA:
        return PIPELINE_CAMERA_LABEL_BY_STREAM_CAMERA[stream_camera]
    cam = text.upper()
    if cam in STREAM_CAMERA_BY_PIPELINE_LABEL:
        return cam
    return ""


def _stream_camera_name(value) -> str:
    label = _pipeline_camera_label(value)
    return STREAM_CAMERA_BY_PIPELINE_LABEL.get(label, str(value or "").strip().lower())


def _cam_enabled(cam_label: str) -> bool:
    cam = _pipeline_camera_label(cam_label)
    if cam == "CAM0":
        return ENABLE_FIXED_CAMERA
    if cam == "CAM2":
        return ENABLE_PTZ_CAMERA
    return False


def _ai_enabled(cam_label: str) -> bool:
    if not ENABLE_AI_GLOBAL:
        return False
    cam = _pipeline_camera_label(cam_label)
    if cam == "CAM0":
        return ENABLE_FIXED_CAMERA and ENABLE_FIXED_CAMERA_AI
    if cam == "CAM2":
        return ENABLE_PTZ_CAMERA and ENABLE_PTZ_CAMERA_AI
    return False


def _ai_stream_enabled(cam_label: str) -> bool:
    if not _ai_enabled(cam_label):
        return False
    cam = _pipeline_camera_label(cam_label)
    if cam == "CAM0":
        return ENABLE_FIXED_CAMERA_AI_STREAM
    if cam == "CAM2":
        return ENABLE_PTZ_CAMERA_AI_STREAM
    return False


# Program outputs:
# - program_clean: H264 preview for browser WebRTC with restamped timestamps.
# - program_stream: H264 source for the RTMP worker.
PROGRAM_CLEAN_BITRATE = 9000
PROGRAM_CLEAN_KEYINT = 15
PROGRAM_CLEAN_THREADS = 1
PROGRAM_CLEAN_PRESET = "ultrafast"
PROGRAM_CLEAN_TUNE = "zerolatency"
PROGRAM_WEBRTC_RTSP_PATH = "program_clean"
PROGRAM_STREAM_RTSP_PATH = "program_stream"
PROGRAM_WEBRTC_WIDTH = 1920
PROGRAM_WEBRTC_HEIGHT = 1080
PROGRAM_WEBRTC_BITRATE = 8000
PROGRAM_WEBRTC_KEYINT = 6
PROGRAM_WEBRTC_THREADS = 1
PROGRAM_PREVIEW_FRAME_DURATION_NS = Gst.SECOND // 30
PROGRAM_SWITCH_DEBOUNCE_MS = 50
PROGRAM_SWITCH_SETTLE_MS = 250

# AI branch encoder settings
AI_BITRATE = 3500
AI_KEYINT = 20
AI_THREADS = 1
AI_PRESET = "ultrafast"
AI_TUNE = "zerolatency"

# Local AI recording settings (debug / training capture)
ENABLE_PTZ_CAMERA_AI_RECORDING = False
RECORDINGS_DIR = os.path.join(SCRIPT_DIR, "recordings")
RECORD_SEGMENT_SECONDS = 300
RECORD_MUXER_FACTORY = "matroskamux"
RECORD_FILE_EXTENSION = "mkv"
RECORD_QUEUE_BUFFERS = 120

# Default RTMP bitrate for worker config and the legacy embedded RTMP branch.
RTMP_BITRATE = 9000

# Encoder references populated by build_pipeline().
_encoders: dict[str, Gst.Element] = {}
_program_selector: Gst.Element | None = None
_program_selector_pads: dict[str, Gst.Pad] = {}
_program_enc: Gst.Element | None = None
_program_preview_enc: Gst.Element | None = None
_program_active_camera: str = PTZ_CAMERA
_program_previous_camera: str = PTZ_CAMERA
_program_switch_seq = 0
_program_last_switch_at_ms = 0
_last_program_cfg: dict | None = None
_program_switch_lock = threading.Lock()
_program_switch_requested_camera: str | None = None
_program_switch_timer_queued = False
_program_switch_settle_until_monotonic = 0.0
_program_preview_frame_index = 0

# ---------------------------------------------------------------------------
# RTMP stream status tracking
#
# Problem solved: Go's bridge may not be connected when stream_status fires
# (Python restarts → Go loses connection → Go reconnects after 2s delay →
# but stream_status may have already fired and been dropped because no Go
# client was connected yet).
#
# Solution: Cache the stream_status result. When Go connects and we run
# _handle_go_connection, replay the cached status alongside _push_state().
# This guarantees Go always receives it regardless of connection timing.
# ---------------------------------------------------------------------------
_rtmp_status_sent = False
_rtmp_status_lock = threading.Lock()
# Cached stream_status message to replay on new Go connections.
# None = not yet determined. Dict = already resolved.
_rtmp_status_cached: dict | None = None


def _send_stream_status(active: bool, error: str = "") -> None:
    """Send stream_status to Go when it changes and cache latest state."""
    global _rtmp_status_sent, _rtmp_status_cached
    msg: dict = {"type": "stream_status", "active": active}
    if error:
        msg["error"] = error

    with _rtmp_status_lock:
        if _rtmp_status_cached == msg:
            return
        _rtmp_status_sent = True
        _rtmp_status_cached = msg

    _go_bridge_out_q.put(msg)
    if active:
        print("[stream_status] RTMP stream verified active")
    else:
        print(f"[stream_status] RTMP stream inactive: {error}")


def _get_cached_stream_status() -> dict | None:
    """Return the cached stream_status message, or None if not yet determined."""
    with _rtmp_status_lock:
        return _rtmp_status_cached


# ---------------------------------------------------------------------------
# FPS tracking
# ---------------------------------------------------------------------------
_fps_counters: dict[str, int] = {"CAM0": 0, "CAM2": 0}
_fps_lock = threading.Lock()


def _ai_fps_metric_enabled(cam_label: str) -> bool:
    return (
            ENABLE_TERMINAL_FPS_METRICS
            and ENABLE_AI_FPS_METRICS
            and _ai_enabled(cam_label)
    )


def _fps_report() -> bool:
    with _fps_lock:
        for cam, count in _fps_counters.items():
            _fps_counters[cam] = 0
            if not _ai_fps_metric_enabled(cam) or count <= 0:
                continue
            print(f"[fps] {_stream_camera_name(cam)} AI: {count / TERMINAL_FPS_INTERVAL_SEC:.1f} fps")
    return True


# ---------------------------------------------------------------------------
# Score state
# ---------------------------------------------------------------------------
score_state = default_score_state()
score_lock = threading.Lock()
_SCORE_STR_FIELDS = frozenset({"home_name", "away_name", "clock"})
_SCORE_TEAM_NAME_FIELDS = frozenset({"home_name", "away_name"})
_SCORE_INT_FIELDS = frozenset({
    "home_points", "away_points", "home_fouls", "away_fouls",
    "home_timeouts", "away_timeouts", "quarter", "game_id",
})
_SCORE_NUMBER_FIELDS = frozenset({"updated_at"})
_SCORE_BOOL_FIELDS = frozenset({"visible"})


# ---------------------------------------------------------------------------
# Scoreboard overlay elements
# ---------------------------------------------------------------------------
class _OverlayPropertyElement(Protocol):
    def set_property(self, name: str, value: Any) -> None:
        ...

    def get_property(self, name: str) -> Any:
        ...


_osd_elements: dict[str, _OverlayPropertyElement] = {}
_osd_lock = threading.Lock()


def _render_scoreboard_bg() -> None:
    if not os.path.exists(SCOREBOARD_PNG):
        print(f"WARNING: Scoreboard PNG not found: {SCOREBOARD_PNG}")
        print("         Place scoreboard.png next to pipeline.py")


def _osd_elements_snapshot() -> dict[str, _OverlayPropertyElement]:
    with _osd_lock:
        return dict(_osd_elements)


def _osd_element(els: dict[str, _OverlayPropertyElement], key: str) -> _OverlayPropertyElement | None:
    return els.get(key)


def _set_osd_silent(els: dict[str, _OverlayPropertyElement], key: str, silent: bool) -> None:
    el = _osd_element(els, key)
    if el:
        el.set_property("silent", silent)


def _set_osd_alpha(els: dict[str, _OverlayPropertyElement], key: str, alpha: float) -> None:
    el = _osd_element(els, key)
    if el:
        el.set_property("alpha", alpha)


def _set_many_osd_silent(els: dict[str, _OverlayPropertyElement], keys, silent: bool) -> None:
    for key in keys:
        _set_osd_silent(els, key, silent)


def _update_team_name_text(
        el: _OverlayPropertyElement | None,
        value,
        fallback: str,
        visible: bool,
) -> None:
    if not el:
        return
    el.set_property("silent", not visible)
    if visible:
        el.set_property("text", truncate_team_name(value or fallback))


def _update_foul_bar(
        el: _OverlayPropertyElement | None,
        team: str,
        fouls,
        visible: bool,
) -> None:
    if not el:
        return
    path = foul_png_path(team, fouls)
    if path is None:
        el.set_property("alpha", 0.0)
        return
    el.set_property("location", path)
    el.set_property("alpha", 1.0 if visible else 0.0)


def _update_standard_scoreboard_overlays(
        state: dict,
        els: dict[str, _OverlayPropertyElement],
        visible: bool,
) -> None:
    update_quarter_overlay(_osd_element(els, "osd_quarter"), visible, state)
    _update_team_name_text(_osd_element(els, "osd_home"), state.get("home_name", "HOME"), "HOME", visible)
    _update_team_name_text(_osd_element(els, "osd_away"), state.get("away_name", "AWAY"), "AWAY", visible)
    update_score_clock_overlays(
        _osd_element(els, "osd_home_score"),
        _osd_element(els, "osd_away_score"),
        _osd_element(els, "osd_clock"),
        visible,
        state,
    )
    _update_foul_bar(_osd_element(els, "osd_home_fouls_bar"), "home", state.get("home_fouls", 0), visible)
    _update_foul_bar(_osd_element(els, "osd_away_fouls_bar"), "away", state.get("away_fouls", 0), visible)
    _set_osd_alpha(els, "osd_bg", 1.0 if visible else 0.0)
    update_milestone_overlays(
        _osd_element(els, "osd_milestone_player"),
        _osd_element(els, "osd_milestone_text"),
        state,
    )


def _active_timeout_stats(state: dict) -> dict | None:
    timeout_stats = state.get("timeout_stats")
    if not isinstance(timeout_stats, dict):
        return None
    if timeout_stats.get("show_until", 0) <= int(time.time() * 1000):
        return None
    return timeout_stats


def _show_timeout_overlays(timeout_stats: dict, state: dict, els: dict) -> None:
    for key in ("osd_bg", "osd_home_fouls_bar", "osd_away_fouls_bar"):
        _set_osd_alpha(els, key, 0.0)
    _set_many_osd_silent(
        els,
        (
            "osd_quarter", "osd_home", "osd_away", "osd_home_score",
            "osd_away_score", "osd_clock", "osd_milestone_player",
            "osd_milestone_text",
        ),
        True,
    )
    populate_timeout_texts(timeout_stats, state, els)
    _set_osd_alpha(els, "osd_timeout_bg", 1.0)
    _set_many_osd_silent(els, TIMEOUT_TEXT_KEYS, False)


def _hide_timeout_overlays(els: dict) -> None:
    _set_osd_alpha(els, "osd_timeout_bg", 0.0)
    _set_many_osd_silent(els, TIMEOUT_TEXT_KEYS, True)


def _update_timeout_overlays(state: dict, els: dict) -> None:
    timeout_stats = _active_timeout_stats(state)
    if timeout_stats is None:
        _hide_timeout_overlays(els)
        return
    _show_timeout_overlays(timeout_stats, state, els)


def _update_osd_texts(state: dict) -> None:
    els = _osd_elements_snapshot()
    if not els:
        return

    if update_blitzball_end_stats(state, els):
        return

    visible = state.get("visible", False)
    if state.get("sport_code", "") != "BLITZBALL":
        _update_standard_scoreboard_overlays(state, els, visible)

    update_blitzball_overlay(state, els)
    _update_timeout_overlays(state, els)


def _is_score_str(value) -> bool:
    return isinstance(value, str)


def _is_score_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_score_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_score_bool(value) -> bool:
    return isinstance(value, bool)


def _normalize_score_str_field(field: str, value: str) -> str:
    if field in _SCORE_TEAM_NAME_FIELDS:
        return truncate_team_name(value)
    return value


def _apply_score_fields(data: dict, fields, predicate: Callable[[Any], bool], normalize=None) -> None:
    for field in fields:
        value = data.get(field)
        if predicate(value):
            score_state[field] = normalize(field, value) if normalize else value


def _apply_score_milestone(data: dict) -> None:
    if "milestone" not in data:
        return
    milestone = data["milestone"]
    if milestone is None or isinstance(milestone, dict):
        score_state["milestone"] = milestone


def _apply_score_patch(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValueError("score patch must be a JSON object")

    with score_lock:
        _apply_score_fields(data, _SCORE_STR_FIELDS, _is_score_str, _normalize_score_str_field)
        _apply_score_fields(data, _SCORE_INT_FIELDS, _is_score_int)
        _apply_score_fields(data, _SCORE_NUMBER_FIELDS, _is_score_number)
        _apply_score_fields(data, _SCORE_BOOL_FIELDS, _is_score_bool)
        _apply_score_milestone(data)
        state = score_state.copy()
    _update_osd_texts(state)
    _persist_score_state()


# ---------------------------------------------------------------------------
# PTZ manual command relay (pipeline -> ptz_control manual socket)
# ---------------------------------------------------------------------------
_ptz_manual_q: queue.SimpleQueue = queue.SimpleQueue()
_pan_deg = 0.0
_pan_deg_lock = threading.Lock()


def _close_socket_quietly(conn: socket.socket | None) -> None:
    if not conn:
        return
    try:
        conn.close()
    except OSError:
        pass


def _connect_ptz_manual_socket() -> socket.socket:
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(PTZ_MANUAL_SOCK)
    print("[ptz-manual] relay connected")
    return conn


def _send_ptz_manual_messages(conn: socket.socket) -> None:
    while True:
        msg = _ptz_manual_q.get()
        conn.sendall((json.dumps(msg) + "\n").encode())


def _ptz_manual_relay_loop() -> None:
    while True:
        conn = None
        try:
            conn = _connect_ptz_manual_socket()
            _send_ptz_manual_messages(conn)
        except OSError:
            time.sleep(1)
        finally:
            _close_socket_quietly(conn)


def start_ptz_manual_relay() -> None:
    threading.Thread(target=_ptz_manual_relay_loop, daemon=True, name="ptz-manual-relay").start()


def _send_ptz_manual_ack(
        msg_type: str,
        msg_id,
        ok: bool,
        resp: dict | None = None,
        error: str = "",
) -> None:
    ack: dict = {"type": "ack", "action": msg_type, "id": msg_id, "ok": ok}
    if resp is not None:
        ack["payload"] = resp
    if error:
        ack["error"] = error
    _go_bridge_out_q.put(ack)


def _ptz_payload(msg: dict) -> dict:
    payload = msg.get("payload") or {}
    return payload if isinstance(payload, dict) else {}


def _ptz_ack_error(msg_type: str, msg_id, error: str) -> None:
    _send_ptz_manual_ack(msg_type, msg_id, False, error=error)


def _handle_ptz_pan_step(payload: dict, msg_type: str, msg_id) -> None:
    global _pan_deg
    direction = payload.get("direction")
    if direction not in ("left", "right"):
        _ptz_ack_error(msg_type, msg_id, f"direction must be left|right, got {direction!r}")
        return
    steps = payload.get("steps", 1)
    if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
        steps = 1
    sign = 1 if direction == "right" else -1
    with _pan_deg_lock:
        _pan_deg += sign * steps * PAN_DEG_PER_STEP
        pan_deg = _pan_deg
    _ptz_manual_q.put({"type": "pan_step", "direction": direction, "steps": steps})
    _send_ptz_manual_ack(msg_type, msg_id, True, {"panDeg": round(pan_deg, 2)})


def _handle_ptz_move_start(payload: dict, msg_type: str, msg_id) -> None:
    direction = payload.get("direction")
    if direction not in ("left", "right"):
        _ptz_ack_error(msg_type, msg_id, f"direction must be left|right, got {direction!r}")
        return
    sps = payload.get("stepsPerSecond", 10)
    if not isinstance(sps, (int, float)) or isinstance(sps, bool) or sps <= 0:
        sps = 10
    _ptz_manual_q.put({"type": "move_start", "direction": direction, "steps_per_second": int(sps)})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


def _handle_ptz_move_stop(_payload: dict, msg_type: str, msg_id) -> None:
    _ptz_manual_q.put({"type": "move_stop"})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


def _handle_ptz_set_mode(payload: dict, msg_type: str, msg_id) -> None:
    mode = payload.get("mode")
    if mode not in ("manual", "automatic"):
        _ptz_ack_error(msg_type, msg_id, f"mode must be manual|automatic, got {mode!r}")
        return
    _ptz_manual_q.put({"type": "set_mode", "mode": mode})
    _send_ptz_manual_ack(msg_type, msg_id, True)


def _handle_ptz_zoom_step(payload: dict, msg_type: str, msg_id) -> None:
    direction = payload.get("direction")
    if direction not in ("in", "out"):
        _ptz_ack_error(msg_type, msg_id, f"direction must be in|out, got {direction!r}")
        return
    steps = int(payload.get("steps", 1))
    if steps <= 0:
        _ptz_ack_error(msg_type, msg_id, "steps must be > 0")
        return
    _ptz_manual_q.put({"type": "zoom_step", "direction": direction, "steps": steps})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


def _handle_ptz_zoom_start(payload: dict, msg_type: str, msg_id) -> None:
    direction = payload.get("direction")
    if direction not in ("in", "out"):
        _ptz_ack_error(msg_type, msg_id, f"direction must be in|out, got {direction!r}")
        return
    sps = max(1, int(payload.get("stepsPerSecond", 10)))
    _ptz_manual_q.put({"type": "zoom_start", "direction": direction, "steps_per_second": sps})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


def _handle_ptz_zoom_stop(_payload: dict, msg_type: str, msg_id) -> None:
    _ptz_manual_q.put({"type": "zoom_stop"})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


def _handle_ptz_focus_offset(payload: dict, msg_type: str, msg_id) -> None:
    offset = payload.get("offset", 0)
    if not isinstance(offset, int) or offset == 0:
        _ptz_ack_error(msg_type, msg_id, "offset must be a non-zero integer")
        return
    _ptz_manual_q.put({"type": "focus_offset", "offset": offset})
    _send_ptz_manual_ack(msg_type, msg_id, True, {})


_PTZ_MANUAL_HANDLERS = {
    "cmd.cam_pan_step": _handle_ptz_pan_step,
    "cmd.cam_move_start": _handle_ptz_move_start,
    "cmd.cam_move_stop": _handle_ptz_move_stop,
    "cmd.set_cam_mode": _handle_ptz_set_mode,
    "cmd.cam_zoom_step": _handle_ptz_zoom_step,
    "cmd.cam_zoom_start": _handle_ptz_zoom_start,
    "cmd.cam_zoom_stop": _handle_ptz_zoom_stop,
    "cmd.cam_focus_offset": _handle_ptz_focus_offset,
}


def _dispatch_ptz_manual_cmd(msg: dict) -> None:
    msg_type = msg.get("type", "")
    handler = _PTZ_MANUAL_HANDLERS.get(msg_type)
    if handler:
        handler(_ptz_payload(msg), msg_type, msg.get("id", ""))


# ---------------------------------------------------------------------------
# PTZ control socket server
# ---------------------------------------------------------------------------
_ptz_control_clients: list[socket.socket] = []
_ptz_control_clients_lock = threading.Lock()
_ptz_control_q: queue.SimpleQueue = queue.SimpleQueue()


def _json_socket_sender_loop(
        out_q: queue.SimpleQueue,
        clients: list[socket.socket],
        clients_lock,
) -> None:
    while True:
        msg = out_q.get()
        line = (json.dumps(msg) + "\n").encode()
        with clients_lock:
            dead = []
            for conn in clients:
                try:
                    conn.sendall(line)
                except OSError:
                    dead.append(conn)
            for conn in dead:
                clients.remove(conn)
                try:
                    conn.close()
                except OSError:
                    pass


def start_ptz_control_server() -> None:
    srv = create_unix_stream_server(PTZ_CONTROL_SOCK, "PTZ control socket")

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                with _ptz_control_clients_lock:
                    _ptz_control_clients.append(conn)
                print("[ptz-control] client connected")
            except OSError:
                break

    threading.Thread(
        target=_json_socket_sender_loop,
        args=(_ptz_control_q, _ptz_control_clients, _ptz_control_clients_lock),
        daemon=True,
        name="ptz-control-sender",
    ).start()
    threading.Thread(target=_accept_loop, daemon=True, name="ptz-control-accept").start()


def send_to_ptz_control(cam_label: str, frame_num: int, detections: list) -> None:
    _ptz_control_q.put({
        "camera": _stream_camera_name(cam_label),
        "frame": frame_num,
        "timestamp": time.time(),
        "detections": detections,
    })


# ---------------------------------------------------------------------------
# Go bridge socket server
# ---------------------------------------------------------------------------
_go_bridge_clients: list[socket.socket] = []
_go_bridge_clients_lock = threading.Lock()
_go_bridge_out_q: queue.SimpleQueue = queue.SimpleQueue()


def _register_go_connection(conn: socket.socket) -> None:
    with _go_bridge_clients_lock:
        _go_bridge_clients.append(conn)
    _push_state()
    _replay_cached_stream_status()


def _replay_cached_stream_status() -> None:
    cached_status = _get_cached_stream_status()
    if cached_status is None:
        return
    _go_bridge_out_q.put(cached_status)
    print(f"[stream_status] replayed to new Go connection: active={cached_status.get('active')}")


def _decode_go_message(line: bytes) -> dict | None:
    line = line.strip()
    if not line:
        return None
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        return None
    if isinstance(msg, dict):
        return msg
    print(f"[go] ignoring non-object message: {msg!r}")
    return None


def _dispatch_go_message(msg: dict) -> None:
    msg_type = msg.get("type")
    if msg_type == "cmd":
        _dispatch_cmd(msg)
    elif msg_type == "ping":
        _go_bridge_out_q.put({"type": "pong"})
    elif msg_type in _PTZ_CMD_TYPES:
        _dispatch_ptz_manual_cmd(msg)


def _read_go_messages(conn: socket.socket) -> None:
    buf = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            return
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            msg = _decode_go_message(line)
            if msg is not None:
                _dispatch_go_message(msg)


def _unregister_go_connection(conn: socket.socket) -> None:
    with _go_bridge_clients_lock:
        try:
            _go_bridge_clients.remove(conn)
        except ValueError:
            pass
    _close_socket_quietly(conn)


def _handle_go_connection(conn: socket.socket) -> None:
    _register_go_connection(conn)
    try:
        _read_go_messages(conn)
    except OSError:
        pass
    finally:
        _unregister_go_connection(conn)


def _ack(action: str, ok: bool, error: str = "") -> None:
    msg: dict = {"type": "ack", "action": action, "ok": ok}
    if error:
        msg["error"] = error
    _go_bridge_out_q.put(msg)


def _cmd_fail(action: str, err: str) -> None:
    print(f"[cmd] {action}: {err}")
    _ack(action, False, err)


def _normalize_rtmp_url(raw_url: object) -> tuple[str | None, str]:
    if not isinstance(raw_url, str):
        return None, f"rtmp_url must be string, got {type(raw_url).__name__}"
    rtmp_url = raw_url.strip()
    if rtmp_url.startswith(("rtmp://", "rtmps://")):
        return rtmp_url, ""
    return None, f"invalid rtmp_url: {rtmp_url!r}"


def _handle_start_stream_cmd(msg: dict) -> None:
    raw_url = msg.get("rtmp_url")
    rtmp_url, err = _normalize_rtmp_url(raw_url)
    if rtmp_url is None:
        _cmd_fail("start_stream", err)
        return

    _atomic_write_text(STREAM_CONF, rtmp_url + "\n")
    ok, info = _start_stream_worker()
    if not ok:
        _cmd_fail("start_stream", f"failed to start stream worker: {info}")
        return

    print(f"[cmd] start_stream -> {rtmp_url[:60]} ({info})")
    _ack("start_stream", True)
    _push_state()
    _poll_stream_worker_status()


def _handle_stop_stream_cmd(_msg: dict) -> None:
    _atomic_write_text(STREAM_CONF, "# disabled\n")
    ok, info = _stop_stream_worker()
    print(f"[cmd] stop_stream ({info})")
    _ack("stop_stream", ok, "" if ok else info)
    _push_state()
    _sync_stream_status_cache(False)


def _handle_set_config_cmd(msg: dict) -> None:
    bitrate = msg.get("bitrateKbps")
    if not isinstance(bitrate, int) or isinstance(bitrate, bool) or not (100 <= bitrate <= 50000):
        _cmd_fail("set_config", f"bitrateKbps must be int 100-50000, got {bitrate!r}")
        return
    _persist_stream_worker_config(bitrate_kbps=bitrate)
    running = _is_stream_worker_running()
    print(f"[cmd] set_config bitrateKbps={bitrate} -> worker config (running={running})")
    _ack("set_config", True)


def _handle_switch_cam_cmd(msg: dict) -> None:
    raw_cam = msg.get("camId", msg.get("camera", msg.get("cam")))
    normalized = _normalize_stream_camera(raw_cam)
    if normalized is None:
        err = (
            "camId must be fixed or ptz "
            f"(legacy aliases 0,2,cam0,cam2,camera0,camera2 also work); got {raw_cam!r}"
        )
        _cmd_fail("switch_cam", err)
        return
    _persist_stream_worker_config(active_camera=normalized)
    _request_program_camera_switch(normalized)
    running = _is_stream_worker_running()
    print(f"[cmd] switch_cam -> {normalized} (running={running})")
    _ack("switch_cam", True)
    _push_state()


def _handle_set_osd_cmd(msg: dict) -> None:
    visible = msg.get("visible")
    if not isinstance(visible, bool):
        _cmd_fail("set_osd", f"visible must be bool, got {visible!r}")
        return
    _apply_score_patch({"visible": visible})
    print(f"[cmd] set_osd visible={visible}")
    _ack("set_osd", True)


def _handle_set_score_cmd(msg: dict) -> None:
    try:
        _apply_score_patch(msg)
    except ValueError as e:
        _cmd_fail("set_score", str(e))
        return
    print("[cmd] set_score applied")
    _ack("set_score", True)


_CMD_HANDLERS = {
    "start_stream": _handle_start_stream_cmd,
    "stop_stream": _handle_stop_stream_cmd,
    "set_config": _handle_set_config_cmd,
    "switch_cam": _handle_switch_cam_cmd,
    "set_osd": _handle_set_osd_cmd,
    "set_score": _handle_set_score_cmd,
}


def _dispatch_cmd(msg: dict) -> None:
    action = msg.get("action", "")
    if not isinstance(action, str):
        err = f"action must be string, got {action!r}"
        print(f"[cmd] {err}")
        _ack("", False, err)
        return

    handler = _CMD_HANDLERS.get(action)
    if handler:
        handler(msg)
        return

    err = f"unknown action: {action!r}"
    print(f"[cmd] {err}")
    _ack(action, False, err)


MODEL_NAME = "Basketball"
AVAILABLE_MODELS = ["Basketball"]


def _push_state() -> None:
    url = read_stream_url()
    worker_running = _is_stream_worker_running()
    worker_cfg = _read_stream_worker_config()
    active_camera = (
        _program_active_camera
        if _program_selector is not None
        else worker_cfg.get("activeCamera", PTZ_CAMERA)
    )

    webrtc = {}
    internal_streams = {}
    program_clean_rtsp_url = f"rtsp://{JETSON_HOST}:8554/{PROGRAM_WEBRTC_RTSP_PATH}"
    program_clean_webrtc_url = f"http://{JETSON_HOST}:8889/{PROGRAM_WEBRTC_RTSP_PATH}"
    program_stream_rtsp_url = f"rtsp://{JETSON_HOST}:8554/{PROGRAM_STREAM_RTSP_PATH}"
    switch_meta = {
        "seq": _program_switch_seq,
        "at_ms": _program_last_switch_at_ms,
        "active_camera": active_camera,
        "previous_camera": _program_previous_camera,
    }

    webrtc["program_clean"] = program_clean_webrtc_url
    internal_streams["program_clean"] = program_clean_rtsp_url
    internal_streams["program_stream"] = program_stream_rtsp_url

    if ENABLE_FIXED_CAMERA and _ai_stream_enabled("CAM0"):
        fixed_ai_url = f"http://{JETSON_HOST}:8889/camera0_ai"
        webrtc["fixed_ai"] = fixed_ai_url
        webrtc["cam0_ai"] = fixed_ai_url

    if ENABLE_PTZ_CAMERA and _ai_stream_enabled("CAM2"):
        ptz_ai_url = f"http://{JETSON_HOST}:8889/camera2_ai"
        webrtc["ptz_ai"] = ptz_ai_url
        webrtc["cam2_ai"] = ptz_ai_url

    _go_bridge_out_q.put({
        "type": "state",
        "streaming": bool(url) and worker_running,
        "stream_configured": bool(url),
        "stream_worker_running": worker_running,
        "stream_active_camera": active_camera,
        "stream_switch_seq": _program_switch_seq,
        "stream_switch_at_ms": _program_last_switch_at_ms,
        "stream_previous_camera": _program_previous_camera,
        "model": MODEL_NAME,
        "available_models": AVAILABLE_MODELS,
        "enabled_cameras": {
            FIXED_CAMERA: ENABLE_FIXED_CAMERA,
            PTZ_CAMERA: ENABLE_PTZ_CAMERA,
            "cam0": ENABLE_FIXED_CAMERA,
            "cam2": ENABLE_PTZ_CAMERA,
        },
        "enabled_ai": {
            FIXED_CAMERA: _ai_enabled("CAM0"),
            PTZ_CAMERA: _ai_enabled("CAM2"),
            "cam0": _ai_enabled("CAM0"),
            "cam2": _ai_enabled("CAM2"),
        },
        "enabled_ai_streams": {
            FIXED_CAMERA: _ai_stream_enabled("CAM0"),
            PTZ_CAMERA: _ai_stream_enabled("CAM2"),
            "cam0": _ai_stream_enabled("CAM0"),
            "cam2": _ai_stream_enabled("CAM2"),
        },
        "webrtc": webrtc,
        "internal_streams": internal_streams,
        "program_switch": switch_meta,
    })


def start_go_bridge_server() -> None:
    try:
        os.unlink(GO_BRIDGE_SOCK)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(GO_BRIDGE_SOCK)
    os.chmod(GO_BRIDGE_SOCK, 0o660)
    srv.listen(4)
    print(f"Go bridge socket -> {GO_BRIDGE_SOCK}")
    threading.Thread(
        target=_json_socket_sender_loop,
        args=(_go_bridge_out_q, _go_bridge_clients, _go_bridge_clients_lock),
        daemon=True,
        name="go-bridge-sender",
    ).start()

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=_handle_go_connection,
                                 args=(conn,), daemon=True).start()
            except OSError:
                break

    threading.Thread(target=_accept_loop, daemon=True, name="go-bridge-accept").start()


# ---------------------------------------------------------------------------
# Pipeline element helpers
# ---------------------------------------------------------------------------
def _make(factory: str, name: str) -> Gst.Element:
    el = Gst.ElementFactory.make(factory, name)
    if not el:
        sys.stderr.write(f"ERROR: Unable to create '{factory}' (name='{name}')\n")
        sys.exit(1)
    return el


def _link(src: Gst.Element, dst: Gst.Element) -> None:
    if not src.link(dst):
        sys.stderr.write(f"ERROR: Failed to link {src.get_name()} -> {dst.get_name()}\n")
        sys.exit(1)


def _link_many(*elements: Gst.Element) -> None:
    for src, dst in zip(elements, elements[1:]):
        _link(src, dst)


def _link_filtered(src: Gst.Element, dst: Gst.Element, caps_str: str) -> None:
    caps = Gst.Caps.from_string(caps_str)
    if not src.link_filtered(dst, caps):
        sys.stderr.write(
            f"ERROR: Failed to link (filtered) {src.get_name()} -> {dst.get_name()}\n"
            f"       caps: {caps_str}\n"
        )
        sys.exit(1)


def _request_mux_sinkpad(mux: Gst.Element, pad_name: str) -> Gst.Pad:
    pad = mux.request_pad_simple(pad_name)
    if not pad:
        sys.stderr.write(f"ERROR: Unable to get pad '{pad_name}' from {mux.get_name()}\n")
        sys.exit(1)
    return pad


def _get_static_pad(el: Gst.Element, pad_name: str) -> Gst.Pad:
    pad = el.get_static_pad(pad_name)
    if not pad:
        sys.stderr.write(f"ERROR: Unable to get pad '{pad_name}' from {el.get_name()}\n")
        sys.exit(1)
    return pad


def _tee_branch(tee: Gst.Element, first_el: Gst.Element) -> None:
    tee_src = tee.request_pad_simple("src_%u")
    if not tee_src:
        sys.stderr.write(f"ERROR: Unable to request src pad from {tee.get_name()}\n")
        sys.exit(1)
    sink_pad = _get_static_pad(first_el, "sink")
    if tee_src.link(sink_pad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link tee -> {first_el.get_name()}.sink\n")
        sys.exit(1)


def _link_src_to_request_pad(src: Gst.Element, sink: Gst.Element, pad_name: str,
                             err_label: str) -> None:
    src_pad = _get_static_pad(src, "src")
    sink_pad = sink.request_pad_simple(pad_name)
    if not sink_pad:
        sys.stderr.write(
            f"ERROR: Unable to request pad '{pad_name}' from {sink.get_name()} for {err_label}\n"
        )
        sys.exit(1)
    if src_pad.link(sink_pad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(
            f"ERROR: Failed to link {src.get_name()} -> {sink.get_name()}.{pad_name} for {err_label}\n"
        )
        sys.exit(1)


def _recording_enabled(cam_label: str) -> bool:
    cam = _pipeline_camera_label(cam_label)
    return cam == "CAM2" and ENABLE_PTZ_CAMERA_AI_RECORDING and _ai_enabled(cam)


def _recording_location_pattern(cam_label: str) -> str:
    cam = _stream_camera_name(cam_label)
    os.makedirs(RECORDINGS_DIR, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    return os.path.join(
        RECORDINGS_DIR,
        f"{cam}_ai_{ts}_part%02d.{RECORD_FILE_EXTENSION}",
    )


def _make_nvconv(name: str) -> Gst.Element:
    el = _make("nvvideoconvert", name)
    el.set_property("gpu-id", 0)
    el.set_property("copy-hw", 2)
    return el


def _capsfilter(name: str, caps_str: str) -> Gst.Element:
    el = _make("capsfilter", name)
    el.set_property("caps", Gst.Caps.from_string(caps_str))
    return el


def _set_if_supported(el: Gst.Element, prop: str, value: Any) -> None:
    if el.find_property(prop) is not None:
        el.set_property(prop, value)


def _request_program_camera_switch(active_camera: str) -> None:
    global _program_switch_requested_camera, _program_switch_timer_queued

    normalized = _normalize_stream_camera(active_camera) or PTZ_CAMERA

    schedule_timer = False
    with _program_switch_lock:
        _program_switch_requested_camera = normalized
        if not _program_switch_timer_queued:
            _program_switch_timer_queued = True
            schedule_timer = True

    if schedule_timer:
        GLib.idle_add(_queue_program_camera_switch_timer)


def _queue_program_camera_switch_timer() -> bool:
    delay_ms = PROGRAM_SWITCH_DEBOUNCE_MS

    with _program_switch_lock:
        remaining_ms = max(
            0,
            int((_program_switch_settle_until_monotonic - time.monotonic()) * 1000),
        )
        if remaining_ms > delay_ms:
            delay_ms = remaining_ms

    GLib.timeout_add(delay_ms, _drain_program_camera_switch_request)
    return False


def _drain_program_camera_switch_request() -> bool:
    global _program_switch_requested_camera, _program_switch_timer_queued
    global _program_switch_settle_until_monotonic

    requested_camera: str | None = None

    with _program_switch_lock:
        remaining_ms = max(
            0,
            int((_program_switch_settle_until_monotonic - time.monotonic()) * 1000),
        )
        if remaining_ms > 0:
            return True
        else:
            requested_camera = _program_switch_requested_camera
            _program_switch_requested_camera = None
            _program_switch_timer_queued = False

    if requested_camera is not None:
        previous_switch_seq = _program_switch_seq
        _switch_program_camera(requested_camera)
        if _program_switch_seq != previous_switch_seq:
            with _program_switch_lock:
                _program_switch_settle_until_monotonic = (
                        time.monotonic() + (PROGRAM_SWITCH_SETTLE_MS / 1000.0)
                )
    return False


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def read_stream_url() -> str | None:
    if not os.path.exists(STREAM_CONF):
        _atomic_write_text(STREAM_CONF, "# disabled\n")
        return None
    with open(STREAM_CONF) as f:
        url = f.read().strip()
    return None if (not url or url.startswith("#")) else url


def _request_restart() -> None:
    def _kill():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGUSR1)

    threading.Thread(target=_kill, daemon=True).start()


def _atomic_write_text(path: str, content: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _atomic_write_json(path: str, data: dict) -> None:
    _atomic_write_text(path, json.dumps(data, indent=2, sort_keys=True) + "\n")


def _persist_score_state() -> None:
    with score_lock:
        state = score_state.copy()
    _atomic_write_json(SCORE_STATE_FILE, state)


def _read_stream_worker_config() -> dict:
    default = {"bitrateKbps": RTMP_BITRATE, "activeCamera": PTZ_CAMERA}
    try:
        with open(STREAM_WORKER_CONFIG) as f:
            data = json.load(f)
            if isinstance(data, dict):
                cfg = default.copy()
                cfg.update(data)
                normalized = _normalize_stream_camera(cfg.get("activeCamera"))
                cfg["activeCamera"] = normalized or PTZ_CAMERA
                return cfg
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return default


def _normalize_stream_camera(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return CAMERA_DEVICE_ALIASES.get(text)


def _persist_stream_worker_config(*, bitrate_kbps: int | None = None, active_camera: str | None = None) -> dict:
    cfg = _read_stream_worker_config()
    if bitrate_kbps is not None:
        cfg["bitrateKbps"] = bitrate_kbps
    if active_camera is not None:
        normalized = _normalize_stream_camera(active_camera)
        if normalized is not None:
            cfg["activeCamera"] = normalized
    cfg.setdefault("activeCamera", PTZ_CAMERA)
    _atomic_write_json(STREAM_WORKER_CONFIG, cfg)
    return cfg


def _read_stream_worker_status() -> dict:
    default = {"worker_alive": False, "stream_active": False, "last_error": ""}
    try:
        with open(STREAM_WORKER_STATUS) as f:
            data = json.load(f)
        if isinstance(data, dict):
            default.update(data)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return default


_last_worker_status_seen: dict | None = None
_worker_ctl_lock = threading.Lock()


def _poll_stream_worker_status() -> bool:
    global _last_worker_status_seen

    configured = bool(read_stream_url())
    running = _is_stream_worker_running()
    status = _read_stream_worker_status()

    if not configured or not running:
        active = False
        error = str(status.get("last_error", "") if configured else "")
    else:
        active = bool(status.get("stream_active", False))
        error = str(status.get("last_error", "") or "")

    next_state = {"active": active, "error": error}
    if _last_worker_status_seen != next_state:
        _last_worker_status_seen = next_state
        _send_stream_status(active, error)
    return True


def _process_start_ticks(pid: int) -> int | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields_after_comm = stat.rsplit(") ", 1)[1].split()
        return int(fields_after_comm[19])
    except (OSError, IndexError, ValueError):
        return None


def _process_cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    return [os.fsdecode(part) for part in raw.split(b"\0") if part]


def _worker_pid_payload(pid: int) -> dict:
    payload = {
        "pid": pid,
        "role": STREAM_WORKER_PID_ROLE,
        "script": STREAM_WORKER_WRAPPER,
        "owner_pid": os.getpid(),
    }
    start_ticks = _process_start_ticks(pid)
    if start_ticks is not None:
        payload["start_ticks"] = start_ticks
    owner_start_ticks = _process_start_ticks(os.getpid())
    if owner_start_ticks is not None:
        payload["owner_start_ticks"] = owner_start_ticks
    return payload


def _write_worker_pid(pid: int) -> None:
    _atomic_write_json(STREAM_WORKER_PID, _worker_pid_payload(pid))


def _set_worker_status(worker_alive: bool, stream_active: bool, last_error: str = "") -> None:
    _atomic_write_json(STREAM_WORKER_STATUS, {
        "worker_alive": worker_alive,
        "stream_active": stream_active,
        "last_error": last_error,
    })


def _read_worker_pid_info() -> dict | None:
    try:
        raw = Path(STREAM_WORKER_PID).read_text().strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            pid = int(raw)
        except ValueError:
            return None
        return {"pid": pid, "legacy": True}

    if isinstance(data, int) and not isinstance(data, bool):
        return {"pid": data, "legacy": True}

    if isinstance(data, dict):
        try:
            pid = int(data.get("pid", 0))
        except (TypeError, ValueError):
            return None
        if pid <= 0:
            return None
        data = dict(data)
        data["pid"] = pid
        return data

    return None


def _pid_exists(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _worker_cmdline_matches(pid: int) -> bool:
    wrapper_path = os.path.abspath(STREAM_WORKER_WRAPPER)
    for arg in _process_cmdline(pid):
        if os.path.abspath(arg) == wrapper_path:
            return True
    return False


def _pid_metadata_int(info: dict, key: str, *, required: bool = False) -> int | None:
    if key not in info:
        if required:
            raise ValueError(f"missing {key}")
        return None

    raw_value = info[key]
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, str)):
        raise ValueError(f"{key} must be int-compatible")
    return int(raw_value)


def _worker_pid_from_info(info: dict) -> int | None:
    try:
        pid = _pid_metadata_int(info, "pid", required=True)
    except ValueError:
        return None
    return pid if pid is not None and _pid_exists(pid) else None


def _worker_role_matches(info: dict) -> bool:
    role = info.get("role")
    return role is None or role == STREAM_WORKER_PID_ROLE


def _worker_script_matches(info: dict) -> bool:
    script = info.get("script")
    return (
            script is None
            or os.path.abspath(str(script)) == os.path.abspath(STREAM_WORKER_WRAPPER)
    )


def _worker_owner_pid_matches(info: dict) -> bool:
    try:
        owner_pid = _pid_metadata_int(info, "owner_pid")
    except ValueError:
        return False
    return owner_pid is None or owner_pid in (0, os.getpid())


def _worker_owner_start_ticks_match(info: dict) -> bool:
    try:
        owner_start_ticks = _pid_metadata_int(info, "owner_start_ticks")
    except ValueError:
        return False
    if owner_start_ticks is None:
        return True
    current_owner_start_ticks = _process_start_ticks(os.getpid())
    return current_owner_start_ticks is not None and current_owner_start_ticks == owner_start_ticks


def _worker_start_ticks_match(info: dict, pid: int) -> bool:
    try:
        expected_start_ticks = _pid_metadata_int(info, "start_ticks")
    except ValueError:
        return False
    if expected_start_ticks is None:
        return True
    current_start_ticks = _process_start_ticks(pid)
    return current_start_ticks is not None and current_start_ticks == expected_start_ticks


def _worker_pid_metadata_matches(info: dict, pid: int) -> bool:
    return (
            _worker_role_matches(info)
            and _worker_script_matches(info)
            and _worker_owner_pid_matches(info)
            and _worker_owner_start_ticks_match(info)
            and _worker_start_ticks_match(info, pid)
    )


def _worker_pid_info_is_current(info: dict | None) -> bool:
    if not info:
        return False

    pid = _worker_pid_from_info(info)
    return (
            pid is not None
            and _worker_pid_metadata_matches(info, pid)
            and _worker_cmdline_matches(pid)
    )


def _signal_worker_process(pid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pid, sig)
        return
    except ProcessLookupError:
        pass
    except OSError:
        pass
    os.kill(pid, sig)


def _is_stream_worker_running() -> bool:
    return _worker_pid_info_is_current(_read_worker_pid_info())


def _start_stream_worker() -> tuple[bool, str]:
    with _worker_ctl_lock:
        if _is_stream_worker_running():
            return True, "already running"
        if not os.path.exists(STREAM_WORKER_WRAPPER):
            return False, f"missing stream worker wrapper: {STREAM_WORKER_WRAPPER}"
        try:
            env = os.environ.copy()
            env["STREAM_OWNER_PID"] = str(os.getpid())
            owner_start_ticks = _process_start_ticks(os.getpid())
            if owner_start_ticks is not None:
                env["STREAM_OWNER_START_TICKS"] = str(owner_start_ticks)
            proc = subprocess.Popen(
                [sys.executable, STREAM_WORKER_WRAPPER],
                cwd=SCRIPT_DIR,
                env=env,
                start_new_session=True,
                stdout=None,
                stderr=None,
            )
            # Write the wrapper PID immediately so duplicate start commands
            # cannot race in before the wrapper writes its own pid file.
            _write_worker_pid(proc.pid)
            _set_worker_status(worker_alive=True, stream_active=False, last_error="")
        except OSError as e:
            return False, str(e)
        print(f"[stream_worker] launched wrapper pid={proc.pid}")
        return True, "started"


def _stop_stream_worker(timeout_sec: float = 5.0) -> tuple[bool, str]:
    with _worker_ctl_lock:
        pid_info = _read_worker_pid_info()
        if pid_info is None or not _worker_pid_info_is_current(pid_info):
            try:
                os.unlink(STREAM_WORKER_PID)
            except FileNotFoundError:
                pass
            _set_worker_status(worker_alive=False, stream_active=False, last_error="")
            return True, "already stopped"

        pid = _worker_pid_from_info(pid_info)
        if pid is None:
            return True, "already stopped"

        try:
            _signal_worker_process(pid, signal.SIGTERM)
        except OSError as e:
            return False, str(e)

        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if not _worker_pid_info_is_current(pid_info):
                break
            time.sleep(0.1)

        if _worker_pid_info_is_current(pid_info):
            try:
                _signal_worker_process(pid, signal.SIGKILL)
            except OSError as e:
                return False, f"failed to terminate stream worker pid {pid}: {e}"

        try:
            os.unlink(STREAM_WORKER_PID)
        except FileNotFoundError:
            pass

        _set_worker_status(worker_alive=False, stream_active=False, last_error="")
        return True, "stopped"


def _sync_stream_status_cache(active: bool, error: str = "") -> None:
    """Update local status caches without emitting a new Go event."""
    global _last_worker_status_seen, _rtmp_status_sent, _rtmp_status_cached
    normalized_error = str(error or "")
    _last_worker_status_seen = {"active": active, "error": normalized_error}
    msg: dict = {"type": "stream_status", "active": active}
    if normalized_error:
        msg["error"] = normalized_error
    with _rtmp_status_lock:
        _rtmp_status_sent = True
        _rtmp_status_cached = msg


def _emit_stream_status_and_sync_cache(active: bool, error: str = "") -> None:
    _sync_stream_status_cache(active, error)
    _go_bridge_out_q.put(_get_cached_stream_status())
    if active:
        print("[stream_status] RTMP stream verified active")
    else:
        print(f"[stream_status] RTMP stream inactive: {error}")


# ---------------------------------------------------------------------------
# Detection probe
# ---------------------------------------------------------------------------
def _iter_nvds_meta_list(node, caster):
    while node is not None:
        try:
            yield caster(node.data)
        except StopIteration:
            return

        try:
            node = node.next
        except StopIteration:
            return


def _record_ai_frame(cam_label: str) -> None:
    if not _ai_fps_metric_enabled(cam_label):
        return
    with _fps_lock:
        if cam_label in _fps_counters:
            _fps_counters[cam_label] += 1


def _should_collect_detections(frame_meta, cam_label: str) -> bool:
    return _ai_enabled(cam_label) and frame_meta.frame_num % PROBE_EVERY_N_FRAMES == 0


def _object_detection_payload(obj_meta) -> dict | None:
    class_name = CLASS_NAMES.get(obj_meta.class_id)
    if class_name is None:
        return None

    rect = obj_meta.rect_params
    return {
        "class": class_name,
        "class_id": obj_meta.class_id,
        "tracker_id": obj_meta.object_id,
        "center_x": round(rect.left + rect.width / 2.0, 1),
        "center_y": round(rect.top + rect.height / 2.0, 1),
        "width": round(rect.width, 1),
        "height": round(rect.height, 1),
        "left": round(rect.left, 1),
        "top": round(rect.top, 1),
        "confidence": round(obj_meta.confidence, 4),
    }


def _collect_frame_detections(frame_meta) -> list[dict]:
    detections = []
    for obj_meta in _iter_nvds_meta_list(frame_meta.obj_meta_list, pyds.NvDsObjectMeta.cast):
        detection = _object_detection_payload(obj_meta)
        if detection is not None:
            detections.append(detection)
    return detections


def _process_detection_frame(frame_meta, cam_label: str) -> None:
    _record_ai_frame(cam_label)
    if not _should_collect_detections(frame_meta, cam_label):
        return

    detections = _collect_frame_detections(frame_meta)
    if detections:
        send_to_ptz_control(cam_label, frame_meta.frame_num, detections)


def _batch_meta_from_probe_info(info):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return None
    return pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))


def pgie_src_pad_buffer_probe(_pad, info, cam_label):
    batch_meta = _batch_meta_from_probe_info(info)
    if not batch_meta:
        return Gst.PadProbeReturn.OK

    frame_meta_list = getattr(batch_meta, "frame_meta_list", None)
    for frame_meta in _iter_nvds_meta_list(frame_meta_list, pyds.NvDsFrameMeta.cast):
        _process_detection_frame(frame_meta, cam_label)

    return Gst.PadProbeReturn.OK


# ---------------------------------------------------------------------------
# Bus message handler
# ---------------------------------------------------------------------------
def bus_call(_bus, message, loop):
    t = message.type
    if t == Gst.MessageType.EOS:
        print("End-of-stream")
        loop.quit()
    elif t == Gst.MessageType.WARNING:
        err, dbg = message.parse_warning()
        print(f"WARNING: {err}: {dbg}")
    elif t == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        src_name = message.src.get_name() if message.src else "unknown"
        print(f"ERROR: {err}: {dbg} (src={src_name})")
        loop.quit()
    return True


# ---------------------------------------------------------------------------
# Camera source
# ---------------------------------------------------------------------------
def _build_camera_source(pipeline, device: str, suffix: str):
    src = _make("v4l2src", f"src{suffix}")
    caps_src = _capsfilter(f"caps{suffix}_src",
                           "image/jpeg,width=1920,height=1080,framerate=30/1")
    jparse = _make("jpegparse", f"jparse{suffix}")
    dec = _make("nvv4l2decoder", f"dec{suffix}")
    conv_src = _make_nvconv(f"conv{suffix}_src")
    caps_nvmm = _capsfilter(f"caps{suffix}_nvmm",
                            "video/x-raw(memory:NVMM),format=NV12")
    tee = _make("tee", f"tee{suffix}")

    src.set_property("device", device)
    _set_if_supported(src, "do-timestamp", True)
    dec.set_property("mjpeg", 1)

    for el in (src, caps_src, jparse, dec, conv_src, caps_nvmm, tee):
        pipeline.add(el)

    _link(src, caps_src)
    _link(caps_src, jparse)
    _link(jparse, dec)
    _link(dec, conv_src)
    _link(conv_src, caps_nvmm)
    _link(caps_nvmm, tee)

    return tee


def _configure_x264_encoder(
        enc: Gst.Element,
        *,
        tune: str,
        preset: str,
        bitrate: int,
        keyint: int,
        threads: int,
) -> None:
    enc.set_property("tune", tune)
    enc.set_property("speed-preset", preset)
    enc.set_property("bitrate", bitrate)
    enc.set_property("key-int-max", keyint)
    enc.set_property("threads", threads)
    _set_if_supported(enc, "byte-stream", True)
    _set_if_supported(enc, "bframes", 0)
    _set_if_supported(enc, "b-adapt", False)
    _set_if_supported(enc, "rc-lookahead", 0)
    _set_if_supported(enc, "sync-lookahead", 0)
    _set_if_supported(enc, "cabac", False)
    _set_if_supported(enc, "dct8x8", False)
    _set_if_supported(enc, "mb-tree", False)
    _set_if_supported(enc, "ref", 1)
    _set_if_supported(enc, "sliced-threads", True)


def _configure_rtsp_sink(sink: Gst.Element, rtsp_path: str) -> None:
    sink.set_property("location", f"rtsp://127.0.0.1:8554/{rtsp_path}")
    sink.set_property("protocols", 4)


def _build_simple_rtsp_encode_branch(
        pipeline,
        tee,
        suffix: str,
        branch_name: str,
        rtsp_path: str,
        caps_name: str | None = None,
) -> Gst.Element:
    q = _make("queue", f"q{suffix}_{branch_name}")
    conv = _make_nvconv(f"conv{suffix}_{branch_name}")
    caps = _capsfilter(caps_name or f"caps{suffix}_{branch_name}", RAW_I420_CAPS)
    enc = _make("x264enc", f"enc{suffix}_{branch_name}")
    parse = _make("h264parse", f"parse{suffix}_{branch_name}")
    sink = _make("rtspclientsink", f"sink{suffix}_{branch_name}")

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)
    _set_if_supported(parse, "config-interval", -1)
    _configure_rtsp_sink(sink, rtsp_path)

    for el in (q, conv, caps, enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q)
    _link_many(q, conv, caps, enc, parse, sink)

    return enc


# ---------------------------------------------------------------------------
# AI branch
# ---------------------------------------------------------------------------
def _configure_queue(
        queue_el: Gst.Element,
        *,
        max_size_buffers: int = 2,
        leaky: int = 2,
) -> None:
    queue_el.set_property("max-size-buffers", max_size_buffers)
    queue_el.set_property("max-size-bytes", 0)
    queue_el.set_property("max-size-time", 0)
    queue_el.set_property("leaky", leaky)


def _configure_ai_mux(mux: Gst.Element) -> None:
    mux.set_property("width", 1280)
    mux.set_property("height", 720)
    mux.set_property("batch-size", 1)
    mux.set_property("batched-push-timeout", 33333)
    mux.set_property("live-source", 1)
    mux.set_property("nvbuf-memory-type", 0)


def _configure_ai_tracker(tracker: Gst.Element) -> None:
    tracker.set_property(
        "ll-lib-file",
        "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so",
    )
    tracker.set_property(
        "ll-config-file",
        "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_IOU.yml",
    )
    tracker.set_property("tracker-width", 1280)
    tracker.set_property("tracker-height", 736)
    tracker.set_property("gpu-id", 0)
    tracker.set_property("display-tracking-id", 1)


def _configure_ai_recording_sink(rec: Gst.Element, cam_label: str) -> None:
    rec.set_property("location", _recording_location_pattern(cam_label))
    rec.set_property("max-size-time", RECORD_SEGMENT_SECONDS * Gst.SECOND)
    rec.set_property("muxer-factory", RECORD_MUXER_FACTORY)
    rec.set_property("send-keyframe-requests", True)
    rec.set_property("async-finalize", True)


def _configure_ai_drain(drain: Gst.Element) -> None:
    _set_if_supported(drain, "sync", False)
    _set_if_supported(drain, "async", False)
    _set_if_supported(drain, "enable-last-sample", False)


def _build_ai_base_elements(suffix: str, infer_config: str) -> dict[str, Gst.Element]:
    mux = _make("nvstreammux", f"mux{suffix}")
    pgie = _make("nvinfer", f"pgie{suffix}")
    tracker = _make("nvtracker", f"tracker{suffix}")
    pgie.set_property("config-file-path", infer_config)
    _configure_ai_mux(mux)
    _configure_ai_tracker(tracker)
    return {
        "q_ai": _make("queue", f"q{suffix}_ai"),
        "conv_ai": _make_nvconv(f"conv{suffix}_ai"),
        "caps_ai": _capsfilter(
            f"caps{suffix}_ai",
            "video/x-raw(memory:NVMM),format=NV12,width=1280,height=720",
        ),
        "mux": mux,
        "pgie": pgie,
        "tracker": tracker,
    }


def _build_ai_debug_elements(suffix: str) -> dict[str, Gst.Element]:
    nvosd = _make("nvdsosd", f"nvosd{suffix}")
    nvosd.set_property("process-mode", 1)
    enc = _make("x264enc", f"enc{suffix}_ai")
    _configure_x264_encoder(
        enc,
        tune=AI_TUNE,
        preset=AI_PRESET,
        bitrate=AI_BITRATE,
        keyint=AI_KEYINT,
        threads=AI_THREADS,
    )
    return {
        "conv_pre": _make_nvconv(f"conv{suffix}_pre"),
        "nvosd": nvosd,
        "conv_post": _make_nvconv(f"conv{suffix}_post"),
        "caps_post": _capsfilter(f"caps{suffix}_post", RAW_I420_CAPS),
        "q_post": _make("queue", f"q{suffix}_post"),
        "enc": enc,
        "parse": _make("h264parse", f"parse{suffix}_ai"),
        "parse_tee": _make("tee", f"tee{suffix}_ai_parse"),
    }


def _build_ai_rtsp_elements(suffix: str, rtsp_path: str) -> dict[str, Gst.Element]:
    sink = _make("rtspclientsink", f"sink{suffix}_ai")
    _configure_rtsp_sink(sink, rtsp_path)
    return {
        "q_rtsp": _make("queue", f"q{suffix}_ai_rtsp"),
        "sink": sink,
    }


def _build_ai_recording_elements(suffix: str, cam_label: str) -> dict[str, Gst.Element]:
    rec = _make("splitmuxsink", f"rec{suffix}_ai")
    _configure_ai_recording_sink(rec, cam_label)
    return {
        "q_rec": _make("queue", f"q{suffix}_ai_record"),
        "rec": rec,
    }


def _build_ai_drain_element(suffix: str) -> dict[str, Gst.Element]:
    drain = _make("fakesink", f"sink{suffix}_ai_process")
    _configure_ai_drain(drain)
    return {"drain": drain}


def _add_pipeline_elements(pipeline, elements: list[Gst.Element | None]) -> None:
    for el in elements:
        if el is not None:
            pipeline.add(el)


def _ai_branch_elements(ctx: dict) -> list[Gst.Element | None]:
    elements = [
        ctx["q_ai"], ctx["conv_ai"], ctx["caps_ai"],
        ctx["mux"], ctx["pgie"], ctx["tracker"],
    ]
    if ctx["debug_video_enabled"]:
        elements.extend([
            ctx["conv_pre"], ctx["nvosd"], ctx["conv_post"], ctx["caps_post"],
            ctx["q_post"], ctx["enc"], ctx["parse"], ctx["parse_tee"],
            ctx.get("q_rtsp"), ctx.get("sink"),
        ])
    else:
        elements.append(ctx.get("drain"))
    elements.extend([ctx.get("q_rec"), ctx.get("rec")])
    return elements


def _configure_ai_branch_queues(ctx: dict) -> None:
    _configure_queue(ctx["q_ai"])
    if ctx.get("q_post") is not None:
        _configure_queue(ctx["q_post"])
    if ctx.get("q_rtsp") is not None:
        _configure_queue(ctx["q_rtsp"])
    if ctx.get("q_rec") is not None:
        _configure_queue(ctx["q_rec"], max_size_buffers=RECORD_QUEUE_BUFFERS)


def _link_ai_input_to_mux(tee, ctx: dict, suffix: str) -> None:
    _tee_branch(tee, ctx["q_ai"])
    _link(ctx["q_ai"], ctx["conv_ai"])
    _link(ctx["conv_ai"], ctx["caps_ai"])
    caps_ai_src = _get_static_pad(ctx["caps_ai"], "src")
    mux_sinkpad = _request_mux_sinkpad(ctx["mux"], "sink_0")
    if caps_ai_src.link(mux_sinkpad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link caps{suffix}_ai -> mux{suffix}.sink_0\n")
        sys.exit(1)


def _link_ai_debug_path(ctx: dict, cam_label: str) -> None:
    _link_many(
        ctx["mux"], ctx["pgie"], ctx["tracker"], ctx["conv_pre"], ctx["nvosd"],
        ctx["conv_post"], ctx["caps_post"], ctx["q_post"], ctx["enc"],
        ctx["parse"], ctx["parse_tee"],
    )
    if ctx.get("q_rtsp") is not None and ctx.get("sink") is not None:
        _tee_branch(ctx["parse_tee"], ctx["q_rtsp"])
        _link(ctx["q_rtsp"], ctx["sink"])
    _link_ai_recording_path(ctx, cam_label)


def _link_ai_recording_path(ctx: dict, cam_label: str) -> None:
    if ctx.get("q_rec") is None or ctx.get("rec") is None:
        print(f"{_stream_camera_name(cam_label)} AI recording disabled")
        return
    _tee_branch(ctx["parse_tee"], ctx["q_rec"])
    _link_src_to_request_pad(ctx["q_rec"], ctx["rec"], "video", f"{_stream_camera_name(cam_label)} recording")
    print(
        f"{_stream_camera_name(cam_label)} AI recording enabled -> {RECORDINGS_DIR} "
        f"({RECORD_SEGMENT_SECONDS}s segments, .{RECORD_FILE_EXTENSION})"
    )


def _link_ai_processing_path(ctx: dict) -> None:
    _link_many(ctx["mux"], ctx["pgie"], ctx["tracker"], ctx["drain"])


def _build_ai_branch_context(suffix: str, rtsp_path: str, infer_config: str, cam_label: str) -> dict:
    stream_enabled = _ai_stream_enabled(cam_label)
    recording_enabled = _recording_enabled(cam_label)
    ctx = _build_ai_base_elements(suffix, infer_config)
    ctx.update({
        "stream_enabled": stream_enabled,
        "recording_enabled": recording_enabled,
        "debug_video_enabled": stream_enabled or recording_enabled,
    })
    if ctx["debug_video_enabled"]:
        ctx.update(_build_ai_debug_elements(suffix))
        if stream_enabled:
            ctx.update(_build_ai_rtsp_elements(suffix, rtsp_path))
        if recording_enabled:
            ctx.update(_build_ai_recording_elements(suffix, cam_label))
    else:
        ctx.update(_build_ai_drain_element(suffix))
    return ctx


def _build_ai_branch(pipeline, tee, suffix: str, rtsp_path: str,
                     infer_config: str, cam_label: str):
    ctx = _build_ai_branch_context(suffix, rtsp_path, infer_config, cam_label)
    _configure_ai_branch_queues(ctx)
    _add_pipeline_elements(pipeline, _ai_branch_elements(ctx))
    _link_ai_input_to_mux(tee, ctx, suffix)

    if ctx["debug_video_enabled"]:
        _link_ai_debug_path(ctx, cam_label)
    else:
        _link_ai_processing_path(ctx)
        print(f"{_stream_camera_name(cam_label)} AI debug stream disabled")

    return ctx["pgie"], ctx.get("enc") if ctx["stream_enabled"] else None


def _link_tee_to_program_selector(
        pipeline: Gst.Pipeline,
        tee: Gst.Element,
        suffix: str,
        selector: Gst.Element,
        camera_name: str,
) -> None:
    q = _make("queue", f"q{suffix}_program_sel")
    caps = _capsfilter(
        f"caps{suffix}_program_sel",
        "video/x-raw(memory:NVMM),format=NV12,width=1920,height=1080,framerate=30/1",
    )

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)

    for el in (q, caps):
        pipeline.add(el)

    _tee_branch(tee, q)
    _link(q, caps)

    src_pad = _get_static_pad(caps, "src")
    sel_pad = selector.request_pad_simple("sink_%u")
    if sel_pad is None:
        sys.stderr.write(f"ERROR: Unable to request selector sink pad for {camera_name}\n")
        sys.exit(1)
    if src_pad.link(sel_pad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link {camera_name} to program selector\n")
        sys.exit(1)

    _program_selector_pads[camera_name] = sel_pad


def _switch_program_camera(active_camera: str, *, force_keyframe: bool = True) -> None:
    global _program_active_camera, _program_previous_camera
    global _program_switch_seq, _program_last_switch_at_ms

    normalized = _normalize_stream_camera(active_camera) or PTZ_CAMERA
    available = set(_program_selector_pads)
    if normalized not in available:
        if PTZ_CAMERA in available:
            normalized = PTZ_CAMERA
        elif FIXED_CAMERA in available:
            normalized = FIXED_CAMERA
        else:
            return

    pad = _program_selector_pads.get(normalized)
    selector: _OverlayPropertyElement | None = _program_selector
    if selector is None:
        return
    if pad is None:
        return

    current = selector.get_property("active-pad")
    if current == pad and _program_active_camera == normalized:
        return

    previous_camera = _program_active_camera
    selector.set_property("active-pad", pad)
    _program_previous_camera = previous_camera
    _program_active_camera = normalized
    _program_switch_seq += 1
    _program_last_switch_at_ms = int(time.time() * 1000)
    print(f"[program] switched source -> {normalized}")
    if force_keyframe:
        force_key_unit(_program_enc, PROGRAM_STREAM_RTSP_PATH, "program-stream")
        force_key_unit(_program_preview_enc, PROGRAM_WEBRTC_RTSP_PATH, "program-preview")
    _push_state()


def _poll_program_config() -> bool:
    global _last_program_cfg

    cfg = _read_stream_worker_config()
    if cfg != _last_program_cfg:
        previous = _last_program_cfg or {}
        _last_program_cfg = dict(cfg)
        if cfg.get("activeCamera") != previous.get("activeCamera"):
            _request_program_camera_switch(cfg.get("activeCamera", PTZ_CAMERA))
        else:
            _push_state()
    return True


def _program_preview_restamp_probe(_pad: Gst.Pad, info: Gst.PadProbeInfo) -> Gst.PadProbeReturn:
    global _program_preview_frame_index

    buf = info.get_buffer()
    if buf is None:
        return Gst.PadProbeReturn.OK

    pts = _program_preview_frame_index * PROGRAM_PREVIEW_FRAME_DURATION_NS
    _program_preview_frame_index += 1
    buf.pts = pts
    buf.dts = pts
    buf.duration = PROGRAM_PREVIEW_FRAME_DURATION_NS

    return Gst.PadProbeReturn.OK


def _build_program_clean_branch(
        pipeline: Gst.Pipeline,
        camera_tees: dict[str, Gst.Element],
) -> tuple[Gst.Element, Gst.Element]:
    global _program_selector, _program_enc, _program_preview_enc, _last_program_cfg

    selector = _make("input-selector", "program_selector")
    tee = _make("tee", "tee_program_outputs")

    q_stream = _make("queue", "q_program_stream")
    conv_stream = _make_nvconv("conv_program_stream")
    caps_stream = _capsfilter("caps_program_stream_i420", RAW_I420_CAPS)
    enc_stream = _make("x264enc", "enc_program_stream")
    h264_stream_caps = _capsfilter(
        "caps_program_stream_h264",
        "video/x-h264,profile=constrained-baseline,stream-format=byte-stream,alignment=au",
    )
    parse_stream = _make("h264parse", "parse_program_stream")
    sink_stream = _make("rtspclientsink", "sink_program_stream")

    q_preview = _make("queue", "q_program_clean")
    conv_preview = _make_nvconv("conv_program_clean")
    caps_preview = _capsfilter(
        "caps_program_clean_i420",
        (
            "video/x-raw,format=I420,"
            f"width={PROGRAM_WEBRTC_WIDTH},height={PROGRAM_WEBRTC_HEIGHT},"
            "framerate=30/1"
        ),
    )
    enc_preview = _make("x264enc", "enc_program_clean")
    h264_preview_caps = _capsfilter(
        "caps_program_clean_h264",
        "video/x-h264,profile=constrained-baseline,stream-format=byte-stream,alignment=au",
    )
    parse_preview = _make("h264parse", "parse_program_clean")
    sink_preview = _make("rtspclientsink", "sink_program_clean")

    selector.set_property("sync-streams", True)
    _set_if_supported(selector, "cache-buffers", True)
    _set_if_supported(selector, "sync-mode", 1)
    _set_if_supported(selector, "drop-backwards", True)

    for gst_queue in (q_stream, q_preview):
        gst_queue.set_property("max-size-buffers", 2)
        gst_queue.set_property("max-size-bytes", 0)
        gst_queue.set_property("max-size-time", 0)
        gst_queue.set_property("leaky", 2)

    _configure_x264_encoder(
        enc_stream,
        tune=PROGRAM_CLEAN_TUNE,
        preset=PROGRAM_CLEAN_PRESET,
        bitrate=PROGRAM_CLEAN_BITRATE,
        keyint=PROGRAM_CLEAN_KEYINT,
        threads=PROGRAM_CLEAN_THREADS,
    )
    _set_if_supported(parse_stream, "config-interval", -1)
    _configure_rtsp_sink(sink_stream, PROGRAM_STREAM_RTSP_PATH)

    _configure_x264_encoder(
        enc_preview,
        tune=PROGRAM_CLEAN_TUNE,
        preset=PROGRAM_CLEAN_PRESET,
        bitrate=PROGRAM_WEBRTC_BITRATE,
        keyint=PROGRAM_WEBRTC_KEYINT,
        threads=PROGRAM_WEBRTC_THREADS,
    )
    _set_if_supported(parse_preview, "config-interval", -1)
    _configure_rtsp_sink(sink_preview, PROGRAM_WEBRTC_RTSP_PATH)

    for el in (
            selector, tee,
            q_stream, conv_stream, caps_stream, enc_stream, h264_stream_caps,
            parse_stream, sink_stream,
            q_preview, conv_preview, caps_preview, enc_preview, h264_preview_caps,
            parse_preview, sink_preview,
    ):
        pipeline.add(el)

    _program_selector_pads.clear()

    if FIXED_CAMERA in camera_tees:
        _link_tee_to_program_selector(
            pipeline,
            camera_tees[FIXED_CAMERA],
            "0",
            selector,
            FIXED_CAMERA,
        )
    if PTZ_CAMERA in camera_tees:
        _link_tee_to_program_selector(
            pipeline,
            camera_tees[PTZ_CAMERA],
            "2",
            selector,
            PTZ_CAMERA,
        )

    _link(selector, tee)

    _tee_branch(tee, q_stream)
    _link_many(
        q_stream, conv_stream, caps_stream, enc_stream, h264_stream_caps,
        parse_stream, sink_stream,
    )

    _tee_branch(tee, q_preview)
    _link_many(
        q_preview, conv_preview, caps_preview, enc_preview, h264_preview_caps,
        parse_preview, sink_preview,
    )
    _get_static_pad(caps_preview, "src").add_probe(
        Gst.PadProbeType.BUFFER,
        _program_preview_restamp_probe,
    )

    _program_selector = selector
    _program_enc = enc_stream
    _program_preview_enc = enc_preview
    program_cfg = _read_stream_worker_config()
    _last_program_cfg = program_cfg
    _switch_program_camera(
        program_cfg.get("activeCamera", PTZ_CAMERA),
        force_keyframe=False,
    )

    return enc_stream, enc_preview


# ---------------------------------------------------------------------------
# Main pipeline builder
# ---------------------------------------------------------------------------
def _reset_program_pipeline_state() -> None:
    global _encoders, _program_selector, _program_enc, _program_preview_enc, _last_program_cfg
    global _program_previous_camera, _program_switch_seq, _program_last_switch_at_ms
    global _program_switch_requested_camera, _program_switch_timer_queued
    global _program_switch_settle_until_monotonic
    global _program_preview_frame_index
    _encoders = {}
    _program_selector = None
    _program_enc = None
    _program_preview_enc = None
    _last_program_cfg = None
    _program_previous_camera = PTZ_CAMERA
    _program_switch_seq = 0
    _program_last_switch_at_ms = 0
    _program_switch_requested_camera = None
    _program_switch_timer_queued = False
    _program_switch_settle_until_monotonic = 0.0
    _program_preview_frame_index = 0


def _new_pipeline() -> Gst.Pipeline:
    pipeline = Gst.Pipeline()
    if not pipeline:
        sys.stderr.write("ERROR: Unable to create Pipeline\n")
        sys.exit(1)
    return pipeline


def _ai_branch_message(camera_name: str, cam_label: str) -> str:
    if _ai_stream_enabled(cam_label):
        return f"Building {camera_name} AI RTSP branch (720p debug) ..."
    return f"Building {camera_name} AI processing branch (debug RTSP disabled) ..."


def _build_camera_pipeline_branch(
        pipeline: Gst.Pipeline,
        camera_tees: dict[str, Gst.Element],
        *,
        enabled: bool,
        camera_key: str,
        camera_name: str,
        device: str,
        suffix: str,
        cam_label: str,
        rtsp_path: str,
        infer_config: str,
        encoder_key: str,
) -> Gst.Element | None:
    if not enabled:
        print(f"{camera_name} disabled — skipping source and all branches")
        return None

    print(f"Building {camera_name} source ...")
    tee = _build_camera_source(pipeline, device, suffix)
    camera_tees[camera_key] = tee

    if not _ai_enabled(cam_label):
        print(f"{camera_name} AI disabled — skipping AI branch")
        return None

    print(_ai_branch_message(camera_name, cam_label))
    pgie, enc_ai = _build_ai_branch(pipeline, tee, suffix, rtsp_path, infer_config, cam_label)
    if enc_ai is not None:
        _encoders[encoder_key] = enc_ai
    return pgie


def _ensure_any_camera_enabled() -> None:
    if ENABLE_FIXED_CAMERA or ENABLE_PTZ_CAMERA:
        return
    sys.stderr.write("ERROR: All cameras are disabled\n")
    sys.exit(1)


def _build_program_outputs(pipeline: Gst.Pipeline, camera_tees: dict[str, Gst.Element]) -> None:
    print("Building switched program RTSP branches for stream worker/WebRTC ...")
    enc_program_stream, enc_program_clean = _build_program_clean_branch(pipeline, camera_tees)
    _encoders["enc_program_stream"] = enc_program_stream
    _encoders["enc_program_clean"] = enc_program_clean


def build_pipeline() -> tuple:
    _reset_program_pipeline_state()
    pipeline = _new_pipeline()
    camera_tees: dict[str, Gst.Element] = {}

    pgie0 = _build_camera_pipeline_branch(
        pipeline,
        camera_tees,
        enabled=ENABLE_FIXED_CAMERA,
        camera_key=FIXED_CAMERA,
        camera_name="Fixed camera",
        device=FIXED_CAMERA_DEVICE,
        suffix="0",
        cam_label="CAM0",
        rtsp_path="camera0_ai",
        infer_config="config_infer_primary_yoloV8_cam0.txt",
        encoder_key="enc0_ai",
    )
    pgie2 = _build_camera_pipeline_branch(
        pipeline,
        camera_tees,
        enabled=ENABLE_PTZ_CAMERA,
        camera_key=PTZ_CAMERA,
        camera_name="PTZ camera",
        device=PTZ_CAMERA_DEVICE,
        suffix="2",
        cam_label="CAM2",
        rtsp_path="camera2_ai",
        infer_config="config_infer_primary_yoloV8_cam2.txt",
        encoder_key="enc2_ai",
    )

    _ensure_any_camera_enabled()
    _build_program_outputs(pipeline, camera_tees)

    return pipeline, pgie0, pgie2


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _reset_rtmp_status() -> None:
    global _rtmp_status_sent, _rtmp_status_cached
    _rtmp_status_sent = False
    _rtmp_status_cached = None


def _ensure_display_env() -> None:
    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        os.environ.setdefault("DISPLAY", ":0")
        print("WARNING: DISPLAY not set — defaulting to :0")


def _install_restart_signal_handler() -> None:
    def _restart_handler(_sig, _frame):
        raise SystemExit(int(ProcessExitCode.RESTART))

    signal.signal(signal.SIGUSR1, _restart_handler)


def _start_control_services() -> None:
    start_go_bridge_server()
    start_ptz_control_server()
    start_ptz_manual_relay()


def _initialize_persistent_state() -> None:
    _persist_score_state()
    _persist_stream_worker_config()
    _emit_stream_status_and_sync_cache(False)


def _attach_detection_probe(pgie, cam_label: str) -> None:
    if pgie is None:
        print(f"Probe skipped -> {_stream_camera_name(cam_label)} (camera or AI disabled)")
        return

    srcpad = pgie.get_static_pad("src")
    if not srcpad:
        sys.stderr.write(f"ERROR: Cannot get src pad of {pgie.get_name()}\n")
        sys.exit(1)

    srcpad.add_probe(Gst.PadProbeType.BUFFER, pgie_src_pad_buffer_probe, cam_label)
    print(f"Probe attached -> {pgie.get_name()} ({_stream_camera_name(cam_label)})")


def _attach_detection_probes(pgie0, pgie2) -> None:
    for pgie, cam_label in [(pgie0, "CAM0"), (pgie2, "CAM2")]:
        _attach_detection_probe(pgie, cam_label)


def _make_main_loop(pipeline: Gst.Pipeline) -> GLib.MainLoop:
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)
    return loop


def _install_periodic_tasks() -> None:
    if ENABLE_TERMINAL_FPS_METRICS and ENABLE_AI_FPS_METRICS:
        GLib.timeout_add_seconds(TERMINAL_FPS_INTERVAL_SEC, _fps_report)
    GLib.timeout_add_seconds(1, _poll_stream_worker_status)
    GLib.timeout_add(200, _poll_program_config)


def _start_worker_after_main_ready() -> bool:
    ok, info = _start_stream_worker()
    if ok:
        print(f"[startup] stream worker {info}")
    else:
        print(f"[startup] stream worker failed to start: {info}")
    return False


def _schedule_startup_stream_if_needed(startup_stream_requested: bool) -> None:
    if startup_stream_requested:
        GLib.timeout_add_seconds(2, _start_worker_after_main_ready)


def _cleanup_runtime_sockets() -> None:
    for sock_path in (GO_BRIDGE_SOCK, PTZ_CONTROL_SOCK):
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass


def _shutdown_pipeline(pipeline: Gst.Pipeline) -> None:
    _stop_stream_worker()
    print("Stopping pipeline ...")
    pipeline.set_state(Gst.State.NULL)
    pipeline.get_state(Gst.CLOCK_TIME_NONE)
    _cleanup_runtime_sockets()


def main():
    _reset_rtmp_status()
    _ensure_display_env()
    _install_restart_signal_handler()

    Gst.init(None)
    _start_control_services()
    _initialize_persistent_state()

    print("Building pipeline ...")
    pipeline, pgie0, pgie2 = build_pipeline()
    _attach_detection_probes(pgie0, pgie2)
    startup_stream_requested = bool(read_stream_url())
    loop = _make_main_loop(pipeline)
    _install_periodic_tasks()

    print("Starting pipeline ...")
    pipeline.set_state(Gst.State.PLAYING)
    _schedule_startup_stream_if_needed(startup_stream_requested)

    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown_pipeline(pipeline)


if __name__ == "__main__":
    main()
