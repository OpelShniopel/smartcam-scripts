#!/usr/bin/env python3
"""
DeepStream Basketball Detection Pipeline
==========================================
Fixed and PTZ cameras with clean RTSP streams and optional AI RTSP streams via
MediaMTX.
Optional live-streaming to YouTube/Twitch/Kick is handled by a separate RTMP
worker with scoreboard overlay.

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
    - clean branch: 1080p high quality low latency → WebRTC tablet viewing
    - AI branch:    720p with bounding boxes when enabled → debug only
    - stream branch: 1080p internal RTSP feed for the external RTMP worker

SERVICES (for Go backend):
  Unix socket  /tmp/smartcam.sock  — bidirectional newline-delimited JSON
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

  Unix socket  /tmp/pycam.sock  — outbound only, newline-delimited JSON
    Python -> camera control: {"camera":"fixed","frame":N,"timestamp":T,"detections":[...]}

HTTP API (internal / debug only):
  GET  /status
  POST /score

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
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

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
from rtmp_elements import (
    configure_rtmp_branch,
    make_rtmp_elements,
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
UNIX_SOCK = os.environ.get("SMARTCAM_SOCK", "/tmp/smartcam.sock")
PYCAM_SOCK = "/tmp/pycam.sock"
HTTP_PORT = 9101

CLASS_ID_RIM = 0
CLASS_ID_BALL = 1
CLASS_NAMES = {CLASS_ID_RIM: "RIM", CLASS_ID_BALL: "BALL"}

PROBE_EVERY_N_FRAMES = 2

# ---------------------------------------------------------------------------
# Camera / AI feature flags
# ---------------------------------------------------------------------------
ENABLE_FIXED_CAMERA = True
ENABLE_PTZ_CAMERA = True

ENABLE_AI_GLOBAL = True
ENABLE_FIXED_CAMERA_AI = True
ENABLE_PTZ_CAMERA_AI = False

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


# Clean branch encoder settings — tuned for low latency local WebRTC viewing
CLEAN_BITRATE = 8000
CLEAN_KEYINT = 15
CLEAN_THREADS = 2
CLEAN_PRESET = "ultrafast"
CLEAN_TUNE = "zerolatency"

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
RTMP_BITRATE = 6800

# Encoder references populated by build_pipeline() and reported by /status.
_encoders: dict[str, Gst.Element] = {}

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

    _out_q.put(msg)
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

# ---------------------------------------------------------------------------
# Scoreboard overlay elements
# ---------------------------------------------------------------------------
_osd_elements: dict[str, Gst.Element] = {}
_osd_lock = threading.Lock()


def _render_scoreboard_bg() -> None:
    if not os.path.exists(SCOREBOARD_PNG):
        print(f"WARNING: Scoreboard PNG not found: {SCOREBOARD_PNG}")
        print("         Place scoreboard.png next to pipeline.py")


def _update_osd_texts(state: dict) -> None:
    with _osd_lock:
        els = dict(_osd_elements)
    if not els:
        return

    home = els.get("osd_home")
    away = els.get("osd_away")
    score = els.get("osd_score")
    clock = els.get("osd_clock")
    quarter = els.get("osd_quarter")
    fouls = els.get("osd_fouls")
    bg = els.get("osd_bg")
    milestone_player = els.get("osd_milestone_player")
    milestone_text = els.get("osd_milestone_text")
    visible = state.get("visible", False)

    update_quarter_overlay(quarter, visible, state)
    if home:
        home.set_property("silent", not visible)
        if visible:
            home.set_property(
                "text",
                truncate_team_name(
                    "home_name",
                    state.get("home_name", "HOME"),
                    log_prefix="[score]",
                ),
            )
    if away:
        away.set_property("silent", not visible)
        if visible:
            away.set_property(
                "text",
                truncate_team_name(
                    "away_name",
                    state.get("away_name", "AWAY"),
                    log_prefix="[score]",
                ),
            )
    update_score_clock_overlays(score, clock, visible, state)
    if fouls:
        fouls.set_property("silent", not visible)
        if visible:
            fouls.set_property("text",
                               f"F:{state.get('home_fouls', 0)} T:{state.get('home_timeouts', 3)}"
                               f"          "
                               f"F:{state.get('away_fouls', 0)} T:{state.get('away_timeouts', 3)}")
    if bg:
        bg.set_property("alpha", 1.0 if visible else 0.0)
    update_milestone_overlays(milestone_player, milestone_text, state)


def _apply_score_patch(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValueError("score patch must be a JSON object")

    allowed_str = {"home_name", "away_name", "clock"}
    allowed_int = {"home_points", "away_points", "home_fouls",
                   "away_fouls", "home_timeouts", "away_timeouts", "quarter",
                   "game_id"}
    allowed_number = {"updated_at"}
    allowed_bool = {"visible"}
    with score_lock:
        for k in allowed_str:
            if k in data and isinstance(data[k], str):
                if k in {"home_name", "away_name"}:
                    score_state[k] = truncate_team_name(k, data[k], log_prefix="[score]")
                else:
                    score_state[k] = data[k]
        for k in allowed_int:
            if k in data and isinstance(data[k], int) and not isinstance(data[k], bool):
                score_state[k] = data[k]
        for k in allowed_number:
            if (
                    k in data
                    and isinstance(data[k], (int, float))
                    and not isinstance(data[k], bool)
            ):
                score_state[k] = data[k]
        for k in allowed_bool:
            if k in data and isinstance(data[k], bool):
                score_state[k] = data[k]
        if (
                "milestone" in data
                and (data["milestone"] is None or isinstance(data["milestone"], dict))
        ):
            score_state["milestone"] = data["milestone"]
        state = score_state.copy()
    _update_osd_texts(state)
    _persist_score_state()


# ---------------------------------------------------------------------------
# Pycam socket server
# ---------------------------------------------------------------------------
_pycam_clients: list[socket.socket] = []
_pycam_clients_lock = threading.Lock()
_pycam_q: queue.SimpleQueue = queue.SimpleQueue()


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


def start_pycam_server() -> None:
    try:
        os.unlink(PYCAM_SOCK)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(PYCAM_SOCK)
    os.chmod(PYCAM_SOCK, 0o660)
    srv.listen(2)
    print(f"Pycam socket -> {PYCAM_SOCK}")

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                with _pycam_clients_lock:
                    _pycam_clients.append(conn)
                print("[pycam] camera control client connected")
            except OSError:
                break

    threading.Thread(
        target=_json_socket_sender_loop,
        args=(_pycam_q, _pycam_clients, _pycam_clients_lock),
        daemon=True,
        name="pycam-sender",
    ).start()
    threading.Thread(target=_accept_loop, daemon=True, name="pycam-accept").start()


def send_to_pycam(cam_label: str, frame_num: int, detections: list) -> None:
    _pycam_q.put({
        "camera": _stream_camera_name(cam_label),
        "frame": frame_num,
        "timestamp": time.time(),
        "detections": detections,
    })


# ---------------------------------------------------------------------------
# Unix socket server (Go bridge)
# ---------------------------------------------------------------------------
_sock_clients: list[socket.socket] = []
_sock_clients_lock = threading.Lock()
_out_q: queue.SimpleQueue = queue.SimpleQueue()


def _handle_go_connection(conn: socket.socket) -> None:
    with _sock_clients_lock:
        _sock_clients.append(conn)
    _push_state()

    # Replay cached stream_status if already determined.
    # Fixes the race where stream_status fired before Go reconnected
    # (Python restarts faster than Go's 2s reconnect delay, so the
    # original stream_status was sent to an empty _sock_clients list).
    cached_status = _get_cached_stream_status()
    if cached_status is not None:
        _out_q.put(cached_status)
        print(f"[stream_status] replayed to new Go connection: active={cached_status.get('active')}")

    buf = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(msg, dict):
                    print(f"[go] ignoring non-object message: {msg!r}")
                    continue
                if msg.get("type") == "cmd":
                    _dispatch_cmd(msg)
                elif msg.get("type") == "ping":
                    _out_q.put({"type": "pong"})
    except OSError:
        pass
    finally:
        with _sock_clients_lock:
            try:
                _sock_clients.remove(conn)
            except ValueError:
                pass
        try:
            conn.close()
        except OSError:
            pass


def _ack(action: str, ok: bool, error: str = "") -> None:
    msg: dict = {"type": "ack", "action": action, "ok": ok}
    if error:
        msg["error"] = error
    _out_q.put(msg)


def _dispatch_cmd(msg: dict) -> None:
    raw_action = msg.get("action", "")
    if not isinstance(raw_action, str):
        err = f"action must be string, got {raw_action!r}"
        print(f"[cmd] {err}")
        _ack("", False, err)
        return
    action = raw_action

    if action == "start_stream":
        raw_url = msg.get("rtmp_url")
        if not isinstance(raw_url, str):
            err = f"rtmp_url must be string, got {raw_url!r}"
            print(f"[cmd] start_stream: {err}")
            _ack("start_stream", False, err)
            return
        rtmp_url = raw_url.strip()
        if not (rtmp_url.startswith("rtmp://") or rtmp_url.startswith("rtmps://")):
            err = f"invalid rtmp_url: {rtmp_url!r}"
            print(f"[cmd] start_stream: {err}")
            _ack("start_stream", False, err)
            return

        _atomic_write_text(STREAM_CONF, rtmp_url + "\n")

        ok, info = _start_stream_worker()
        if ok:
            print(f"[cmd] start_stream -> {rtmp_url[:60]} ({info})")
            _ack("start_stream", True)
            _push_state()
            _poll_stream_worker_status()
        else:
            err = f"failed to start stream worker: {info}"
            print(f"[cmd] start_stream: {err}")
            _ack("start_stream", False, err)

    elif action == "stop_stream":
        _atomic_write_text(STREAM_CONF, "# disabled\n")
        ok, info = _stop_stream_worker()
        print(f"[cmd] stop_stream ({info})")
        _ack("stop_stream", ok, "" if ok else info)
        _push_state()
        _sync_stream_status_cache(False)

    elif action == "set_config":
        bitrate = msg.get("bitrateKbps")
        if not isinstance(bitrate, int) or isinstance(bitrate, bool) or not (100 <= bitrate <= 50000):
            err = f"bitrateKbps must be int 100-50000, got {bitrate!r}"
            print(f"[cmd] set_config: {err}")
            _ack("set_config", False, err)
            return
        _persist_stream_worker_config(bitrate_kbps=bitrate)
        running = _is_stream_worker_running()
        print(f"[cmd] set_config bitrateKbps={bitrate} -> worker config (running={running})")
        _ack("set_config", True)

    elif action == "switch_cam":
        raw_cam = msg.get("camId", msg.get("camera", msg.get("cam")))
        normalized = _normalize_stream_camera(raw_cam)
        if normalized is None:
            err = (
                "camId must be fixed or ptz "
                f"(legacy aliases 0,2,cam0,cam2,camera0,camera2 also work); got {raw_cam!r}"
            )
            print(f"[cmd] switch_cam: {err}")
            _ack("switch_cam", False, err)
            return
        _persist_stream_worker_config(active_camera=normalized)
        running = _is_stream_worker_running()
        print(f"[cmd] switch_cam -> {normalized} (running={running})")
        _ack("switch_cam", True)
        _push_state()

    elif action == "set_osd":
        visible = msg.get("visible")
        if not isinstance(visible, bool):
            err = f"visible must be bool, got {visible!r}"
            print(f"[cmd] set_osd: {err}")
            _ack("set_osd", False, err)
            return
        _apply_score_patch({"visible": visible})
        print(f"[cmd] set_osd visible={visible}")
        _ack("set_osd", True)

    elif action == "set_score":
        try:
            _apply_score_patch(msg)
        except ValueError as e:
            err = str(e)
            print(f"[cmd] set_score: {err}")
            _ack("set_score", False, err)
            return
        print("[cmd] set_score applied")
        _ack("set_score", True)

    else:
        err = f"unknown action: {action!r}"
        print(f"[cmd] {err}")
        _ack(action, False, err)


MODEL_NAME = "Basketball"
AVAILABLE_MODELS = ["Basketball"]


def _push_state() -> None:
    url = read_stream_url()
    worker_running = _is_stream_worker_running()
    worker_cfg = _read_stream_worker_config()

    webrtc = {}
    internal_streams = {}

    if ENABLE_FIXED_CAMERA:
        fixed_clean_url = f"http://{JETSON_HOST}:8889/camera0_clean"
        fixed_stream_url = f"rtsp://{JETSON_HOST}:8554/camera0_stream"
        webrtc["fixed_clean"] = fixed_clean_url
        webrtc["cam0_clean"] = fixed_clean_url
        if _ai_enabled("CAM0"):
            fixed_ai_url = f"http://{JETSON_HOST}:8889/camera0_ai"
            webrtc["fixed_ai"] = fixed_ai_url
            webrtc["cam0_ai"] = fixed_ai_url
        internal_streams["fixed_stream"] = fixed_stream_url
        internal_streams["cam0_stream"] = fixed_stream_url

    if ENABLE_PTZ_CAMERA:
        ptz_clean_url = f"http://{JETSON_HOST}:8889/camera2_clean"
        ptz_stream_url = f"rtsp://{JETSON_HOST}:8554/camera2_stream"
        webrtc["ptz_clean"] = ptz_clean_url
        webrtc["cam2_clean"] = ptz_clean_url
        if _ai_enabled("CAM2"):
            ptz_ai_url = f"http://{JETSON_HOST}:8889/camera2_ai"
            webrtc["ptz_ai"] = ptz_ai_url
            webrtc["cam2_ai"] = ptz_ai_url
        internal_streams["ptz_stream"] = ptz_stream_url
        internal_streams["cam2_stream"] = ptz_stream_url

    _out_q.put({
        "type": "state",
        "streaming": bool(url) and worker_running,
        "stream_configured": bool(url),
        "stream_worker_running": worker_running,
        "stream_active_camera": worker_cfg.get("activeCamera", PTZ_CAMERA),
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
        "webrtc": webrtc,
        "internal_streams": internal_streams,
    })


def start_unix_server() -> None:
    try:
        os.unlink(UNIX_SOCK)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(UNIX_SOCK)
    os.chmod(UNIX_SOCK, 0o660)
    srv.listen(4)
    print(f"Unix socket -> {UNIX_SOCK}")
    threading.Thread(
        target=_json_socket_sender_loop,
        args=(_out_q, _sock_clients, _sock_clients_lock),
        daemon=True,
        name="unix-sender",
    ).start()

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=_handle_go_connection,
                                 args=(conn,), daemon=True).start()
            except OSError:
                break

    threading.Thread(target=_accept_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
class ControlHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/status":
            url = read_stream_url()
            with score_lock:
                score_visible = score_state["visible"]
            self._json(200, {
                "alive": True,
                "pid": os.getpid(),
                "streaming": bool(url) and _is_stream_worker_running(),
                "stream_configured": bool(url),
                "stream_worker_running": _is_stream_worker_running(),
                "stream_active_camera": _read_stream_worker_config().get("activeCamera", PTZ_CAMERA),
                "rtmp_url": url or "",
                "score_overlay": score_visible,
                "unix_sock": UNIX_SOCK,
                "pycam_sock": PYCAM_SOCK,
                "encoders": list(_encoders.keys()),
                "cameras": {
                    FIXED_CAMERA: {
                        "device": FIXED_CAMERA_DEVICE,
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera0_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera0_ai",
                        "rtsp_stream": f"rtsp://{JETSON_HOST}:8554/camera0_stream",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera0_ai",
                    },
                    "cam0": {
                        "device": FIXED_CAMERA_DEVICE,
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera0_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera0_ai",
                        "rtsp_stream": f"rtsp://{JETSON_HOST}:8554/camera0_stream",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera0_ai",
                    },
                    PTZ_CAMERA: {
                        "device": PTZ_CAMERA_DEVICE,
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera2_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera2_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera2_ai",
                        "rtsp_stream": f"rtsp://{JETSON_HOST}:8554/camera2_stream",
                    },
                    "cam2": {
                        "device": PTZ_CAMERA_DEVICE,
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera2_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera2_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera2_ai",
                        "rtsp_stream": f"rtsp://{JETSON_HOST}:8554/camera2_stream",
                    },
                },
            })
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/score":
            body = self._read_body()
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, AttributeError):
                self._json(400, {"error": "invalid json"})
                return
            try:
                _apply_score_patch(data)
            except ValueError as e:
                self._json(400, {"error": str(e)})
                return
            with score_lock:
                self._json(200, score_state.copy())
        else:
            self._json(404, {"error": "not found"})

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            length = 0
        return self.rfile.read(length).decode() if length else ""

    def log_message(self, format_string, *args):
        pass


def start_http_server() -> None:
    server = HTTPServer(("127.0.0.1", HTTP_PORT), ControlHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"HTTP API -> http://127.0.0.1:{HTTP_PORT}")


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


def _worker_pid_info_is_current(info: dict | None) -> bool:
    if not info:
        return False

    try:
        pid = _pid_metadata_int(info, "pid", required=True)
    except ValueError:
        return False
    assert pid is not None
    if not _pid_exists(pid):
        return False

    role = info.get("role")
    if role is not None and role != STREAM_WORKER_PID_ROLE:
        return False

    script = info.get("script")
    if script is not None and os.path.abspath(str(script)) != os.path.abspath(STREAM_WORKER_WRAPPER):
        return False

    try:
        owner_pid = _pid_metadata_int(info, "owner_pid")
    except ValueError:
        return False
    if owner_pid is not None:
        if owner_pid not in (0, os.getpid()):
            return False

    try:
        owner_start_ticks = _pid_metadata_int(info, "owner_start_ticks")
    except ValueError:
        return False
    if owner_start_ticks is not None:
        current_owner_start_ticks = _process_start_ticks(os.getpid())
        if current_owner_start_ticks is None or current_owner_start_ticks != owner_start_ticks:
            return False

    try:
        expected_start_ticks = _pid_metadata_int(info, "start_ticks")
    except ValueError:
        return False
    if expected_start_ticks is not None:
        current_start_ticks = _process_start_ticks(pid)
        if current_start_ticks is None or current_start_ticks != expected_start_ticks:
            return False

    return _worker_cmdline_matches(pid)


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
        if not _worker_pid_info_is_current(pid_info):
            try:
                os.unlink(STREAM_WORKER_PID)
            except FileNotFoundError:
                pass
            _set_worker_status(worker_alive=False, stream_active=False, last_error="")
            return True, "already stopped"

        pid = int(pid_info["pid"])
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
    _out_q.put(_get_cached_stream_status())
    if active:
        print("[stream_status] RTMP stream verified active")
    else:
        print(f"[stream_status] RTMP stream inactive: {error}")


# ---------------------------------------------------------------------------
# Detection probe
# ---------------------------------------------------------------------------
def pgie_src_pad_buffer_probe(_pad, info, cam_label):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return Gst.PadProbeReturn.OK

    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    if not batch_meta:
        return Gst.PadProbeReturn.OK

    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        if _ai_fps_metric_enabled(cam_label):
            with _fps_lock:
                if cam_label in _fps_counters:
                    _fps_counters[cam_label] += 1

        # AI disabled for this camera -> do nothing else, but keep pipeline healthy
        if not _ai_enabled(cam_label):
            try:
                l_frame = l_frame.next
            except StopIteration:
                break
            continue

        # Only inspect metadata every Nth frame
        if frame_meta.frame_num % PROBE_EVERY_N_FRAMES == 0:
            detections = []
            l_obj = frame_meta.obj_meta_list

            while l_obj is not None:
                try:
                    obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
                except StopIteration:
                    break

                cid = obj_meta.class_id
                if cid in CLASS_NAMES:
                    r = obj_meta.rect_params
                    detections.append({
                        "class": CLASS_NAMES[cid],
                        "class_id": cid,
                        "tracker_id": obj_meta.object_id,
                        "center_x": round(r.left + r.width / 2.0, 1),
                        "center_y": round(r.top + r.height / 2.0, 1),
                        "width": round(r.width, 1),
                        "height": round(r.height, 1),
                        "left": round(r.left, 1),
                        "top": round(r.top, 1),
                        "confidence": round(obj_meta.confidence, 4),
                    })

                try:
                    l_obj = l_obj.next
                except StopIteration:
                    break

            if detections:
                send_to_pycam(cam_label, frame_meta.frame_num, detections)

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

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
    caps = _capsfilter(caps_name or f"caps{suffix}_{branch_name}", "video/x-raw,format=I420")
    enc = _make("x264enc", f"enc{suffix}_{branch_name}")
    parse = _make("h264parse", f"parse{suffix}_{branch_name}")
    sink = _make("rtspclientsink", f"sink{suffix}_{branch_name}")

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)
    _configure_rtsp_sink(sink, rtsp_path)

    for el in (q, conv, caps, enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q)
    _link_many(q, conv, caps, enc, parse, sink)

    return enc


# ---------------------------------------------------------------------------
# Clean branch
# ---------------------------------------------------------------------------
def _build_clean_branch(pipeline, tee, suffix: str, rtsp_path: str) -> Gst.Element:
    enc = _build_simple_rtsp_encode_branch(
        pipeline, tee, suffix, "clean", rtsp_path, caps_name=f"caps{suffix}_i420",
    )

    _configure_x264_encoder(
        enc,
        tune=CLEAN_TUNE,
        preset=CLEAN_PRESET,
        bitrate=CLEAN_BITRATE,
        keyint=CLEAN_KEYINT,
        threads=CLEAN_THREADS,
    )

    return enc


# ---------------------------------------------------------------------------
# AI branch
# ---------------------------------------------------------------------------
def _build_ai_branch(pipeline, tee, suffix: str, rtsp_path: str,
                     infer_config: str, cam_label: str):
    q_ai = _make("queue", f"q{suffix}_ai")
    conv_ai = _make_nvconv(f"conv{suffix}_ai")
    caps_ai = _capsfilter(f"caps{suffix}_ai",
                          "video/x-raw(memory:NVMM),format=NV12,width=1280,height=720")
    mux = _make("nvstreammux", f"mux{suffix}")
    pgie = _make("nvinfer", f"pgie{suffix}")
    tracker = _make("nvtracker", f"tracker{suffix}")
    conv_pre = _make_nvconv(f"conv{suffix}_pre")
    nvosd = _make("nvdsosd", f"nvosd{suffix}")
    conv_post = _make_nvconv(f"conv{suffix}_post")
    caps_post = _capsfilter(f"caps{suffix}_post", "video/x-raw,format=I420")
    q_post = _make("queue", f"q{suffix}_post")
    enc = _make("x264enc", f"enc{suffix}_ai")
    parse = _make("h264parse", f"parse{suffix}_ai")
    parse_tee = _make("tee", f"tee{suffix}_ai_parse")
    q_rtsp = _make("queue", f"q{suffix}_ai_rtsp")
    sink = _make("rtspclientsink", f"sink{suffix}_ai")

    q_rec = None
    rec = None
    recording_enabled = _recording_enabled(cam_label)
    if recording_enabled:
        q_rec = _make("queue", f"q{suffix}_ai_record")
        rec = _make("splitmuxsink", f"rec{suffix}_ai")

    mux.set_property("width", 1280)
    mux.set_property("height", 720)
    mux.set_property("batch-size", 1)
    mux.set_property("batched-push-timeout", 33333)
    mux.set_property("live-source", 1)
    mux.set_property("nvbuf-memory-type", 0)

    pgie.set_property("config-file-path", infer_config)

    tracker.set_property("ll-lib-file",
                         "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so")
    tracker.set_property("ll-config-file",
                         "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_IOU.yml")
    tracker.set_property("tracker-width", 1280)
    tracker.set_property("tracker-height", 736)
    tracker.set_property("gpu-id", 0)
    tracker.set_property("display-tracking-id", 1)

    nvosd.set_property("process-mode", 1)

    q_ai.set_property("max-size-buffers", 2)
    q_ai.set_property("max-size-bytes", 0)
    q_ai.set_property("max-size-time", 0)
    q_ai.set_property("leaky", 2)

    q_post.set_property("max-size-buffers", 2)
    q_post.set_property("max-size-bytes", 0)
    q_post.set_property("max-size-time", 0)
    q_post.set_property("leaky", 2)

    q_rtsp.set_property("max-size-buffers", 2)
    q_rtsp.set_property("max-size-bytes", 0)
    q_rtsp.set_property("max-size-time", 0)
    q_rtsp.set_property("leaky", 2)

    if q_rec is not None:
        q_rec.set_property("max-size-buffers", RECORD_QUEUE_BUFFERS)
        q_rec.set_property("max-size-bytes", 0)
        q_rec.set_property("max-size-time", 0)
        q_rec.set_property("leaky", 2)

    _configure_x264_encoder(
        enc,
        tune=AI_TUNE,
        preset=AI_PRESET,
        bitrate=AI_BITRATE,
        keyint=AI_KEYINT,
        threads=AI_THREADS,
    )
    _configure_rtsp_sink(sink, rtsp_path)

    if rec is not None:
        rec.set_property("location", _recording_location_pattern(cam_label))
        rec.set_property("max-size-time", RECORD_SEGMENT_SECONDS * Gst.SECOND)
        rec.set_property("muxer-factory", RECORD_MUXER_FACTORY)
        rec.set_property("send-keyframe-requests", True)
        rec.set_property("async-finalize", True)

    elements = [q_ai, conv_ai, caps_ai, mux, pgie, tracker,
                conv_pre, nvosd, conv_post, caps_post, q_post,
                enc, parse, parse_tee, q_rtsp, sink]
    if q_rec is not None and rec is not None:
        elements.extend([q_rec, rec])

    for el in elements:
        pipeline.add(el)

    _tee_branch(tee, q_ai)
    _link(q_ai, conv_ai)
    _link(conv_ai, caps_ai)

    caps_ai_src = _get_static_pad(caps_ai, "src")
    mux_sinkpad = _request_mux_sinkpad(mux, "sink_0")
    if caps_ai_src.link(mux_sinkpad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link caps{suffix}_ai -> mux{suffix}.sink_0\n")
        sys.exit(1)

    _link_many(
        mux, pgie, tracker, conv_pre, nvosd, conv_post,
        caps_post, q_post, enc, parse, parse_tee,
    )

    _tee_branch(parse_tee, q_rtsp)
    _link(q_rtsp, sink)

    if q_rec is not None and rec is not None:
        _tee_branch(parse_tee, q_rec)
        _link_src_to_request_pad(q_rec, rec, "video", f"{_stream_camera_name(cam_label)} recording")
        print(
            f"{_stream_camera_name(cam_label)} AI recording enabled -> {RECORDINGS_DIR} "
            f"({RECORD_SEGMENT_SECONDS}s segments, .{RECORD_FILE_EXTENSION})"
        )
    else:
        print(f"{_stream_camera_name(cam_label)} AI recording disabled")

    return pgie, enc


# ---------------------------------------------------------------------------
# Internal camera stream branch for external RTMP worker
# ---------------------------------------------------------------------------
def _build_internal_stream_branch(pipeline, tee, suffix: str, rtsp_path: str) -> Gst.Element:
    enc = _build_simple_rtsp_encode_branch(
        pipeline, tee, suffix, "streamsrc", rtsp_path,
    )

    _configure_x264_encoder(
        enc,
        tune=CLEAN_TUNE,
        preset=CLEAN_PRESET,
        bitrate=10000,
        keyint=CLEAN_KEYINT,
        threads=CLEAN_THREADS,
    )

    return enc


# ---------------------------------------------------------------------------
# Legacy embedded RTMP stream branch. The main runtime now uses stream_worker.py.
# ---------------------------------------------------------------------------
def _build_stream_branch(pipeline, tee, rtmp_url: str) -> tuple[Gst.Element | None, Gst.Element | None]:
    """Returns (enc_stream, rtmpsink)."""
    global _osd_elements

    if not os.path.exists(SCOREBOARD_PNG):
        print(f"ERROR: Scoreboard PNG not found: {SCOREBOARD_PNG}")
        print("       Streaming without scoreboard overlay is not supported.")
        print("       Place scoreboard.png next to pipeline.py, then restart.")
        _send_stream_status(
            active=False,
            error=f"Missing scoreboard.png: {SCOREBOARD_PNG}",
        )
        return None, None

    _render_scoreboard_bg()

    q_stream = _make("queue", "strm_queue")
    conv_strm = _make_nvconv("strm_conv")
    caps_strm = _capsfilter("strm_caps_i420", "video/x-raw,format=I420")
    rtmp = make_rtmp_elements(_make)
    configure_rtmp_branch(rtmp, q_stream, RTMP_BITRATE, rtmp_url)

    for el in (q_stream, conv_strm, caps_strm, *rtmp.base_elements()):
        pipeline.add(el)

    _tee_branch(tee, q_stream)
    _link_many(
        q_stream, conv_strm, caps_strm, *rtmp.overlay_chain(), rtmp.flvmux,
    )

    _link_filtered(rtmp.audiosrc, rtmp.aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src = _get_static_pad(rtmp.aacenc, "src")
    flvmux_audio = rtmp.flvmux.request_pad_simple("audio")
    if not flvmux_audio:
        sys.stderr.write("ERROR: Unable to get audio pad from flvmux\n")
        sys.exit(1)
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        sys.stderr.write("ERROR: Failed to link aacenc -> flvmux.audio\n")
        sys.exit(1)

    _link(rtmp.flvmux, rtmp.rtmpsink)

    with _osd_lock:
        _osd_elements.update(rtmp.osd_map())

    print("Scoreboard overlay: gdkpixbufoverlay (bg PNG) + textoverlay x8 (text)")
    return rtmp.enc, rtmp.rtmpsink


# ---------------------------------------------------------------------------
# Main pipeline builder
# ---------------------------------------------------------------------------
def build_pipeline() -> tuple:
    global _encoders
    _encoders = {}

    pipeline = Gst.Pipeline()
    if not pipeline:
        sys.stderr.write("ERROR: Unable to create Pipeline\n")
        sys.exit(1)

    pgie0 = None
    pgie2 = None

    if ENABLE_FIXED_CAMERA:
        print("Building fixed camera source ...")
        tee0 = _build_camera_source(pipeline, FIXED_CAMERA_DEVICE, "0")

        print("Building fixed camera clean RTSP branch (1080p high quality low latency) ...")
        enc0_clean = _build_clean_branch(pipeline, tee0, "0", "camera0_clean")
        _encoders["enc0_clean"] = enc0_clean

        if _ai_enabled("CAM0"):
            print("Building fixed camera AI RTSP branch (720p debug) ...")
            pgie0, enc0_ai = _build_ai_branch(
                pipeline, tee0, "0", "camera0_ai",
                "config_infer_primary_yoloV8_cam0.txt", "CAM0")
            _encoders["enc0_ai"] = enc0_ai
        else:
            print("Fixed camera AI disabled — skipping AI RTSP branch")

        print("Building fixed camera internal RTSP branch for stream worker ...")
        enc0_streamsrc = _build_internal_stream_branch(pipeline, tee0, "0", "camera0_stream")
        _encoders["enc0_streamsrc"] = enc0_streamsrc
    else:
        print("Fixed camera disabled — skipping source and all branches")

    if ENABLE_PTZ_CAMERA:
        print("Building PTZ camera source ...")
        tee2 = _build_camera_source(pipeline, PTZ_CAMERA_DEVICE, "2")

        print("Building PTZ camera clean RTSP branch (1080p high quality low latency) ...")
        enc2_clean = _build_clean_branch(pipeline, tee2, "2", "camera2_clean")
        _encoders["enc2_clean"] = enc2_clean

        if _ai_enabled("CAM2"):
            print("Building PTZ camera AI RTSP branch (720p debug) ...")
            pgie2, enc2_ai = _build_ai_branch(
                pipeline, tee2, "2", "camera2_ai",
                "config_infer_primary_yoloV8_cam2.txt", "CAM2")
            _encoders["enc2_ai"] = enc2_ai
        else:
            print("PTZ camera AI disabled — skipping AI RTSP branch")

        print("Building PTZ camera internal RTSP branch for stream worker ...")
        enc2_streamsrc = _build_internal_stream_branch(pipeline, tee2, "2", "camera2_stream")
        _encoders["enc2_streamsrc"] = enc2_streamsrc
    else:
        print("PTZ camera disabled — skipping source and all branches")

    if not ENABLE_FIXED_CAMERA and not ENABLE_PTZ_CAMERA:
        sys.stderr.write("ERROR: All cameras are disabled\n")
        sys.exit(1)

    return pipeline, pgie0, pgie2


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _rtmp_status_sent, _rtmp_status_cached
    _rtmp_status_sent = False
    _rtmp_status_cached = None

    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        os.environ.setdefault("DISPLAY", ":0")
        print("WARNING: DISPLAY not set — defaulting to :0")

    def _restart_handler(_sig, _frame):
        raise SystemExit(int(ProcessExitCode.RESTART))

    signal.signal(signal.SIGUSR1, _restart_handler)

    Gst.init(None)

    start_unix_server()
    start_http_server()
    start_pycam_server()

    _persist_score_state()
    _persist_stream_worker_config()
    _emit_stream_status_and_sync_cache(False)

    print("Building pipeline ...")
    pipeline, pgie0, pgie2 = build_pipeline()

    for pgie, cam_label in [(pgie0, "CAM0"), (pgie2, "CAM2")]:
        if pgie is None:
            print(f"Probe skipped -> {_stream_camera_name(cam_label)} (camera or AI disabled)")
            continue

        srcpad = pgie.get_static_pad("src")
        if not srcpad:
            sys.stderr.write(f"ERROR: Cannot get src pad of {pgie.get_name()}\n")
            sys.exit(1)

        srcpad.add_probe(Gst.PadProbeType.BUFFER, pgie_src_pad_buffer_probe, cam_label)
        print(f"Probe attached -> {pgie.get_name()} ({_stream_camera_name(cam_label)})")

    _startup_stream_requested = bool(read_stream_url())

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    if ENABLE_TERMINAL_FPS_METRICS and ENABLE_AI_FPS_METRICS:
        GLib.timeout_add_seconds(TERMINAL_FPS_INTERVAL_SEC, _fps_report)
    GLib.timeout_add_seconds(1, _poll_stream_worker_status)

    print("Starting pipeline ...")
    pipeline.set_state(Gst.State.PLAYING)

    if _startup_stream_requested:
        def _start_worker_after_main_ready() -> bool:
            ok, info = _start_stream_worker()
            if ok:
                print(f"[startup] stream worker {info}")
            else:
                print(f"[startup] stream worker failed to start: {info}")
            return False

        GLib.timeout_add_seconds(2, _start_worker_after_main_ready)

    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        _stop_stream_worker()
        print("Stopping pipeline ...")
        pipeline.set_state(Gst.State.NULL)
        pipeline.get_state(Gst.CLOCK_TIME_NONE)
        try:
            os.unlink(UNIX_SOCK)
        except FileNotFoundError:
            pass
        try:
            os.unlink(PYCAM_SOCK)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
