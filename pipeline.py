#!/usr/bin/env python3
"""
DeepStream Basketball Detection Pipeline
==========================================
Two cameras with clean RTSP + AI RTSP streams via MediaMTX.
Optional live-streaming to YouTube/Twitch/Kick via RTMP with scoreboard overlay.

KNOWN ISSUES / HISTORY:
  - JetPack 6.2 + DeepStream 7.1 bug: cudaErrorIllegalAddress (700) in
    nvbufsurftransform_copy.cpp. Fix: copy-hw=2 on all nvvideoconvert elements.
  - link_filtered() on NVMM paths causes NULL caps assertions at runtime.
    Fix: use real capsfilter elements for every inline caps constraint.
  - nvv4l2decoder has a static src pad — do NOT use pad-added signal.
  - nvdsosd process-mode=1 requires NVMM RGBA input — always insert
    nvvideoconvert before nvosd to convert NV12->RGBA.
  - cairooverlay was too slow (CPU BGRA conversion every frame). Replaced with
    gdkpixbufoverlay (static PNG) + textoverlay x5 (dynamic text) on RTMP branch.
  - stream_status race: Go bridge may not be connected when stream_status fires
    (Python restarts faster than Go's 2s reconnect delay). Fix: cache the
    stream_status result and replay it when Go connects in _handle_go_connection.

STREAM DESIGN:
  CAM0 + CAM2:
    - clean branch: 1080p high quality low latency → WebRTC tablet viewing
    - AI branch:    720p with bounding boxes → debug only
  CAM2 only:
    - RTMP branch:  1080p → YouTube/Twitch/Kick with gdkpixbufoverlay + textoverlay scoreboard

SERVICES (for Go backend):
  Unix socket  /tmp/smartcam.sock  — bidirectional newline-delimited JSON
    Python -> Go: {"type":"detection", "camera":"CAM0", "frame":N, "timestamp":T, "detections":[...]}
                  {"type":"state", "streaming":bool, "webrtc":{"cam0":"...","cam2":"..."}}
                  {"type":"stream_status", "active":bool, "error":"..."}
    Go -> Python: {"type":"cmd", "action":"start_stream", "rtmp_url":"rtmp://..."}
                  {"type":"cmd", "action":"stop_stream"}
                  {"type":"cmd", "action":"set_config", "bitrateKbps":N}
                  {"type":"cmd", "action":"set_osd", "visible":bool}
                  {"type":"cmd", "action":"set_score", ...score fields...}
                  {"type":"ping"}

  Unix socket  /tmp/pycam.sock  — outbound only, newline-delimited JSON
    Python -> camera control: {"camera":"CAM0","frame":N,"timestamp":T,"detections":[...]}

HTTP API (internal / debug only):
  GET  /status
  POST /score

Class IDs (model v9):  0=RIM  1=BALL
"""

import sys
import os
import json
import queue
import time
import socket
import threading
import signal
import socket as _socket
from http.server import HTTPServer, BaseHTTPRequestHandler

import gi
gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst

import pyds



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
UNIX_SOCK    = os.environ.get("SMARTCAM_SOCK", "/tmp/smartcam.sock")
PYCAM_SOCK   = "/tmp/pycam.sock"
HTTP_PORT    = 9101

CLASS_ID_RIM  = 0
CLASS_ID_BALL = 1
CLASS_NAMES   = {CLASS_ID_RIM: "RIM", CLASS_ID_BALL: "BALL"}

PROBE_EVERY_N_FRAMES = 2

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
STREAM_CONF = os.path.join(SCRIPT_DIR, "stream.conf")

RESTART_EXIT_CODE      = 42
STREAM_ERROR_EXIT_CODE = 43  # RTMP failed — clear stream.conf and restart without streaming

# Mutable flag set by bus_call when an RTMP error triggers a stream-error exit.
# Using a list so the nested bus_call closure can mutate it.
_stream_error_exit: list[bool] = [False]

# Clean branch encoder settings — tuned for low latency local WebRTC viewing
CLEAN_BITRATE    = 8000
CLEAN_KEYINT     = 15
CLEAN_THREADS    = 2
CLEAN_PRESET     = "ultrafast"
CLEAN_TUNE       = "zerolatency"

# AI branch encoder settings
AI_BITRATE   = 2000
AI_KEYINT    = 20
AI_THREADS   = 1
AI_PRESET    = "ultrafast"
AI_TUNE      = "zerolatency"

# RTMP stream encoder settings — YouTube 1080p
RTMP_BITRATE = 6800
RTMP_KEYINT  = 60
RTMP_THREADS = 2
RTMP_PRESET  = "ultrafast"
RTMP_TUNE    = "zerolatency"

# Encoder references populated by build_pipeline() — used by set_config cmd
_encoders: dict[str, Gst.Element] = {}

# RTMP stream verification timeout (seconds)
RTMP_VERIFY_TIMEOUT_SEC = 15

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
    """Send stream_status to Go. Only fires once per pipeline lifecycle.
    Also caches the result for replay when Go (re)connects."""
    global _rtmp_status_sent, _rtmp_status_cached
    with _rtmp_status_lock:
        if _rtmp_status_sent:
            return
        _rtmp_status_sent = True
        msg: dict = {"type": "stream_status", "active": active}
        if error:
            msg["error"] = error
        _rtmp_status_cached = msg

    _out_q.put(msg)
    if active:
        print("[stream_status] RTMP stream verified active")
    else:
        print(f"[stream_status] RTMP stream failed: {error}")


def _get_cached_stream_status() -> dict | None:
    """Return the cached stream_status message, or None if not yet determined."""
    with _rtmp_status_lock:
        return _rtmp_status_cached


def _rtmp_sink_pad_probe(_pad, info, _user_data):
    """
    Pad probe on rtmpsink's sink pad. Fires on the first buffer reaching
    the RTMP sink — means GStreamer did the RTMP handshake and is sending data.
    """
    _send_stream_status(active=True)
    return Gst.PadProbeReturn.REMOVE


def _rtmp_verify_timeout() -> bool:
    """
    GLib timeout callback. If no stream_status sent yet after
    RTMP_VERIFY_TIMEOUT_SEC, report failure.
    """
    with _rtmp_status_lock:
        already_sent = _rtmp_status_sent
    if not already_sent:
        _send_stream_status(
            active=False,
            error="RTMP connection timed out — no data flowing after %ds" % RTMP_VERIFY_TIMEOUT_SEC,
        )
    return False


# ---------------------------------------------------------------------------
# FPS tracking
# ---------------------------------------------------------------------------
_fps_counters: dict[str, int] = {"CAM0": 0, "CAM2": 0}
_fps_lock = threading.Lock()


def _fps_report() -> bool:
    with _fps_lock:
        for cam, count in _fps_counters.items():
            print(f"[fps] {cam}: {count / 5:.1f} fps")
            _fps_counters[cam] = 0
    return True


# ---------------------------------------------------------------------------
# Score state
# ---------------------------------------------------------------------------
score_state = {
    "home_name":     "HOME",
    "away_name":     "AWAY",
    "home_points":   0,
    "away_points":   0,
    "home_fouls":    0,
    "away_fouls":    0,
    "home_timeouts": 3,
    "away_timeouts": 3,
    "quarter":       1,
    "clock":         "10:00",
    "visible":       False,
}
score_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Scoreboard overlay elements
# ---------------------------------------------------------------------------
_osd_elements: dict[str, Gst.Element] = {}
_osd_lock = threading.Lock()

SCOREBOARD_PNG = os.path.join(SCRIPT_DIR, "scoreboard.png")
SCOREBOARD_W = 410
SCOREBOARD_H = 129
SCOREBOARD_OFFSET_X = 755
SCOREBOARD_OFFSET_Y = 931


def _render_scoreboard_bg() -> None:
    if not os.path.exists(SCOREBOARD_PNG):
        print(f"WARNING: Scoreboard PNG not found: {SCOREBOARD_PNG}")
        print("         Place scoreboard.png next to pipeline.py")


def _update_osd_texts(state: dict) -> None:
    with _osd_lock:
        els = dict(_osd_elements)
    if not els:
        return

    home  = els.get("osd_home")
    away  = els.get("osd_away")
    score = els.get("osd_score")
    clock = els.get("osd_clock")
    fouls = els.get("osd_fouls")
    bg    = els.get("osd_bg")
    visible = state.get("visible", False)

    if home:
        home.set_property("silent", not visible)
        if visible:
            home.set_property("text", state["home_name"][:8])
    if away:
        away.set_property("silent", not visible)
        if visible:
            away.set_property("text", state["away_name"][:8])
    if score:
        score.set_property("silent", not visible)
        if visible:
            score.set_property("text",
                f"{state['home_points']} - {state['away_points']}")
    if clock:
        clock.set_property("silent", not visible)
        if visible:
            clock.set_property("text",
                f"Q{state['quarter']}  {state['clock']}")
    if fouls:
        fouls.set_property("silent", not visible)
        if visible:
            fouls.set_property("text",
                f"F:{state['home_fouls']} T:{state['home_timeouts']}"
                f"          "
                f"F:{state['away_fouls']} T:{state['away_timeouts']}")
    if bg:
        bg.set_property("alpha", 1.0 if visible else 0.0)


def _apply_score_patch(data: dict) -> None:
    allowed_str  = {"home_name", "away_name", "clock"}
    allowed_int  = {"home_points", "away_points", "home_fouls",
                    "away_fouls", "home_timeouts", "away_timeouts", "quarter"}
    allowed_bool = {"visible"}
    with score_lock:
        for k in allowed_str:
            if k in data and isinstance(data[k], str):
                score_state[k] = data[k]
        for k in allowed_int:
            if k in data and isinstance(data[k], int):
                score_state[k] = data[k]
        for k in allowed_bool:
            if k in data and isinstance(data[k], bool):
                score_state[k] = data[k]
        state = score_state.copy()
    _update_osd_texts(state)


# ---------------------------------------------------------------------------
# Pycam socket server
# ---------------------------------------------------------------------------
_pycam_clients: list[socket.socket] = []
_pycam_clients_lock = threading.Lock()
_pycam_q: queue.SimpleQueue = queue.SimpleQueue()


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

    def _sender_loop():
        while True:
            msg = _pycam_q.get()
            line = (json.dumps(msg) + "\n").encode()
            with _pycam_clients_lock:
                dead = []
                for conn in _pycam_clients:
                    try:
                        conn.sendall(line)
                    except OSError:
                        dead.append(conn)
                for c in dead:
                    _pycam_clients.remove(c)
                    try:
                        c.close()
                    except OSError:
                        pass

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                with _pycam_clients_lock:
                    _pycam_clients.append(conn)
                print("[pycam] camera control client connected")
            except OSError:
                break

    threading.Thread(target=_sender_loop, daemon=True, name="pycam-sender").start()
    threading.Thread(target=_accept_loop, daemon=True, name="pycam-accept").start()


def send_to_pycam(cam_label: str, frame_num: int, detections: list) -> None:
    _pycam_q.put({
        "camera":     cam_label,
        "frame":      frame_num,
        "timestamp":  time.time(),
        "detections": detections,
    })


# ---------------------------------------------------------------------------
# Unix socket server (Go bridge)
# ---------------------------------------------------------------------------
_sock_clients: list[socket.socket] = []
_sock_clients_lock = threading.Lock()
_out_q: queue.SimpleQueue = queue.SimpleQueue()


def _sender_loop() -> None:
    while True:
        msg = _out_q.get()
        line = (json.dumps(msg) + "\n").encode()
        with _sock_clients_lock:
            dead = []
            for conn in _sock_clients:
                try:
                    conn.sendall(line)
                except OSError:
                    dead.append(conn)
            for conn in dead:
                _sock_clients.remove(conn)
                try:
                    conn.close()
                except OSError:
                    pass


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
    action = msg.get("action", "")

    if action == "start_stream":
        rtmp_url = msg.get("rtmp_url", "").strip()
        if rtmp_url.startswith("rtmp://") or rtmp_url.startswith("rtmps://"):
            with open(STREAM_CONF, "w") as f:
                f.write(rtmp_url + "\n")
                f.flush()
                os.fsync(f.fileno())
            print(f"[cmd] start_stream -> {rtmp_url[:60]}")
            _ack("start_stream", True)
            _request_restart()
        else:
            err = f"invalid rtmp_url: {rtmp_url!r}"
            print(f"[cmd] start_stream: {err}")
            _ack("start_stream", False, err)

    elif action == "stop_stream":
        with open(STREAM_CONF, "w") as f:
            f.write("# disabled\n")
            f.flush()
            os.fsync(f.fileno())
        print("[cmd] stop_stream")
        _ack("stop_stream", True)
        _request_restart()

    elif action == "set_config":
        bitrate = msg.get("bitrateKbps")
        if not isinstance(bitrate, int) or not (100 <= bitrate <= 50000):
            err = f"bitrateKbps must be int 100-50000, got {bitrate!r}"
            print(f"[cmd] set_config: {err}")
            _ack("set_config", False, err)
            return
        enc_stream = _encoders.get("enc_stream")
        if not enc_stream:
            err = "no RTMP stream active — start a stream first"
            print(f"[cmd] set_config: {err}")
            _ack("set_config", False, err)
            return
        enc_stream.set_property("bitrate", bitrate)
        print(f"[cmd] set_config bitrateKbps={bitrate} -> enc_stream")
        _ack("set_config", True)

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
        _apply_score_patch(msg)
        print("[cmd] set_score applied")
        _ack("set_score", True)

    else:
        err = f"unknown action: {action!r}"
        print(f"[cmd] {err}")
        _ack(action, False, err)


MODEL_NAME       = "Basketball"
AVAILABLE_MODELS = ["Basketball"]


def _push_state() -> None:
    url = read_stream_url()
    _out_q.put({
        "type":             "state",
        "streaming":        url is not None,
        "model":            MODEL_NAME,
        "available_models": AVAILABLE_MODELS,
        "webrtc": {
            "cam0_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
            "cam0_ai":    f"http://{JETSON_HOST}:8889/camera0_ai",
            "cam2_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
            "cam2_ai":    f"http://{JETSON_HOST}:8889/camera2_ai",
        },
    })


def send_detection(data_dict: dict) -> None:
    data_dict["type"] = "detection"
    _out_q.put(data_dict)


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
    threading.Thread(target=_sender_loop, daemon=True, name="unix-sender").start()

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
                "alive":         True,
                "pid":           os.getpid(),
                "streaming":     url is not None,
                "rtmp_url":      url or "",
                "score_overlay": score_visible,
                "unix_sock":     UNIX_SOCK,
                "pycam_sock":    PYCAM_SOCK,
                "encoders":      list(_encoders.keys()),
                "cameras": {
                    "cam0": {
                        "device":       "/dev/video0",
                        "rtsp_clean":   f"rtsp://{JETSON_HOST}:8554/camera0_clean",
                        "rtsp_ai":      f"rtsp://{JETSON_HOST}:8554/camera0_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
                        "webrtc_ai":    f"http://{JETSON_HOST}:8889/camera0_ai",
                    },
                    "cam2": {
                        "device":       "/dev/video2",
                        "rtsp_clean":   f"rtsp://{JETSON_HOST}:8554/camera2_clean",
                        "rtsp_ai":      f"rtsp://{JETSON_HOST}:8554/camera2_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
                        "webrtc_ai":    f"http://{JETSON_HOST}:8889/camera2_ai",
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
            _apply_score_patch(data)
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
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length).decode() if length else ""

    def log_message(self, format, *args):
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


def _make_nvconv(name: str) -> Gst.Element:
    el = _make("nvvideoconvert", name)
    el.set_property("gpu-id",  0)
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
        with open(STREAM_CONF, "w") as f:
            f.write("# disabled\n")
        return None
    with open(STREAM_CONF) as f:
        url = f.read().strip()
    return None if (not url or url.startswith("#")) else url


def _request_restart() -> None:
    def _kill():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGUSR1)
    threading.Thread(target=_kill, daemon=True).start()


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

        if frame_meta.frame_num % PROBE_EVERY_N_FRAMES == 0:
            with _fps_lock:
                _fps_counters[cam_label] += 1

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
                        "class":      CLASS_NAMES[cid],
                        "class_id":   cid,
                        "tracker_id": obj_meta.object_id,
                        "center_x":   round(r.left + r.width  / 2.0, 1),
                        "center_y":   round(r.top  + r.height / 2.0, 1),
                        "width":      round(r.width,  1),
                        "height":     round(r.height, 1),
                        "left":       round(r.left,   1),
                        "top":        round(r.top,    1),
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
        if src_name in ("rtmpsink", "enc_stream", "flvmux", "parse_stream"):
            _send_stream_status(active=False, error=f"GStreamer error in {src_name}: {err}")
            # Clear stream.conf so the pipeline restarts without the broken RTMP URL
            try:
                with open(STREAM_CONF, "w") as f:
                    f.write("# disabled — cleared after RTMP error\n")
                    f.flush()
                    os.fsync(f.fileno())
            except OSError as e:
                print(f"WARNING: could not clear stream.conf: {e}")
            loop.quit()
            # Signal the wrapper to restart without streaming via exit code
            # Use a flag so the finally block in main() can raise the right exit
            _stream_error_exit[0] = True
        else:
            loop.quit()
    return True


# ---------------------------------------------------------------------------
# Camera source
# ---------------------------------------------------------------------------
def _build_camera_source(pipeline, device: str, suffix: str):
    src       = _make("v4l2src",       f"src{suffix}")
    caps_src  = _capsfilter(f"caps{suffix}_src",
                            "image/jpeg,width=1920,height=1080,framerate=30/1")
    jparse    = _make("jpegparse",     f"jparse{suffix}")
    dec       = _make("nvv4l2decoder", f"dec{suffix}")
    conv_src  = _make_nvconv(f"conv{suffix}_src")
    caps_nvmm = _capsfilter(f"caps{suffix}_nvmm",
                            "video/x-raw(memory:NVMM),format=NV12")
    tee       = _make("tee",           f"tee{suffix}")

    src.set_property("device", device)
    dec.set_property("mjpeg",  1)

    for el in (src, caps_src, jparse, dec, conv_src, caps_nvmm, tee):
        pipeline.add(el)

    _link(src,       caps_src)
    _link(caps_src,  jparse)
    _link(jparse,    dec)
    _link(dec,       conv_src)
    _link(conv_src,  caps_nvmm)
    _link(caps_nvmm, tee)

    return tee


# ---------------------------------------------------------------------------
# Clean branch
# ---------------------------------------------------------------------------
def _build_clean_branch(pipeline, tee, suffix: str, rtsp_path: str) -> Gst.Element:
    q     = _make("queue",          f"q{suffix}_clean")
    conv  = _make_nvconv(f"conv{suffix}_clean")
    caps  = _capsfilter(f"caps{suffix}_i420", "video/x-raw,format=I420")
    enc   = _make("x264enc",         f"enc{suffix}_clean")
    parse = _make("h264parse",       f"parse{suffix}_clean")
    sink  = _make("rtspclientsink",  f"sink{suffix}_clean")

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes",   0)
    q.set_property("max-size-time",    0)
    q.set_property("leaky",            2)

    enc.set_property("tune",         CLEAN_TUNE)
    enc.set_property("speed-preset", CLEAN_PRESET)
    enc.set_property("bitrate",      CLEAN_BITRATE)
    enc.set_property("key-int-max",  CLEAN_KEYINT)
    enc.set_property("threads",      CLEAN_THREADS)

    sink.set_property("location",  f"rtsp://127.0.0.1:8554/{rtsp_path}")
    sink.set_property("protocols", 4)

    for el in (q, conv, caps, enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q)
    _link(q,     conv)
    _link(conv,  caps)
    _link(caps,  enc)
    _link(enc,   parse)
    _link(parse, sink)

    return enc


# ---------------------------------------------------------------------------
# AI branch
# ---------------------------------------------------------------------------
def _build_ai_branch(pipeline, tee, suffix: str, rtsp_path: str,
                     infer_config: str, cam_label: str):
    q_ai      = _make("queue",           f"q{suffix}_ai")
    conv_ai   = _make_nvconv(f"conv{suffix}_ai")
    caps_ai   = _capsfilter(f"caps{suffix}_ai",
                            "video/x-raw(memory:NVMM),format=NV12,width=1280,height=720")
    mux       = _make("nvstreammux",     f"mux{suffix}")
    pgie      = _make("nvinfer",         f"pgie{suffix}")
    tracker   = _make("nvtracker",       f"tracker{suffix}")
    conv_pre  = _make_nvconv(f"conv{suffix}_pre")
    nvosd     = _make("nvdsosd",         f"nvosd{suffix}")
    conv_post = _make_nvconv(f"conv{suffix}_post")
    caps_post = _capsfilter(f"caps{suffix}_post", "video/x-raw,format=I420")
    q_post    = _make("queue",           f"q{suffix}_post")
    enc       = _make("x264enc",         f"enc{suffix}_ai")
    parse     = _make("h264parse",       f"parse{suffix}_ai")
    sink      = _make("rtspclientsink",  f"sink{suffix}_ai")

    mux.set_property("width",                1280)
    mux.set_property("height",               720)
    mux.set_property("batch-size",           1)
    mux.set_property("batched-push-timeout", 33333)
    mux.set_property("live-source",          1)
    mux.set_property("nvbuf-memory-type",    0)

    pgie.set_property("config-file-path", infer_config)

    tracker.set_property("ll-lib-file",
        "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so")
    tracker.set_property("ll-config-file",
        "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_IOU.yml")
    tracker.set_property("tracker-width",       1280)
    tracker.set_property("tracker-height",      736)
    tracker.set_property("gpu-id",              0)
    tracker.set_property("display-tracking-id", 1)

    nvosd.set_property("process-mode", 1)

    q_ai.set_property("max-size-buffers", 2)
    q_ai.set_property("max-size-bytes",   0)
    q_ai.set_property("max-size-time",    0)
    q_ai.set_property("leaky",            2)

    q_post.set_property("max-size-buffers", 2)
    q_post.set_property("max-size-bytes",   0)
    q_post.set_property("max-size-time",    0)
    q_post.set_property("leaky",            2)

    enc.set_property("tune",         AI_TUNE)
    enc.set_property("speed-preset", AI_PRESET)
    enc.set_property("bitrate",      AI_BITRATE)
    enc.set_property("key-int-max",  AI_KEYINT)
    enc.set_property("threads",      AI_THREADS)

    sink.set_property("location",  f"rtsp://127.0.0.1:8554/{rtsp_path}")
    sink.set_property("protocols", 4)

    for el in (q_ai, conv_ai, caps_ai, mux, pgie, tracker,
               conv_pre, nvosd, conv_post, caps_post, q_post,
               enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q_ai)
    _link(q_ai,    conv_ai)
    _link(conv_ai, caps_ai)

    caps_ai_src = _get_static_pad(caps_ai, "src")
    mux_sinkpad = _request_mux_sinkpad(mux, "sink_0")
    if caps_ai_src.link(mux_sinkpad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link caps{suffix}_ai -> mux{suffix}.sink_0\n")
        sys.exit(1)

    _link(mux,       pgie)
    _link(pgie,      tracker)
    _link(tracker,   conv_pre)
    _link(conv_pre,  nvosd)
    _link(nvosd,     conv_post)
    _link(conv_post, caps_post)
    _link(caps_post, q_post)
    _link(q_post,    enc)
    _link(enc,       parse)
    _link(parse,     sink)

    return pgie, enc


# ---------------------------------------------------------------------------
# RTMP stream branch
# ---------------------------------------------------------------------------
def _build_stream_branch(pipeline, tee, rtmp_url: str) -> tuple[Gst.Element | None, Gst.Element | None]:
    """Returns (enc_stream, rtmpsink)."""
    global _osd_elements

    _render_scoreboard_bg()

    q_stream  = _make("queue",              "q_stream")
    conv_strm = _make_nvconv("conv_strm")
    caps_strm = _capsfilter("caps_strm_i420", "video/x-raw,format=I420")
    osd_bg    = _make("gdkpixbufoverlay",   "osd_bg")
    osd_home  = _make("textoverlay",        "osd_home")
    osd_away  = _make("textoverlay",        "osd_away")
    osd_score = _make("textoverlay",        "osd_score")
    osd_clock = _make("textoverlay",        "osd_clock")
    osd_fouls = _make("textoverlay",        "osd_fouls")
    enc_stream   = _make("x264enc",         "enc_stream")
    parse_stream = _make("h264parse",       "parse_stream")
    flvmux       = _make("flvmux",          "flvmux")
    rtmpsink     = _make("rtmpsink",        "rtmpsink")
    audiosrc     = _make("audiotestsrc",    "audiosrc")
    aacenc       = _make("voaacenc",        "aacenc")

    q_stream.set_property("max-size-buffers", 2)
    q_stream.set_property("max-size-bytes",   0)
    q_stream.set_property("max-size-time",    0)
    q_stream.set_property("leaky",            2)

    osd_bg.set_property("location",       SCOREBOARD_PNG)
    osd_bg.set_property("offset-x",       SCOREBOARD_OFFSET_X)
    osd_bg.set_property("offset-y",       SCOREBOARD_OFFSET_Y)
    osd_bg.set_property("overlay-width",  SCOREBOARD_W)
    osd_bg.set_property("overlay-height", SCOREBOARD_H)
    osd_bg.set_property("alpha",          0.0)

    def _setup_text(el, text, xpos, ypos, font="Sans Bold 20",
                    color=0xFFFFFFFF, shadow=True):
        el.set_property("text",        text)
        el.set_property("font-desc",   font)
        el.set_property("halignment",  4)
        el.set_property("valignment",  3)
        el.set_property("xpos",        xpos)
        el.set_property("ypos",        ypos)
        el.set_property("color",       color)
        el.set_property("draw-shadow", shadow)
        el.set_property("auto-resize", False)
        el.set_property("wait-text",   False)
        el.set_property("silent",      True)

    _setup_text(osd_home,  "HOME",     xpos=0.022, ypos=0.040,
                font="Sans Bold 22", color=0xFFFFFFFF)
    _setup_text(osd_away,  "AWAY",     xpos=0.230, ypos=0.040,
                font="Sans Bold 22", color=0xFFFFFFFF)
    _setup_text(osd_score, "0 - 0",   xpos=0.120, ypos=0.040,
                font="Sans Bold 22", color=0xFFD916FF)
    _setup_text(osd_clock, "Q1 10:00", xpos=0.330, ypos=0.040,
                font="Sans Bold 22", color=0xB2E5FFFF)
    _setup_text(osd_fouls, "",         xpos=0.022, ypos=0.068,
                font="Sans 13",      color=0xA6A6A6FF)

    enc_stream.set_property("pass",             "cbr")
    enc_stream.set_property("bitrate",          RTMP_BITRATE)
    enc_stream.set_property("vbv-buf-capacity", 200)
    enc_stream.set_property("tune",             RTMP_TUNE)
    enc_stream.set_property("speed-preset",     RTMP_PRESET)
    enc_stream.set_property("key-int-max",      RTMP_KEYINT)
    enc_stream.set_property("threads",          RTMP_THREADS)

    flvmux.set_property("streamable",  True)
    rtmpsink.set_property("location",  rtmp_url)
    rtmpsink.set_property("async",     False)
    audiosrc.set_property("wave",      4)
    aacenc.set_property("bitrate",     128000)

    for el in (q_stream, conv_strm, caps_strm,
               osd_bg, osd_home, osd_away, osd_score, osd_clock, osd_fouls,
               enc_stream, parse_stream, flvmux, rtmpsink, audiosrc, aacenc):
        pipeline.add(el)

    _tee_branch(tee, q_stream)
    _link(q_stream,  conv_strm)
    _link(conv_strm, caps_strm)
    _link(caps_strm, osd_bg)
    _link(osd_bg,    osd_home)
    _link(osd_home,  osd_away)
    _link(osd_away,  osd_score)
    _link(osd_score, osd_clock)
    _link(osd_clock, osd_fouls)
    _link(osd_fouls, enc_stream)
    _link(enc_stream,   parse_stream)
    _link(parse_stream, flvmux)

    _link_filtered(audiosrc, aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src   = _get_static_pad(aacenc, "src")
    flvmux_audio = flvmux.request_pad_simple("audio")
    if not flvmux_audio:
        sys.stderr.write("ERROR: Unable to get audio pad from flvmux\n")
        sys.exit(1)
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        sys.stderr.write("ERROR: Failed to link aacenc -> flvmux.audio\n")
        sys.exit(1)

    _link(flvmux, rtmpsink)

    with _osd_lock:
        _osd_elements.update({
            "osd_bg":    osd_bg,
            "osd_home":  osd_home,
            "osd_away":  osd_away,
            "osd_score": osd_score,
            "osd_clock": osd_clock,
            "osd_fouls": osd_fouls,
        })

    print("Scoreboard overlay: gdkpixbufoverlay (bg PNG) + textoverlay x5 (text)")
    return enc_stream, rtmpsink


# ---------------------------------------------------------------------------
# Main pipeline builder
# ---------------------------------------------------------------------------
def build_pipeline(enable_stream: bool = True) -> tuple:
    global _encoders
    _encoders = {}

    rtmp_url = read_stream_url() if enable_stream else None
    if enable_stream and not rtmp_url:
        print("No RTMP URL in stream.conf — streaming disabled.")

    pipeline = Gst.Pipeline()
    if not pipeline:
        sys.stderr.write("ERROR: Unable to create Pipeline\n")
        sys.exit(1)

    print("Building CAM0 source ...")
    tee0 = _build_camera_source(pipeline, "/dev/video0", "0")

    print("Building CAM0 clean RTSP branch (1080p high quality low latency) ...")
    enc0_clean = _build_clean_branch(pipeline, tee0, "0", "camera0_clean")
    _encoders["enc0_clean"] = enc0_clean

    print("Building CAM0 AI RTSP branch (720p debug) ...")
    pgie0, enc0_ai = _build_ai_branch(
        pipeline, tee0, "0", "camera0_ai",
        "config_infer_primary_yoloV8_cam0.txt", "CAM0")
    _encoders["enc0_ai"] = enc0_ai

    print("Building CAM2 source ...")
    tee2 = _build_camera_source(pipeline, "/dev/video2", "2")

    print("Building CAM2 clean RTSP branch (1080p high quality low latency) ...")
    enc2_clean = _build_clean_branch(pipeline, tee2, "2", "camera2_clean")
    _encoders["enc2_clean"] = enc2_clean

    print("Building CAM2 AI RTSP branch (720p debug) ...")
    pgie2, enc2_ai = _build_ai_branch(
        pipeline, tee2, "2", "camera2_ai",
        "config_infer_primary_yoloV8_cam2.txt", "CAM2")
    _encoders["enc2_ai"] = enc2_ai

    rtmpsink_el = None
    if rtmp_url:
        print(f"Building RTMP stream branch -> {rtmp_url[:60]}...")
        enc_stream, rtmpsink_el = _build_stream_branch(pipeline, tee2, rtmp_url)
        _encoders["enc_stream"] = enc_stream
    else:
        print("RTMP streaming disabled.")

    return pipeline, pgie0, pgie2, rtmpsink_el


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _rtmp_status_sent, _rtmp_status_cached
    _rtmp_status_sent = False
    _rtmp_status_cached = None
    _stream_error_exit[0] = False

    no_stream = "--no-stream" in sys.argv

    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        os.environ.setdefault("DISPLAY", ":0")
        print("WARNING: DISPLAY not set — defaulting to :0")

    def _restart_handler(_sig, _frame):
        raise SystemExit(RESTART_EXIT_CODE)
    signal.signal(signal.SIGUSR1, _restart_handler)

    Gst.init(None)

    start_unix_server()
    start_http_server()
    start_pycam_server()

    print("Building pipeline ...")
    pipeline, pgie0, pgie2, rtmpsink_el = build_pipeline(enable_stream=not no_stream)

    for pgie, cam_label in [(pgie0, "CAM0"), (pgie2, "CAM2")]:
        srcpad = pgie.get_static_pad("src")
        if not srcpad:
            sys.stderr.write(f"ERROR: Cannot get src pad of {pgie.get_name()}\n")
            sys.exit(1)
        srcpad.add_probe(Gst.PadProbeType.BUFFER, pgie_src_pad_buffer_probe, cam_label)
        print(f"Probe attached -> {pgie.get_name()} ({cam_label})")

    if rtmpsink_el is not None:
        sink_pad = rtmpsink_el.get_static_pad("sink")
        if sink_pad:
            sink_pad.add_probe(
                Gst.PadProbeType.BUFFER,
                _rtmp_sink_pad_probe,
                None,
            )
            print("RTMP verification probe attached -> rtmpsink.sink")
        else:
            print("WARNING: Could not get rtmpsink sink pad for verification probe")

        GLib.timeout_add_seconds(RTMP_VERIFY_TIMEOUT_SEC, _rtmp_verify_timeout)
        print(f"RTMP verification timeout set: {RTMP_VERIFY_TIMEOUT_SEC}s")

    loop = GLib.MainLoop()
    bus  = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    GLib.timeout_add_seconds(5, _fps_report)

    print("Starting pipeline ...")
    pipeline.set_state(Gst.State.PLAYING)

    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
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
        if _stream_error_exit[0]:
            raise SystemExit(STREAM_ERROR_EXIT_CODE)


if __name__ == "__main__":
    main()