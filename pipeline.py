#!/usr/bin/env python3
"""
DeepStream Basketball Detection Pipeline
==========================================
Two cameras (CAM0=/dev/video0, CAM2=/dev/video2) each producing:
  - clean RTSP branch: 1080p H.264 → MediaMTX → WebRTC tablet viewing
  - AI RTSP branch:    720p H.264 with bounding boxes → MediaMTX → debug viewing
CAM2 additionally produces an optional RTMP branch for YouTube/Twitch/Kick.

KNOWN ISSUES / HISTORY:
  - JetPack 6.2 + DeepStream 7.1 bug: cudaErrorIllegalAddress (700) in
    nvbufsurftransform_copy.cpp. Fix: copy-hw=2 on all nvvideoconvert elements.
  - link_filtered() on NVMM paths causes NULL caps assertions at runtime.
    Fix: use real capsfilter elements for every inline caps constraint.
  - nvv4l2decoder has a static src pad — do NOT use pad-added signal.
  - nvdsosd process-mode=1 requires NVMM RGBA input — always insert
    nvvideoconvert before nvosd to convert NV12->RGBA.
  - cairooverlay requires CPU-side BGRA colorspace conversion on every frame
    which kills FPS on Orin Nano. ENABLE_CAIRO is disabled; scoreboard overlay
    code is retained but inactive. RTMP branch streams plain video with no OSD.

STREAM DESIGN:
  CAM0 + CAM2:
    - clean branch: 1080p 8000kbps, keyframe every 0.5s → WebRTC via MediaMTX
    - AI branch:    720p 2000kbps, nvinfer + nvtracker + nvdsosd bounding boxes
  CAM2 only:
    - RTMP branch:  1080p 6800kbps CBR → flvmux → rtmpsink (YouTube 2s keyframe)
                    silent audio track (audiotestsrc silence + voaacenc 128kbps)
                    enabled only when stream.conf contains a valid rtmp:// URL

DETECTION:
  - Only CAM2 detections are forwarded to the camera control process
  - CAM0 AI branch runs inference for its debug RTSP stream only
  - Probe fires every 2nd frame (PROBE_EVERY_N_FRAMES = 2)
  - Only frames with at least one detection are forwarded (no empty sends)
  - Coordinates are in 1280x720 space (AI branch resolution)

SERVICES:
  Unix socket  /tmp/smartcam.sock  — Go backend bridge, bidirectional JSON
    Python -> Go: {"type":"state","streaming":bool,"model":str,
                   "available_models":[str],"webrtc":{cam0_clean,cam0_ai,
                   cam2_clean,cam2_ai}}
                  (detection forwarding to Go is intentionally disabled —
                   detections go to pycam.sock instead)
    Go -> Python: {"type":"cmd","action":"start_stream","rtmp_url":"rtmp://..."}
                  {"type":"cmd","action":"stop_stream"}
                  {"type":"cmd","action":"set_config","bitrateKbps":N}
                  {"type":"cmd","action":"set_osd","visible":bool}
                  {"type":"cmd","action":"set_score",...score fields...}

  Unix socket  /tmp/pycam.sock  — camera control bridge, outbound only
    Python -> pan_control: {"camera":"CAM2","frame":N,"timestamp":T,
                            "detections":[{"class":str,"class_id":int,
                            "tracker_id":int,"center_x":float,"center_y":float,
                            "width":float,"height":float,"left":float,
                            "top":float,"confidence":float}]}

  HTTP API  http://127.0.0.1:9101  — internal/debug only
    GET  /status  → pipeline state, encoder list, stream URLs, socket paths
    POST /score   → update scoreboard overlay fields (JSON body)

RESTART MECHANISM:
  start_stream / stop_stream write stream.conf then send SIGUSR1 to self.
  The process exits with code 42 (RESTART_EXIT_CODE); the caller (systemd or
  a wrapper script) is responsible for restarting it.

Class IDs (model v9):  0=RIM  1=BALL
"""

import json
import os
import queue
import signal
import socket
import socket as _socket
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst

import pyds

# Cairo disabled — replaced by nvdsosd GPU scoreboard on RTMP branch only.
ENABLE_CAIRO = False

try:
    import cairo

    HAS_CAIRO = ENABLE_CAIRO
except ImportError:
    cairo = None
    HAS_CAIRO = False

if HAS_CAIRO:
    print("Scoreboard overlay: ENABLED (cairo)")
else:
    print("Scoreboard overlay: DISABLED")


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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STREAM_CONF = os.path.join(SCRIPT_DIR, "stream.conf")

RESTART_EXIT_CODE = 42

# Clean branch encoder settings — tuned for low latency local WebRTC viewing
CLEAN_BITRATE = 8000
CLEAN_KEYINT = 15
CLEAN_THREADS = 2
CLEAN_PRESET = "ultrafast"
CLEAN_TUNE = "zerolatency"

# AI branch encoder settings
AI_BITRATE = 2000
AI_KEYINT = 20
AI_THREADS = 1
AI_PRESET = "ultrafast"
AI_TUNE = "zerolatency"

# RTMP stream encoder settings — YouTube 1080p
RTMP_BITRATE = 6800
RTMP_KEYINT = 60
RTMP_THREADS = 2
RTMP_PRESET = "ultrafast"
RTMP_TUNE = "zerolatency"

# Encoder references populated by build_pipeline() — used by set_config cmd
_encoders: dict[str, Gst.Element] = {}

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
    "home_name": "HOME",
    "away_name": "AWAY",
    "home_points": 0,
    "away_points": 0,
    "home_fouls": 0,
    "away_fouls": 0,
    "home_timeouts": 3,
    "away_timeouts": 3,
    "quarter": 1,
    "clock": "10:00",
    "visible": False,
}
score_lock = threading.Lock()


def _apply_score_patch(data: dict) -> None:
    allowed_str = {"home_name", "away_name", "clock"}
    allowed_int = {"home_points", "away_points", "home_fouls",
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


# ---------------------------------------------------------------------------
# Stream OSD probe — RTMP branch ONLY
# ---------------------------------------------------------------------------
def stream_osd_probe(_pad, info, _user_data):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return Gst.PadProbeReturn.OK

    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    if not batch_meta:
        return Gst.PadProbeReturn.OK

    with score_lock:
        state = score_state.copy()

    if not state["visible"]:
        return Gst.PadProbeReturn.OK

    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        display_meta = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
        display_meta.num_rects = 1
        display_meta.num_labels = 4

        bg = display_meta.rect_params[0]
        bg.left = 20
        bg.top = 20
        bg.width = 580
        bg.height = 70
        bg.has_bg_color = 1
        bg.bg_color.set(0.05, 0.05, 0.05, 0.78)
        bg.border_width = 0

        t0 = display_meta.text_params[0]
        t0.display_text = state["home_name"][:8]
        t0.x_offset = 34
        t0.y_offset = 30
        t0.font_params.font_name = "Sans"
        t0.font_params.font_size = 20
        t0.font_params.font_color.set(1.0, 1.0, 1.0, 1.0)
        t0.set_bg_clr = 0

        t1 = display_meta.text_params[1]
        t1.display_text = state["away_name"][:8]
        t1.x_offset = 295
        t1.y_offset = 30
        t1.font_params.font_name = "Sans"
        t1.font_params.font_size = 20
        t1.font_params.font_color.set(1.0, 1.0, 1.0, 1.0)
        t1.set_bg_clr = 0

        t2 = display_meta.text_params[2]
        t2.display_text = f"{state['home_points']} - {state['away_points']}"
        t2.x_offset = 150
        t2.y_offset = 30
        t2.font_params.font_name = "Sans"
        t2.font_params.font_size = 20
        t2.font_params.font_color.set(1.0, 0.85, 0.1, 1.0)
        t2.set_bg_clr = 0

        t3 = display_meta.text_params[3]
        t3.display_text = f"Q{state['quarter']}  {state['clock']}"
        t3.x_offset = 430
        t3.y_offset = 30
        t3.font_params.font_name = "Sans"
        t3.font_params.font_size = 20
        t3.font_params.font_color.set(0.7, 0.9, 1.0, 1.0)
        t3.set_bg_clr = 0

        pyds.nvds_add_display_meta_to_frame(frame_meta, display_meta)

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK


# ---------------------------------------------------------------------------
# Pycam socket server — outbound detections to camera control process
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
        "camera": cam_label,
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


MODEL_NAME = "Basketball"
AVAILABLE_MODELS = ["Basketball"]


def _push_state() -> None:
    url = read_stream_url()
    _out_q.put({
        "type": "state",
        "streaming": url is not None,
        "model": MODEL_NAME,
        "available_models": AVAILABLE_MODELS,
        "webrtc": {
            "cam0_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
            "cam0_ai": f"http://{JETSON_HOST}:8889/camera0_ai",
            "cam2_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
            "cam2_ai": f"http://{JETSON_HOST}:8889/camera2_ai",
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
                "alive": True,
                "pid": os.getpid(),
                "streaming": url is not None,
                "rtmp_url": url or "",
                "score_overlay": score_visible,
                "unix_sock": UNIX_SOCK,
                "pycam_sock": PYCAM_SOCK,
                "encoders": list(_encoders.keys()),
                "cameras": {
                    "cam0": {
                        "device": "/dev/video0",
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera0_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera0_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera0_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera0_ai",
                    },
                    "cam2": {
                        "device": "/dev/video2",
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera2_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera2_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera2_clean",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera2_ai",
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
        print(f"ERROR: {err}: {dbg}")
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


# ---------------------------------------------------------------------------
# Clean branch
# ---------------------------------------------------------------------------
def _build_clean_branch(pipeline, tee, suffix: str, rtsp_path: str) -> Gst.Element:
    q = _make("queue", f"q{suffix}_clean")
    conv = _make_nvconv(f"conv{suffix}_clean")
    caps = _capsfilter(f"caps{suffix}_i420", "video/x-raw,format=I420")
    enc = _make("x264enc", f"enc{suffix}_clean")
    parse = _make("h264parse", f"parse{suffix}_clean")
    sink = _make("rtspclientsink", f"sink{suffix}_clean")

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)

    enc.set_property("tune", CLEAN_TUNE)
    enc.set_property("speed-preset", CLEAN_PRESET)
    enc.set_property("bitrate", CLEAN_BITRATE)
    enc.set_property("key-int-max", CLEAN_KEYINT)
    enc.set_property("threads", CLEAN_THREADS)

    sink.set_property("location", f"rtsp://127.0.0.1:8554/{rtsp_path}")
    sink.set_property("protocols", 4)

    for el in (q, conv, caps, enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q)
    _link(q, conv)
    _link(conv, caps)
    _link(caps, enc)
    _link(enc, parse)
    _link(parse, sink)

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
    sink = _make("rtspclientsink", f"sink{suffix}_ai")

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

    enc.set_property("tune", AI_TUNE)
    enc.set_property("speed-preset", AI_PRESET)
    enc.set_property("bitrate", AI_BITRATE)
    enc.set_property("key-int-max", AI_KEYINT)
    enc.set_property("threads", AI_THREADS)

    sink.set_property("location", f"rtsp://127.0.0.1:8554/{rtsp_path}")
    sink.set_property("protocols", 4)

    for el in (q_ai, conv_ai, caps_ai, mux, pgie, tracker,
               conv_pre, nvosd, conv_post, caps_post, q_post,
               enc, parse, sink):
        pipeline.add(el)

    _tee_branch(tee, q_ai)
    _link(q_ai, conv_ai)
    _link(conv_ai, caps_ai)

    caps_ai_src = _get_static_pad(caps_ai, "src")
    mux_sinkpad = _request_mux_sinkpad(mux, "sink_0")
    if caps_ai_src.link(mux_sinkpad) != Gst.PadLinkReturn.OK:
        sys.stderr.write(f"ERROR: Failed to link caps{suffix}_ai -> mux{suffix}.sink_0\n")
        sys.exit(1)

    _link(mux, pgie)
    _link(pgie, tracker)
    _link(tracker, conv_pre)
    _link(conv_pre, nvosd)
    _link(nvosd, conv_post)
    _link(conv_post, caps_post)
    _link(caps_post, q_post)
    _link(q_post, enc)
    _link(enc, parse)
    _link(parse, sink)

    return pgie, enc


# ---------------------------------------------------------------------------
# RTMP stream branch
# ---------------------------------------------------------------------------
def _build_stream_branch(pipeline, tee, rtmp_url: str) -> Gst.Element | None:
    q_stream = _make("queue", "q_stream")
    conv_strm = _make_nvconv("conv_strm")

    q_stream.set_property("max-size-buffers", 2)
    q_stream.set_property("max-size-bytes", 0)
    q_stream.set_property("max-size-time", 0)
    q_stream.set_property("leaky", 2)

    for el in (q_stream, conv_strm):
        pipeline.add(el)

    _tee_branch(tee, q_stream)
    _link(q_stream, conv_strm)

    if HAS_CAIRO:
        vconv_bgra = _make("videoconvert", "vconv_bgra")
        score_overlay = _make("cairooverlay", "score_overlay")
        vconv_i420 = _make("videoconvert", "vconv_i420")
        for el in (vconv_bgra, score_overlay, vconv_i420):
            pipeline.add(el)
        _link_filtered(conv_strm, vconv_bgra, "video/x-raw,format=I420")
        _link_filtered(vconv_bgra, score_overlay, "video/x-raw,format=BGRA")
        _link(score_overlay, vconv_i420)
        score_overlay.connect("draw", on_score_draw)
        enc_input = vconv_i420
        enc_input_caps = "video/x-raw,format=I420"
        print("Scoreboard overlay: ACTIVE (cairo)")
    else:
        enc_input = conv_strm
        enc_input_caps = "video/x-raw,format=I420"

    enc_stream = _make("x264enc", "enc_stream")
    parse_stream = _make("h264parse", "parse_stream")
    flvmux = _make("flvmux", "flvmux")
    rtmpsink = _make("rtmpsink", "rtmpsink")
    audiosrc = _make("audiotestsrc", "audiosrc")
    aacenc = _make("voaacenc", "aacenc")

    enc_stream.set_property("pass", "cbr")
    enc_stream.set_property("bitrate", RTMP_BITRATE)
    enc_stream.set_property("vbv-buf-capacity", 200)
    enc_stream.set_property("tune", RTMP_TUNE)
    enc_stream.set_property("speed-preset", RTMP_PRESET)
    enc_stream.set_property("key-int-max", RTMP_KEYINT)
    enc_stream.set_property("threads", RTMP_THREADS)

    flvmux.set_property("streamable", True)
    rtmpsink.set_property("location", rtmp_url)
    rtmpsink.set_property("async", False)
    audiosrc.set_property("wave", 4)
    aacenc.set_property("bitrate", 128000)

    for el in (enc_stream, parse_stream, flvmux, rtmpsink, audiosrc, aacenc):
        pipeline.add(el)

    _link_filtered(enc_input, enc_stream, enc_input_caps)
    _link(enc_stream, parse_stream)
    _link(parse_stream, flvmux)

    _link_filtered(audiosrc, aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src = _get_static_pad(aacenc, "src")
    flvmux_audio = flvmux.request_pad_simple("audio")
    if not flvmux_audio:
        sys.stderr.write("ERROR: Unable to get audio pad from flvmux\n")
        sys.exit(1)
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        sys.stderr.write("ERROR: Failed to link aacenc -> flvmux.audio\n")
        sys.exit(1)

    _link(flvmux, rtmpsink)

    return enc_stream


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

    if rtmp_url:
        print(f"Building RTMP stream branch -> {rtmp_url[:60]}...")
        enc_stream = _build_stream_branch(pipeline, tee2, rtmp_url)
        _encoders["enc_stream"] = enc_stream
    else:
        print("RTMP streaming disabled.")

    return pipeline, pgie0, pgie2


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
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
    pipeline, pgie0, pgie2 = build_pipeline(enable_stream=not no_stream)

    for pgie, cam_label in [(pgie0, "CAM0"), (pgie2, "CAM2")]:
        srcpad = pgie.get_static_pad("src")
        if not srcpad:
            sys.stderr.write(f"ERROR: Cannot get src pad of {pgie.get_name()}\n")
            sys.exit(1)
        srcpad.add_probe(Gst.PadProbeType.BUFFER, pgie_src_pad_buffer_probe, cam_label)
        print(f"Probe attached -> {pgie.get_name()} ({cam_label})")

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
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


if __name__ == "__main__":
    main()
