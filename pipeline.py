#!/usr/bin/env python3
"""
DeepStream Basketball Detection Pipeline
==========================================
Two cameras with clean RTSP + AI RTSP streams via MediaMTX.
Optional live-streaming to YouTube/Twitch/Kick via RTMP with scoreboard overlay.

SERVICES (for Go backend):
  Unix socket  /tmp/smartcam.sock  → bidirectional newline-delimited JSON
    Python → Go:  {"type":"detection", "camera":"CAM0", "frame":N, "timestamp":T, "detections":[...]}
                  {"type":"state", "streaming":bool, "webrtc":{"cam0":"...","cam2":"..."}}
    Go    → Python: {"type":"cmd", "action":"start_stream", "rtmp_url":"rtmp://..."}
                    {"type":"cmd", "action":"stop_stream"}
                    {"type":"cmd", "action":"set_config", "bitrateKbps":N}
                    {"type":"cmd", "action":"set_osd", "visible":bool}
                    {"type":"cmd", "action":"set_score", ...score fields...}

HTTP API (internal / debug only):
  GET  /status
  POST /score

Class IDs (model v9):  0=BACKGROUND  1=HOOP  2=BASKETBALL

Score state (set_score / POST /score fields, all optional):
  home_name, away_name, home_points, away_points,
  home_fouls, away_fouls, home_timeouts, away_timeouts,
  quarter, clock, visible
"""

import json
import os
import queue
import signal
import socket
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst

import pyds

try:
    import cairo

    HAS_CAIRO = True
except ImportError:
    cairo = None
    HAS_CAIRO = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JETSON_HOST = os.environ.get("JETSON_HOST", "localhost")
UNIX_SOCK = os.environ.get("SMARTCAM_SOCK", "/tmp/smartcam.sock")
HTTP_PORT = 9101  # kept for debug / score updates without Go

CLASS_ID_RIM = 0
CLASS_ID_BALL = 1
CLASS_NAMES = {CLASS_ID_RIM: "RIM", CLASS_ID_BALL: "BALL"}

PROBE_EVERY_N_FRAMES = 1

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STREAM_CONF = os.path.join(SCRIPT_DIR, "stream.conf")

encoder_elements = []  # filled in main() — enc_stream only

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


def _apply_score_patch(data: dict):
    """Apply a partial score update dict. Called from both unix socket and HTTP."""
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
# Cairo scoreboard draw callback
# ---------------------------------------------------------------------------
def on_score_draw(overlay, ctx, timestamp, duration):  # noqa: ARG001
    with score_lock:
        state = score_state.copy()

    if not state["visible"]:
        return

    BAR_X = 20
    BAR_Y = 20
    BAR_W = 580
    BAR_H = 70
    PADDING = 14
    FONT_LARGE = 28
    FONT_SMALL = 13

    # Background
    ctx.set_source_rgba(0.05, 0.05, 0.05, 0.78)
    ctx.rectangle(BAR_X, BAR_Y, BAR_W, BAR_H)
    ctx.fill()

    # Top accent line
    ctx.set_source_rgba(0.9, 0.6, 0.1, 1.0)
    ctx.rectangle(BAR_X, BAR_Y, BAR_W, 3)
    ctx.fill()

    ctx.select_font_face("Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)

    def draw_text(text, x, y, size, r, g, b, a=1.0):
        ctx.set_font_size(size)
        ctx.set_source_rgba(r, g, b, a)
        ctx.move_to(x, y)
        ctx.show_text(text)

    # Layout columns:
    # [PADDING] HOME_NAME [130] HOME_PTS [185] — [210] AWAY_PTS [265] AWAY_NAME [400] | Q# [480] CLOCK [580]
    text_y = BAR_Y + 44
    small_y = BAR_Y + BAR_H - 8

    # Home name (right-aligned to col 125)
    home_name = state["home_name"][:8]
    ctx.set_font_size(FONT_LARGE)
    te = ctx.text_extents(home_name)
    draw_text(home_name, BAR_X + PADDING, text_y, FONT_LARGE, 1, 1, 1)

    # Home score
    draw_text(str(state["home_points"]), BAR_X + 135, text_y, FONT_LARGE, 1, 0.85, 0.1)

    # Dash separator
    draw_text("—", BAR_X + 190, text_y, FONT_LARGE, 0.5, 0.5, 0.5)

    # Away score
    draw_text(str(state["away_points"]), BAR_X + 220, text_y, FONT_LARGE, 1, 0.85, 0.1)

    # Away name
    draw_text(state["away_name"][:8], BAR_X + 275, text_y, FONT_LARGE, 1, 1, 1)

    # Divider
    ctx.set_source_rgba(0.4, 0.4, 0.4, 0.8)
    ctx.rectangle(BAR_X + 415, BAR_Y + 10, 2, BAR_H - 20)
    ctx.fill()

    # Quarter
    draw_text(f"Q{state['quarter']}", BAR_X + 428, text_y, FONT_LARGE, 0.7, 0.9, 1.0)

    # Clock
    draw_text(state["clock"], BAR_X + 478, text_y, FONT_LARGE, 1, 1, 1)

    # Small stats row
    draw_text(f"F:{state['home_fouls']}  T:{state['home_timeouts']}",
              BAR_X + PADDING, small_y, FONT_SMALL, 0.65, 0.65, 0.65)
    draw_text(f"F:{state['away_fouls']}  T:{state['away_timeouts']}",
              BAR_X + 275, small_y, FONT_SMALL, 0.65, 0.65, 0.65)


# ---------------------------------------------------------------------------
# Unix socket server  (Python = server, Go = client)
# ---------------------------------------------------------------------------
_sock_clients: list[socket.socket] = []
_sock_clients_lock = threading.Lock()

# Outbound queue — probe callbacks put messages here; sender thread writes to Go.
# This prevents a slow/stalled Go reader from blocking the GStreamer pipeline thread.
_out_q: queue.SimpleQueue = queue.SimpleQueue()


def _sender_loop():
    """Drain _out_q and write to all connected Go clients. Runs in daemon thread."""
    while True:
        msg = _out_q.get()
        _send_to_go(msg)


def _send_to_go(msg: dict):
    """Send a JSON line to all connected Go clients."""
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


def _handle_go_connection(conn: socket.socket):
    """Handle one persistent Go connection: read commands, dispatch them."""
    with _sock_clients_lock:
        _sock_clients.append(conn)

    # Send current state immediately so Go has fresh URLs on reconnect
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


def _ack(action: str, ok: bool, error: str = ""):
    """Enqueue an ack message back to Go."""
    msg: dict = {"type": "ack", "action": action, "ok": ok}
    if error:
        msg["error"] = error
    _out_q.put(msg)


def _dispatch_cmd(msg: dict):
    action = msg.get("action", "")

    if action == "start_stream":
        rtmp_url = msg.get("rtmp_url", "").strip()
        if rtmp_url.startswith("rtmp://") or rtmp_url.startswith("rtmps://"):
            with open(STREAM_CONF, "w") as f:
                f.write(rtmp_url + "\n")
                f.flush()
                os.fsync(f.fileno())
            print(f"[unix] start_stream → {rtmp_url[:60]}")
            _ack("start_stream", True)
            _request_restart()
        else:
            print(f"[unix] start_stream: invalid rtmp_url: {rtmp_url!r}")
            _ack("start_stream", False, f"invalid rtmp_url: {rtmp_url!r}")

    elif action == "stop_stream":
        with open(STREAM_CONF, "w") as f:
            f.write("# disabled\n")
            f.flush()
            os.fsync(f.fileno())
        print("[unix] stop_stream")
        _ack("stop_stream", True)
        _request_restart()

    elif action == "set_config":
        bitrate = msg.get("bitrateKbps")
        if not encoder_elements:
            err = "no stream encoder active — start a stream first"
            print(f"[unix] set_config: {err}")
            _ack("set_config", False, err)
            return
        if isinstance(bitrate, int) and 100 <= bitrate <= 50000:
            for enc in encoder_elements:
                old = enc.get_property("bitrate")
                enc.set_property("bitrate", bitrate)
                actual = enc.get_property("bitrate")
                print(f"[unix] set_config bitrateKbps: {old} → set {bitrate} → actual {actual} kbps ({enc.get_name()})")
            _ack("set_config", True)
        else:
            err = f"bitrateKbps must be int 100-50000, got {bitrate!r}"
            print(f"[unix] set_config: {err}")
            _ack("set_config", False, err)

    elif action == "set_osd":
        visible = msg.get("visible")
        if isinstance(visible, bool):
            _apply_score_patch({"visible": visible})
            print(f"[unix] set_osd visible={visible}")
            _ack("set_osd", True)
        else:
            err = f"visible must be bool, got {visible!r}"
            print(f"[unix] set_osd: {err}")
            _ack("set_osd", False, err)

    elif action == "set_score":
        _apply_score_patch(msg)
        print("[unix] set_score applied")
        _ack("set_score", True)

    else:
        err = f"unknown action: {action!r}"
        print(f"[unix] {err}")
        _ack(action, False, err)


def _push_state():
    """Push current pipeline state to Go (called on connect and after changes)."""
    url = read_stream_url()
    _out_q.put({
        "type": "state",
        "streaming": url is not None,
        "webrtc": {
            "cam0_clean": f"http://{JETSON_HOST}:8889/camera0_clean/whep",
            "cam0_ai": f"http://{JETSON_HOST}:8889/camera0_ai/whep",
            "cam2_clean": f"http://{JETSON_HOST}:8889/camera2_clean/whep",
            "cam2_ai": f"http://{JETSON_HOST}:8889/camera2_ai/whep",
        },
    })


def send_detection(data_dict: dict):
    """Enqueue a detection event for the sender thread. Never blocks the probe."""
    data_dict["type"] = "detection"
    _out_q.put(data_dict)


def start_unix_server():
    try:
        os.unlink(UNIX_SOCK)
    except FileNotFoundError:
        pass

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(UNIX_SOCK)
    os.chmod(UNIX_SOCK, 0o660)
    srv.listen(4)
    print(f"Unix socket → {UNIX_SOCK}")

    # Start the single sender thread that drains _out_q → Go clients.
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
# HTTP API  (debug / direct score updates — Go uses unix socket)
# ---------------------------------------------------------------------------
class ControlHandler(BaseHTTPRequestHandler):

    def do_GET(self):  # noqa: N802
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
                "cameras": {
                    "cam0": {
                        "device": "/dev/video0",
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera0_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera0_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera0_clean/whep",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera0_ai/whep",
                    },
                    "cam2": {
                        "device": "/dev/video2",
                        "rtsp_clean": f"rtsp://{JETSON_HOST}:8554/camera2_clean",
                        "rtsp_ai": f"rtsp://{JETSON_HOST}:8554/camera2_ai",
                        "webrtc_clean": f"http://{JETSON_HOST}:8889/camera2_clean/whep",
                        "webrtc_ai": f"http://{JETSON_HOST}:8889/camera2_ai/whep",
                    },
                },
            })
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
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

    def do_OPTIONS(self):  # noqa: N802
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

    def log_message(self, format, *args):  # noqa: A002
        pass


def start_http_server():
    server = HTTPServer(("127.0.0.1", HTTP_PORT), ControlHandler)  # type: ignore[arg-type]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"HTTP API (debug) → http://0.0.0.0:{HTTP_PORT}")


# ---------------------------------------------------------------------------
# Pipeline templates
# ---------------------------------------------------------------------------
CAM0_PIPELINE = """
    v4l2src device=/dev/video0 io-mode=2 !
    image/jpeg,width=1920,height=1080,framerate=30/1 !
    jpegparse ! nvv4l2decoder mjpeg=1 !
    nvvidconv !
    video/x-raw(memory:NVMM),format=NV12 !
    tee name=t0

    t0. ! queue ! nvvidconv !
    video/x-raw,format=I420 !
    x264enc name=enc0_clean tune=zerolatency speed-preset=ultrafast bitrate=6800 key-int-max=30 threads=4 !
    h264parse !
    rtspclientsink location=rtsp://127.0.0.1:8554/camera0_clean protocols=tcp

    t0. ! queue ! nvvidconv !
    video/x-raw(memory:NVMM),format=NV12,width=1280,height=720 !
    mux0.sink_0 nvstreammux name=mux0 width=1280 height=720 batch-size=1 batched-push-timeout=50000 live-source=1 !
    nvinfer name=pgie0 config-file-path=config_infer_primary_yoloV8_cam0.txt !
    nvdsosd process-mode=1 ! nvvidconv !
    video/x-raw,format=I420 !
    queue max-size-buffers=4 !
    x264enc name=enc0_ai tune=zerolatency speed-preset=ultrafast bitrate=2000 key-int-max=20 threads=2 !
    h264parse !
    rtspclientsink location=rtsp://127.0.0.1:8554/camera0_ai protocols=tcp
"""

CAM2_BASE = """
    v4l2src device=/dev/video2 io-mode=2 !
    image/jpeg,width=1920,height=1080,framerate=30/1 !
    jpegparse ! nvv4l2decoder mjpeg=1 !
    nvvidconv !
    video/x-raw(memory:NVMM),format=NV12 !
    tee name=t2

    t2. ! queue ! nvvidconv !
    video/x-raw,format=I420 !
    x264enc name=enc2_clean tune=zerolatency speed-preset=ultrafast bitrate=6800 key-int-max=30 threads=4 !
    h264parse !
    rtspclientsink location=rtsp://127.0.0.1:8554/camera2_clean protocols=tcp

    t2. ! queue ! nvvidconv !
    video/x-raw(memory:NVMM),format=NV12,width=1280,height=720 !
    mux2.sink_0 nvstreammux name=mux2 width=1280 height=720 batch-size=1 batched-push-timeout=50000 live-source=1 !
    nvinfer name=pgie2 config-file-path=config_infer_primary_yoloV8_cam2.txt !
    nvdsosd process-mode=1 ! nvvidconv !
    video/x-raw,format=I420 !
    queue max-size-buffers=4 !
    x264enc name=enc2_ai tune=zerolatency speed-preset=ultrafast bitrate=2000 key-int-max=20 threads=2 !
    h264parse !
    rtspclientsink location=rtsp://127.0.0.1:8554/camera2_ai protocols=tcp
"""

STREAM_BRANCH_TEMPLATE_CAIRO = """
    t2. ! queue max-size-buffers=5 leaky=downstream ! nvvidconv !
    video/x-raw,format=I420 !
    videoconvert !
    video/x-raw,format=BGRA !
    cairooverlay name=score_overlay !
    videoconvert !
    video/x-raw,format=I420 !
    queue max-size-buffers=4 !
    x264enc name=enc_stream pass=cbr speed-preset=ultrafast bitrate=3000 key-int-max=60 threads=2 !
    h264parse !
    flvmux streamable=true name=flvmux !
    rtmpsink location={rtmp_url} async=false
    audiotestsrc wave=silence !
    audio/x-raw,rate=44100,channels=2 !
    voaacenc bitrate=128000 !
    flvmux.
"""

STREAM_BRANCH_TEMPLATE_PLAIN = """
    t2. ! queue max-size-buffers=5 leaky=downstream ! nvvidconv !
    video/x-raw,format=I420 !
    queue max-size-buffers=4 !
    x264enc name=enc_stream pass=cbr speed-preset=ultrafast bitrate=3000 key-int-max=60 threads=2 !
    h264parse !
    flvmux streamable=true name=flvmux !
    rtmpsink location={rtmp_url} async=false
    audiotestsrc wave=silence !
    audio/x-raw,rate=44100,channels=2 !
    voaacenc bitrate=128000 !
    flvmux.
"""


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def read_stream_url():
    if not os.path.exists(STREAM_CONF):
        # First run — create disabled conf so we never accidentally stream
        with open(STREAM_CONF, "w") as f:
            f.write("# disabled\n")
        return None
    with open(STREAM_CONF) as f:
        url = f.read().strip()
    return None if (not url or url.startswith("#")) else url


RESTART_EXIT_CODE = 42  # intentional restart — wrapper will relaunch


def _request_restart():
    def _kill():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGUSR1)

    threading.Thread(target=_kill, daemon=True).start()


def build_pipeline_string(stream=True):
    rtmp_url = read_stream_url() if stream else None
    if stream and not rtmp_url:
        print("No RTMP URL in stream.conf — starting without streaming.")

    pipeline = CAM0_PIPELINE + CAM2_BASE

    if rtmp_url:
        tmpl = STREAM_BRANCH_TEMPLATE_CAIRO if HAS_CAIRO else STREAM_BRANCH_TEMPLATE_PLAIN
        pipeline += tmpl.format(rtmp_url=rtmp_url)
        overlay = "scoreboard overlay enabled" if HAS_CAIRO else "cairo not installed — no scoreboard"
        print(f"Streaming → {rtmp_url[:60]}... ({overlay})")
    else:
        print("Streaming disabled")

    return pipeline


# ---------------------------------------------------------------------------
# Probe callback
# ---------------------------------------------------------------------------
def pgie_src_pad_buffer_probe(_pad, info, u_data):
    cam_label = u_data
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

        if frame_meta.frame_num % PROBE_EVERY_N_FRAMES != 0:
            try:
                l_frame = l_frame.next
            except StopIteration:
                break
            continue

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
            pass
            # send_detection({
            #     "camera":     cam_label,
            #     "frame":      frame_meta.frame_num,
            #     "timestamp":  time.time(),
            #     "detections": detections,
            # })

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
# Main
# ---------------------------------------------------------------------------
def main():
    no_stream = "--no-stream" in sys.argv

    if not HAS_CAIRO:
        print("WARNING: 'cairo' not installed — scoreboard overlay disabled.")
        print("         pip install pycairo --break-system-packages")

    # nvdsosd needs a display context (same as gst-launch uses).
    # If DISPLAY/WAYLAND_DISPLAY aren't set (e.g. launched from systemd),
    # set a fallback so EGL initialises correctly.
    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        os.environ.setdefault("DISPLAY", ":0")
        print("WARNING: DISPLAY not set — defaulting to :0")

    # SIGUSR1 = intentional restart requested (stream config changed)
    def _restart_handler(_sig, _frame):
        raise SystemExit(RESTART_EXIT_CODE)

    signal.signal(signal.SIGUSR1, _restart_handler)

    Gst.init(None)

    start_unix_server()
    start_http_server()

    pipeline_str = build_pipeline_string(stream=not no_stream)

    print("Building pipeline …")
    try:
        pipeline = Gst.parse_launch(pipeline_str)
    except GLib.Error as e:
        sys.stderr.write(f"ERROR: Failed to parse pipeline: {e.message}\n")
        sys.exit(1)

    if HAS_CAIRO:
        score_elem = pipeline.get_by_name("score_overlay")
        if score_elem:
            score_elem.connect("draw", on_score_draw)
            print("Scoreboard overlay → score_overlay (cairo)")
        else:
            print("WARNING: score_overlay element not found")

    enc = pipeline.get_by_name("enc_stream")
    if enc:
        encoder_elements.append(enc)
        print("Encoder → enc_stream")

    for name, cam_label in [("pgie0", "CAM0"), ("pgie2", "CAM2")]:
        pgie = pipeline.get_by_name(name)
        if not pgie:
            sys.stderr.write(f"ERROR: Cannot find element '{name}'\n")
            sys.exit(1)
        srcpad = pgie.get_static_pad("src")
        if not srcpad:
            sys.stderr.write(f"ERROR: Cannot get src pad of '{name}'\n")
            sys.exit(1)
        srcpad.add_probe(Gst.PadProbeType.BUFFER, pgie_src_pad_buffer_probe, cam_label)
        print(f"Probe → {name} ({cam_label})")

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    print("Starting pipeline …")
    pipeline.set_state(Gst.State.PLAYING)

    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        print("Stopping pipeline …")
        pipeline.set_state(Gst.State.NULL)
        pipeline.get_state(Gst.CLOCK_TIME_NONE)
        try:
            os.unlink(UNIX_SOCK)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
