#!/usr/bin/env python3
"""
Phase 1 RTMP worker v2
-------------------
Reads the internal CAM2 RTSP feed from the main pipeline and forwards it to
RTMP with scoreboard overlay. This worker is intentionally isolated so RTMP
failures do not tear down the local camera/AI service.
"""

from __future__ import annotations

import json
import os
import signal
import socket as _socket
import threading
import time

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STREAM_CONF = os.path.join(SCRIPT_DIR, "stream.conf")
SCORE_STATE_FILE = os.path.join(SCRIPT_DIR, "score_state.json")
STREAM_WORKER_CONFIG = os.path.join(SCRIPT_DIR, "stream_worker_config.json")
STREAM_WORKER_STATUS = os.path.join(SCRIPT_DIR, "stream_worker_status.json")

RTMP_BITRATE_DEFAULT = 6800
RTMP_KEYINT = 60
RTMP_THREADS = 2
RTMP_PRESET = "ultrafast"
RTMP_TUNE = "zerolatency"

SCOREBOARD_PNG = os.path.join(SCRIPT_DIR, "scoreboard.png")
SCOREBOARD_W = 410
SCOREBOARD_H = 129
SCOREBOARD_OFFSET_X = 755
SCOREBOARD_OFFSET_Y = 931

VERIFY_TIMEOUT_SEC = 15
CONFIG_POLL_SEC = 1
STATE_POLL_SEC = 1
STALL_CHECK_SEC = 1
STALL_TIMEOUT_SEC = 8

STREAM_ERROR_EXIT_CODE = 43

_loop: GLib.MainLoop | None = None
_status_lock = threading.Lock()
_status_payload: dict = {
    "worker_alive": True,
    "stream_active": False,
    "last_error": "",
    "active_camera": "cam2",
    "updated_at": time.time(),
}
_worker_state: dict = {
    "stream_status_sent": False,
}
_activity_lock = threading.Lock()
_last_buffer_monotonic: float = 0.0
_stall_triggered = False
_exit_code = 0
_last_score_state: dict | None = None
_last_config: dict | None = None
_enc_stream: Gst.Element | None = None
_selector: Gst.Element | None = None
_selector_pads: dict[str, Gst.Pad] = {}
_current_active_camera: str = "cam2"
_osd_elements: dict[str, Gst.Element] = {}


def _get_local_ip() -> str:
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "127.0.0.1"


LOCAL_HOST = os.environ.get("JETSON_HOST") or _get_local_ip()
SOURCE_RTSP_CAM0_URL = os.environ.get("STREAM_SOURCE_CAM0_RTSP") or "rtsp://127.0.0.1:8554/camera0_stream"
SOURCE_RTSP_CAM2_URL = os.environ.get("STREAM_SOURCE_CAM2_RTSP") or "rtsp://127.0.0.1:8554/camera2_stream"


def _atomic_write_json(path: str, data: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


DEFAULT_SCORE_STATE = {
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


def _read_json(path: str, default: dict) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default.copy()


def read_stream_url() -> str | None:
    try:
        with open(STREAM_CONF) as f:
            url = f.read().strip()
    except FileNotFoundError:
        return None
    return None if (not url or url.startswith("#")) else url


def read_score_state() -> dict:
    state = DEFAULT_SCORE_STATE.copy()
    state.update(_read_json(SCORE_STATE_FILE, DEFAULT_SCORE_STATE))
    return state


def _normalize_camera(value) -> str:
    text = str(value or "cam2").strip().lower()
    mapping = {
        "0": "cam0",
        "cam0": "cam0",
        "camera0": "cam0",
        "/dev/video0": "cam0",
        "2": "cam2",
        "cam2": "cam2",
        "camera2": "cam2",
        "/dev/video2": "cam2",
    }
    normalized = mapping.get(text, "cam2")

    cam0_exists = os.path.exists("/dev/video0")
    cam2_exists = os.path.exists("/dev/video2")

    if normalized == "cam2" and not cam2_exists and cam0_exists:
        return "cam0"
    if normalized == "cam0" and not cam0_exists and cam2_exists:
        return "cam2"

    return normalized


def read_worker_config() -> dict:
    cfg = {"bitrateKbps": RTMP_BITRATE_DEFAULT, "activeCamera": "cam2"}
    cfg.update(_read_json(STREAM_WORKER_CONFIG, cfg))
    cfg["activeCamera"] = _normalize_camera(cfg.get("activeCamera", "cam2"))
    return cfg


def _set_status(**updates) -> None:
    with _status_lock:
        _status_payload.update(updates)
        _status_payload["updated_at"] = time.time()
        snapshot = dict(_status_payload)
    _atomic_write_json(STREAM_WORKER_STATUS, snapshot)


def _make(factory: str, name: str) -> Gst.Element:
    el = Gst.ElementFactory.make(factory, name)
    if not el:
        raise RuntimeError(f"Unable to create element {factory!r} ({name!r})")
    return el


def _link(src: Gst.Element, dst: Gst.Element) -> None:
    if not src.link(dst):
        raise RuntimeError(f"Failed to link {src.get_name()} -> {dst.get_name()}")


def _link_filtered(src: Gst.Element, dst: Gst.Element, caps_str: str) -> None:
    caps = Gst.Caps.from_string(caps_str)
    if not src.link_filtered(dst, caps):
        raise RuntimeError(
            f"Failed to link filtered {src.get_name()} -> {dst.get_name()} with caps {caps_str}"
        )


def _get_static_pad(el: Gst.Element, pad_name: str) -> Gst.Pad:
    pad = el.get_static_pad(pad_name)
    if not pad:
        raise RuntimeError(f"Unable to get pad {pad_name!r} from {el.get_name()}")
    return pad


def _setup_text(el: Gst.Element, text: str, xpos: float, ypos: float,
                font: str = "Sans Bold 20", color: int = 0xFFFFFFFF,
                shadow: bool = True) -> None:
    el.set_property("text", text)
    el.set_property("font-desc", font)
    el.set_property("halignment", 4)
    el.set_property("valignment", 3)
    el.set_property("xpos", xpos)
    el.set_property("ypos", ypos)
    el.set_property("color", color)
    el.set_property("draw-shadow", shadow)
    el.set_property("auto-resize", False)
    el.set_property("wait-text", False)
    el.set_property("silent", True)


def _update_overlay(state: dict) -> None:
    global _last_score_state
    _last_score_state = dict(state)
    els = dict(_osd_elements)
    if not els:
        return

    visible = state.get("visible", False)
    home = els.get("osd_home")
    away = els.get("osd_away")
    score = els.get("osd_score")
    clock = els.get("osd_clock")
    fouls = els.get("osd_fouls")
    bg = els.get("osd_bg")

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
            score.set_property("text", f"{state['home_points']} - {state['away_points']}")
    if clock:
        clock.set_property("silent", not visible)
        if visible:
            clock.set_property("text", f"Q{state['quarter']}  {state['clock']}")
    if fouls:
        fouls.set_property("silent", not visible)
        if visible:
            fouls.set_property(
                "text",
                f"F:{state['home_fouls']} T:{state['home_timeouts']}"
                f"          "
                f"F:{state['away_fouls']} T:{state['away_timeouts']}",
            )
    if bg:
        bg.set_property("alpha", 1.0 if visible else 0.0)


def _poll_score_state() -> bool:
    state = read_score_state()
    if state != _last_score_state:
        _update_overlay(state)
    return True


def _switch_active_camera(active_camera: str) -> None:
    global _current_active_camera
    normalized = _normalize_camera(active_camera)
    pad = _selector_pads.get(normalized)
    if _selector is None or pad is None:
        return
    current = _selector.get_property("active-pad")
    if current == pad and _current_active_camera == normalized:
        return
    _selector.set_property("active-pad", pad)
    _current_active_camera = normalized
    print(f"[worker] switched stream source -> {normalized}")
    _set_status(active_camera=normalized)


def _poll_worker_config() -> bool:
    global _last_config
    cfg = read_worker_config()
    if cfg != _last_config:
        _last_config = dict(cfg)
        bitrate = cfg.get("bitrateKbps", RTMP_BITRATE_DEFAULT)
        if _enc_stream is not None:
            _enc_stream.set_property("bitrate", int(bitrate))
            print(f"[worker] updated stream bitrate -> {bitrate} kbps")
        _switch_active_camera(cfg.get("activeCamera", "cam2"))
    return True


def _mark_buffer_activity() -> None:
    global _last_buffer_monotonic
    with _activity_lock:
        _last_buffer_monotonic = time.monotonic()


def _get_last_buffer_activity() -> float:
    with _activity_lock:
        return _last_buffer_monotonic


def _activity_probe(_pad, _info, _user_data):
    _mark_buffer_activity()
    return Gst.PadProbeReturn.OK


def _stall_check() -> bool:
    global _stall_triggered, _exit_code
    if _loop is None:
        return False
    if not _worker_state["stream_status_sent"]:
        return True
    last = _get_last_buffer_activity()
    if not last:
        return True
    idle_for = time.monotonic() - last
    if idle_for < STALL_TIMEOUT_SEC:
        return True
    if _stall_triggered:
        return False

    _stall_triggered = True
    msg = f"RTMP stalled — no outbound buffers for {idle_for:.1f}s"
    print(f"[worker] {msg}")
    _set_status(stream_active=False, last_error=msg, active_camera=_current_active_camera)
    _exit_code = STREAM_ERROR_EXIT_CODE
    _loop.quit()
    return False


def _notify_stream_active() -> Gst.PadProbeReturn:
    if not _worker_state["stream_status_sent"]:
        _worker_state["stream_status_sent"] = True
        _mark_buffer_activity()
        print("[worker] RTMP flow verified active")
        _set_status(stream_active=True, last_error="", active_camera=_current_active_camera)
    return Gst.PadProbeReturn.REMOVE


def _rtmp_probe(_pad, _info, _user_data):
    return _notify_stream_active()


def _verify_timeout() -> bool:
    global _exit_code
    if not _worker_state["stream_status_sent"]:
        msg = f"RTMP connection timed out after {VERIFY_TIMEOUT_SEC}s"
        print(f"[worker] {msg}")
        _set_status(stream_active=False, last_error=msg, active_camera=_current_active_camera)
        _exit_code = STREAM_ERROR_EXIT_CODE
        if _loop is not None:
            _loop.quit()
    return False


def _on_rtsp_pad_added(src: Gst.Element, pad: Gst.Pad, depay: Gst.Element) -> None:
    sink_pad = depay.get_static_pad("sink")
    if sink_pad is None or sink_pad.is_linked():
        return
    caps = pad.get_current_caps() or pad.query_caps(None)
    caps_str = caps.to_string() if caps else ""
    if "application/x-rtp" not in caps_str:
        return
    if pad.link(sink_pad) != Gst.PadLinkReturn.OK:
        raise RuntimeError(f"Failed to link {src.get_name()} to {depay.get_name()}")


def _link_branch_to_selector(last_el: Gst.Element, selector: Gst.Element, cam_name: str) -> Gst.Pad:
    src_pad = _get_static_pad(last_el, "src")
    sel_pad = selector.request_pad_simple("sink_%u")
    if sel_pad is None:
        raise RuntimeError(f"Unable to request selector sink pad for {cam_name}")
    if src_pad.link(sel_pad) != Gst.PadLinkReturn.OK:
        raise RuntimeError(f"Failed to link {last_el.get_name()} -> selector for {cam_name}")
    _selector_pads[cam_name] = sel_pad
    return sel_pad


def build_pipeline() -> tuple[Gst.Pipeline, Gst.Element]:
    global _enc_stream, _selector, _current_active_camera

    rtmp_url = read_stream_url()
    if not rtmp_url:
        raise RuntimeError("stream.conf does not contain an RTMP URL")
    if not os.path.exists(SCOREBOARD_PNG):
        raise RuntimeError(f"Missing scoreboard PNG: {SCOREBOARD_PNG}")

    cfg = read_worker_config()

    pipeline = Gst.Pipeline.new("stream-worker")
    if pipeline is None:
        raise RuntimeError("Unable to create stream-worker pipeline")

    # CAM0 branch
    src0 = _make("rtspsrc", "src0_rtsp")
    depay0 = _make("rtph264depay", "src0_depay")
    parse0 = _make("h264parse", "src0_parse")
    dec0 = _make("nvv4l2decoder", "src0_dec")
    conv0 = _make("nvvideoconvert", "src0_conv")
    caps0 = _make("capsfilter", "src0_caps_i420")
    q0 = _make("queue", "src0_queue")

    # CAM2 branch
    src2 = _make("rtspsrc", "src2_rtsp")
    depay2 = _make("rtph264depay", "src2_depay")
    parse2 = _make("h264parse", "src2_parse")
    dec2 = _make("nvv4l2decoder", "src2_dec")
    conv2 = _make("nvvideoconvert", "src2_conv")
    caps2 = _make("capsfilter", "src2_caps_i420")
    q2 = _make("queue", "src2_queue")

    selector = _make("input-selector", "strm_selector")
    q = _make("queue", "strm_queue")
    osd_bg = _make("gdkpixbufoverlay", "strm_osd_bg")
    osd_home = _make("textoverlay", "strm_osd_home")
    osd_away = _make("textoverlay", "strm_osd_away")
    osd_score = _make("textoverlay", "strm_osd_score")
    osd_clock = _make("textoverlay", "strm_osd_clock")
    osd_fouls = _make("textoverlay", "strm_osd_fouls")
    enc = _make("x264enc", "strm_enc")
    parse_out = _make("h264parse", "strm_parse")
    flvmux = _make("flvmux", "strm_flvmux")
    watchdog = Gst.ElementFactory.make("watchdog", "strm_watchdog")
    rtmpsink = _make("rtmpsink", "strm_rtmpsink")
    audiosrc = _make("audiotestsrc", "strm_audiosrc")
    aacenc = _make("voaacenc", "strm_aacenc")

    src0.set_property("location", SOURCE_RTSP_CAM0_URL)
    src0.set_property("protocols", 4)
    src0.set_property("latency", 100)
    src2.set_property("location", SOURCE_RTSP_CAM2_URL)
    src2.set_property("protocols", 4)
    src2.set_property("latency", 100)

    for conv in (conv0, conv2):
        conv.set_property("gpu-id", 0)
        conv.set_property("copy-hw", 2)
    caps0.set_property("caps", Gst.Caps.from_string("video/x-raw,format=I420"))
    caps2.set_property("caps", Gst.Caps.from_string("video/x-raw,format=I420"))

    for q_in in (q0, q2):
        q_in.set_property("max-size-buffers", 2)
        q_in.set_property("max-size-bytes", 0)
        q_in.set_property("max-size-time", 0)
        q_in.set_property("leaky", 2)

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)

    osd_bg.set_property("location", SCOREBOARD_PNG)
    osd_bg.set_property("offset-x", SCOREBOARD_OFFSET_X)
    osd_bg.set_property("offset-y", SCOREBOARD_OFFSET_Y)
    osd_bg.set_property("overlay-width", SCOREBOARD_W)
    osd_bg.set_property("overlay-height", SCOREBOARD_H)
    osd_bg.set_property("alpha", 0.0)

    _setup_text(osd_home, "HOME", xpos=0.022, ypos=0.040, font="Sans Bold 22")
    _setup_text(osd_away, "AWAY", xpos=0.230, ypos=0.040, font="Sans Bold 22")
    _setup_text(osd_score, "0 - 0", xpos=0.120, ypos=0.040, font="Sans Bold 22", color=0xFFD916FF)
    _setup_text(osd_clock, "Q1 10:00", xpos=0.330, ypos=0.040, font="Sans Bold 22", color=0xB2E5FFFF)
    _setup_text(osd_fouls, "", xpos=0.022, ypos=0.068, font="Sans 13", color=0xA6A6A6FF)

    enc.set_property("pass", "cbr")
    enc.set_property("bitrate", int(cfg.get("bitrateKbps", RTMP_BITRATE_DEFAULT)))
    enc.set_property("vbv-buf-capacity", 200)
    enc.set_property("tune", RTMP_TUNE)
    enc.set_property("speed-preset", RTMP_PRESET)
    enc.set_property("key-int-max", RTMP_KEYINT)
    enc.set_property("threads", RTMP_THREADS)

    flvmux.set_property("streamable", True)
    if watchdog is not None:
        watchdog.set_property("timeout", int(STALL_TIMEOUT_SEC * 1000))
    else:
        print("[worker] WARNING: watchdog plugin unavailable; using custom stall detector only")
    rtmpsink.set_property("location", rtmp_url)
    rtmpsink.set_property("async", False)
    audiosrc.set_property("wave", 4)
    aacenc.set_property("bitrate", 128000)

    elements = [
        src0, depay0, parse0, dec0, conv0, caps0, q0,
        src2, depay2, parse2, dec2, conv2, caps2, q2,
        selector, q,
        osd_bg, osd_home, osd_away, osd_score, osd_clock, osd_fouls,
        enc, parse_out, flvmux, rtmpsink, audiosrc, aacenc,
    ]
    if watchdog is not None:
        elements.insert(-3, watchdog)

    for el in elements:
        pipeline.add(el)

    src0.connect("pad-added", _on_rtsp_pad_added, depay0)
    src2.connect("pad-added", _on_rtsp_pad_added, depay2)

    _link(depay0, parse0)
    _link(parse0, dec0)
    _link(dec0, conv0)
    _link(conv0, caps0)
    _link(caps0, q0)

    _link(depay2, parse2)
    _link(parse2, dec2)
    _link(dec2, conv2)
    _link(conv2, caps2)
    _link(caps2, q2)

    _selector_pads.clear()
    _link_branch_to_selector(q0, selector, "cam0")
    _link_branch_to_selector(q2, selector, "cam2")

    _link(selector, q)
    _link(q, osd_bg)
    _link(osd_bg, osd_home)
    _link(osd_home, osd_away)
    _link(osd_away, osd_score)
    _link(osd_score, osd_clock)
    _link(osd_clock, osd_fouls)
    _link(osd_fouls, enc)
    _link(enc, parse_out)
    if watchdog is not None:
        _link(parse_out, watchdog)
        _link(watchdog, flvmux)
    else:
        _link(parse_out, flvmux)

    _link_filtered(audiosrc, aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src = _get_static_pad(aacenc, "src")
    flvmux_audio = flvmux.request_pad_simple("audio")
    if flvmux_audio is None:
        raise RuntimeError("Unable to request flvmux audio pad")
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        raise RuntimeError("Failed to link audio encoder to flvmux audio pad")

    _link(flvmux, rtmpsink)

    activity_pad_owner = watchdog if watchdog is not None else parse_out
    activity_pad = activity_pad_owner.get_static_pad("src")
    if activity_pad is not None:
        activity_pad.add_probe(Gst.PadProbeType.BUFFER, _activity_probe, None)

    sink_pad = rtmpsink.get_static_pad("sink")
    if sink_pad is not None:
        sink_pad.add_probe(Gst.PadProbeType.BUFFER, _rtmp_probe, None)

    _osd_elements.clear()
    _osd_elements.update({
        "osd_bg": osd_bg,
        "osd_home": osd_home,
        "osd_away": osd_away,
        "osd_score": osd_score,
        "osd_clock": osd_clock,
        "osd_fouls": osd_fouls,
    })

    _enc_stream = enc
    _selector = selector
    _current_active_camera = _normalize_camera(cfg.get("activeCamera", "cam2"))
    _switch_active_camera(_current_active_camera)
    _update_overlay(read_score_state())
    _last_config = read_worker_config()
    _set_status(active_camera=_current_active_camera)
    return pipeline, rtmpsink


def bus_call(_bus, message, loop: GLib.MainLoop):
    global _exit_code
    t = message.type
    if t == Gst.MessageType.EOS:
        print("[worker] EOS")
        loop.quit()
    elif t == Gst.MessageType.WARNING:
        err, dbg = message.parse_warning()
        print(f"[worker] WARNING: {err}: {dbg}")
    elif t == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        src_name = message.src.get_name() if message.src else "unknown"
        msg = f"GStreamer error in {src_name}: {err}"
        print(f"[worker] ERROR: {err}: {dbg} (src={src_name})")
        _set_status(stream_active=False, last_error=msg, active_camera=_current_active_camera)
        _exit_code = STREAM_ERROR_EXIT_CODE
        loop.quit()
    return True


def main() -> None:
    global _loop

    rtmp_url = read_stream_url()
    if not rtmp_url:
        raise SystemExit("stream.conf does not contain an RTMP URL")

    Gst.init(None)

    global _stall_triggered, _exit_code
    _stall_triggered = False
    _exit_code = 0
    _mark_buffer_activity()

    try:
        pipeline, _rtmpsink = build_pipeline()
    except Exception as e:
        _set_status(stream_active=False, last_error=str(e))
        raise

    loop = GLib.MainLoop()
    _loop = loop

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    GLib.timeout_add_seconds(VERIFY_TIMEOUT_SEC, _verify_timeout)
    GLib.timeout_add_seconds(STALL_CHECK_SEC, _stall_check)
    GLib.timeout_add_seconds(STATE_POLL_SEC, _poll_score_state)
    GLib.timeout_add_seconds(CONFIG_POLL_SEC, _poll_worker_config)

    def _signal_handler(_sig, _frame):
        if _loop is not None:
            _loop.quit()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _set_status(worker_alive=True, stream_active=False, last_error="")
    print(f"[worker] source cam0: {SOURCE_RTSP_CAM0_URL}")
    print(f"[worker] source cam2: {SOURCE_RTSP_CAM2_URL}")
    print(f"[worker] active source: {_current_active_camera}")
    print(f"[worker] sink:   {rtmp_url[:80]}")
    print("[worker] starting pipeline ...")
    pipeline.set_state(Gst.State.PLAYING)

    try:
        loop.run()
    finally:
        print("[worker] stopping pipeline ...")
        pipeline.set_state(Gst.State.NULL)
        pipeline.get_state(Gst.CLOCK_TIME_NONE)
        _set_status(worker_alive=False, stream_active=False, last_error=_status_payload.get("last_error", ""))

    if _exit_code:
        raise SystemExit(_exit_code)
    if not _worker_state["stream_status_sent"]:
        raise SystemExit(STREAM_ERROR_EXIT_CODE)


if __name__ == "__main__":
    main()
