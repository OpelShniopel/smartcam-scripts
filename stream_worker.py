#!/usr/bin/env python3
"""
RTMP worker
-----------
Reads the selected internal camera RTSP feed from the main pipeline and forwards
it to RTMP with scoreboard overlay. This worker is intentionally isolated so
RTMP failures do not tear down the local camera/AI service.
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

from camera_config import (
    CAMERA_DEVICE_ALIASES,
    CAMERA_DEVICE_BY_STREAM_CAMERA,
    FIXED_CAMERA,
    PTZ_CAMERA,
)
from exit_codes import ProcessExitCode
from runtime_paths import (
    SCOREBOARD_PNG,
    SCORE_STATE_FILE,
    STREAM_CONF,
    STREAM_WORKER_CONFIG,
    STREAM_WORKER_STATUS,
)
from rtmp_elements import (
    configure_rtmp_branch,
    make_rtmp_elements,
    update_milestone_overlays,
    update_quarter_overlay,
    update_score_clock_overlays,
)
from score_utils import DEFAULT_SCORE_STATE, default_score_state, truncate_team_name

RTMP_BITRATE_DEFAULT = 6800

VERIFY_TIMEOUT_SEC = 15
CONFIG_POLL_SEC = 1
STATE_POLL_SEC = 1
STALL_CHECK_SEC = 1
STALL_TIMEOUT_SEC = 8

# Terminal FPS metrics for the RTMP worker. Disable the global flag to silence
# all worker FPS logs, or disable the RTMP flag to keep future metrics available.
ENABLE_TERMINAL_FPS_METRICS = True
ENABLE_RTMP_FPS_METRICS = True
TERMINAL_FPS_INTERVAL_SEC = 5

_loop: GLib.MainLoop | None = None
_status_lock = threading.Lock()
_status_payload: dict = {
    "worker_alive": True,
    "stream_active": False,
    "last_error": "",
    "active_camera": PTZ_CAMERA,
    "updated_at": time.time(),
}
_worker_state: dict = {
    "stream_status_sent": False,
}
_activity_lock = threading.Lock()
_last_buffer_monotonic: float = 0.0
_fps_lock = threading.Lock()
_rtmp_fps_frames = 0
_stall_triggered = False
_exit_code = 0
_last_score_state: dict | None = None
_last_config: dict | None = None
_enc_stream: Gst.Element | None = None
_selector: Gst.Element | None = None
_selector_pads: dict[str, Gst.Pad] = {}
_current_active_camera: str = PTZ_CAMERA
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
SOURCE_RTSP_FIXED_URL = (
    os.environ.get("STREAM_SOURCE_FIXED_RTSP")
    or os.environ.get("STREAM_SOURCE_CAM0_RTSP")
    or "rtsp://127.0.0.1:8554/camera0_stream"
)
SOURCE_RTSP_PTZ_URL = (
    os.environ.get("STREAM_SOURCE_PTZ_RTSP")
    or os.environ.get("STREAM_SOURCE_CAM2_RTSP")
    or "rtsp://127.0.0.1:8554/camera2_stream"
)
AVAILABLE_CAMERAS = {
    camera: os.path.exists(device)
    for camera, device in CAMERA_DEVICE_BY_STREAM_CAMERA.items()
}
CAMERA_SOURCE_ENV_KEYS = {
    FIXED_CAMERA: ("STREAM_SOURCE_FIXED_RTSP", "STREAM_SOURCE_CAM0_RTSP"),
    PTZ_CAMERA: ("STREAM_SOURCE_PTZ_RTSP", "STREAM_SOURCE_CAM2_RTSP"),
}
CAMERA_SOURCE_URLS = {
    FIXED_CAMERA: SOURCE_RTSP_FIXED_URL,
    PTZ_CAMERA: SOURCE_RTSP_PTZ_URL,
}
CAMERA_SOURCE_SUFFIXES = {
    FIXED_CAMERA: "0",
    PTZ_CAMERA: "2",
}
CAMERA_FALLBACK_ORDER = (PTZ_CAMERA, FIXED_CAMERA)


def _atomic_write_json(path: str, data: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


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
    state = default_score_state()
    state.update(_read_json(SCORE_STATE_FILE, DEFAULT_SCORE_STATE))
    return state


def _camera_input_available(camera: str) -> bool:
    env_keys = CAMERA_SOURCE_ENV_KEYS.get(camera, ())
    return bool(
        AVAILABLE_CAMERAS.get(camera, False)
        or any(os.environ.get(env_key) for env_key in env_keys)
    )


def _available_camera_names() -> list[str]:
    return [
        camera
        for camera in CAMERA_FALLBACK_ORDER
        if _camera_input_available(camera)
    ]


def _normalize_camera(value, available_cameras: set[str] | None = None) -> str:
    text = str(value or PTZ_CAMERA).strip().lower()
    normalized = CAMERA_DEVICE_ALIASES.get(text, PTZ_CAMERA)

    if available_cameras is None:
        available_cameras = set(_available_camera_names())
    if not available_cameras or normalized in available_cameras:
        return normalized

    for camera in CAMERA_FALLBACK_ORDER:
        if camera in available_cameras:
            return camera

    return normalized


def read_worker_config() -> dict:
    cfg = {"bitrateKbps": RTMP_BITRATE_DEFAULT, "activeCamera": PTZ_CAMERA}
    cfg.update(_read_json(STREAM_WORKER_CONFIG, cfg))
    cfg["activeCamera"] = _normalize_camera(cfg.get("activeCamera", PTZ_CAMERA))
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


def _link_many(*elements: Gst.Element) -> None:
    for src, dst in zip(elements, elements[1:]):
        _link(src, dst)


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


def _update_overlay(state: dict) -> None:
    global _last_score_state
    _last_score_state = dict(state)
    els = dict(_osd_elements)
    if not els:
        return

    visible = state.get("visible", False)
    quarter = els.get("osd_quarter")
    home = els.get("osd_home")
    away = els.get("osd_away")
    home_score = els.get("osd_home_score")
    away_score = els.get("osd_away_score")
    clock = els.get("osd_clock")
    fouls = els.get("osd_fouls")
    bg = els.get("osd_bg")
    milestone_player = els.get("osd_milestone_player")
    milestone_text = els.get("osd_milestone_text")

    update_quarter_overlay(quarter, visible, state)
    if home:
        home.set_property("silent", not visible)
        if visible:
            home.set_property(
                "text",
                truncate_team_name(
                    "home_name",
                    state.get("home_name", "HOME"),
                    log_prefix="[worker]",
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
                    log_prefix="[worker]",
                ),
            )
    update_score_clock_overlays(home_score, away_score, clock, visible, state)
    if fouls:
        fouls.set_property("silent", not visible)
        if visible:
            fouls.set_property(
                "text",
                f"F:{state.get('home_fouls', 0)} T:{state.get('home_timeouts', 3)}"
                f"          "
                f"F:{state.get('away_fouls', 0)} T:{state.get('away_timeouts', 3)}",
            )
    if bg:
        bg.set_property("alpha", 1.0 if visible else 0.0)
    update_milestone_overlays(milestone_player, milestone_text, state)


def _poll_score_state() -> bool:
    state = read_score_state()
    if state != _last_score_state:
        _update_overlay(state)
    return True


def _switch_active_camera(active_camera: str) -> None:
    global _current_active_camera
    normalized = _normalize_camera(active_camera, set(_selector_pads))
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
        _switch_active_camera(cfg.get("activeCamera", PTZ_CAMERA))
    return True


def _mark_buffer_activity() -> None:
    global _last_buffer_monotonic
    with _activity_lock:
        _last_buffer_monotonic = time.monotonic()


def _get_last_buffer_activity() -> float:
    with _activity_lock:
        return _last_buffer_monotonic


def _count_rtmp_fps_frame() -> None:
    global _rtmp_fps_frames
    if not (ENABLE_TERMINAL_FPS_METRICS and ENABLE_RTMP_FPS_METRICS):
        return
    with _fps_lock:
        _rtmp_fps_frames += 1


def _activity_probe(_pad, _info, _user_data):
    _mark_buffer_activity()
    _count_rtmp_fps_frame()
    return Gst.PadProbeReturn.OK


def _rtmp_fps_report() -> bool:
    global _rtmp_fps_frames
    if not (ENABLE_TERMINAL_FPS_METRICS and ENABLE_RTMP_FPS_METRICS):
        return True

    with _fps_lock:
        frames = _rtmp_fps_frames
        _rtmp_fps_frames = 0

    if not _worker_state["stream_status_sent"] or frames <= 0:
        return True

    fps = frames / TERMINAL_FPS_INTERVAL_SEC
    print(f"[fps] RTMP {_current_active_camera}: {fps:.1f} fps")
    return True


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
    _exit_code = int(ProcessExitCode.STREAM_ERROR)
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
        _exit_code = int(ProcessExitCode.STREAM_ERROR)
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


def _build_rtsp_input_branch(pipeline: Gst.Pipeline, suffix: str, url: str) -> Gst.Element:
    src = _make("rtspsrc", f"src{suffix}_rtsp")
    depay = _make("rtph264depay", f"src{suffix}_depay")
    parse = _make("h264parse", f"src{suffix}_parse")
    dec = _make("nvv4l2decoder", f"src{suffix}_dec")
    conv = _make("nvvideoconvert", f"src{suffix}_conv")
    caps = _make("capsfilter", f"src{suffix}_caps_i420")
    q = _make("queue", f"src{suffix}_queue")

    src.set_property("location", url)
    src.set_property("protocols", 4)
    src.set_property("latency", 100)

    conv.set_property("gpu-id", 0)
    conv.set_property("copy-hw", 2)
    caps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=I420"))

    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    q.set_property("leaky", 2)

    for el in (src, depay, parse, dec, conv, caps, q):
        pipeline.add(el)

    src.connect("pad-added", _on_rtsp_pad_added, depay)

    _link(depay, parse)
    _link(parse, dec)
    _link(dec, conv)
    _link(conv, caps)
    _link(caps, q)

    return q


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
    global _enc_stream, _selector, _current_active_camera, _last_config

    rtmp_url = read_stream_url()
    if not rtmp_url:
        raise RuntimeError("stream.conf does not contain an RTMP URL")
    if not os.path.exists(SCOREBOARD_PNG):
        raise RuntimeError(f"Missing scoreboard PNG: {SCOREBOARD_PNG}")

    cfg = read_worker_config()

    pipeline = Gst.Pipeline.new("stream-worker")
    if pipeline is None:
        raise RuntimeError("Unable to create stream-worker pipeline")

    input_branches: dict[str, Gst.Element] = {}
    for camera in (FIXED_CAMERA, PTZ_CAMERA):
        if not _camera_input_available(camera):
            env_keys = "/".join(CAMERA_SOURCE_ENV_KEYS[camera])
            print(
                f"[worker] skipping {camera} RTSP source: "
                f"device unavailable and {env_keys} not set"
            )
            continue
        input_branches[camera] = _build_rtsp_input_branch(
            pipeline,
            CAMERA_SOURCE_SUFFIXES[camera],
            CAMERA_SOURCE_URLS[camera],
        )

    if not input_branches:
        raise RuntimeError(
            "No available RTSP input sources; expected configured camera devices "
            "or explicit STREAM_SOURCE_FIXED_RTSP/STREAM_SOURCE_PTZ_RTSP override"
        )

    selector = _make("input-selector", "strm_selector")
    q = _make("queue", "strm_queue")
    rtmp = make_rtmp_elements(_make)
    watchdog = Gst.ElementFactory.make("watchdog", "strm_watchdog")
    configure_rtmp_branch(
        rtmp,
        q,
        int(cfg.get("bitrateKbps", RTMP_BITRATE_DEFAULT)),
        rtmp_url,
    )
    if watchdog is not None:
        watchdog.set_property("timeout", int(STALL_TIMEOUT_SEC * 1000))
    else:
        print("[worker] WARNING: watchdog plugin unavailable; using custom stall detector only")

    elements = [selector, q, *rtmp.base_elements()]
    if watchdog is not None:
        elements.insert(-3, watchdog)

    for el in elements:
        pipeline.add(el)

    _selector_pads.clear()
    for camera, branch in input_branches.items():
        _link_branch_to_selector(branch, selector, camera)

    _link(selector, q)
    _link_many(q, *rtmp.overlay_chain())
    if watchdog is not None:
        _link(rtmp.parse, watchdog)
        _link(watchdog, rtmp.flvmux)
    else:
        _link(rtmp.parse, rtmp.flvmux)

    _link_filtered(rtmp.audiosrc, rtmp.aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src = _get_static_pad(rtmp.aacenc, "src")
    flvmux_audio = rtmp.flvmux.request_pad_simple("audio")
    if flvmux_audio is None:
        raise RuntimeError("Unable to request flvmux audio pad")
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        raise RuntimeError("Failed to link audio encoder to flvmux audio pad")

    _link(rtmp.flvmux, rtmp.rtmpsink)

    activity_pad_owner = watchdog if watchdog is not None else rtmp.parse
    activity_pad = activity_pad_owner.get_static_pad("src")
    if activity_pad is not None:
        activity_pad.add_probe(Gst.PadProbeType.BUFFER, _activity_probe, None)

    sink_pad = rtmp.rtmpsink.get_static_pad("sink")
    if sink_pad is not None:
        sink_pad.add_probe(Gst.PadProbeType.BUFFER, _rtmp_probe, None)

    _osd_elements.clear()
    _osd_elements.update(rtmp.osd_map())

    _enc_stream = rtmp.enc
    _selector = selector
    _current_active_camera = _normalize_camera(cfg.get("activeCamera", PTZ_CAMERA), set(_selector_pads))
    _switch_active_camera(_current_active_camera)
    _update_overlay(read_score_state())
    _last_config = read_worker_config()
    _set_status(active_camera=_current_active_camera)
    return pipeline, rtmp.rtmpsink


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
        _exit_code = int(ProcessExitCode.STREAM_ERROR)
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
    if ENABLE_TERMINAL_FPS_METRICS and ENABLE_RTMP_FPS_METRICS:
        GLib.timeout_add_seconds(TERMINAL_FPS_INTERVAL_SEC, _rtmp_fps_report)

    def _signal_handler(_sig, _frame):
        if _loop is not None:
            _loop.quit()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _set_status(worker_alive=True, stream_active=False, last_error="")
    print(f"[worker] source fixed: {SOURCE_RTSP_FIXED_URL}")
    print(f"[worker] source ptz:   {SOURCE_RTSP_PTZ_URL}")
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
        raise SystemExit(int(ProcessExitCode.STREAM_ERROR))


if __name__ == "__main__":
    main()
