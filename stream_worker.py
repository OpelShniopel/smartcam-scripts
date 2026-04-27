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
import math
import os
import signal
import socket as _socket
import threading
import time
from collections.abc import Callable
from typing import Any

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstVideo", "1.0")
from gi.repository import GLib, Gst, GstVideo

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
    _milestone_show_until,
    RtmpElements,
    configure_rtmp_branch,
    foul_png_path,
    make_rtmp_elements,
    populate_timeout_texts,
    TIMEOUT_TEXT_KEYS,
    update_blitzball_end_stats,
    update_blitzball_overlay,
    update_milestone_overlays,
    update_quarter_overlay,
    update_score_clock_overlays,
)
from score_utils import DEFAULT_SCORE_STATE, default_score_state, truncate_team_name

RTMP_BITRATE_DEFAULT = 6800

VERIFY_TIMEOUT_SEC = 15
CONFIG_POLL_SEC = 0.2
STATE_POLL_SEC = 0.2
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
_milestone_display_until: int = 0
_last_milestone_show_until: float = 0.0
_milestone_alpha: float = 0.0
_milestone_fading_in: bool = False
_milestone_fading_out: bool = False
_milestone_fade_active: bool = False
_timeout_alpha: float = 0.0
_timeout_fade_in: bool = False
_timeout_fade_out: bool = False
_timeout_fade_active: bool = False
_pre_timeout_bg_alpha: float = 0.0
_pre_timeout_home_foul_alpha: float = 0.0
_pre_timeout_away_foul_alpha: float = 0.0
_sb_timeout_alpha: float = 1.0   # scoreboard alpha during timeout transition
_timeout_pause_ticks: int = 0    # countdown ticks for pause between phases
TIMEOUT_TRANSITION_PAUSE_TICKS = 20  # 20 × 100 ms = 2 s

_blitz_pulse_active: bool = False
_blitz_pulse_alpha: float = 0.6
_blitz_pulse_phase: float = 0.0

_end_stats_show_until: int = 0

_SCOREBOARD_TEXT_KEYS: tuple[str, ...] = (
    "osd_quarter", "osd_home", "osd_away",
    "osd_home_score", "osd_away_score", "osd_clock",
    "osd_milestone_player", "osd_milestone_text",
)
_SCOREBOARD_PIXEL_KEYS: tuple[str, ...] = (
    "osd_bg", "osd_home_fouls_bar", "osd_away_fouls_bar",
)
# Scoreboard text keys that participate in the timeout cross-fade (milestone has its own fade).
_SCOREBOARD_CROSS_FADE_KEYS: tuple[str, ...] = (
    "osd_quarter", "osd_home", "osd_away",
    "osd_home_score", "osd_away_score", "osd_clock",
)

_last_config: dict | None = None
_enc_stream: Gst.Element | None = None
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
SOURCE_RTSP_PROGRAM_URL = (
        os.environ.get("STREAM_SOURCE_PROGRAM_RTSP")
        or "rtsp://127.0.0.1:8554/program_clean"
)
AVAILABLE_CAMERAS = {
    camera: os.path.exists(device)
    for camera, device in CAMERA_DEVICE_BY_STREAM_CAMERA.items()
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
    return bool(AVAILABLE_CAMERAS.get(camera, False))


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


def _force_key_unit(enc: Gst.Element | None, label: str) -> None:
    if enc is None:
        return
    sink_pad = enc.get_static_pad("sink")
    if sink_pad is None:
        return
    event = GstVideo.video_event_new_downstream_force_key_unit(
        Gst.CLOCK_TIME_NONE,
        Gst.CLOCK_TIME_NONE,
        Gst.CLOCK_TIME_NONE,
        True,
        0,
    )
    if sink_pad.send_event(event):
        print(f"[worker] forced keyframe -> {label}")


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


def _blitz_pulse_step() -> bool:
    global _blitz_pulse_active, _blitz_pulse_alpha, _blitz_pulse_phase

    if not _blitz_pulse_active:
        return False

    state = _last_score_state
    if not (state and state.get("blitz_active", False) and state.get("sport_code") == "BLITZBALL"):
        _blitz_pulse_active = False
        els = dict(_osd_elements)
        el = els.get("osd_blitz_active")
        if el:
            el.set_property("alpha", 0.0)
        return False

    _blitz_pulse_phase += 0.15
    alpha = 0.5 + 0.4 * math.sin(_blitz_pulse_phase)
    _blitz_pulse_alpha = alpha
    els = dict(_osd_elements)
    el = els.get("osd_blitz_active")
    if el:
        el.set_property("alpha", alpha)
    return True


def _timeout_fade_step() -> bool:
    global _timeout_alpha, _timeout_fade_in, _timeout_fade_out, _timeout_fade_active
    global _sb_timeout_alpha, _timeout_pause_ticks

    els = dict(_osd_elements)
    timeout_bg = els.get("osd_timeout_bg")
    visible = _last_score_state.get("visible", False) if _last_score_state else False

    if _timeout_fade_in:
        # Phase 1 — scoreboard fades out
        if _sb_timeout_alpha > 0.0:
            _sb_timeout_alpha = max(0.0, _sb_timeout_alpha - 0.05)
            bg_el = els.get("osd_bg")
            if bg_el:
                bg_el.set_property("alpha", _pre_timeout_bg_alpha * _sb_timeout_alpha)
            home_foul_el = els.get("osd_home_fouls_bar")
            if home_foul_el:
                home_foul_el.set_property("alpha", _pre_timeout_home_foul_alpha * _sb_timeout_alpha)
            away_foul_el = els.get("osd_away_fouls_bar")
            if away_foul_el:
                away_foul_el.set_property("alpha", _pre_timeout_away_foul_alpha * _sb_timeout_alpha)
            if visible:
                sb_a = int(_sb_timeout_alpha * 255)
                sb_fg = (sb_a << 24) | 0x00FFFFFF
                sb_outline = (sb_a << 24) | 0x00000000
                for key in _SCOREBOARD_CROSS_FADE_KEYS:
                    el = els.get(key)
                    if el:
                        el.set_property("color", sb_fg)
                        el.set_property("outline-color", sb_outline)
            if _sb_timeout_alpha <= 0.0:
                if visible:
                    for key in _SCOREBOARD_CROSS_FADE_KEYS:
                        el = els.get(key)
                        if el:
                            el.set_property("silent", True)
                _timeout_pause_ticks = TIMEOUT_TRANSITION_PAUSE_TICKS

        # Phase 2 — pause between scoreboard out and timeout in
        elif _timeout_pause_ticks > 0:
            _timeout_pause_ticks -= 1

        # Phase 3 — timeout fades in
        else:
            _timeout_alpha = min(1.0, _timeout_alpha + 0.05)
            if timeout_bg:
                timeout_bg.set_property("alpha", _timeout_alpha)
            t_a = int(_timeout_alpha * 255)
            t_fg = (t_a << 24) | 0x00FFFFFF
            t_outline = (t_a << 24) | 0x00000000
            for key in TIMEOUT_TEXT_KEYS:
                el = els.get(key)
                if el:
                    el.set_property("color", t_fg)
                    el.set_property("outline-color", t_outline)
            if _timeout_alpha >= 1.0:
                _timeout_fade_in = False
                for key in TIMEOUT_TEXT_KEYS:
                    el = els.get(key)
                    if el:
                        el.set_property("draw-shadow", True)

    elif _timeout_fade_out:
        # Phase 1 — timeout fades out
        if _timeout_alpha > 0.0:
            _timeout_alpha = max(0.0, _timeout_alpha - 0.05)
            if timeout_bg:
                timeout_bg.set_property("alpha", _timeout_alpha)
            t_a = int(_timeout_alpha * 255)
            t_fg = (t_a << 24) | 0x00FFFFFF
            t_outline = (t_a << 24) | 0x00000000
            for key in TIMEOUT_TEXT_KEYS:
                el = els.get(key)
                if el:
                    el.set_property("color", t_fg)
                    el.set_property("outline-color", t_outline)
            if _timeout_alpha <= 0.0:
                for key in TIMEOUT_TEXT_KEYS:
                    el = els.get(key)
                    if el:
                        el.set_property("silent", True)
                if timeout_bg:
                    timeout_bg.set_property("alpha", 0.0)
                if visible:
                    for key in _SCOREBOARD_CROSS_FADE_KEYS:
                        el = els.get(key)
                        if el:
                            el.set_property("draw-shadow", False)
                            el.set_property("color", 0x00FFFFFF)
                            el.set_property("outline-color", 0x00000000)
                            el.set_property("silent", False)
                _timeout_pause_ticks = TIMEOUT_TRANSITION_PAUSE_TICKS

        # Phase 2 — pause between timeout out and scoreboard in
        elif _timeout_pause_ticks > 0:
            _timeout_pause_ticks -= 1

        # Phase 3 — scoreboard fades in
        else:
            _sb_timeout_alpha = min(1.0, _sb_timeout_alpha + 0.05)
            bg_el = els.get("osd_bg")
            if bg_el:
                bg_el.set_property("alpha", _pre_timeout_bg_alpha * _sb_timeout_alpha)
            home_foul_el = els.get("osd_home_fouls_bar")
            if home_foul_el:
                home_foul_el.set_property("alpha", _pre_timeout_home_foul_alpha * _sb_timeout_alpha)
            away_foul_el = els.get("osd_away_fouls_bar")
            if away_foul_el:
                away_foul_el.set_property("alpha", _pre_timeout_away_foul_alpha * _sb_timeout_alpha)
            if visible:
                sb_a = int(_sb_timeout_alpha * 255)
                sb_fg = (sb_a << 24) | 0x00FFFFFF
                sb_outline = (sb_a << 24) | 0x00000000
                for key in _SCOREBOARD_CROSS_FADE_KEYS:
                    el = els.get(key)
                    if el:
                        el.set_property("color", sb_fg)
                        el.set_property("outline-color", sb_outline)
            if _sb_timeout_alpha >= 1.0:
                if visible:
                    for key in _SCOREBOARD_CROSS_FADE_KEYS:
                        el = els.get(key)
                        if el:
                            el.set_property("color", 0xFFFFFFFF)
                            el.set_property("outline-color", 0xFF000000)
                            el.set_property("draw-shadow", True)
                _timeout_fade_out = False
                _timeout_fade_active = False
                if _last_score_state:
                    _update_overlay(_last_score_state)
                return False

    return True


def update_timeout_overlay(state: dict, els: dict) -> None:
    global _timeout_fade_in, _timeout_fade_out, _timeout_fade_active, _timeout_alpha
    global _pre_timeout_bg_alpha, _pre_timeout_home_foul_alpha, _pre_timeout_away_foul_alpha

    timeout_stats = state.get("timeout_stats")
    now_ms = int(time.time() * 1000)
    timeout_active = (
        isinstance(timeout_stats, dict)
        and timeout_stats.get("show_until", 0) > now_ms
    )

    if timeout_active:
        if not _timeout_fade_active:
            _timeout_fade_in = True
            _timeout_fade_out = False
            _timeout_fade_active = True
            _timeout_alpha = 0.0
            _sb_timeout_alpha = 1.0
            _timeout_pause_ticks = 0

            # Capture current scoreboard pixel alphas for sequential fade
            bg_el = els.get("osd_bg")
            _pre_timeout_bg_alpha = bg_el.get_property("alpha") if bg_el else 0.0
            home_foul_el = els.get("osd_home_fouls_bar")
            _pre_timeout_home_foul_alpha = home_foul_el.get_property("alpha") if home_foul_el else 0.0
            away_foul_el = els.get("osd_away_fouls_bar")
            _pre_timeout_away_foul_alpha = away_foul_el.get_property("alpha") if away_foul_el else 0.0

            # Prepare scoreboard text for fade-out (disable shadow before alpha fade)
            visible = state.get("visible", False)
            if visible:
                for key in _SCOREBOARD_CROSS_FADE_KEYS:
                    el = els.get(key)
                    if el:
                        el.set_property("draw-shadow", False)

            # Silence milestone during timeout (has its own fade)
            for key in ("osd_milestone_player", "osd_milestone_text"):
                el = els.get(key)
                if el:
                    el.set_property("silent", True)

            populate_timeout_texts(timeout_stats, state, els)
            for key in TIMEOUT_TEXT_KEYS:
                el = els.get(key)
                if el:
                    el.set_property("draw-shadow", False)
                    el.set_property("color", 0x00FFFFFF)
                    el.set_property("outline-color", 0x00000000)
                    el.set_property("silent", False)
            GLib.timeout_add(100, _timeout_fade_step)
    else:
        if _timeout_fade_active and not _timeout_fade_out:
            _timeout_fade_in = False
            _timeout_fade_out = True
            _timeout_pause_ticks = 0
            for key in TIMEOUT_TEXT_KEYS:
                el = els.get(key)
                if el:
                    el.set_property("draw-shadow", False)


def _milestone_fade_step() -> bool:
    global _milestone_alpha, _milestone_fading_in, _milestone_fading_out, _milestone_fade_active

    els = dict(_osd_elements)
    player_el = els.get("osd_milestone_player")
    text_el = els.get("osd_milestone_text")

    if _milestone_fading_in:
        _milestone_alpha = min(1.0, _milestone_alpha + 0.1)
        if _milestone_alpha >= 1.0:
            _milestone_fading_in = False
            for el in (player_el, text_el):
                if el:
                    el.set_property("draw-shadow", True)
    elif _milestone_fading_out:
        _milestone_alpha = max(0.0, _milestone_alpha - 0.05)
        if _milestone_alpha <= 0.0:
            _milestone_fading_out = False
            _milestone_fade_active = False
            for el in (player_el, text_el):
                if el:
                    el.set_property("silent", True)
            return False

    a = int(_milestone_alpha * 255)
    fg = (a << 24) | 0x00FFFFFF
    outline = (a << 24) | 0x00000000
    for el in (player_el, text_el):
        if el:
            el.set_property("color", fg)
            el.set_property("outline-color", outline)

    return True


def _show_blitzball_end_stats(state: dict, els: dict) -> None:
    # Step 1 — hide regular scoreboard pixel elements (gdkpixbufoverlay → alpha only)
    for key in ("osd_bg", "osd_home_fouls_bar", "osd_away_fouls_bar"):
        el = els.get(key)
        if el:
            el.set_property("alpha", 0.0)
    # Hide regular scoreboard text elements (textoverlay → silent only)
    for key in ("osd_quarter", "osd_home", "osd_away",
                "osd_home_score", "osd_away_score", "osd_clock",
                "osd_milestone_player", "osd_milestone_text"):
        el = els.get(key)
        if el:
            el.set_property("silent", True)

    # Step 2 — hide blitz pixel elements (gdkpixbufoverlay → alpha only)
    for key in ("osd_blitz_bg", "osd_blitz_active"):
        el = els.get(key)
        if el:
            el.set_property("alpha", 0.0)
    # Hide blitz text elements (textoverlay → silent only)
    for key in ("osd_blitz_home_name", "osd_blitz_away_name",
                "osd_blitz_home_pts", "osd_blitz_home_blitz",
                "osd_blitz_away_pts", "osd_blitz_away_blitz",
                "osd_blitz_quarter", "osd_blitz_clock",
                "osd_blitz_home_streak", "osd_blitz_away_streak"):
        el = els.get(key)
        if el:
            el.set_property("silent", True)

    # Step 3 — show dark background
    end_bg = els.get("osd_end_bg")
    if end_bg:
        end_bg.set_property("alpha", 0.85)

    # Step 4 — populate stats text
    winner = state.get("winner", "")
    home_name = state.get("home_name", "HOME")
    away_name = state.get("away_name", "AWAY")
    home_pts = state.get("home_points", 0)
    away_pts = state.get("away_points", 0)
    home_blitz = state.get("home_blitz_score", 0)
    away_blitz = state.get("away_blitz_score", 0)

    if winner == "home":
        winner_text = f"{home_name} WINS!"
    elif winner == "away":
        winner_text = f"{away_name} WINS!"
    else:
        winner_text = "DRAW!"

    def show_el(key: str, text: str, color: int | None = None) -> None:
        el = els.get(key)
        if not el:
            return
        el.set_property("text", text)
        el.set_property("silent", False)
        if color is not None:
            el.set_property("color", color)

    show_el("osd_end_winner",      winner_text,                          0xFFFFD700)
    show_el("osd_end_header_home", home_name)
    show_el("osd_end_header_away", away_name)
    show_el("osd_end_home_pts",    f"TOTAL  {home_pts} PTS")
    show_el("osd_end_home_blitz",  f"BLITZ  {home_blitz}",              0xFFFFD700)
    show_el("osd_end_home_inner",     f"INNER   {state.get('home_inner_scores', 0)}")
    show_el("osd_end_home_middle",    f"MIDDLE  {state.get('home_middle_scores', 0)}")
    show_el("osd_end_home_outer",     f"OUTER   {state.get('home_outer_scores', 0)}")
    show_el("osd_end_home_intercept", f"INTERCEPTS  {state.get('home_interceptions', 0)}")
    show_el("osd_end_away_pts",       f"TOTAL  {away_pts} PTS")
    show_el("osd_end_away_blitz",     f"BLITZ  {away_blitz}",              0xFFFFD700)
    show_el("osd_end_away_inner",     f"INNER   {state.get('away_inner_scores', 0)}")
    show_el("osd_end_away_middle",    f"MIDDLE  {state.get('away_middle_scores', 0)}")
    show_el("osd_end_away_outer",     f"OUTER   {state.get('away_outer_scores', 0)}")
    show_el("osd_end_away_intercept", f"INTERCEPTS  {state.get('away_interceptions', 0)}")
    for key in ("osd_end_home_blitz_rate", "osd_end_away_blitz_rate"):
        el = els.get(key)
        if el:
            el.set_property("silent", True)


def _update_overlay(state: dict) -> None:
    global _last_score_state, _end_stats_show_until
    _last_score_state = dict(state)
    els = dict(_osd_elements)
    if not els:
        return

    game_finished = state.get("game_finished", False)
    sport_code = state.get("sport_code", "")
    now_ms = int(time.time() * 1000)

    if not game_finished:
        _end_stats_show_until = 0

    if game_finished and sport_code == "BLITZBALL":
        if _end_stats_show_until == 0:
            _end_stats_show_until = now_ms + 20000
        if now_ms < _end_stats_show_until:
            _show_blitzball_end_stats(state, els)
            return
        else:
            _end_stats_show_until = 0

    end_showing = update_blitzball_end_stats(state, els)
    if end_showing:
        return

    if not _timeout_fade_active and sport_code != "BLITZBALL":
        visible = state.get("visible", False)
        quarter = els.get("osd_quarter")
        home = els.get("osd_home")
        away = els.get("osd_away")
        home_score = els.get("osd_home_score")
        away_score = els.get("osd_away_score")
        clock = els.get("osd_clock")
        home_fouls_bar = els.get("osd_home_fouls_bar")
        away_fouls_bar = els.get("osd_away_fouls_bar")
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
        if home_fouls_bar:
            path = foul_png_path("home", state.get("home_fouls", 0))
            if path is None:
                home_fouls_bar.set_property("alpha", 0.0)
            else:
                home_fouls_bar.set_property("location", path)
                home_fouls_bar.set_property("alpha", 1.0 if visible else 0.0)
        if away_fouls_bar:
            path = foul_png_path("away", state.get("away_fouls", 0))
            if path is None:
                away_fouls_bar.set_property("alpha", 0.0)
            else:
                away_fouls_bar.set_property("location", path)
                away_fouls_bar.set_property("alpha", 1.0 if visible else 0.0)
        if bg:
            bg.set_property("alpha", 1.0 if visible else 0.0)

        global _milestone_alpha, _milestone_fading_in, _milestone_fading_out, _milestone_fade_active
        milestone_active = int(time.time() * 1000) < _milestone_display_until

        if milestone_active:
            if not _milestone_fade_active:
                _milestone_fading_in = True
                _milestone_fading_out = False
                _milestone_fade_active = True
                _milestone_alpha = 0.0
                update_milestone_overlays(milestone_player, milestone_text, state, force_visible=True)
                for el in (milestone_player, milestone_text):
                    if el:
                        el.set_property("draw-shadow", False)
                        el.set_property("color", 0x00FFFFFF)
                        el.set_property("outline-color", 0x00000000)
                GLib.timeout_add(100, _milestone_fade_step)
            elif _milestone_fading_out:
                _milestone_fading_in = True
                _milestone_fading_out = False
                for el in (milestone_player, milestone_text):
                    if el:
                        el.set_property("draw-shadow", False)
                update_milestone_overlays(milestone_player, milestone_text, state, force_visible=True)
        elif _milestone_fade_active and not _milestone_fading_out:
            _milestone_fading_in = False
            _milestone_fading_out = True
        elif not _milestone_fade_active:
            update_milestone_overlays(milestone_player, milestone_text, state)

    global _blitz_pulse_active, _blitz_pulse_alpha
    blitz_pulse_needed = update_blitzball_overlay(state, els)
    if blitz_pulse_needed and not _blitz_pulse_active:
        _blitz_pulse_active = True
        _blitz_pulse_alpha = 0.6
        GLib.timeout_add(50, _blitz_pulse_step)
    elif not blitz_pulse_needed:
        _blitz_pulse_active = False

    update_timeout_overlay(state, els)


def _poll_score_state() -> bool:
    global _milestone_display_until, _last_milestone_show_until
    state = read_score_state()
    milestone = state.get("milestone")
    now_ms = int(time.time() * 1000)

    if isinstance(milestone, dict):
        show_until = _milestone_show_until(milestone)
        if show_until > 0 and show_until != _last_milestone_show_until:
            _last_milestone_show_until = show_until
            _milestone_display_until = now_ms + 10000
        milestone_active = now_ms < _milestone_display_until
    else:
        _last_milestone_show_until = 0.0
        _milestone_display_until = 0
        milestone_active = False

    timeout_stats = state.get("timeout_stats")
    timeout_active = (
        isinstance(timeout_stats, dict)
        and timeout_stats.get("show_until", 0) > now_ms
    )

    end_active = (
        state.get("game_finished", False)
        and _end_stats_show_until > int(time.time() * 1000)
    )
    if state != _last_score_state or milestone_active or timeout_active or _timeout_fade_active or end_active:
        _update_overlay(state)
    return True


def _switch_active_camera(active_camera: str) -> None:
    global _current_active_camera
    normalized = _normalize_camera(active_camera)
    if _current_active_camera == normalized:
        return
    _current_active_camera = normalized
    print(f"[worker] updated active camera -> {normalized}")
    _force_key_unit(_enc_stream, "rtmp")
    _set_status(active_camera=normalized)


def _poll_worker_config() -> bool:
    global _last_config
    cfg = read_worker_config()
    if cfg != _last_config:
        previous = _last_config or {}
        _last_config = dict(cfg)
        bitrate = cfg.get("bitrateKbps", RTMP_BITRATE_DEFAULT)
        if _enc_stream is not None and bitrate != previous.get("bitrateKbps", RTMP_BITRATE_DEFAULT):
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
    if ENABLE_TERMINAL_FPS_METRICS and ENABLE_RTMP_FPS_METRICS:
        with _fps_lock:
            frames = _rtmp_fps_frames
            _rtmp_fps_frames = 0

        if _worker_state["stream_status_sent"] and frames > 0:
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
    # Uses TCP for the RTSP connection.
    src.set_property("protocols", 4)
    # Buffers 100 ms of RTSP data before decode.
    src.set_property("latency", 100)

    conv.set_property("gpu-id", 0)
    conv.set_property("copy-hw", 2)
    caps.set_property(
        "caps",
        Gst.Caps.from_string(
            os.environ.get("RTMP_RAW_CAPS", "video/x-raw,format=I420")
        ),
    )

    # Keeps up to 2 frames in this queue.
    q.set_property("max-size-buffers", 2)
    q.set_property("max-size-bytes", 0)
    q.set_property("max-size-time", 0)
    # Drops old frames if the queue falls behind.
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


def _build_program_input_branch(pipeline: Gst.Pipeline) -> Gst.Element:
    return _build_rtsp_input_branch(pipeline, "program", SOURCE_RTSP_PROGRAM_URL)


def _make_watchdog() -> Gst.Element | None:
    watchdog = Gst.ElementFactory.make("watchdog", "strm_watchdog")
    if watchdog is None:
        print("[worker] WARNING: watchdog plugin unavailable; using custom stall detector only")
        return None

    watchdog.set_property("timeout", int(STALL_TIMEOUT_SEC * 1000))
    return watchdog


def _add_output_elements(
        pipeline: Gst.Pipeline,
        q: Gst.Element,
        rtmp: RtmpElements,
        watchdog: Gst.Element | None,
) -> None:
    elements = [q, *rtmp.base_elements()]
    if watchdog is not None:
        elements.insert(-3, watchdog)

    for el in elements:
        pipeline.add(el)


def _link_parse_to_mux(rtmp: RtmpElements, watchdog: Gst.Element | None) -> None:
    if watchdog is None:
        _link(rtmp.parse, rtmp.flvmux)
        return

    _link(rtmp.parse, watchdog)
    _link(watchdog, rtmp.flvmux)


def _link_audio_encoder_to_mux(rtmp: RtmpElements) -> None:
    _link_filtered(rtmp.audiosrc, rtmp.aacenc, "audio/x-raw,rate=44100,channels=2")
    aacenc_src = _get_static_pad(rtmp.aacenc, "src")
    flvmux_audio = rtmp.flvmux.request_pad_simple("audio")
    if flvmux_audio is None:
        raise RuntimeError("Unable to request flvmux audio pad")
    if aacenc_src.link(flvmux_audio) != Gst.PadLinkReturn.OK:
        raise RuntimeError("Failed to link audio encoder to flvmux audio pad")


def _add_buffer_probe_if_present(
        element: Gst.Element,
        pad_name: str,
        callback: Callable[[Any, Any, Any], Gst.PadProbeReturn],
) -> None:
    pad = element.get_static_pad(pad_name)
    if pad is not None:
        pad.add_probe(Gst.PadProbeType.BUFFER, callback, None)


def build_pipeline() -> tuple[Gst.Pipeline, Gst.Element]:
    global _enc_stream, _current_active_camera, _last_config

    rtmp_url = read_stream_url()
    if not rtmp_url:
        raise RuntimeError("stream.conf does not contain an RTMP URL")
    if not os.path.exists(SCOREBOARD_PNG):
        raise RuntimeError(f"Missing scoreboard PNG: {SCOREBOARD_PNG}")

    cfg = read_worker_config()

    pipeline = Gst.Pipeline.new("stream-worker")
    if pipeline is None:
        raise RuntimeError("Unable to create stream-worker pipeline")

    program_input = _build_program_input_branch(pipeline)
    q = _make("queue", "strm_queue")
    rtmp = make_rtmp_elements(_make)
    watchdog = _make_watchdog()
    configure_rtmp_branch(
        rtmp,
        q,
        int(cfg.get("bitrateKbps", RTMP_BITRATE_DEFAULT)),
        rtmp_url,
    )
    _add_output_elements(pipeline, q, rtmp, watchdog)

    _link(program_input, q)
    _link_many(q, *rtmp.overlay_chain())
    _link_parse_to_mux(rtmp, watchdog)
    _link_audio_encoder_to_mux(rtmp)
    _link(rtmp.flvmux, rtmp.rtmpsink)

    activity_pad_owner = watchdog if watchdog is not None else rtmp.parse
    _add_buffer_probe_if_present(activity_pad_owner, "src", _activity_probe)
    _add_buffer_probe_if_present(rtmp.rtmpsink, "sink", _rtmp_probe)

    _osd_elements.clear()
    _osd_elements.update(rtmp.osd_map())

    _enc_stream = rtmp.enc
    _current_active_camera = _normalize_camera(cfg.get("activeCamera", PTZ_CAMERA))
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


def create_dark_bg_png() -> None:
    from PIL import Image
    from runtime_paths import END_STATS_BG_PNG
    if os.path.exists(END_STATS_BG_PNG):
        return
    try:
        img = Image.new("RGBA", (1920, 1080), (26, 26, 26, 230))
        img.save(END_STATS_BG_PNG)
        print(f"[worker] created dark bg: {END_STATS_BG_PNG}")
    except Exception as e:
        print(f"[worker] failed to create dark bg: {e}")


def main() -> None:
    global _loop

    create_dark_bg_png()

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
    GLib.timeout_add(int(STATE_POLL_SEC * 1000), _poll_score_state)
    GLib.timeout_add(int(CONFIG_POLL_SEC * 1000), _poll_worker_config)
    if ENABLE_TERMINAL_FPS_METRICS and ENABLE_RTMP_FPS_METRICS:
        GLib.timeout_add_seconds(TERMINAL_FPS_INTERVAL_SEC, _rtmp_fps_report)

    def _signal_handler(_sig, _frame):
        if _loop is not None:
            _loop.quit()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _set_status(worker_alive=True, stream_active=False, last_error="")
    print(f"[worker] source program: {SOURCE_RTSP_PROGRAM_URL}")
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
