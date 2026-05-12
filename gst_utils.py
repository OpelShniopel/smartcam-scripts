"""Small shared GStreamer helpers used by both pipeline processes."""

from __future__ import annotations

from typing import Any

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstVideo", "1.0")
from gi.repository import Gst, GstVideo


def force_key_unit(enc: Any | None, label: str, log_prefix: str) -> None:
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
        print(f"[{log_prefix}] forced keyframe -> {label}")
