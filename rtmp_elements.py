"""Shared RTMP branch element construction and static overlay setup."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from runtime_paths import SCOREBOARD_PNG


RTMP_KEYINT = 60
RTMP_THREADS = 2
RTMP_PRESET = "ultrafast"
RTMP_TUNE = "zerolatency"

SCOREBOARD_W = 410
SCOREBOARD_H = 129
SCOREBOARD_OFFSET_X = 755
SCOREBOARD_OFFSET_Y = 931


@dataclass
class RtmpElements:
    osd_bg: Any
    osd_home: Any
    osd_away: Any
    osd_score: Any
    osd_clock: Any
    osd_fouls: Any
    enc: Any
    parse: Any
    flvmux: Any
    rtmpsink: Any
    audiosrc: Any
    aacenc: Any

    def osd_map(self) -> dict[str, Any]:
        return {
            "osd_bg": self.osd_bg,
            "osd_home": self.osd_home,
            "osd_away": self.osd_away,
            "osd_score": self.osd_score,
            "osd_clock": self.osd_clock,
            "osd_fouls": self.osd_fouls,
        }

    def base_elements(self) -> tuple[Any, ...]:
        return (
            self.osd_bg,
            self.osd_home,
            self.osd_away,
            self.osd_score,
            self.osd_clock,
            self.osd_fouls,
            self.enc,
            self.parse,
            self.flvmux,
            self.rtmpsink,
            self.audiosrc,
            self.aacenc,
        )

    def overlay_chain(self) -> tuple[Any, ...]:
        return (
            self.osd_bg,
            self.osd_home,
            self.osd_away,
            self.osd_score,
            self.osd_clock,
            self.osd_fouls,
            self.enc,
            self.parse,
        )


def make_rtmp_elements(make_element: Callable[[str, str], Any]) -> RtmpElements:
    return RtmpElements(
        osd_bg=make_element("gdkpixbufoverlay", "strm_osd_bg"),
        osd_home=make_element("textoverlay", "strm_osd_home"),
        osd_away=make_element("textoverlay", "strm_osd_away"),
        osd_score=make_element("textoverlay", "strm_osd_score"),
        osd_clock=make_element("textoverlay", "strm_osd_clock"),
        osd_fouls=make_element("textoverlay", "strm_osd_fouls"),
        enc=make_element("x264enc", "strm_enc"),
        parse=make_element("h264parse", "strm_parse"),
        flvmux=make_element("flvmux", "strm_flvmux"),
        rtmpsink=make_element("rtmpsink", "strm_rtmpsink"),
        audiosrc=make_element("audiotestsrc", "strm_audiosrc"),
        aacenc=make_element("voaacenc", "strm_aacenc"),
    )


def configure_leaky_queue(queue_element: Any) -> None:
    queue_element.set_property("max-size-buffers", 2)
    queue_element.set_property("max-size-bytes", 0)
    queue_element.set_property("max-size-time", 0)
    queue_element.set_property("leaky", 2)


def configure_scoreboard_background(osd_bg: Any) -> None:
    osd_bg.set_property("location", SCOREBOARD_PNG)
    osd_bg.set_property("offset-x", SCOREBOARD_OFFSET_X)
    osd_bg.set_property("offset-y", SCOREBOARD_OFFSET_Y)
    osd_bg.set_property("overlay-width", SCOREBOARD_W)
    osd_bg.set_property("overlay-height", SCOREBOARD_H)
    osd_bg.set_property("alpha", 0.0)


def setup_text_overlay(
    element: Any,
    text: str,
    xpos: float,
    ypos: float,
    font: str = "Sans Bold 20",
    color: int = 0xFFFFFFFF,
    shadow: bool = True,
) -> None:
    element.set_property("text", text)
    element.set_property("font-desc", font)
    element.set_property("halignment", 4)
    element.set_property("valignment", 3)
    element.set_property("xpos", xpos)
    element.set_property("ypos", ypos)
    element.set_property("color", color)
    element.set_property("draw-shadow", shadow)
    element.set_property("auto-resize", False)
    element.set_property("wait-text", False)
    element.set_property("silent", True)


def configure_scoreboard_texts(elements: RtmpElements) -> None:
    setup_text_overlay(
        elements.osd_home,
        "HOME",
        xpos=0.022,
        ypos=0.040,
        font="Sans Bold 22",
    )
    setup_text_overlay(
        elements.osd_away,
        "AWAY",
        xpos=0.230,
        ypos=0.040,
        font="Sans Bold 22",
    )
    setup_text_overlay(
        elements.osd_score,
        "0 - 0",
        xpos=0.120,
        ypos=0.040,
        font="Sans Bold 22",
        color=0xFFD916FF,
    )
    setup_text_overlay(
        elements.osd_clock,
        "Q1 10:00",
        xpos=0.330,
        ypos=0.040,
        font="Sans Bold 22",
        color=0xB2E5FFFF,
    )
    setup_text_overlay(
        elements.osd_fouls,
        "",
        xpos=0.022,
        ypos=0.068,
        font="Sans 13",
        color=0xA6A6A6FF,
    )


def configure_rtmp_encoder(enc: Any, bitrate: int) -> None:
    enc.set_property("pass", "cbr")
    enc.set_property("bitrate", bitrate)
    enc.set_property("vbv-buf-capacity", 200)
    enc.set_property("tune", RTMP_TUNE)
    enc.set_property("speed-preset", RTMP_PRESET)
    enc.set_property("key-int-max", RTMP_KEYINT)
    enc.set_property("threads", RTMP_THREADS)


def configure_rtmp_output(elements: RtmpElements, rtmp_url: str) -> None:
    elements.flvmux.set_property("streamable", True)
    elements.rtmpsink.set_property("location", rtmp_url)
    elements.rtmpsink.set_property("async", False)
    elements.audiosrc.set_property("wave", 4)
    elements.aacenc.set_property("bitrate", 128000)


def configure_rtmp_branch(
    elements: RtmpElements,
    queue_element: Any,
    bitrate: int,
    rtmp_url: str,
) -> None:
    configure_leaky_queue(queue_element)
    configure_scoreboard_background(elements.osd_bg)
    configure_scoreboard_texts(elements)
    configure_rtmp_encoder(elements.enc, bitrate)
    configure_rtmp_output(elements, rtmp_url)
