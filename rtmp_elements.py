"""Shared RTMP branch element construction and static overlay setup."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from runtime_paths import SCOREBOARD_PNG, SCRIPT_DIR, TIMEOUT_BG_PNG

RTMP_KEYINT = 60
RTMP_THREADS = 2
RTMP_PRESET = "ultrafast"
RTMP_TUNE = "zerolatency"

SCOREBOARD_W = 650
SCOREBOARD_H = 130
SCOREBOARD_OFFSET_X = 100
SCOREBOARD_OFFSET_Y = 900


@dataclass
class RtmpElements:
    osd_bg: Any
    osd_home_fouls_bar: Any
    osd_away_fouls_bar: Any
    osd_quarter: Any
    osd_home: Any
    osd_away: Any
    osd_home_score: Any
    osd_away_score: Any
    osd_clock: Any
    osd_milestone_player: Any
    osd_milestone_text: Any
    osd_timeout_bg: Any
    osd_timeout_header: Any
    osd_timeout_calling: Any
    osd_timeout_home_name: Any
    osd_timeout_home_pts: Any
    osd_timeout_home_fg: Any
    osd_timeout_home_3p: Any
    osd_timeout_home_reb: Any
    osd_timeout_home_ast: Any
    osd_timeout_home_stl: Any
    osd_timeout_home_blk: Any
    osd_timeout_home_foul: Any
    osd_timeout_away_name: Any
    osd_timeout_away_pts: Any
    osd_timeout_away_fg: Any
    osd_timeout_away_3p: Any
    osd_timeout_away_reb: Any
    osd_timeout_away_ast: Any
    osd_timeout_away_stl: Any
    osd_timeout_away_blk: Any
    osd_timeout_away_foul: Any
    osd_timeout_player_h1: Any
    osd_timeout_player_h2: Any
    osd_timeout_player_h3: Any
    osd_timeout_player_a1: Any
    osd_timeout_player_a2: Any
    osd_timeout_player_a3: Any
    enc: Any
    parse: Any
    flvmux: Any
    rtmpsink: Any
    audiosrc: Any
    aacenc: Any

    def osd_map(self) -> dict[str, Any]:
        return {
            "osd_bg": self.osd_bg,
            "osd_home_fouls_bar": self.osd_home_fouls_bar,
            "osd_away_fouls_bar": self.osd_away_fouls_bar,
            "osd_quarter": self.osd_quarter,
            "osd_home": self.osd_home,
            "osd_away": self.osd_away,
            "osd_home_score": self.osd_home_score,
            "osd_away_score": self.osd_away_score,
            "osd_clock": self.osd_clock,
            "osd_milestone_player": self.osd_milestone_player,
            "osd_milestone_text": self.osd_milestone_text,
            "osd_timeout_bg": self.osd_timeout_bg,
            "osd_timeout_header": self.osd_timeout_header,
            "osd_timeout_calling": self.osd_timeout_calling,
            "osd_timeout_home_name": self.osd_timeout_home_name,
            "osd_timeout_home_pts": self.osd_timeout_home_pts,
            "osd_timeout_home_fg": self.osd_timeout_home_fg,
            "osd_timeout_home_3p": self.osd_timeout_home_3p,
            "osd_timeout_home_reb": self.osd_timeout_home_reb,
            "osd_timeout_home_ast": self.osd_timeout_home_ast,
            "osd_timeout_home_stl": self.osd_timeout_home_stl,
            "osd_timeout_home_blk": self.osd_timeout_home_blk,
            "osd_timeout_home_foul": self.osd_timeout_home_foul,
            "osd_timeout_away_name": self.osd_timeout_away_name,
            "osd_timeout_away_pts": self.osd_timeout_away_pts,
            "osd_timeout_away_fg": self.osd_timeout_away_fg,
            "osd_timeout_away_3p": self.osd_timeout_away_3p,
            "osd_timeout_away_reb": self.osd_timeout_away_reb,
            "osd_timeout_away_ast": self.osd_timeout_away_ast,
            "osd_timeout_away_stl": self.osd_timeout_away_stl,
            "osd_timeout_away_blk": self.osd_timeout_away_blk,
            "osd_timeout_away_foul": self.osd_timeout_away_foul,
            "osd_timeout_player_h1": self.osd_timeout_player_h1,
            "osd_timeout_player_h2": self.osd_timeout_player_h2,
            "osd_timeout_player_h3": self.osd_timeout_player_h3,
            "osd_timeout_player_a1": self.osd_timeout_player_a1,
            "osd_timeout_player_a2": self.osd_timeout_player_a2,
            "osd_timeout_player_a3": self.osd_timeout_player_a3,
        }

    def base_elements(self) -> tuple[Any, ...]:
        return (
            self.osd_bg,
            self.osd_home_fouls_bar,
            self.osd_away_fouls_bar,
            self.osd_quarter,
            self.osd_home,
            self.osd_away,
            self.osd_home_score,
            self.osd_away_score,
            self.osd_clock,
            self.osd_milestone_player,
            self.osd_milestone_text,
            self.osd_timeout_bg,
            self.osd_timeout_header,
            self.osd_timeout_calling,
            self.osd_timeout_home_name,
            self.osd_timeout_home_pts,
            self.osd_timeout_home_fg,
            self.osd_timeout_home_3p,
            self.osd_timeout_home_reb,
            self.osd_timeout_home_ast,
            self.osd_timeout_home_stl,
            self.osd_timeout_home_blk,
            self.osd_timeout_home_foul,
            self.osd_timeout_away_name,
            self.osd_timeout_away_pts,
            self.osd_timeout_away_fg,
            self.osd_timeout_away_3p,
            self.osd_timeout_away_reb,
            self.osd_timeout_away_ast,
            self.osd_timeout_away_stl,
            self.osd_timeout_away_blk,
            self.osd_timeout_away_foul,
            self.osd_timeout_player_h1,
            self.osd_timeout_player_h2,
            self.osd_timeout_player_h3,
            self.osd_timeout_player_a1,
            self.osd_timeout_player_a2,
            self.osd_timeout_player_a3,
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
            self.osd_home_fouls_bar,
            self.osd_away_fouls_bar,
            self.osd_quarter,
            self.osd_home,
            self.osd_away,
            self.osd_home_score,
            self.osd_away_score,
            self.osd_clock,
            self.osd_milestone_player,
            self.osd_milestone_text,
            self.osd_timeout_bg,
            self.osd_timeout_header,
            self.osd_timeout_calling,
            self.osd_timeout_home_name,
            self.osd_timeout_home_pts,
            self.osd_timeout_home_fg,
            self.osd_timeout_home_3p,
            self.osd_timeout_home_reb,
            self.osd_timeout_home_ast,
            self.osd_timeout_home_stl,
            self.osd_timeout_home_blk,
            self.osd_timeout_home_foul,
            self.osd_timeout_away_name,
            self.osd_timeout_away_pts,
            self.osd_timeout_away_fg,
            self.osd_timeout_away_3p,
            self.osd_timeout_away_reb,
            self.osd_timeout_away_ast,
            self.osd_timeout_away_stl,
            self.osd_timeout_away_blk,
            self.osd_timeout_away_foul,
            self.osd_timeout_player_h1,
            self.osd_timeout_player_h2,
            self.osd_timeout_player_h3,
            self.osd_timeout_player_a1,
            self.osd_timeout_player_a2,
            self.osd_timeout_player_a3,
            self.enc,
            self.parse,
        )


def make_rtmp_elements(make_element: Callable[[str, str], Any]) -> RtmpElements:
    return RtmpElements(
        osd_bg=make_element("gdkpixbufoverlay", "strm_osd_bg"),
        osd_home_fouls_bar=make_element("gdkpixbufoverlay", "strm_osd_home_fouls_bar"),
        osd_away_fouls_bar=make_element("gdkpixbufoverlay", "strm_osd_away_fouls_bar"),
        osd_quarter=make_element("textoverlay", "strm_osd_quarter"),
        osd_home=make_element("textoverlay", "strm_osd_home"),
        osd_away=make_element("textoverlay", "strm_osd_away"),
        osd_home_score=make_element("textoverlay", "strm_osd_home_score"),
        osd_away_score=make_element("textoverlay", "strm_osd_away_score"),
        osd_clock=make_element("textoverlay", "strm_osd_clock"),
        osd_milestone_player=make_element(
            "textoverlay",
            "strm_osd_milestone_player",
        ),
        osd_milestone_text=make_element("textoverlay", "strm_osd_milestone_text"),
        osd_timeout_bg=make_element("gdkpixbufoverlay", "strm_osd_timeout_bg"),
        osd_timeout_header=make_element("textoverlay", "strm_osd_timeout_header"),
        osd_timeout_calling=make_element("textoverlay", "strm_osd_timeout_calling"),
        osd_timeout_home_name=make_element("textoverlay", "strm_osd_timeout_home_name"),
        osd_timeout_home_pts=make_element("textoverlay", "strm_osd_timeout_home_pts"),
        osd_timeout_home_fg=make_element("textoverlay", "strm_osd_timeout_home_fg"),
        osd_timeout_home_3p=make_element("textoverlay", "strm_osd_timeout_home_3p"),
        osd_timeout_home_reb=make_element("textoverlay", "strm_osd_timeout_home_reb"),
        osd_timeout_home_ast=make_element("textoverlay", "strm_osd_timeout_home_ast"),
        osd_timeout_home_stl=make_element("textoverlay", "strm_osd_timeout_home_stl"),
        osd_timeout_home_blk=make_element("textoverlay", "strm_osd_timeout_home_blk"),
        osd_timeout_home_foul=make_element("textoverlay", "strm_osd_timeout_home_foul"),
        osd_timeout_away_name=make_element("textoverlay", "strm_osd_timeout_away_name"),
        osd_timeout_away_pts=make_element("textoverlay", "strm_osd_timeout_away_pts"),
        osd_timeout_away_fg=make_element("textoverlay", "strm_osd_timeout_away_fg"),
        osd_timeout_away_3p=make_element("textoverlay", "strm_osd_timeout_away_3p"),
        osd_timeout_away_reb=make_element("textoverlay", "strm_osd_timeout_away_reb"),
        osd_timeout_away_ast=make_element("textoverlay", "strm_osd_timeout_away_ast"),
        osd_timeout_away_stl=make_element("textoverlay", "strm_osd_timeout_away_stl"),
        osd_timeout_away_blk=make_element("textoverlay", "strm_osd_timeout_away_blk"),
        osd_timeout_away_foul=make_element("textoverlay", "strm_osd_timeout_away_foul"),
        osd_timeout_player_h1=make_element("textoverlay", "strm_osd_timeout_player_h1"),
        osd_timeout_player_h2=make_element("textoverlay", "strm_osd_timeout_player_h2"),
        osd_timeout_player_h3=make_element("textoverlay", "strm_osd_timeout_player_h3"),
        osd_timeout_player_a1=make_element("textoverlay", "strm_osd_timeout_player_a1"),
        osd_timeout_player_a2=make_element("textoverlay", "strm_osd_timeout_player_a2"),
        osd_timeout_player_a3=make_element("textoverlay", "strm_osd_timeout_player_a3"),
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
        elements.osd_quarter,
        "Q1",
        xpos=0.073,
        ypos=0.933,
        font="Sans Bold 20",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_home,
        "HOME",
        xpos=0.232,
        ypos=0.870,
        font="Sans Bold 17",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_away,
        "AWAY",
        xpos=0.232,
        ypos=0.927,
        font="Sans Bold 17",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_home_score,
        "0",
        xpos=0.355,
        ypos=0.873,
        font="Sans Bold 28",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_away_score,
        "0",
        xpos=0.355,
        ypos=0.933,
        font="Sans Bold 28",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_clock,
        "10:00",
        xpos=0.130,
        ypos=0.933,
        font="Sans Bold 20",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_milestone_player,
        "",
        xpos=0.200,
        ypos=0.755,
        font="Sans Bold 20",
        color=0xFFFFFFFF,
    )
    setup_text_overlay(
        elements.osd_milestone_text,
        "",
        xpos=0.200,
        ypos=0.790,
        font="Sans Bold 20",
        color=0xFFFFFFFF,
    )


TIMEOUT_TEXT_KEYS: tuple[str, ...] = (
    "osd_timeout_header",
    "osd_timeout_home_name", "osd_timeout_home_pts",
    "osd_timeout_home_fg", "osd_timeout_home_3p",
    "osd_timeout_home_reb", "osd_timeout_home_ast",
    "osd_timeout_home_stl", "osd_timeout_home_blk",
    "osd_timeout_home_foul",
    "osd_timeout_away_name", "osd_timeout_away_pts",
    "osd_timeout_away_fg", "osd_timeout_away_3p",
    "osd_timeout_away_reb", "osd_timeout_away_ast",
    "osd_timeout_away_stl", "osd_timeout_away_blk",
    "osd_timeout_away_foul",
    "osd_timeout_player_h1", "osd_timeout_player_h2", "osd_timeout_player_h3",
    "osd_timeout_player_a1", "osd_timeout_player_a2", "osd_timeout_player_a3",
)


def populate_timeout_texts(
        timeout_stats: Mapping[str, Any],
        state: Mapping[str, Any],
        els: Mapping[str, Any],
) -> None:
    home_name = str(state.get("home_name", "HOME"))
    away_name = str(state.get("away_name", "AWAY"))
    calling = timeout_stats.get("calling_team", "")
    home_stats = timeout_stats.get("home_stats") or {}
    away_stats = timeout_stats.get("away_stats") or {}

    def _set(key: str, text: str) -> None:
        el = els.get(key)
        if el:
            el.set_property("text", text)

    calling_name = home_name if calling == "home" else away_name
    _set("osd_timeout_header", f"TIMEOUT  {calling_name}")

    _set("osd_timeout_home_name", home_name)
    _set("osd_timeout_home_pts",  f"PTS  {home_stats.get('points', 0)}")
    _set("osd_timeout_home_fg",   f"FG%  {home_stats.get('fg_pct', 0.0):.1f}")
    _set("osd_timeout_home_3p",   f"3P%  {home_stats.get('tp_pct', 0.0):.1f}")
    _set("osd_timeout_home_reb",  f"REB  {home_stats.get('rebounds', 0)}")
    _set("osd_timeout_home_ast",  f"AST  {home_stats.get('assists', 0)}")
    _set("osd_timeout_home_stl",  f"STL  {home_stats.get('steals', 0)}")
    _set("osd_timeout_home_blk",  f"BLK  {home_stats.get('blocks', 0)}")
    _set("osd_timeout_home_foul", f"FOULS  {home_stats.get('fouls', 0)}")

    _set("osd_timeout_away_name", away_name)
    _set("osd_timeout_away_pts",  f"PTS  {away_stats.get('points', 0)}")
    _set("osd_timeout_away_fg",   f"FG%  {away_stats.get('fg_pct', 0.0):.1f}")
    _set("osd_timeout_away_3p",   f"3P%  {away_stats.get('tp_pct', 0.0):.1f}")
    _set("osd_timeout_away_reb",  f"REB  {away_stats.get('rebounds', 0)}")
    _set("osd_timeout_away_ast",  f"AST  {away_stats.get('assists', 0)}")
    _set("osd_timeout_away_stl",  f"STL  {away_stats.get('steals', 0)}")
    _set("osd_timeout_away_blk",  f"BLK  {away_stats.get('blocks', 0)}")
    _set("osd_timeout_away_foul", f"FOULS  {away_stats.get('fouls', 0)}")

    home_players = timeout_stats.get("home_top_players") or []
    away_players = timeout_stats.get("away_top_players") or []

    for i, slot in enumerate(("h1", "h2", "h3")):
        el = els.get(f"osd_timeout_player_{slot}")
        if not el:
            continue
        if i < len(home_players):
            p = home_players[i]
            el.set_property("text", f"{p.get('player_name', '')}  {p.get('points', 0)}pts  {p.get('rebounds', 0)}reb  {p.get('assists', 0)}ast  {p.get('fg_pct', 0.0):.0f}%")
        else:
            el.set_property("text", "")
            el.set_property("silent", True)

    for i, slot in enumerate(("a1", "a2", "a3")):
        el = els.get(f"osd_timeout_player_{slot}")
        if not el:
            continue
        if i < len(away_players):
            p = away_players[i]
            el.set_property("text", f"{p.get('player_name', '')}  {p.get('points', 0)}pts  {p.get('rebounds', 0)}reb  {p.get('assists', 0)}ast  {p.get('fg_pct', 0.0):.0f}%")
        else:
            el.set_property("text", "")
            el.set_property("silent", True)


def foul_png_path(team: str, count: int) -> str | None:
    if count <= 0:
        return None
    count = min(count, 5)
    return os.path.join(SCRIPT_DIR, f"fouls_{team}_{count}.png")


def configure_foul_bars(elements: RtmpElements) -> None:
    for bar in (elements.osd_home_fouls_bar, elements.osd_away_fouls_bar):
        bar.set_property("offset-x", SCOREBOARD_OFFSET_X)
        bar.set_property("offset-y", SCOREBOARD_OFFSET_Y)
        bar.set_property("overlay-width", SCOREBOARD_W)
        bar.set_property("overlay-height", SCOREBOARD_H)
        bar.set_property("alpha", 0.0)


def configure_timeout_overlay(elements: RtmpElements) -> None:
    bg = elements.osd_timeout_bg
    bg.set_property("location", TIMEOUT_BG_PNG)
    bg.set_property("offset-x", 510)
    bg.set_property("offset-y", 110)
    bg.set_property("overlay-width", 900)
    bg.set_property("overlay-height", 860)
    bg.set_property("alpha", 0.0)

    setup_text_overlay(elements.osd_timeout_header,  "", xpos=0.510, ypos=0.138, font="Sans Bold 32", color=0xFFFFFFFF)

    setup_text_overlay(elements.osd_timeout_home_name, "", xpos=0.370, ypos=0.225, font="Sans Bold 20", color=0xFF6B00FF)
    setup_text_overlay(elements.osd_timeout_home_pts,  "", xpos=0.370, ypos=0.290, font="Sans Bold 18", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_home_fg,   "", xpos=0.370, ypos=0.340, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_3p,   "", xpos=0.370, ypos=0.385, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_reb,  "", xpos=0.370, ypos=0.430, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_ast,  "", xpos=0.370, ypos=0.475, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_stl,  "", xpos=0.370, ypos=0.520, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_blk,  "", xpos=0.370, ypos=0.565, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_foul, "", xpos=0.370, ypos=0.610, font="Sans 16",      color=0xCCCCCCFF)

    setup_text_overlay(elements.osd_timeout_away_name, "", xpos=0.640, ypos=0.225, font="Sans Bold 20", color=0xFF6B00FF)
    setup_text_overlay(elements.osd_timeout_away_pts,  "", xpos=0.640, ypos=0.290, font="Sans Bold 18", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_away_fg,   "", xpos=0.640, ypos=0.340, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_3p,   "", xpos=0.640, ypos=0.385, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_reb,  "", xpos=0.640, ypos=0.430, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_ast,  "", xpos=0.640, ypos=0.475, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_stl,  "", xpos=0.640, ypos=0.520, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_blk,  "", xpos=0.640, ypos=0.565, font="Sans 16",      color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_foul, "", xpos=0.640, ypos=0.610, font="Sans 16",      color=0xCCCCCCFF)

    setup_text_overlay(elements.osd_timeout_player_h1, "", xpos=0.370, ypos=0.740, font="Sans Bold 15", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_player_h2, "", xpos=0.370, ypos=0.800, font="Sans Bold 15", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_player_h3, "", xpos=0.370, ypos=0.860, font="Sans Bold 15", color=0xFFFFFFFF)

    setup_text_overlay(elements.osd_timeout_player_a1, "", xpos=0.640, ypos=0.740, font="Sans Bold 15", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_player_a2, "", xpos=0.640, ypos=0.800, font="Sans Bold 15", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_player_a3, "", xpos=0.640, ypos=0.860, font="Sans Bold 15", color=0xFFFFFFFF)


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
    configure_foul_bars(elements)
    configure_scoreboard_texts(elements)
    configure_timeout_overlay(elements)
    configure_rtmp_encoder(elements.enc, bitrate)
    configure_rtmp_output(elements, rtmp_url)


def set_overlay_text(element: Any | None, visible: bool, text: str) -> None:
    if not element:
        return
    element.set_property("silent", not visible)
    if visible:
        element.set_property("text", text)


def update_score_clock_overlays(
        home_score_element: Any | None,
        away_score_element: Any | None,
        clock_element: Any | None,
        visible: bool,
        state: Mapping[str, Any],
) -> None:
    set_overlay_text(
        home_score_element,
        visible,
        str(state.get("home_points", 0)),
    )
    set_overlay_text(
        away_score_element,
        visible,
        str(state.get("away_points", 0)),
    )
    set_overlay_text(
        clock_element,
        visible,
        str(state.get("clock", "10:00")),
    )


def update_quarter_overlay(
        quarter_element: Any | None,
        visible: bool,
        state: Mapping[str, Any],
) -> None:
    set_overlay_text(
        quarter_element,
        visible,
        f"Q{state.get('quarter', 1)}",
    )


MILESTONE_DISPLAY_NAMES = {
    "PERSONAL_BEST_POINTS": "CAREER HIGH",
    "Milestone": "CAREER HIGH"
}


def _milestone_show_until(milestone: Mapping[str, Any]) -> float:
    show_until = milestone.get("show_until", 0)
    if isinstance(show_until, bool):
        return 0
    try:
        return float(show_until) + 5000
    except (TypeError, ValueError):
        return 0


def update_milestone_overlays(
        player_element: Any | None,
        text_element: Any | None,
        state: Mapping[str, Any],
        force_visible: bool = False,
) -> None:
    milestone = state.get("milestone")
    show_milestone = force_visible or (
            isinstance(milestone, Mapping)
            and _milestone_show_until(milestone) > int(time.time() * 1000)
    )

    if not show_milestone:
        set_overlay_text(player_element, False, "")
        set_overlay_text(text_element, False, "")
        return

    raw = str(milestone.get("milestone_name", ""))
    display = MILESTONE_DISPLAY_NAMES.get(raw, raw.replace("_", " "))
    set_overlay_text(player_element, True, display)
    set_overlay_text(
        text_element,
        True,
        f"{milestone.get('player_name', '')}  {milestone.get('value_achieved', 0)} PTS",
    )
