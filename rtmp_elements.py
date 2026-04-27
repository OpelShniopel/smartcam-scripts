"""Shared RTMP branch element construction and static overlay setup."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from runtime_paths import (
    BLITZBALL_ACTIVE_PNG, BLITZBALL_SCOREBOARD_PNG, END_STATS_BG_PNG,
    FOULS_AWAY_DIR, FOULS_HOME_DIR,
    SCOREBOARD_PNG, TIMEOUT_BG_PNG,
)

# Sets a keyframe every 60 encoded frames.
RTMP_KEYINT = 60
# Sets the x264 encoder to use 2 threads.
RTMP_THREADS = 2
# Uses x264's fastest preset.
RTMP_PRESET = "ultrafast"
# Tunes x264 for low-latency streaming.
RTMP_TUNE = "zerolatency"

RTMP_OVERLAY_W = 1920
RTMP_OVERLAY_H = 1080

SCOREBOARD_W = 650
SCOREBOARD_H = 130
SCOREBOARD_OFFSET_X = 100
SCOREBOARD_OFFSET_Y = 900

TEXT_HALIGN_LEFT = 0
TEXT_HALIGN_CENTER = 1
TEXT_HALIGN_POSITION = 4
TEXT_VALIGN_TOP = 2
TEXT_VALIGN_POSITION = 3


def _set_if_supported(el: Any, prop: str, value) -> None:
    if el.find_property(prop) is not None:
        el.set_property(prop, value)


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
    osd_blitz_bg: Any
    osd_blitz_active: Any
    osd_blitz_home_name: Any
    osd_blitz_away_name: Any
    osd_blitz_home_pts: Any
    osd_blitz_home_blitz: Any
    osd_blitz_away_pts: Any
    osd_blitz_away_blitz: Any
    osd_blitz_quarter: Any
    osd_blitz_clock: Any
    osd_blitz_home_streak: Any
    osd_blitz_away_streak: Any
    osd_end_bg: Any
    osd_end_winner: Any
    osd_end_header_home: Any
    osd_end_header_away: Any
    osd_end_home_pts: Any
    osd_end_home_inner: Any
    osd_end_home_middle: Any
    osd_end_home_outer: Any
    osd_end_home_blitz: Any
    osd_end_home_blitz_rate: Any
    osd_end_home_intercept: Any
    osd_end_away_pts: Any
    osd_end_away_inner: Any
    osd_end_away_middle: Any
    osd_end_away_outer: Any
    osd_end_away_blitz: Any
    osd_end_away_blitz_rate: Any
    osd_end_away_intercept: Any
    osd_end_players_header: Any
    osd_end_player_h1: Any
    osd_end_player_h2: Any
    osd_end_player_h3: Any
    osd_end_player_a1: Any
    osd_end_player_a2: Any
    osd_end_player_a3: Any
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
            "osd_blitz_bg": self.osd_blitz_bg,
            "osd_blitz_active": self.osd_blitz_active,
            "osd_blitz_home_name": self.osd_blitz_home_name,
            "osd_blitz_away_name": self.osd_blitz_away_name,
            "osd_blitz_home_pts": self.osd_blitz_home_pts,
            "osd_blitz_home_blitz": self.osd_blitz_home_blitz,
            "osd_blitz_away_pts": self.osd_blitz_away_pts,
            "osd_blitz_away_blitz": self.osd_blitz_away_blitz,
            "osd_blitz_quarter": self.osd_blitz_quarter,
            "osd_blitz_clock": self.osd_blitz_clock,
            "osd_blitz_home_streak": self.osd_blitz_home_streak,
            "osd_blitz_away_streak": self.osd_blitz_away_streak,
            "osd_end_bg": self.osd_end_bg,
            "osd_end_winner": self.osd_end_winner,
            "osd_end_header_home": self.osd_end_header_home,
            "osd_end_header_away": self.osd_end_header_away,
            "osd_end_home_pts": self.osd_end_home_pts,
            "osd_end_home_inner": self.osd_end_home_inner,
            "osd_end_home_middle": self.osd_end_home_middle,
            "osd_end_home_outer": self.osd_end_home_outer,
            "osd_end_home_blitz": self.osd_end_home_blitz,
            "osd_end_home_blitz_rate": self.osd_end_home_blitz_rate,
            "osd_end_home_intercept": self.osd_end_home_intercept,
            "osd_end_away_pts": self.osd_end_away_pts,
            "osd_end_away_inner": self.osd_end_away_inner,
            "osd_end_away_middle": self.osd_end_away_middle,
            "osd_end_away_outer": self.osd_end_away_outer,
            "osd_end_away_blitz": self.osd_end_away_blitz,
            "osd_end_away_blitz_rate": self.osd_end_away_blitz_rate,
            "osd_end_away_intercept": self.osd_end_away_intercept,
            "osd_end_players_header": self.osd_end_players_header,
            "osd_end_player_h1": self.osd_end_player_h1,
            "osd_end_player_h2": self.osd_end_player_h2,
            "osd_end_player_h3": self.osd_end_player_h3,
            "osd_end_player_a1": self.osd_end_player_a1,
            "osd_end_player_a2": self.osd_end_player_a2,
            "osd_end_player_a3": self.osd_end_player_a3,
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
            self.osd_blitz_bg,
            self.osd_blitz_active,
            self.osd_blitz_home_name,
            self.osd_blitz_away_name,
            self.osd_blitz_home_pts,
            self.osd_blitz_home_blitz,
            self.osd_blitz_away_pts,
            self.osd_blitz_away_blitz,
            self.osd_blitz_quarter,
            self.osd_blitz_clock,
            self.osd_blitz_home_streak,
            self.osd_blitz_away_streak,
            self.osd_end_bg,
            self.osd_end_winner,
            self.osd_end_header_home,
            self.osd_end_header_away,
            self.osd_end_home_pts,
            self.osd_end_home_inner,
            self.osd_end_home_middle,
            self.osd_end_home_outer,
            self.osd_end_home_blitz,
            self.osd_end_home_blitz_rate,
            self.osd_end_home_intercept,
            self.osd_end_away_pts,
            self.osd_end_away_inner,
            self.osd_end_away_middle,
            self.osd_end_away_outer,
            self.osd_end_away_blitz,
            self.osd_end_away_blitz_rate,
            self.osd_end_away_intercept,
            self.osd_end_players_header,
            self.osd_end_player_h1,
            self.osd_end_player_h2,
            self.osd_end_player_h3,
            self.osd_end_player_a1,
            self.osd_end_player_a2,
            self.osd_end_player_a3,
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
            self.osd_blitz_bg,
            self.osd_blitz_active,
            self.osd_blitz_home_name,
            self.osd_blitz_away_name,
            self.osd_blitz_home_pts,
            self.osd_blitz_home_blitz,
            self.osd_blitz_away_pts,
            self.osd_blitz_away_blitz,
            self.osd_blitz_quarter,
            self.osd_blitz_clock,
            self.osd_blitz_home_streak,
            self.osd_blitz_away_streak,
            self.osd_end_bg,
            self.osd_end_winner,
            self.osd_end_header_home,
            self.osd_end_header_away,
            self.osd_end_home_pts,
            self.osd_end_home_inner,
            self.osd_end_home_middle,
            self.osd_end_home_outer,
            self.osd_end_home_blitz,
            self.osd_end_home_blitz_rate,
            self.osd_end_home_intercept,
            self.osd_end_away_pts,
            self.osd_end_away_inner,
            self.osd_end_away_middle,
            self.osd_end_away_outer,
            self.osd_end_away_blitz,
            self.osd_end_away_blitz_rate,
            self.osd_end_away_intercept,
            self.osd_end_players_header,
            self.osd_end_player_h1,
            self.osd_end_player_h2,
            self.osd_end_player_h3,
            self.osd_end_player_a1,
            self.osd_end_player_a2,
            self.osd_end_player_a3,
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
        osd_blitz_bg=make_element("gdkpixbufoverlay", "strm_osd_blitz_bg"),
        osd_blitz_active=make_element("gdkpixbufoverlay", "strm_osd_blitz_active"),
        osd_blitz_home_name=make_element("textoverlay", "strm_osd_blitz_home_name"),
        osd_blitz_away_name=make_element("textoverlay", "strm_osd_blitz_away_name"),
        osd_blitz_home_pts=make_element("textoverlay", "strm_osd_blitz_home_pts"),
        osd_blitz_home_blitz=make_element("textoverlay", "strm_osd_blitz_home_blitz"),
        osd_blitz_away_pts=make_element("textoverlay", "strm_osd_blitz_away_pts"),
        osd_blitz_away_blitz=make_element("textoverlay", "strm_osd_blitz_away_blitz"),
        osd_blitz_quarter=make_element("textoverlay", "strm_osd_blitz_quarter"),
        osd_blitz_clock=make_element("textoverlay", "strm_osd_blitz_clock"),
        osd_blitz_home_streak=make_element("textoverlay", "strm_osd_blitz_home_streak"),
        osd_blitz_away_streak=make_element("textoverlay", "strm_osd_blitz_away_streak"),
        osd_end_bg=make_element("gdkpixbufoverlay", "strm_osd_end_bg"),
        osd_end_winner=make_element("textoverlay", "strm_osd_end_winner"),
        osd_end_header_home=make_element("textoverlay", "strm_osd_end_header_home"),
        osd_end_header_away=make_element("textoverlay", "strm_osd_end_header_away"),
        osd_end_home_pts=make_element("textoverlay", "strm_osd_end_home_pts"),
        osd_end_home_inner=make_element("textoverlay", "strm_osd_end_home_inner"),
        osd_end_home_middle=make_element("textoverlay", "strm_osd_end_home_middle"),
        osd_end_home_outer=make_element("textoverlay", "strm_osd_end_home_outer"),
        osd_end_home_blitz=make_element("textoverlay", "strm_osd_end_home_blitz"),
        osd_end_home_blitz_rate=make_element("textoverlay", "strm_osd_end_home_blitz_rate"),
        osd_end_home_intercept=make_element("textoverlay", "strm_osd_end_home_intercept"),
        osd_end_away_pts=make_element("textoverlay", "strm_osd_end_away_pts"),
        osd_end_away_inner=make_element("textoverlay", "strm_osd_end_away_inner"),
        osd_end_away_middle=make_element("textoverlay", "strm_osd_end_away_middle"),
        osd_end_away_outer=make_element("textoverlay", "strm_osd_end_away_outer"),
        osd_end_away_blitz=make_element("textoverlay", "strm_osd_end_away_blitz"),
        osd_end_away_blitz_rate=make_element("textoverlay", "strm_osd_end_away_blitz_rate"),
        osd_end_away_intercept=make_element("textoverlay", "strm_osd_end_away_intercept"),
        osd_end_players_header=make_element("textoverlay", "strm_osd_end_players_header"),
        osd_end_player_h1=make_element("textoverlay", "strm_osd_end_player_h1"),
        osd_end_player_h2=make_element("textoverlay", "strm_osd_end_player_h2"),
        osd_end_player_h3=make_element("textoverlay", "strm_osd_end_player_h3"),
        osd_end_player_a1=make_element("textoverlay", "strm_osd_end_player_a1"),
        osd_end_player_a2=make_element("textoverlay", "strm_osd_end_player_a2"),
        osd_end_player_a3=make_element("textoverlay", "strm_osd_end_player_a3"),
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
        anchor_top_left: bool = False,
) -> None:
    element.set_property("text", text)
    element.set_property("font-desc", font)
    if anchor_top_left:
        element.set_property("halignment", TEXT_HALIGN_LEFT)
        element.set_property("valignment", TEXT_VALIGN_TOP)
        element.set_property("xpad", round(RTMP_OVERLAY_W * xpos))
        element.set_property("ypad", round(RTMP_OVERLAY_H * ypos))
    else:
        element.set_property("halignment", TEXT_HALIGN_POSITION)
        element.set_property("valignment", TEXT_VALIGN_POSITION)
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

    def _format_pct(value: Any) -> str:
        try:
            pct = float(value)
        except (TypeError, ValueError):
            pct = 0.0
        if 0.0 <= pct <= 1.0:
            pct *= 100.0
        return f"{pct:.1f}%"

    calling_name = home_name if calling == "home" else away_name
    _set("osd_timeout_header", f"TIMEOUT  {calling_name}")

    _set("osd_timeout_home_name", home_name)
    _set("osd_timeout_home_pts", f"PTS  {home_stats.get('points', 0)}")
    _set("osd_timeout_home_fg", f"FG%  {_format_pct(home_stats.get('fg_pct', 0.0))}")
    _set("osd_timeout_home_3p", f"3P%  {_format_pct(home_stats.get('tp_pct', 0.0))}")
    _set("osd_timeout_home_reb", f"REB  {home_stats.get('rebounds', 0)}")
    _set("osd_timeout_home_ast", f"AST  {home_stats.get('assists', 0)}")
    _set("osd_timeout_home_stl", f"STL  {home_stats.get('steals', 0)}")
    _set("osd_timeout_home_blk", f"BLK  {home_stats.get('blocks', 0)}")
    _set("osd_timeout_home_foul", f"FOULS  {home_stats.get('fouls', 0)}")

    _set("osd_timeout_away_name", away_name)
    _set("osd_timeout_away_pts", f"PTS  {away_stats.get('points', 0)}")
    _set("osd_timeout_away_fg", f"FG%  {_format_pct(away_stats.get('fg_pct', 0.0))}")
    _set("osd_timeout_away_3p", f"3P%  {_format_pct(away_stats.get('tp_pct', 0.0))}")
    _set("osd_timeout_away_reb", f"REB  {away_stats.get('rebounds', 0)}")
    _set("osd_timeout_away_ast", f"AST  {away_stats.get('assists', 0)}")
    _set("osd_timeout_away_stl", f"STL  {away_stats.get('steals', 0)}")
    _set("osd_timeout_away_blk", f"BLK  {away_stats.get('blocks', 0)}")
    _set("osd_timeout_away_foul", f"FOULS  {away_stats.get('fouls', 0)}")

    top_players = timeout_stats.get("top_players") or []
    home_team_id = home_stats.get("team_id")
    away_team_id = away_stats.get("team_id")
    home_players = [p for p in top_players if p.get("team_id") == home_team_id][:3]
    away_players = [p for p in top_players if p.get("team_id") == away_team_id][:3]

    for i, slot in enumerate(("osd_timeout_player_h1", "osd_timeout_player_h2", "osd_timeout_player_h3")):
        el = els.get(slot)
        if not el:
            continue
        if i < len(home_players):
            p = home_players[i]
            el.set_property("text",
                            f"{p.get('player_name', '')}  {p.get('points', 0)}pts  {p.get('rebounds', 0)}reb  {p.get('assists', 0)}ast")
            el.set_property("silent", False)
        else:
            el.set_property("silent", True)

    for i, slot in enumerate(("osd_timeout_player_a1", "osd_timeout_player_a2", "osd_timeout_player_a3")):
        el = els.get(slot)
        if not el:
            continue
        if i < len(away_players):
            p = away_players[i]
            el.set_property("text",
                            f"{p.get('player_name', '')}  {p.get('points', 0)}pts  {p.get('rebounds', 0)}reb  {p.get('assists', 0)}ast")
            el.set_property("silent", False)
        else:
            el.set_property("silent", True)


def foul_png_path(team: str, count: int) -> str | None:
    if count <= 0:
        return None
    count = min(count, 5)
    base = FOULS_HOME_DIR if team == "home" else FOULS_AWAY_DIR
    return os.path.join(base, f"{count}.png")


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

    setup_text_overlay(elements.osd_timeout_header, "", xpos=0.510, ypos=0.138, font="Sans Bold 32", color=0xFFFFFFFF)

    setup_text_overlay(elements.osd_timeout_home_name, "", xpos=0.370, ypos=0.225, font="Sans Bold 20",
                       color=0xFF6B00FF)
    setup_text_overlay(elements.osd_timeout_home_pts, "", xpos=0.370, ypos=0.290, font="Sans Bold 18", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_home_fg, "", xpos=0.370, ypos=0.340, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_3p, "", xpos=0.370, ypos=0.385, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_reb, "", xpos=0.370, ypos=0.430, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_ast, "", xpos=0.370, ypos=0.475, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_stl, "", xpos=0.370, ypos=0.520, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_blk, "", xpos=0.370, ypos=0.565, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_home_foul, "", xpos=0.370, ypos=0.610, font="Sans 16", color=0xCCCCCCFF)

    setup_text_overlay(elements.osd_timeout_away_name, "", xpos=0.640, ypos=0.225, font="Sans Bold 20",
                       color=0xFF6B00FF)
    setup_text_overlay(elements.osd_timeout_away_pts, "", xpos=0.640, ypos=0.290, font="Sans Bold 18", color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_timeout_away_fg, "", xpos=0.640, ypos=0.340, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_3p, "", xpos=0.640, ypos=0.385, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_reb, "", xpos=0.640, ypos=0.430, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_ast, "", xpos=0.640, ypos=0.475, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_stl, "", xpos=0.640, ypos=0.520, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_blk, "", xpos=0.640, ypos=0.565, font="Sans 16", color=0xCCCCCCFF)
    setup_text_overlay(elements.osd_timeout_away_foul, "", xpos=0.640, ypos=0.610, font="Sans 16", color=0xCCCCCCFF)

    setup_text_overlay(elements.osd_timeout_player_h1, "", xpos=0.270, ypos=0.740, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)
    setup_text_overlay(elements.osd_timeout_player_h2, "", xpos=0.270, ypos=0.800, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)
    setup_text_overlay(elements.osd_timeout_player_h3, "", xpos=0.270, ypos=0.860, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)

    setup_text_overlay(elements.osd_timeout_player_a1, "", xpos=0.515, ypos=0.740, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)
    setup_text_overlay(elements.osd_timeout_player_a2, "", xpos=0.515, ypos=0.800, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)
    setup_text_overlay(elements.osd_timeout_player_a3, "", xpos=0.515, ypos=0.860, font="Sans Bold 15",
                       color=0xFFFFFFFF, anchor_top_left=True)


BLITZ_TEXT_KEYS: tuple[str, ...] = (
    "osd_blitz_home_name", "osd_blitz_away_name",
    "osd_blitz_home_pts", "osd_blitz_home_blitz",
    "osd_blitz_away_pts", "osd_blitz_away_blitz",
    "osd_blitz_quarter", "osd_blitz_clock",
    "osd_blitz_home_streak", "osd_blitz_away_streak",
)
BLITZ_PIXEL_KEYS: tuple[str, ...] = ("osd_blitz_bg", "osd_blitz_active")

_BLITZ_SCOREBOARD_TEXT_KEYS: tuple[str, ...] = (
    "osd_quarter", "osd_home", "osd_away",
    "osd_home_score", "osd_away_score", "osd_clock",
    "osd_milestone_player", "osd_milestone_text",
)


def configure_blitzball_overlay(elements: RtmpElements) -> None:
    bg = elements.osd_blitz_bg
    bg.set_property("location", BLITZBALL_SCOREBOARD_PNG)
    bg.set_property("offset-x", 465)
    bg.set_property("offset-y", 900)
    bg.set_property("overlay-width", 990)
    bg.set_property("overlay-height", 180)
    bg.set_property("alpha", 0.0)

    active = elements.osd_blitz_active
    active.set_property("location", BLITZBALL_ACTIVE_PNG)
    active.set_property("offset-x", 465)
    active.set_property("offset-y", 900)
    active.set_property("overlay-width", 990)
    active.set_property("overlay-height", 180)
    active.set_property("alpha", 0.0)

    setup_text_overlay(elements.osd_blitz_quarter, "", xpos=0.710, ypos=0.895, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_clock, "", xpos=0.710, ypos=0.945, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_home_name, "", xpos=0.450, ypos=0.888, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_away_name, "", xpos=0.450, ypos=0.950, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_home_pts, "", xpos=0.614, ypos=0.895, font="Ubuntu Mono Bold 42",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_home_blitz, "", xpos=0.640, ypos=0.895, font="Ubuntu Mono Bold 42",
                       color=0xFFFFD700)  # gold
    setup_text_overlay(elements.osd_blitz_away_pts, "", xpos=0.614, ypos=0.952, font="Ubuntu Mono Bold 42",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_blitz_away_blitz, "", xpos=0.640, ypos=0.952, font="Ubuntu Mono Bold 42",
                       color=0xFFFFD700)  # gold
    setup_text_overlay(elements.osd_blitz_home_streak, "", xpos=0.500, ypos=0.845, font="Ubuntu Mono Bold 22",
                       color=0xFF4500FF)
    setup_text_overlay(elements.osd_blitz_away_streak, "", xpos=0.500, ypos=0.888, font="Ubuntu Mono Bold 22",
                       color=0xFF4500FF)


def update_blitzball_overlay(state: Mapping[str, Any], els: Mapping[str, Any]) -> bool:
    """Update blitzball overlay. Returns True if blitz pulse should be active."""
    sport_code = state.get("sport_code", "")

    if sport_code != "BLITZBALL":
        for key in BLITZ_TEXT_KEYS:
            el = els.get(key)
            if el:
                el.set_property("silent", True)
        for key in BLITZ_PIXEL_KEYS:
            el = els.get(key)
            if el:
                el.set_property("alpha", 0.0)
        return False

    # Hide regular scoreboard elements
    osd_bg = els.get("osd_bg")
    if osd_bg:
        osd_bg.set_property("alpha", 0.0)
    for key in ("osd_home_fouls_bar", "osd_away_fouls_bar"):
        el = els.get(key)
        if el:
            el.set_property("alpha", 0.0)
    for key in _BLITZ_SCOREBOARD_TEXT_KEYS:
        el = els.get(key)
        if el:
            el.set_property("silent", True)

    visible = state.get("visible", False)
    blitz_bg = els.get("osd_blitz_bg")
    if blitz_bg:
        blitz_bg.set_property("alpha", 1.0 if visible else 0.0)

    if not visible:
        for key in BLITZ_TEXT_KEYS:
            el = els.get(key)
            if el:
                el.set_property("silent", True)
        blitz_active_el = els.get("osd_blitz_active")
        if blitz_active_el:
            blitz_active_el.set_property("alpha", 0.0)
        return False

    home_name = str(state.get("home_name", "HOME"))
    away_name = str(state.get("away_name", "AWAY"))

    set_overlay_text(els.get("osd_blitz_home_name"), True, home_name)
    set_overlay_text(els.get("osd_blitz_away_name"), True, away_name)

    home_pts = state.get("home_points", 0)
    away_pts = state.get("away_points", 0)
    home_blitz = state.get("home_blitz_score", 0)
    away_blitz = state.get("away_blitz_score", 0)
    set_overlay_text(els.get("osd_blitz_home_pts"), True, str(home_pts))
    home_blitz_el = els.get("osd_blitz_home_blitz")
    if home_blitz_el:
        home_blitz_el.set_property("silent", False)
        home_blitz_el.set_property("text", f":{home_blitz}")
        home_blitz_el.set_property("color", 0xFFFFD700)  # gold
    set_overlay_text(els.get("osd_blitz_away_pts"), True, str(away_pts))
    away_blitz_el = els.get("osd_blitz_away_blitz")
    if away_blitz_el:
        away_blitz_el.set_property("silent", False)
        away_blitz_el.set_property("text", f":{away_blitz}")
        away_blitz_el.set_property("color", 0xFFFFD700)  # gold

    quarter = state.get("quarter", 1)
    quarter_text = f"H{quarter}" if quarter <= 2 else "H2"
    set_overlay_text(els.get("osd_blitz_quarter"), True, quarter_text)
    set_overlay_text(els.get("osd_blitz_clock"), True, str(state.get("clock", "10:00")))

    blitz_active = bool(state.get("blitz_active", False))
    if not blitz_active:
        blitz_active_el = els.get("osd_blitz_active")
        if blitz_active_el:
            blitz_active_el.set_property("alpha", 0.0)

    set_overlay_text(
        els.get("osd_blitz_home_streak"),
        bool(state.get("home_hot_streak", False)),
        "🔥",
    )
    set_overlay_text(
        els.get("osd_blitz_away_streak"),
        bool(state.get("away_hot_streak", False)),
        "🔥",
    )

    return blitz_active


def configure_rtmp_encoder(enc: Any, bitrate: int) -> None:
    # Uses constant bitrate mode.
    enc.set_property("pass", "cbr")
    # Sets the RTMP video bitrate in kbps.
    enc.set_property("bitrate", bitrate)
    # Sets the encoder's VBV buffer size.
    enc.set_property("vbv-buf-capacity", 200)
    enc.set_property("tune", RTMP_TUNE)
    enc.set_property("speed-preset", RTMP_PRESET)
    enc.set_property("key-int-max", RTMP_KEYINT)
    enc.set_property("threads", RTMP_THREADS)
    _set_if_supported(enc, "byte-stream", True)
    _set_if_supported(enc, "bframes", 0)
    _set_if_supported(enc, "ref", 1)
    _set_if_supported(enc, "sliced-threads", True)


def configure_rtmp_output(elements: RtmpElements, rtmp_url: str) -> None:
    # Makes the FLV output streamable.
    elements.flvmux.set_property("streamable", True)
    # Sets the RTMP destination URL.
    elements.rtmpsink.set_property("location", rtmp_url)
    # Starts the RTMP sink synchronously.
    elements.rtmpsink.set_property("async", False)
    # Generates a simple audio source for the stream.
    elements.audiosrc.set_property("wave", 4)
    _set_if_supported(elements.audiosrc, "is-live", True)
    # Sets the AAC audio bitrate.
    elements.aacenc.set_property("bitrate", 128000)


END_STATS_TEXT_KEYS: tuple[str, ...] = (
    "osd_end_winner",
    "osd_end_header_home", "osd_end_header_away",
    "osd_end_home_pts", "osd_end_home_inner", "osd_end_home_middle",
    "osd_end_home_outer", "osd_end_home_blitz", "osd_end_home_blitz_rate",
    "osd_end_home_intercept",
    "osd_end_away_pts", "osd_end_away_inner", "osd_end_away_middle",
    "osd_end_away_outer", "osd_end_away_blitz", "osd_end_away_blitz_rate",
    "osd_end_away_intercept",
    "osd_end_players_header",
    "osd_end_player_h1", "osd_end_player_h2", "osd_end_player_h3",
    "osd_end_player_a1", "osd_end_player_a2", "osd_end_player_a3",
)


def configure_end_stats_overlay(elements: RtmpElements) -> None:
    bg = elements.osd_end_bg
    bg.set_property("location", END_STATS_BG_PNG)
    bg.set_property("offset-x", 0)
    bg.set_property("offset-y", 0)
    bg.set_property("overlay-width", 1920)
    bg.set_property("overlay-height", 1080)
    bg.set_property("alpha", 0.0)

    # Winner — horizontally centered
    w = elements.osd_end_winner
    w.set_property("text", "")
    w.set_property("font-desc", "Ubuntu Mono Bold 52")
    w.set_property("halignment", TEXT_HALIGN_CENTER)
    w.set_property("valignment", TEXT_VALIGN_POSITION)
    w.set_property("ypos", 0.080)
    w.set_property("color", 0xFFFFD700)
    w.set_property("draw-shadow", True)
    w.set_property("auto-resize", False)
    w.set_property("wait-text", False)
    w.set_property("silent", True)

    # Two-column layout: left col at 0.080 (154px), right col at 0.520 (998px)
    # Each column is left-aligned; together they span symmetrically around center.
    setup_text_overlay(elements.osd_end_header_home, "", xpos=0.180, ypos=0.200, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_end_header_away, "", xpos=0.720, ypos=0.200, font="Ubuntu Mono Bold 32",
                       color=0xFFFFFFFF)

    setup_text_overlay(elements.osd_end_home_pts, "", xpos=0.180, ypos=0.290, font="Ubuntu Mono Bold 26",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_end_home_blitz, "", xpos=0.180, ypos=0.360, font="Ubuntu Mono Bold 26",
                       color=0xFFFFD700)
    setup_text_overlay(elements.osd_end_home_inner, "", xpos=0.180, ypos=0.430, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_home_middle, "", xpos=0.180, ypos=0.490, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_home_outer, "", xpos=0.180, ypos=0.550, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_home_intercept, "", xpos=0.180, ypos=0.610, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)

    setup_text_overlay(elements.osd_end_away_pts, "", xpos=0.720, ypos=0.290, font="Ubuntu Mono Bold 26",
                       color=0xFFFFFFFF)
    setup_text_overlay(elements.osd_end_away_blitz, "", xpos=0.720, ypos=0.360, font="Ubuntu Mono Bold 26",
                       color=0xFFFFD700)
    setup_text_overlay(elements.osd_end_away_inner, "", xpos=0.720, ypos=0.430, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_away_middle, "", xpos=0.720, ypos=0.490, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_away_outer, "", xpos=0.720, ypos=0.550, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)
    setup_text_overlay(elements.osd_end_away_intercept, "", xpos=0.720, ypos=0.610, font="Ubuntu Mono Bold 22",
                       color=0xFFCCCCCC)

    # Unused elements — silenced
    for el in (elements.osd_end_home_blitz_rate, elements.osd_end_away_blitz_rate,
               elements.osd_end_players_header,
               elements.osd_end_player_h1, elements.osd_end_player_h2, elements.osd_end_player_h3,
               elements.osd_end_player_a1, elements.osd_end_player_a2, elements.osd_end_player_a3):
        el.set_property("silent", True)


def update_blitzball_end_stats(state: Mapping[str, Any], els: Mapping[str, Any]) -> bool:
    """Show end-game stats overlay. Returns True if the overlay is active."""
    end_stats = state.get("end_stats")
    game_finished = state.get("game_finished", False)
    now_ms = int(time.time() * 1000)

    active = (
            game_finished
            and isinstance(end_stats, dict)
            and end_stats.get("show_until", 0) > now_ms
    )

    if not active:
        end_bg = els.get("osd_end_bg")
        if end_bg:
            end_bg.set_property("alpha", 0.0)
        for key in END_STATS_TEXT_KEYS:
            el = els.get(key)
            if el:
                el.set_property("silent", True)
        return False

    end_bg = els.get("osd_end_bg")
    if end_bg:
        end_bg.set_property("alpha", 0.85)

    # Hide regular and blitz scoreboards
    for key in ("osd_bg", "osd_home_fouls_bar", "osd_away_fouls_bar"):
        el = els.get(key)
        if el:
            el.set_property("alpha", 0.0)
    for key in _BLITZ_SCOREBOARD_TEXT_KEYS:
        el = els.get(key)
        if el:
            el.set_property("silent", True)
    for key in BLITZ_TEXT_KEYS:
        el = els.get(key)
        if el:
            el.set_property("silent", True)
    for key in BLITZ_PIXEL_KEYS:
        el = els.get(key)
        if el:
            el.set_property("alpha", 0.0)

    winner = state.get("winner", "")
    home_name = state.get("home_name", "HOME")
    away_name = state.get("away_name", "AWAY")

    if winner == "home":
        winner_text = f"BLITZ  {home_name} WINS!"
    elif winner == "away":
        winner_text = f"BLITZ  {away_name} WINS!"
    else:
        winner_text = "BLITZ  DRAW!"

    winner_el = els.get("osd_end_winner")
    if winner_el:
        winner_el.set_property("text", winner_text)
        winner_el.set_property("silent", False)
        winner_el.set_property("color", 0xFFFFD700)

    header_home = els.get("osd_end_header_home")
    if header_home:
        header_home.set_property("text", home_name)
        header_home.set_property("silent", False)
    header_away = els.get("osd_end_header_away")
    if header_away:
        header_away.set_property("text", away_name)
        header_away.set_property("silent", False)

    home = end_stats.get("home_stats", {})
    away = end_stats.get("away_stats", {})

    _end_stat_pairs = [
        ("osd_end_home_pts", f"TOTAL  {home.get('total_points', 0)} PTS"),
        ("osd_end_home_inner", f"INNER  {home.get('inner_scores', 0)}"),
        ("osd_end_home_middle", f"MIDDLE {home.get('middle_scores', 0)}"),
        ("osd_end_home_outer", f"OUTER  {home.get('outer_scores', 0)}"),
        ("osd_end_home_blitz", f"BLITZ  {home.get('blitz_scores', 0)}"),
        ("osd_end_home_blitz_rate", f"BLITZ% {home.get('blitz_conversion_rate', 0):.0f}%"),
        ("osd_end_home_intercept", f"INTERCEPTS {home.get('interceptions', 0)}"),
        ("osd_end_away_pts", f"TOTAL  {away.get('total_points', 0)} PTS"),
        ("osd_end_away_inner", f"INNER  {away.get('inner_scores', 0)}"),
        ("osd_end_away_middle", f"MIDDLE {away.get('middle_scores', 0)}"),
        ("osd_end_away_outer", f"OUTER  {away.get('outer_scores', 0)}"),
        ("osd_end_away_blitz", f"BLITZ  {away.get('blitz_scores', 0)}"),
        ("osd_end_away_blitz_rate", f"BLITZ% {away.get('blitz_conversion_rate', 0):.0f}%"),
        ("osd_end_away_intercept", f"INTERCEPTS {away.get('interceptions', 0)}"),
    ]
    for key, text in _end_stat_pairs:
        el = els.get(key)
        if el:
            el.set_property("text", text)
            el.set_property("silent", False)

    players = end_stats.get("players", [])
    home_team_id = home.get("team_id")
    away_team_id = away.get("team_id")
    home_players = [p for p in players if p.get("team_id") == home_team_id][:3]
    away_players = [p for p in players if p.get("team_id") == away_team_id][:3]

    home_slots = ["osd_end_player_h1", "osd_end_player_h2", "osd_end_player_h3"]
    away_slots = ["osd_end_player_a1", "osd_end_player_a2", "osd_end_player_a3"]

    for i, slot in enumerate(home_slots):
        el = els.get(slot)
        if not el:
            continue
        if i < len(home_players):
            p = home_players[i]
            el.set_property("text",
                            f"{p.get('player_name', '')}  {p.get('points', 0)}pts  BLITZ:{p.get('blitz_scores', 0)}")
            el.set_property("silent", False)
        else:
            el.set_property("silent", True)

    for i, slot in enumerate(away_slots):
        el = els.get(slot)
        if not el:
            continue
        if i < len(away_players):
            p = away_players[i]
            el.set_property("text",
                            f"{p.get('player_name', '')}  {p.get('points', 0)}pts  BLITZ:{p.get('blitz_scores', 0)}")
            el.set_property("silent", False)
        else:
            el.set_property("silent", True)

    players_header = els.get("osd_end_players_header")
    if players_header:
        players_header.set_property("text", "TOP PLAYERS")
        players_header.set_property("silent", False)

    return True


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
    configure_blitzball_overlay(elements)
    configure_end_stats_overlay(elements)
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
