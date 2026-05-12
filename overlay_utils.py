from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class OverlayElement(Protocol):
    def set_property(self, name: str, value: Any) -> None:
        ...


OverlayMap = Mapping[str, OverlayElement]


def set_overlay_alpha(els: OverlayMap, key: str, alpha: float) -> None:
    element = els.get(key)
    if element:
        element.set_property("alpha", alpha)


def set_overlay_silent(els: OverlayMap, key: str, silent: bool) -> None:
    element = els.get(key)
    if element:
        element.set_property("silent", silent)
