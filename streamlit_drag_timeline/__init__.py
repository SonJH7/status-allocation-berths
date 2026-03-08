"""
Streamlit wrapper for the React drag & drop timeline component.

The frontend is built with Vite and served from ``frontend/build``.
"""

from __future__ import annotations

import os
import typing as t

import streamlit.components.v1 as components

_COMP_DIR = os.path.join(os.path.dirname(__file__), "frontend", "build")
_drag_timeline = components.declare_component("drag_timeline", path=_COMP_DIR)


def drag_timeline(items: t.Sequence[dict[str, t.Any]] | None = None) -> dict[str, t.Any]:
    """
    Render the drag timeline component.

    Returns a payload like:
    {
        "event_id": str,
        "events": [
            {
                "row_id": int,
                "dmin": int,
                "dy": float,
                "target_terminal": str,
                "target_berth": int,
                "target_y_m": float,
                "target_f": float,
                "target_e": float,
            }
        ]
    }
    """
    return _drag_timeline(items=items or [], default={})


__all__ = ["drag_timeline"]
