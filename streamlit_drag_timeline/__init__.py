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


def drag_timeline(items: t.Sequence[dict[str, t.Any]] | None = None) -> list[dict]:
    """
    Render the drag timeline component.

    Parameters
    ----------
    items:
        List of row dictionaries to render. Each dict should contain the keys
        defined in the timeline contract (row_id, vessel, voyage, terminal,
        berth, start, end, f, e, note, plan_status, pilot).

    Returns
    -------
    list[dict]
        Events emitted by the frontend in the form:
        ``{"row_id": int, "dmin": int, "dy": float}``
    """
    return _drag_timeline(items=items or [], default=[])


__all__ = ["drag_timeline"]
