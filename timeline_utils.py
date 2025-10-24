# timeline_utils.py
# Helpers to convert DataFrame <-> vis.js items
from datetime import datetime
import pandas as pd

def df_to_timeline(df: pd.DataFrame, editable: bool = True):
    """Return (items, groups) for st_timeline()"""
    if df is None or df.empty:
        return [], []
    groups = sorted(df["berth"].dropna().astype(str).unique().tolist())
    groups = [{"id": g, "content": g} for g in groups]

    items = []
    for i, r in df.reset_index().iterrows():
        items.append({
            "id": str(i),
            "content": r.get("vessel", f"#{i}"),
            "group": str(r.get("berth")),
            "start": pd.to_datetime(r.get("eta")).isoformat(),
            "end": pd.to_datetime(r.get("etd")).isoformat(),
            "editable": editable,
        })
    return items, groups

def timeline_to_df(df: pd.DataFrame, event: dict, snap_choice: str):
    """Apply a single changed item (id, start, end, group) to dataframe and return updated df."""
    from validate import snap_to_interval
    out = df.copy()
    try:
        idx = int(event["id"])
    except Exception:
        return out
    # bounds
    if "start" in event:
        out.loc[idx, "eta"] = snap_to_interval(pd.to_datetime(event["start"]), snap_choice)
    if "end" in event:
        out.loc[idx, "etd"] = snap_to_interval(pd.to_datetime(event["end"]), snap_choice)
    if "group" in event and pd.notna(event["group"]):
        out.loc[idx, "berth"] = str(event["group"])
    return out

def make_timeline_options(snap_choice: str, editable: bool, start: datetime, end: datetime):
    # We cannot pass JS functions directly; we'll emulate snapping on the Python side.
    # Provide nice defaults for zoom and axis.
    return {
        "stack": False,
        "editable": editable,
        "min": start.isoformat(),
        "max": end.isoformat(),
        "zoomMin": 1000 * 60 * 15,           # 15 minutes
        "zoomMax": 1000 * 60 * 60 * 24 * 14, # 14 days
        "margin": {"item": 6, "axis": 12},
        "orientation": {"axis": "top"},
        "multiselect": False,
        # snap will be handled after events
    }
