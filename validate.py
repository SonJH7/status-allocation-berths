# validate.py
from datetime import datetime, timedelta
import math
import pandas as pd

def snap_to_interval(dt: datetime, interval: str = "1h") -> datetime:
    if pd.isna(dt):
        return dt
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    if interval == "1h":
        minutes = 60
    elif interval == "30m":
        minutes = 30
    elif interval == "15m":
        minutes = 15
    else:
        minutes = 60
    discard = dt.minute % minutes
    return dt.replace(minute=dt.minute - discard, second=0, microsecond=0)

def validate_temporal_overlaps(df: pd.DataFrame):
    """Check same-berth time overlaps"""
    viol = []
    if df.empty:
        return viol
    d = df.copy().sort_values(["berth","eta"])
    for berth, g in d.groupby("berth"):
        prev = None
        for _, r in g.iterrows():
            if prev is not None and prev["etd"] > r["eta"]:
                viol.append(f"[시간겹침] berth {berth}: {prev['vessel']} ↔ {r['vessel']}")
            prev = r
    return viol

def validate_spatial_gap(df: pd.DataFrame, min_gap_m: int = 30):
    """Check concurrent vessels on same berth have >= min_gap_m between [start_meter, start_meter+loa_m] intervals"""
    viol = []
    need_cols = {"loa_m","start_meter","berth","eta","etd","vessel"}
    if not need_cols.issubset(set(df.columns)):
        return viol
    d = df.dropna(subset=["loa_m", "start_meter"]).copy()
    for berth, g in d.groupby("berth"):
        arr = g.to_dict("records")
        for i in range(len(arr)):
            for j in range(i+1, len(arr)):
                a, b = arr[i], arr[j]
                # time overlap?
                if a["etd"] <= b["eta"] or b["etd"] <= a["eta"]:
                    continue
                a_start, a_end = a["start_meter"], a["start_meter"] + a["loa_m"]
                b_start, b_end = b["start_meter"], b["start_meter"] + b["loa_m"]
                gap = max(b_start - a_end, a_start - b_end)
                if gap < min_gap_m:
                    viol.append(f"[30m이격위반] berth {berth}: {a['vessel']} ↔ {b['vessel']} (gap={gap:.1f}m)")
    return viol
