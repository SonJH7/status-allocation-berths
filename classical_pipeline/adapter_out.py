from __future__ import annotations

import pandas as pd

from classical_pipeline.models import SolverInput, SolverResult


def build_visualization_df(data: SolverInput, result: SolverResult) -> pd.DataFrame:
    out = data.source_df.copy()

    if "row_id" not in out.columns:
        out.insert(0, "row_id", range(1, len(out) + 1))

    for idx, row in out.iterrows():
        vid = int(row["row_id"])
        if vid not in result.mooring_slot:
            continue

        start_slot = result.mooring_slot[vid]
        end_slot = result.departure_slot.get(vid, start_slot + data.processing_time[vid])
        bp = result.berth_section.get(vid, 0)

        start_ts = data.base_time + pd.to_timedelta(start_slot * data.slot_minutes, unit="m")
        end_ts = data.base_time + pd.to_timedelta(end_slot * data.slot_minutes, unit="m")
        out.at[idx, "start"] = start_ts
        out.at[idx, "end"] = end_ts
        out.at[idx, "bp"] = bp
        out.at[idx, "y_m"] = float(bp)

        old_f = row.get("f", None)
        old_e = row.get("e", None)
        if pd.notna(old_f) and pd.notna(old_e):
            span = abs(float(old_e) - float(old_f))
        else:
            span = float(data.vessel_length.get(vid, 150))

        out.at[idx, "f"] = float(bp) - span / 2.0
        out.at[idx, "e"] = float(bp) + span / 2.0
        out.at[idx, "plan_status"] = "CLASSICAL_OPTIMIZED"

        old_note = str(row.get("note", "") or "").strip()
        tag = "Classical(Gurobi) 최적화 반영"
        out.at[idx, "note"] = f"{old_note} | {tag}" if old_note else tag

    return out
