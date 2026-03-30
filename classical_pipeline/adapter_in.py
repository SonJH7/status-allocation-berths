from __future__ import annotations

import math

import pandas as pd

from classical_pipeline.models import SolverConfig, SolverInput


def _to_slot(ts: pd.Timestamp, base_time: pd.Timestamp, slot_minutes: int) -> int:
    delta_min = (ts - base_time).total_seconds() / 60.0
    return int(math.floor(delta_min / float(slot_minutes)))


def _safe_int(v, default: int) -> int:
    try:
        if pd.isna(v):
            return default
        return int(round(float(v)))
    except Exception:
        return default


def _default_crane_bounds() -> tuple[dict[int, int], dict[int, int]]:
    lg = {
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 300,
        5: 300,
        6: 478,
        7: 492,
        8: 562,
        9: 687,
        10: 720,
        11: 721,
        12: 800,
        13: 973,
        14: 973,
        15: 1048,
    }
    rg = {
        0: 323,
        1: 324,
        2: 352,
        3: 653,
        4: 654,
        5: 686,
        6: 890,
        7: 891,
        8: 1031,
        9: 1106,
        10: 1172,
        11: 1256,
        12: 1500,
        13: 1500,
        14: 1500,
        15: 1500,
    }
    return lg, rg


def build_solver_input(df: pd.DataFrame, config: SolverConfig) -> SolverInput:
    work = df.copy()
    if work.empty:
        raise ValueError("클래식 최적화 입력 데이터가 비어 있습니다.")
    if "start" not in work.columns or "end" not in work.columns:
        raise ValueError("입력 데이터에 start/end 컬럼이 필요합니다.")

    work["start"] = pd.to_datetime(work["start"], errors="coerce")
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    work = work.dropna(subset=["start", "end"]).copy()
    if work.empty:
        raise ValueError("유효한 start/end 값이 없어 최적화를 수행할 수 없습니다.")

    if "row_id" not in work.columns:
        work.insert(0, "row_id", range(1, len(work) + 1))

    base_time = work["start"].min().normalize()

    eta: dict[int, int] = {}
    ata: dict[int, int | None] = {}
    processing_time: dict[int, int] = {}
    vessel_length: dict[int, int] = {}
    berthing_position: dict[int, int | None] = {}
    containers: dict[int, int] = {}
    weights: dict[int, int] = {}
    vessel_ids: list[int] = []

    for row in work.itertuples(index=False):
        vid = int(getattr(row, "row_id"))
        s_ts = getattr(row, "start")
        e_ts = getattr(row, "end")
        if pd.isna(s_ts) or pd.isna(e_ts):
            continue

        eta_slot = _to_slot(pd.Timestamp(s_ts), base_time, config.slot_minutes)
        end_slot = _to_slot(pd.Timestamp(e_ts), base_time, config.slot_minutes)
        proc = max(1, end_slot - eta_slot)

        length_from_col = None
        if hasattr(row, "Length_m"):
            length_from_col = getattr(row, "Length_m")
        elif hasattr(row, "_asdict") and "Length(m)" in row._asdict():
            length_from_col = row._asdict()["Length(m)"]
        elif hasattr(row, "_asdict") and "length_m" in row._asdict():
            length_from_col = row._asdict()["length_m"]

        if length_from_col is None:
            f_val = getattr(row, "f", None)
            e_val = getattr(row, "e", None)
            if pd.notna(f_val) and pd.notna(e_val):
                length_from_col = abs(float(e_val) - float(f_val))

        length_m = _safe_int(length_from_col, config.default_vessel_length)
        bp = getattr(row, "bp", None)
        if pd.isna(bp):
            bp = getattr(row, "f", None)

        cont_val = 200
        if hasattr(row, "_asdict"):
            rdict = row._asdict()
            for ccol in ("containers", "container", "Import", "Export", "import", "export"):
                if ccol in rdict and pd.notna(rdict[ccol]):
                    cont_val = max(cont_val, _safe_int(rdict[ccol], 200))

        eta[vid] = eta_slot
        ata[vid] = None
        processing_time[vid] = proc
        vessel_length[vid] = max(1, length_m)
        berthing_position[vid] = _safe_int(bp, 0) if bp is not None else None
        containers[vid] = cont_val
        weights[vid] = 1
        vessel_ids.append(vid)

    if not vessel_ids:
        raise ValueError("최적화 대상 선박이 없습니다.")

    lg, rg = _default_crane_bounds()
    cmax: dict[int, int] = {}
    for vid in vessel_ids:
        length = vessel_length[vid]
        if length < 150:
            cmax[vid] = 2
        elif length < 200:
            cmax[vid] = 3
        else:
            cmax[vid] = 4

    return SolverInput(
        source_df=work,
        vessel_ids=vessel_ids,
        eta=eta,
        ata=ata,
        processing_time=processing_time,
        vessel_length=vessel_length,
        berthing_position=berthing_position,
        containers=containers,
        weights=weights,
        base_time=base_time,
        slot_minutes=config.slot_minutes,
        lg=lg,
        rg=rg,
        cmax=cmax,
    )
