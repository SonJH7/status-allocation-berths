from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class SolverConfig:
    slot_minutes: int = 60
    berth_length: int = 1500
    window_size: int = 24
    overlap: int = 8
    time_limit_sec: int = 100
    horizon_padding: int = 30
    default_vessel_length: int = 150
    safety_time_gap: int = 2
    safety_space_gap: int = 10
    use_qcap: bool = True
    quay_cranes: int = 15
    service_rate: int = 20


@dataclass
class SolverInput:
    source_df: pd.DataFrame
    vessel_ids: list[int]
    eta: dict[int, int]
    ata: dict[int, Optional[int]]
    processing_time: dict[int, int]
    vessel_length: dict[int, int]
    berthing_position: dict[int, Optional[int]]
    containers: dict[int, int]
    weights: dict[int, int]
    base_time: pd.Timestamp
    slot_minutes: int
    lg: dict[int, int]
    rg: dict[int, int]
    cmax: dict[int, int]


@dataclass
class SolverResult:
    mooring_slot: dict[int, int]
    departure_slot: dict[int, int]
    berth_section: dict[int, int]
    status: str
    message: str
