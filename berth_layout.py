"""공통 선석 배치 계산 로직과 상수."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# 선석 배치와 관련된 기본 상수
# ---------------------------------------------------------------------------

BP_BASELINE_M = 1500.0
"""B.P. 기준 좌표 (m)."""

BERTH_VERTICAL_SPAN_PX = 300.0
"""vis.js 타임라인에서 선석 한 줄이 차지하는 높이(px)."""


# 실제 B.P. 좌표계를 기준으로 한 선석별 미터 범위.
# B.P. 1500m에서 0m 방향으로 내려오면서 300m 폭으로 선석이 배치된다.
BERTH_METER_RANGES: Dict[str, Tuple[float, float]] = {
    "1": (0.0, 300.0),
    "2": (300.0, 600.0),
    "3": (600.0, 900.0),
    "4": (900.0, 1200.0),
    "5": (1200.0, 1500.0),
}


# ---------------------------------------------------------------------------
# 공통 유틸리티
# ---------------------------------------------------------------------------

def normalize_berth_label(value) -> str:
    """선석 라벨을 숫자 위주로 정규화한다."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    # 괄호 앞쪽에 선석 번호가 있으면 우선 사용
    if "(" in text:
        prefix = text[: text.find("(")]
        prefix_digits = "".join(ch for ch in prefix if ch.isdigit())
        if prefix_digits:
            return str(int(prefix_digits))

    if "(" in text and ")" in text:
        start = text.find("(") + 1
        end = text.find(")", start)
        if end > start:
            inside = "".join(ch for ch in text[start:end] if ch.isdigit())
            if inside:
                return str(int(inside))

    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return str(int(digits))

    return text


def extract_meter_range(row: pd.Series) -> Tuple[Optional[float], Optional[float]]:
    """데이터 행에서 F/E 위치 기반의 미터 범위를 추출."""

    def _to_float(value) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    values: List[float] = []
    for key in ("start_meter", "end_meter", "f_pos", "e_pos"):
        converted = _to_float(row.get(key))
        if converted is not None:
            values.append(converted)

    if not values:
        return None, None

    lower = min(values)
    upper = max(values)
    return lower, upper


def get_berth_meter_range(berth_value: object) -> Optional[Tuple[float, float]]:
    """선석 라벨에 대응하는 미터 범위를 조회."""

    key = normalize_berth_label(berth_value)
    if not key:
        return None
    return BERTH_METER_RANGES.get(key)


def resolve_berth_span(row: pd.Series) -> Optional[float]:
    berth_range = get_berth_meter_range(row.get("berth"))
    if berth_range is None:
        return None
    start, end = berth_range
    span = float(end - start)
    if span <= 0:
        return None
    return span


def compute_item_height(row: pd.Series) -> float:
    """아이템 높이(px)를 F/E 미터 범위에 따라 계산."""
    start_meter, end_meter = extract_meter_range(row)
    if start_meter is not None and end_meter is not None:
        lower = float(min(start_meter, end_meter))
        upper = float(max(start_meter, end_meter))
        span = upper - lower
        if span > 0:
            # F/E 범위에 따라 높이를 설정하되 최소 높이를 24px로 보장
            return float(max(24.0, span))

    # F/E 정보가 없으면 LOA(선박 길이)를 사용
    length_val = row.get("loa_m")
    if length_val is None or pd.isna(length_val):
        length_val = row.get("length_m")
    try:
        numeric = float(length_val)
    except (TypeError, ValueError):
        numeric = None

    # LOA도 없으면 기본 높이 86.0 사용
    if numeric is None or pd.isna(numeric):
        return 86.0
    
    # LOA를 높이로 사용하되 최소 높이 보장
    return float(max(24.0, numeric))


def compute_item_offset(row: pd.Series, item_height: float) -> float:
    """선석 내에서 아이템 상단 여백(px)을 계산.
    표준 좌표계(0m이 위쪽)를 기준으로, 아이템의 위치를 소속된 선석 그룹에 상대적으로 계산한다.
    """

    def _to_float(value) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # 표준 좌표계이므로 아이템 상단 기준점은 F/E 값 중 더 작은 값.
    start_meter, _ = extract_meter_range(row)
    top_anchor = _to_float(start_meter)

    berth_range = get_berth_meter_range(row.get("berth"))

    if top_anchor is None or berth_range is None or berth_range[0] is None:
        # 위치 정보가 없으면 그룹 중앙에 배치
        return (BERTH_VERTICAL_SPAN_PX - item_height) / 2.0

    berth_start = float(berth_range[0])

    # 아이템의 절대 시작 위치에서 선석 그룹의 시작 위치를 뺀 값이
    # 그룹 내에서의 상대적 오프셋이 된다.
    offset = top_anchor - berth_start
    
    return float(offset)


def snap_to_interval(ts: pd.Timestamp, snap_choice: str) -> pd.Timestamp:
    """지정된 스냅 간격(1h/30m/15m)에 맞춰 Timestamp 보정."""

    if pd.isna(ts):
        return ts

    if not isinstance(ts, pd.Timestamp):
        ts = pd.to_datetime(ts, errors="coerce")

    if pd.isna(ts):
        return ts

    minute_map = {"1h": 60, "30m": 30, "15m": 15}
    minutes = minute_map.get(snap_choice, 60)

    return ts.floor(f"{minutes}min")


def normalize_berth_list(values: Iterable[str] | None) -> List[str]:
    """복수의 선석 라벨을 정규화하여 중복 없이 반환."""

    normalized: List[str] = []
    seen = set()
    if not values:
        return normalized

    for value in values:
        norm = normalize_berth_label(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        normalized.append(norm)

    return normalized
