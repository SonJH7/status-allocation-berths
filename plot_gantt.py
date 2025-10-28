"""간단한 선석 스케줄 시각화 도구.

요구 사항에 맞춰서 1~5선석의 미터 범위를 고정하고, 각 선박을
사각형으로 그리는 matplotlib 기반 함수만 제공한다.
"""
from __future__ import annotations

from typing import List, Mapping, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd

# 1선석(0~300m)부터 5선석(1201~1500m)까지 고정된 범위 정의
BERTH_METER_BOUNDARIES = {
    1: (0, 300),
    2: (301, 600),
    3: (601, 900),
    4: (901, 1200),
    5: (1201, 1500),
}

# 사각형 색상 팔레트. 선석 번호를 순환하면서 사용한다.
DEFAULT_BERTH_COLORS = {
    1: "#7fc8f8",
    2: "#fcb9b2",
    3: "#ffe066",
    4: "#a0e3b2",
    5: "#d0aef5",
}

# 그래프에 표시될 기본 컬럼 이름
DEFAULT_COLUMNS = {
    "berth": "berth",
    "start": "eta",
    "end": "etd",
    "start_meter": "f_pos",
    "end_meter": "e_pos",
    "label": "vessel",
}


def _ensure_datetime(series: pd.Series) -> pd.Series:
    """datetime 혹은 pandas Timestamp로 변환."""

    converted = pd.to_datetime(series, errors="coerce")
    return converted


def _ensure_numeric(series: pd.Series) -> pd.Series:
    """meter 좌표 컬럼을 숫자로 변환하고 NaN을 제거."""

    numeric = pd.to_numeric(series, errors="coerce")
    return numeric


def _normalize_dataframe(
    df: pd.DataFrame,
    column_map: Mapping[str, str],
) -> pd.DataFrame:
    """입력 데이터프레임을 시각화가 기대하는 형태로 정규화한다."""

    missing: List[str] = []
    for logical_key, column_name in column_map.items():
        if column_name not in df.columns:
            missing.append(column_name)
    if missing:
        joined = ", ".join(sorted(missing))
        raise KeyError(f"필수 컬럼이 누락되었습니다: {joined}")

    work = df.copy()
    work[column_map["berth"]] = pd.to_numeric(work[column_map["berth"]], errors="coerce").astype("Int64")
    work[column_map["start"]] = _ensure_datetime(work[column_map["start"]])
    work[column_map["end"]] = _ensure_datetime(work[column_map["end"]])
    work[column_map["start_meter"]] = _ensure_numeric(work[column_map["start_meter"]])
    work[column_map["end_meter"]] = _ensure_numeric(work[column_map["end_meter"]])

    filtered = work.dropna(
        subset=[
            column_map["berth"],
            column_map["start"],
            column_map["end"],
            column_map["start_meter"],
            column_map["end_meter"],
        ]
    ).copy()

    filtered[column_map["berth"]] = filtered[column_map["berth"]].astype(int)
    return filtered


def plot_berth_rectangles(
    df: pd.DataFrame,
    *,
    column_map: Optional[Mapping[str, str]] = None,
    colors: Optional[Mapping[int, str]] = None,
    ax: Optional[plt.Axes] = None,
    show_labels: bool = True,
) -> plt.Axes:
    """선박 배치를 시간-미터 좌표계에서 사각형으로 그린다.

    Parameters
    ----------
    df : pandas.DataFrame
        최소한 선석, 입항시간, 출항시간, F/E 좌표, 레이블 컬럼이 포함되어야 한다.
    column_map : dict, optional
        기본 컬럼 이름이 다를 때 매핑을 전달한다.
    colors : dict, optional
        선석 번호 -> 색상(hex) 매핑. 미지정 시 DEFAULT_BERTH_COLORS 사용.
    ax : matplotlib.axes.Axes, optional
        이미 생성된 Axes가 있다면 전달한다.
    show_labels : bool, default True
        사각형 중앙에 선박 이름을 표시할지 여부.
    """

    if df is None or df.empty:
        raise ValueError("시각화할 데이터가 비어 있습니다.")

    column_map = {**DEFAULT_COLUMNS, **(column_map or {})}
    normalized = _normalize_dataframe(df, column_map)

    if normalized.empty:
        raise ValueError("유효한 데이터가 없습니다. 필수 컬럼을 확인하세요.")

    colors = {**DEFAULT_BERTH_COLORS, **(colors or {})}

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    # x축 범위 계산
    start_values = normalized[column_map["start"]]
    end_values = normalized[column_map["end"]]
    min_start = start_values.min()
    max_end = end_values.max()

    # y축 범위는 고정된 선석 범위로 구성
    ax.set_ylim(0, 1500)

    # 선석 영역 배경 표시
    for berth, (y0, y1) in BERTH_METER_BOUNDARIES.items():
        ax.axhspan(y0, y1, facecolor="#f2f4f7" if berth % 2 else "#e8f4ff", alpha=0.5, zorder=0)
        ax.text(
            min_start,
            y0 + (y1 - y0) / 2,
            f"{berth}선석",
            va="center",
            ha="left",
            fontsize=10,
            color="#1f2933",
            zorder=2,
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.8),
        )

    # 사각형 그리기
    for _, row in normalized.iterrows():
        berth = int(row[column_map["berth"]])
        if berth not in BERTH_METER_BOUNDARIES:
            continue
        start_time: pd.Timestamp = row[column_map["start"]]
        end_time: pd.Timestamp = row[column_map["end"]]
        f_meter: float = float(row[column_map["start_meter"]])
        e_meter: float = float(row[column_map["end_meter"]])

        if pd.isna(start_time) or pd.isna(end_time) or pd.isna(f_meter) or pd.isna(e_meter):
            continue

        if end_time <= start_time:
            continue

        # F/E 좌표가 역전되어 있으면 자동 교정
        y0, y1 = sorted([f_meter, e_meter])
        height = y1 - y0
        if height <= 0:
            continue

        start_num = mdates.date2num(start_time)
        end_num = mdates.date2num(end_time)
        width = end_num - start_num
        color = colors.get(berth, "#9ca3af")

        rect = Rectangle(
            (start_num, y0),
            width,
            height,
            facecolor=color,
            edgecolor="#1f2937",
            linewidth=1.0,
            alpha=0.85,
        )
        ax.add_patch(rect)

        if show_labels:
            label_value = row.get(column_map["label"], "")
            if isinstance(label_value, str) and label_value:
                ax.text(
                    start_num + width / 2,
                    y0 + height / 2,
                    label_value,
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="#111827",
                    zorder=3,
                )

    # 날짜 포맷팅
    ax.set_xlim(min_start, max_end)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.set_xlabel("시간")
    ax.set_ylabel("선석 위치 (m)")
    ax.grid(True, which="major", axis="x", linestyle="--", alpha=0.4)
    fig.autofmt_xdate()

    return ax


__all__ = ["plot_berth_rectangles", "BERTH_METER_BOUNDARIES"]
