# app.py
# Streamlit + vis.js 타임라인 기반 BPTC 선석 현황 보드
from __future__ import annotations

import io
import html
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st
from streamlit_timeline import st_timeline

from bptc_vslmsg import fetch_bptc_g_vslmsg
from crawling.main import collect_berth_info

import numpy as np

# ------------------------------------------------------------
# 상수 정의
# ------------------------------------------------------------
BPTC_FORM_PAYLOAD = {
    "v_time": "3days",
    "ROCD": "ALL",
    "ORDER": "item3",
    "v_gu": "A",
}

COLUMN_RENAME_MAP = {
    "구분": "terminal_group",
    "선석": "berth",
    "모선항차": "voyage",
    "선박명": "vessel",
    "접안": "mooring_type",
    "선사": "operator",
    "입항 예정일시": "eta_plan",
    "입항일시": "eta",
    "작업완료일시": "work_complete",
    "출항일시": "etd",
    "반입 마감일시": "inbound_cutoff",
    "양하": "discharge_qty",
    "선적": "load_qty",
    "S/H": "sh_qty",
    "전배": "transfer_qty",
    "항로": "route",
    "검역": "quarantine_flag",
    "Length(m)": "length_m",
    "Beam(m)": "beam_m",
    "f": "f_pos",
    "e": "e_pos",
}

TIME_COLUMNS = ["eta_plan", "eta", "work_complete", "etd", "inbound_cutoff"]
NUMERIC_COLUMNS = [
    "discharge_qty",
    "load_qty",
    "sh_qty",
    "transfer_qty",
    "length_m",
    "beam_m",
    "bp",
    "f_pos",
    "e_pos",
]

PASTEL_COLORS = {
    "gray": "#d9d9d9",
    "pink": "#f8d3f1",
    "cyan": "#bdefff",
    "beige": "#ffe3a3",
}

MOORING_COLOR_RULE = {
    "S": PASTEL_COLORS["gray"],
    "P": PASTEL_COLORS["pink"],
}
DEFAULT_COLOR_SEQUENCE = [
    PASTEL_COLORS["cyan"],
    PASTEL_COLORS["beige"],
    PASTEL_COLORS["gray"],
    PASTEL_COLORS["pink"],
]

REFERENCE_STATUS_COLOR_MAP = {
    "적하프래닝까지완료": PASTEL_COLORS["pink"],
    "적하플래닝까지완료": PASTEL_COLORS["pink"],
    "양하프래닝까지완료": PASTEL_COLORS["cyan"],
    "양하플래닝까지완료": PASTEL_COLORS["cyan"],
    "크레인배정완료": PASTEL_COLORS["beige"],
    "크래인배정완료": PASTEL_COLORS["beige"],
    "크레인미배정": PASTEL_COLORS["gray"],
    "크래인미배정": PASTEL_COLORS["gray"],
}

REFERENCE_COLUMN_CANDIDATES = ("참고", "reference", "remarks", "remark")

AXIS_BACKGROUND_COLOR = "#e5f3ff"

BP_BASELINE_M = 1500.0
BERTH_VERTICAL_SPAN_PX = 300.0
BERTH_METER_RANGES: Dict[str, Tuple[float, float]] = {
    "1": (0.0, 300.0),
    "2": (301.0, 600.0),
    "3": (601.0, 900.0),
    "4": (901.0, 1200.0),
    "5": (1200.0, 1500.0),
}
QUARANTINE_MARKER_KEYS = ("quarantine_flag", "quarantine", "검역")
PILOT_MARKER_KEYS = ("pilot_flag", "pilotage_flag", "pilotage", "pilot", "pilot_text", "도선")

# ------------------------------------------------------------
# 유틸리티 함수
# ------------------------------------------------------------

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """MultiIndex 컬럼을 단일 계층으로 평탄화."""

    if not isinstance(df.columns, pd.MultiIndex):
        return df

    new_columns: List[str] = []
    for column_tuple in df.columns:
        parts = [str(part).strip() for part in column_tuple if str(part).strip()]
        if not parts:
            new_columns.append("")
        else:
            new_columns.append(" ".join(parts))
    df = df.copy()
    df.columns = new_columns
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = flatten_columns(df)
    if df.columns.duplicated().any():
        deduped = ~df.columns.duplicated(keep="last")
        df = df.loc[:, deduped]
    rename_map = {col: COLUMN_RENAME_MAP.get(col, col) for col in df.columns}
    df = df.rename(columns=rename_map)
    return df


def to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ensure_jsonable_value(value: object) -> object:
    """vis.js item data에 포함될 값을 JSON 직렬화 가능 형태로 변환."""

    if isinstance(value, (pd.Timestamp, datetime)):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, (pd.Timedelta, timedelta)):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, np.generic):
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
    if pd.isna(value):
        return None
    return value


def row_to_jsonable(row: pd.Series) -> Dict[str, Any]:
    return {key: ensure_jsonable_value(val) for key, val in row.items()}


@st.cache_data(show_spinner=False)
def fetch_bptc_dataframe() -> pd.DataFrame:
    """crawling 패키지 기반 데이터프레임을 정규화하여 반환."""

    try:
        df = collect_berth_info(
            time=BPTC_FORM_PAYLOAD.get("v_time", "3days"),
            route=BPTC_FORM_PAYLOAD.get("ROCD", "ALL"),
            berth=BPTC_FORM_PAYLOAD.get("v_gu", "A"),
            debug=False,
        )
    except Exception as exc:  # pragma: no cover - 외부 API 오류 래핑
        raise RuntimeError(f"crawling.collect_berth_info 호출 실패: {exc}") from exc

    if not isinstance(df, pd.DataFrame):
        raise RuntimeError("collect_berth_info가 DataFrame을 반환하지 않았습니다.")

    if df.empty:
        return df

    df = normalize_column_names(df)

    if "berth" in df.columns:
        df["berth"] = df["berth"].astype(str).str.extract(r"(\d+)").iloc[:, 0]
        df = df[~df["berth"].isna()].copy()

    for col in TIME_COLUMNS:
        if col in df.columns:
            df[col] = to_datetime(df[col])
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    if {"bp", "f_pos", "e_pos"}.intersection(df.columns):
        def _format_bp(row: pd.Series) -> str | None:
            bp_val = row.get("bp")
            f_val = row.get("f_pos")
            e_val = row.get("e_pos")
            if pd.isna(bp_val) and pd.isna(f_val) and pd.isna(e_val):
                return None
            parts: list[str] = []
            if pd.notna(bp_val):
                try:
                    parts.append(str(int(float(bp_val))))
                except (TypeError, ValueError):
                    parts.append(str(bp_val))
            detail: list[str] = []
            if pd.notna(f_val):
                try:
                    detail.append(f"F: {int(float(f_val))}")
                except (TypeError, ValueError):
                    detail.append(f"F: {f_val}")
            if pd.notna(e_val):
                try:
                    detail.append(f"E: {int(float(e_val))}")
                except (TypeError, ValueError):
                    detail.append(f"E: {e_val}")
            if detail:
                parts.append(f"( {', '.join(detail)} )")
            return " ".join(parts) if parts else None

        bp_text = df.apply(_format_bp, axis=1)
        df["bp_raw"] = bp_text
        if "bitt" not in df.columns:
            df["bitt"] = bp_text

    if "quarantine_flag" in df.columns:
        df["quarantine_flag"] = df["quarantine_flag"].fillna("").astype(str)
    if "mooring_type" in df.columns:
        df["mooring_type"] = df["mooring_type"].fillna("").astype(str)
    if "berth" in df.columns:
        df["berth"] = df["berth"].astype(str)

    if "bp_raw" in df.columns and "bitt" not in df.columns:
        df["bitt"] = df["bp_raw"]
    if "bitt" in df.columns:
        df["bitt"] = df["bitt"].astype(str)
    if "bp_raw" in df.columns:
        df["bp_raw"] = df["bp_raw"].astype(str)

    needs_vslmsg = False
    if "f_pos" not in df.columns or df["f_pos"].isna().all():
        needs_vslmsg = True
    if "e_pos" not in df.columns or df["e_pos"].isna().all():
        needs_vslmsg = True
    if "bitt" not in df.columns or df["bitt"].replace("", pd.NA).isna().all():
        needs_vslmsg = True

    if needs_vslmsg:
        try:
            vslmsg_df = fetch_vslmsg_dataframe()
        except Exception as exc:
            print(f"⚠️ VslMsg 데이터 병합 실패: {exc}")
            vslmsg_df = pd.DataFrame()

        if not vslmsg_df.empty:
            vslmsg_df = vslmsg_df.copy()
            vslmsg_df["start_meter"] = vslmsg_df[["f_pos", "e_pos"]].min(axis=1)
            vslmsg_df["end_meter"] = vslmsg_df[["f_pos", "e_pos"]].max(axis=1)
            merge_keys = [key for key in ("voyage", "vessel") if key in df.columns and key in vslmsg_df.columns]
            if not merge_keys:
                merge_keys = ["vessel"]
            vslmsg_compact = vslmsg_df.drop_duplicates(subset=merge_keys)
            extra_cols = [
                col
                for col in ["bitt", "bp_raw", "f_pos", "e_pos", "length_m", "start_meter", "end_meter"]
                if col in vslmsg_compact.columns
            ]
            merged = df.merge(
                vslmsg_compact[merge_keys + extra_cols],
                on=merge_keys,
                how="left",
                suffixes=("", "_vslmsg"),
            )

            for col in extra_cols:
                fallback_col = f"{col}_vslmsg"
                if fallback_col not in merged.columns:
                    continue
                if col in merged.columns:
                    merged[col] = merged[col].combine_first(merged[fallback_col])
                else:
                    merged[col] = merged[fallback_col]
                merged = merged.drop(columns=[fallback_col])
            df = merged

    if {"f_pos", "e_pos"}.issubset(df.columns):
        df["start_meter"] = df[["f_pos", "e_pos"]].min(axis=1)
        df["end_meter"] = df[["f_pos", "e_pos"]].max(axis=1)

    if "length_m" in df.columns and "loa_m" not in df.columns:
        df["loa_m"] = df["length_m"]

    return df


@st.cache_data(show_spinner=False)
def fetch_vslmsg_dataframe() -> pd.DataFrame:
    try:
        return fetch_bptc_g_vslmsg()
    except Exception as exc:
        print(f"⚠️ VslMsg 크롤링 실패: {exc}")
        return pd.DataFrame()


def get_demo_df(base_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    base = pd.Timestamp(base_date) if base_date is not None else pd.Timestamp.today()
    day0 = base.normalize()

    rows: List[Dict[str, object]] = []

    def add_row(
        berth: str,
        vessel: str,
        voyage: str,
        start_hours: int,
        duration_hours: int,
        load_qty: int,
        discharge_qty: int,
        sh_qty: int,
        transfer_qty: int,
        operator: str,
        route: str,
        mooring: str,
        start_tag: str,
        end_tag: str,
        terminal_group: str,
        badge: Optional[str] = None,
    ) -> None:
        start = day0 + timedelta(hours=start_hours)
        end = start + timedelta(hours=duration_hours)
        rows.append(
            {
                "terminal_group": terminal_group,
                "berth": berth,
                "vessel": vessel,
                "voyage": voyage,
                "eta_plan": start - timedelta(hours=4),
                "eta": start,
                "work_complete": end - timedelta(hours=2),
                "etd": end,
                "inbound_cutoff": start - timedelta(hours=12),
                "load_qty": load_qty,
                "discharge_qty": discharge_qty,
                "sh_qty": sh_qty,
                "transfer_qty": transfer_qty,
                "operator": operator,
                "route": route,
                "mooring_type": mooring,
                "quarantine_flag": badge if badge == "검역" else "",
                "start_tag": start_tag,
                "end_tag": end_tag,
            }
        )

    add_row("1", "CKSM-18", "S-01", -12, 30, 150, 110, 30, 15, "CKSM", "JPN", "S", "15", "11", "신선대", "도선")
    add_row("1", "KOBE STAR", "S-11", 20, 18, 240, 60, 20, 0, "KMTC", "KOR", "P", "24", "06", "신선대", "검역")
    add_row("2", "MOON BAY", "S-07", -5, 26, 90, 70, 10, 5, "PAN", "CHN", "S", "09", "07", "신선대", "도선")
    add_row("2", "HANIL SUN", "S-05", 30, 16, 50, 80, 14, 9, "HAN", "DOM", "P", "05", "08", "신선대", "도선")
    add_row("3", "KARISMA", "S-20", 10, 20, 120, 30, 18, 4, "ONE", "SEA", "S", "22", "03", "신선대", "검역")
    add_row("4", "ORIENT GLORY", "S-13", 5, 30, 160, 40, 22, 7, "EAS", "VNM", "P", "11", "04", "신선대", "도선")
    add_row("5", "TITAN", "S-02", -8, 28, 130, 90, 12, 11, "SIN", "CHN", "S", "13", "09", "신선대", "검역")
    add_row("6", "BLUE PEARL", "G-08", 6, 34, 210, 180, 30, 14, "CKS", "DOM", "S", "17", "25", "감만", "도선")
    add_row("7", "SUNRISE", "G-09", -6, 40, 180, 150, 20, 10, "KMTC", "JPN", "P", "18", "12", "감만", "검역")
    add_row("8", "PACIFIC WIND", "G-05", 18, 26, 200, 210, 24, 18, "HMM", "VNM", "S", "07", "06", "감만", "도선")
    add_row("9", "HAN RIVER", "G-03", 24, 18, 80, 140, 8, 0, "EAS", "KOR", "P", "14", "05", "감만", "도선")

    demo_df = pd.DataFrame(rows)
    return demo_df


def snap_to_interval(ts: pd.Timestamp, key: str) -> pd.Timestamp:
    minutes = {"1h": 60, "30m": 30, "15m": 15}[key]
    ts = ts.to_pydatetime().replace(second=0, microsecond=0)
    return pd.Timestamp(ts) - pd.Timedelta(minutes=ts.minute % minutes)


def ensure_timeline_css() -> None:
    if st.session_state.get("_timeline_css_injected"):
        return
    st.markdown(
        f"""
        <style>
        .vis-panel.vis-top {{
            background-color: {AXIS_BACKGROUND_COLOR};
            border-bottom: 2px solid #89aee6;
        }}
        .vis-panel.vis-left {{
            background-color: #f6f8fb;
        }}
        .vis-labelset:before {{
            content: "선석";
            position: absolute;
            top: 8px;
            left: 12px;
            font-weight: 600;
            color: #1f3b73;
        }}
        .vis-labelset .vis-label .vis-inner {{
            font-weight: 600;
            font-size: 14px;
            color: #163a59;
            display: flex;
            flex-direction: column;
            gap: 6px;
        }}
        .vis-labelset .vis-label {{
            padding-left: 68px;
        }}
        .berth-label {{
            display: flex;
            flex-direction: column;
            gap: 4px;
            align-items: flex-start;
        }}
        .berth-label .berth-name {{
            font-weight: 700;
            font-size: 15px;
            color: #0f2d4c;
        }}
        .berth-label .bp-axis {{
            display: flex;
            flex-direction: column;
            gap: 2px;
            font-size: 11px;
            color: #4b5563;
            line-height: 1.1;
        }}
        .berth-label .bp-axis span:first-child {{
            color: #1f2937;
            font-weight: 700;
        }}
        .berth-label {{
            display: flex;
            flex-direction: column;
            gap: 4px;
            align-items: flex-start;
        }}
        .berth-label .berth-name {{
            font-weight: 700;
            font-size: 15px;
            color: #0f2d4c;
        }}
        .berth-label .bp-axis {{
            display: flex;
            flex-direction: column;
            gap: 2px;
            font-size: 11px;
            color: #4b5563;
            line-height: 1.1;
        }}
        .berth-label .bp-axis span:first-child {{
            color: #1f2937;
            font-weight: 700;
        }}
        .vis-timeline .vis-item.berth-item {{
            border-radius: 12px;
            border: 2px solid rgba(15, 45, 76, 0.2);
            overflow: visible;
        }}
        .vis-timeline .vis-item.berth-item.bp-aligned {{
            transition: transform 0.25s ease, margin-top 0.25s ease;
        }}
        .vis-timeline .vis-item.berth-item .vis-item-content {{
            padding: 0 !important;
            height: 100%;
        }}
        .vis-timeline .vis-item.berth-item.gap-warning {{
            border-color: #d97706;
            box-shadow: 0 0 0 2px rgba(217, 119, 6, 0.3);
        }}
        .berth-item-card {{
            position: relative;
            width: 100%;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 18px 12px 14px 12px;
            box-sizing: border-box;
            gap: 6px;
        }}
        .berth-item-card .time-row {{
            position: absolute;
            top: 6px;
            left: 0;
            right: 0;
            display: flex;
            justify-content: space-between;
            padding: 0 10px;
            font-size: 12px;
            font-weight: 700;
            color: #0f2d4c;
        }}
        .berth-item-card .time-row .time {{
            min-width: 20px;
            text-align: center;
        }}
        .berth-item-card .center-stack {{
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 4px;
            width: 100%;
        }}
        .berth-item-card .vessel-name {{
            text-align: center;
            font-weight: 700;
            font-size: 14px;
            color: #0b1a33;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            width: 100%;
        }}
        .berth-item-card .marker-text {{
            font-size: 11px;
            font-weight: 700;
            color: #0f2d4c;
            line-height: 1.1;
        }}
        .berth-item-card .marker-text.top {{
            font-size: 12px;
        }}
        .berth-item-card .marker-text.bottom {{
            margin-top: 2px;
        }}
        .berth-item-card .length-chip {{
            font-size: 11px;
            font-weight: 600;
            color: #0b1a33;
            background-color: rgba(255, 255, 255, 0.75);
            border-radius: 999px;
            padding: 2px 8px;
            margin-top: 2px;
        }}
        .vis-time-axis .vis-grid.vis-major {{
            border-width: 2px 0 0 0;
            border-color: #9cbbe9;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_timeline_css_injected"] = True


def resolve_reference_status_color(row: pd.Series) -> Optional[str]:
    for column in REFERENCE_COLUMN_CANDIDATES:
        if column not in row.index:
            continue
        value = row.get(column)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized = (
            text.replace(" ", "")
            .replace("\u3000", "")
            .replace("\xa0", "")
            .lower()
        )
        for keyword, color in REFERENCE_STATUS_COLOR_MAP.items():
            if keyword in normalized:
                return color
    return None


def resolve_background_color(row: pd.Series) -> str:
    reference_color = resolve_reference_status_color(row)
    if reference_color:
        return reference_color

    mooring = str(row.get("mooring_type") or "").strip().upper()
    if mooring in MOORING_COLOR_RULE:
        return MOORING_COLOR_RULE[mooring]

    operator = str(row.get("operator") or "").strip().upper()
    if operator:
        idx = sum(ord(ch) for ch in operator) % len(DEFAULT_COLOR_SEQUENCE)
        return DEFAULT_COLOR_SEQUENCE[idx]

    route = str(row.get("route") or "").strip().upper()
    if route:
        idx = sum(ord(ch) for ch in route) % len(DEFAULT_COLOR_SEQUENCE)
        return DEFAULT_COLOR_SEQUENCE[idx]

    return DEFAULT_COLOR_SEQUENCE[0]


def format_time_digits(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%H")


def extract_marker_label(row: pd.Series, keys: Iterable[str]) -> str:
    for key in keys:
        if key not in row.index:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return ""


def extract_meter_range(row: pd.Series) -> Tuple[Optional[float], Optional[float]]:
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
    if berth_value is None or pd.isna(berth_value):
        return None
    key = str(berth_value).strip()
    if not key:
        return None
    return BERTH_METER_RANGES.get(key)


def resolve_berth_span(row: pd.Series) -> Optional[float]:
    berth_range = get_berth_meter_range(row.get("berth"))
    if berth_range is None:
        return None
    start, end = berth_range
    if start is None or end is None:
        return None
    span = float(end - start)
    if span <= 0:
        return None
    return span


def compute_item_height(row: pd.Series) -> float:
    berth_span = resolve_berth_span(row)
    max_height = berth_span if berth_span is not None else BERTH_VERTICAL_SPAN_PX
    start_meter, end_meter = extract_meter_range(row)
    if start_meter is not None and end_meter is not None:
        lower = float(min(start_meter, end_meter))
        upper = float(max(start_meter, end_meter))
        span = upper - lower
        if span > 0:
            if berth_span is not None:
                span = min(span, berth_span)
            return float(max(24.0, min(max_height, span)))

    length_val = row.get("loa_m")
    if length_val is None or pd.isna(length_val):
        length_val = row.get("length_m")
    try:
        numeric = float(length_val)
    except (TypeError, ValueError):
        numeric = None

    if numeric is None or pd.isna(numeric):
        return 86.0
    scaled = numeric
    return float(max(24.0, min(max_height, scaled)))


def compute_item_offset(row: pd.Series, item_height: float) -> float:
    def _to_float(value) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    start_meter, end_meter = extract_meter_range(row)
    berth_range = get_berth_meter_range(row.get("berth"))

    if (
        berth_range is not None
        and berth_range[0] is not None
        and berth_range[1] is not None
        and berth_range[1] > berth_range[0]
    ):
        berth_start = float(berth_range[0])
        berth_end = float(berth_range[1])
        berth_span = berth_end - berth_start

        top_anchor = _to_float(start_meter)
        bottom_anchor = _to_float(end_meter)
        if top_anchor is None and bottom_anchor is not None:
            top_anchor = bottom_anchor
        if top_anchor is None:
            anchor_candidates: List[float] = []
            for key in ("f_pos", "e_pos"):
                converted = _to_float(row.get(key))
                if converted is not None:
                    anchor_candidates.append(converted)
            if anchor_candidates:
                top_anchor = min(anchor_candidates)

        if top_anchor is None:
            return 0.0

        clamped_top = min(max(float(top_anchor), berth_start), berth_end)
        offset = clamped_top - berth_start
        max_offset = max(0.0, berth_span - item_height)
        if max_offset < 0:
            max_offset = 0.0
        if offset > max_offset:
            offset = max_offset
        if offset < 0:
            offset = 0.0
        return float(offset)

    # Fallback: use original baseline-based logic when 선석 범위를 알 수 없는 경우
    anchor_candidates: List[float] = []
    for candidate in (start_meter, end_meter, row.get("f_pos"), row.get("e_pos")):
        converted = _to_float(candidate)
        if converted is not None:
            anchor_candidates.append(converted)

    if not anchor_candidates:
        return 0.0

    anchor = max(anchor_candidates)
    offset = BP_BASELINE_M - anchor
    if offset < 0:
        offset = 0.0
    max_offset = max(0.0, BERTH_VERTICAL_SPAN_PX - item_height)
    if offset > max_offset:
        offset = max_offset
    return float(offset)


def build_group_label(berth: int | str) -> str:
    return (
        "<div class='berth-label'>"
        f"<div class='berth-name'>{html.escape(str(berth))}</div>"
        "</div>"
    )


def collect_spacing_conflicts(
    df: pd.DataFrame, min_gap_m: float = 30.0
) -> List[Dict[str, object]]:
    conflicts: List[Dict[str, object]] = []
    if df is None or df.empty or "berth" not in df.columns:
        return conflicts

    working = df.copy()
    for col in ("start_meter", "end_meter"):
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    if "start_meter" not in working.columns or "end_meter" not in working.columns:
        return conflicts

    for berth, group in working.groupby("berth"):
        valid = group.dropna(subset=["start_meter", "end_meter"])  # type: ignore[arg-type]
        if valid.empty:
            continue
        ordered = valid.sort_values("start_meter")
        prev_row = None
        for idx, row in ordered.iterrows():
            start = float(row["start_meter"])
            end = float(row["end_meter"])
            if start > end:
                start, end = end, start
            if prev_row is not None:
                prev_end = float(prev_row["end_meter"])
                prev_start = float(prev_row["start_meter"])
                if prev_start > prev_end:
                    prev_start, prev_end = prev_end, prev_start
                gap = start - prev_end
                if gap < min_gap_m:
                    conflicts.append(
                        {
                            "berth": berth,
                            "gap": gap,
                            "previous_index": prev_row.name,
                            "current_index": idx,
                            "previous_vessel": prev_row.get("vessel"),
                            "current_vessel": row.get("vessel"),
                            "previous_range": (prev_start, prev_end),
                            "current_range": (start, end),
                        }
                    )
            prev_row = row

    return conflicts


def mark_spacing_warnings(
    df: pd.DataFrame,
    min_gap_m: float = 30.0,
    *,
    conflicts: Optional[List[Dict[str, object]]] = None,
) -> pd.Series:
    if conflicts is None:
        conflicts = collect_spacing_conflicts(df, min_gap_m=min_gap_m)
    flags = pd.Series(False, index=df.index)
    for conflict in conflicts:
        for key in ("previous_index", "current_index"):
            idx = conflict.get(key)
            if idx in flags.index:
                flags.at[idx] = True
    return flags


def format_meter_position(value: object, prefix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return ""
        return f"{prefix}{text}" if prefix else text
    if pd.isna(numeric):
        return ""
    rounded = int(round(numeric))
    label = f"{rounded}m"
    return f"{prefix}{label}" if prefix else label


def build_item_html(row: pd.Series) -> Tuple[str, str]:
    vessel = str(row.get("vessel") or "").strip()
    voyage = str(row.get("voyage") or "").strip()
    title = vessel if not voyage else f"{vessel} ({voyage})"

    range_start, range_end = extract_meter_range(row)

    start_hour = (
        format_time_digits(row.get("gantt_start"))
        or format_time_digits(row.get("eta"))
        or format_time_digits(row.get("eta_plan"))
    )
    end_hour = format_time_digits(row.get("etd"))

    quarantine_text = extract_marker_label(row, QUARANTINE_MARKER_KEYS)
    pilot_text = extract_marker_label(row, PILOT_MARKER_KEYS)

    length_val = row.get("loa_m") if not pd.isna(row.get("loa_m")) else row.get("length_m")
    length_text = ""
    if length_val is not None and not pd.isna(length_val):
        try:
            length_num = float(length_val)
            length_text = f"{int(round(length_num))} m"
        except (TypeError, ValueError):
            length_text = str(length_val)

    bp_text = str(row.get("bitt") or row.get("bp_raw") or "").strip()
    if not bp_text:
        f_pos = row.get("f_pos")
        e_pos = row.get("e_pos")
        if f_pos is not None and e_pos is not None and not pd.isna(f_pos) and not pd.isna(e_pos):
            bp_text = f"F:{int(f_pos)} / E:{int(e_pos)}"

    chip_html = ""
    if length_text:
        chip_body = length_text if not bp_text else f"{length_text} · {bp_text}"
        chip_html = f"<div class='length-chip'>{html.escape(chip_body)}</div>"

    quarantine_html = (
        "<div class='marker-text top'>검역</div>" if quarantine_text else ""
    )

    display_pilot = ""
    if pilot_text:
        display_pilot = pilot_text if "도선" in pilot_text else "도선"

    marker_bottom_html = (
        f"<div class='marker-text bottom'>{html.escape(display_pilot)}</div>"
        if display_pilot
        else ""
    )

    vessel_html = html.escape(vessel)

    time_row_html = ""
    if start_hour or end_hour:
        time_row_html = (
            "<div class='time-row'>"
            f"<span class='time start'>{html.escape(start_hour)}</span>"
            f"<span class='time end'>{html.escape(end_hour)}</span>"
            "</div>"
        )

    center_stack_parts = [part for part in (quarantine_html, f"<div class='vessel-name'>{vessel_html}</div>") if part]
    if marker_bottom_html:
        center_stack_parts.append(marker_bottom_html)
    if chip_html:
        center_stack_parts.append(chip_html)

    center_stack_html = "<div class='center-stack'>" + "".join(center_stack_parts) + "</div>"

    html_t = f"""
    <div class='berth-item-card'>
        {time_row_html}
        {center_stack_html}
    </div>
    """

    tooltip_parts = [
        f"선석: {row.get('berth')}",
        f"선박: {title}",
    ]

    gantt_start = row.get("gantt_start")
    eta_plan_ts = row.get("eta_plan")
    eta_actual_ts = row.get("eta")
    if pd.notna(gantt_start):
        tooltip_parts.append(
            f"ETA 계획: {pd.Timestamp(gantt_start).strftime('%Y-%m-%d %H:%M')}"
        )
    elif pd.notna(eta_plan_ts):
        tooltip_parts.append(
            f"ETA 계획: {pd.Timestamp(eta_plan_ts).strftime('%Y-%m-%d %H:%M')}"
        )
    if pd.notna(eta_actual_ts):
        tooltip_parts.append(
            f"ETA 실제: {pd.Timestamp(eta_actual_ts).strftime('%Y-%m-%d %H:%M')}"
        )
    etd_ts = row.get("etd")
    if pd.notna(etd_ts):
        tooltip_parts.append(
            f"ETD: {pd.Timestamp(etd_ts).strftime('%Y-%m-%d %H:%M')}"
        )
    if length_text:
        tooltip_parts.append(f"길이: {length_text}")
    if bp_text:
        tooltip_parts.append(f"B.P.: {bp_text}")
    start_meter = row.get("start_meter")
    end_meter = row.get("end_meter")
    if start_meter is not None and end_meter is not None and not pd.isna(start_meter) and not pd.isna(end_meter):
        tooltip_parts.append(
            f"배치 구간: {int(start_meter)}m ~ {int(end_meter)}m"
        )

    tooltip = "<br/>".join([part for part in tooltip_parts if part])
    return html_t, tooltip

    options = {
        "stack": False,
        "editable": {
            "updateTime": True,
            "updateGroup": True,
            "remove": False,
            "add": False,
        }
        if editable
        else False,
        "groupEditable": False,
        "min": view_start.isoformat(),
        "max": view_end.isoformat(),
        "orientation": {"axis": "top"},
        "margin": {"item": 6, "axis": 12},
        "multiselect": False,
        "moveable": True,
        "zoomable": True,
        "zoomKey": "ctrlKey",
        "zoomMin": 1000 * 60 * 15,
        "zoomMax": 1000 * 60 * 60 * 24 * 30,
        "timeAxis": {"scale": "day", "step": 1},
        "locale": "ko",
    }

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for col in ("eta_plan", "eta", "etd"):
        if col in work.columns:
            work[col] = to_datetime(work[col])
    for col in ("start_meter", "end_meter", "loa_m", "length_m", "f_pos", "e_pos"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    if "bp_raw" in work.columns:
        work["bp_raw"] = work["bp_raw"].astype(str)
    if "bitt" in work.columns:
        work["bitt"] = work["bitt"].astype(str)
    if "berth" in work.columns:
        work["berth"] = work["berth"].astype(str)
    return work


def render_berth_gantt(
    df: pd.DataFrame,
    base_date: pd.Timestamp,
    days: int = 7,
    editable: bool = True,
    snap_choice: str = "1h",
    berth_range: Tuple[int, int] = (1, 5),
    berth_whitelist: Optional[Iterable[str]] = None,
    group_label_map: Optional[Dict[str, str]] = None,
    height: str = "820px",
    key: str = "gantt",
) -> Tuple[pd.DataFrame, Optional[Dict]]:
    """df의 모든 컬럼을 보존하여 vis.js items/groups로 변환 후 렌더링."""

    ensure_timeline_css()

    prepared = prepare_dataframe(df)
    if prepared.empty:
        st.info("표시할 데이터가 없습니다.")
        return prepared, None

    gantt_start = pd.Series(pd.NaT, index=prepared.index, dtype="datetime64[ns]")
    if "eta_plan" in prepared.columns:
        gantt_start = gantt_start.combine_first(prepared["eta_plan"])
    if "eta" in prepared.columns:
        gantt_start = gantt_start.combine_first(prepared["eta"])
    prepared["gantt_start"] = gantt_start

    base_ts = pd.Timestamp(base_date)
    view_start = base_ts.normalize() - pd.Timedelta(days=1)
    view_end = view_start + pd.Timedelta(days=days) - pd.Timedelta(milliseconds=1)

    if "berth" not in prepared.columns:
        st.warning("선석 정보가 없어 간트 차트를 표시할 수 없습니다.")
        return prepared.drop(columns=["gantt_start"], errors="ignore"), None

    prepared["berth"] = prepared["berth"].astype(str)

    berth_min, berth_max = berth_range
    if berth_whitelist is not None:
        berth_order = [str(b) for b in berth_whitelist]
    else:
        berth_order = [str(b) for b in range(berth_min, berth_max + 1)]

    allowed_berths = set(berth_order)

    mask = (
        prepared["etd"].notna()
        & prepared["gantt_start"].notna()
        & (prepared["etd"] >= view_start)
        & (prepared["gantt_start"] <= view_end)
        & prepared["berth"].isin(allowed_berths)
    )
    view_df = prepared.loc[mask].copy()

    if view_df.empty:
        st.info("선택한 조건에 해당하는 선석 일정이 없습니다.")
        return prepared.drop(columns=["gantt_start"], errors="ignore"), None

    spacing_conflicts = collect_spacing_conflicts(view_df)
    gap_flags = mark_spacing_warnings(view_df, conflicts=spacing_conflicts)
    gap_flags_map = gap_flags.to_dict()
    conflict_texts: List[str] = []

    if spacing_conflicts:
        for conflict in spacing_conflicts:
            berth = conflict.get("berth")
            gap = conflict.get("gap")
            prev_v = conflict.get("previous_vessel") or "-"
            curr_v = conflict.get("current_vessel") or "-"
            prev_range = conflict.get("previous_range")
            curr_range = conflict.get("current_range")
            gap_display = f"{gap:.1f}m" if isinstance(gap, (int, float)) else ""
            if (
                isinstance(prev_range, tuple)
                and len(prev_range) == 2
                and all(val is not None and not pd.isna(val) for val in prev_range)
            ):
                prev_range_txt = f"{int(prev_range[0])}~{int(prev_range[1])}m"
            else:
                prev_range_txt = ""
            if (
                isinstance(curr_range, tuple)
                and len(curr_range) == 2
                and all(val is not None and not pd.isna(val) for val in curr_range)
            ):
                curr_range_txt = f"{int(curr_range[0])}~{int(curr_range[1])}m"
            else:
                curr_range_txt = ""
            conflict_texts.append(
                f"{berth}선석 {prev_v}({prev_range_txt}) ↔ {curr_v}({curr_range_txt}) : 간격 {gap_display}"
            )

    resolved_group_label_map: Dict[str, str] = {}
    if group_label_map is not None:
        resolved_group_label_map = {
            str(key): value for key, value in group_label_map.items()
        }

    groups = []
    for berth in berth_order:
        label_value = (
            resolved_group_label_map.get(str(berth))
            if resolved_group_label_map
            else None
        )
        if not label_value:
            berth_range = get_berth_meter_range(berth)
            if berth_range is not None:
                start, end = berth_range
                start_txt = f"{int(start)}" if start is not None else ""
                end_txt = f"{int(end)}" if end is not None else ""
                if start_txt or end_txt:
                    label_value = f"{berth} ({start_txt}~{end_txt}m)"
        display_label = label_value if label_value else berth
        groups.append(
            {
                "id": str(berth),
                "content": build_group_label(display_label),
                "style": (
                    f"height: {BERTH_VERTICAL_SPAN_PX}px; "
                    f"line-height: {BERTH_VERTICAL_SPAN_PX}px;"
                ),
            }
        )

    items = []
    id_to_index: Dict[str, object] = {}

    for idx, row in view_df.iterrows():
        if pd.isna(row.get("gantt_start")) or pd.isna(row.get("etd")):
            continue
        start = pd.Timestamp(row["gantt_start"]).isoformat()
        end = pd.Timestamp(row["etd"]).isoformat()
        content_html, tooltip = build_item_html(row)
        item_id = str(idx)
        id_to_index[item_id] = idx

        height_px = compute_item_height(row)
        offset_px = compute_item_offset(row, height_px)
        base_class = "berth-item gap-warning" if bool(gap_flags_map.get(idx, False)) else "berth-item"
        item_class = f"{base_class} bp-aligned"
        items.append(
            {
                "id": item_id,
                "group": str(row.get("berth")),
                "start": start,
                "end": end,
                "content": content_html,
                "title": tooltip,
                "style": (
                    f"background-color: {resolve_background_color(row)};"
                    f" height: {height_px}px;"
                    f" margin-top: {offset_px}px;"
                ),
                "className": item_class,
                "type": "range",
                "data": row_to_jsonable(row),
            }
        )

    options = {
        "stack": False,
        "editable": {
            "updateTime": True,
            "updateGroup": True,
            "remove": False,
            "add": False,
        }
        if editable
        else False,
        "groupEditable": False,
        "min": view_start.isoformat(),
        "max": view_end.isoformat(),
        "start": view_start.isoformat(),
        "end": view_end.isoformat(),
        "orientation": {"axis": "top"},
        "margin": {"item": 8, "axis": 12},
        "multiselect": False,
        "moveable": True,
        "zoomable": True,
        "zoomKey": "ctrlKey",
        "zoomMin": 1000 * 60 * 15,
        "zoomMax": 1000 * 60 * 60 * 24 * 30,
        "verticalScroll": True,
        "horizontalScroll": True,
        "timeAxis": {"scale": "day", "step": 1},
        "locale": "ko",
        "groupHeightMode": "fixed",
        "locales": {
            "ko": {
                "current": "ko",
                "months": [
                    "1월",
                    "2월",
                    "3월",
                    "4월",
                    "5월",
                    "6월",
                    "7월",
                    "8월",
                    "9월",
                    "10월",
                    "11월",
                    "12월",
                ],
                "monthsShort": [
                    "1월",
                    "2월",
                    "3월",
                    "4월",
                    "5월",
                    "6월",
                    "7월",
                    "8월",
                    "9월",
                    "10월",
                    "11월",
                    "12월",
                ],
                "weekdays": [
                    "일요일",
                    "월요일",
                    "화요일",
                    "수요일",
                    "목요일",
                    "금요일",
                    "토요일",
                ],
                "weekdaysShort": [
                    "일",
                    "월",
                    "화",
                    "수",
                    "목",
                    "금",
                    "토",
                ],
                "weekdaysMin": [
                    "일",
                    "월",
                    "화",
                    "수",
                    "목",
                    "금",
                    "토",
                ],
                "format": {
                    "date": "YYYY-MM-DD",
                    "time": "HH:mm",
                    "datetime": "YYYY-MM-DD HH:mm",
                },
            }
        },
    }

    event_result = st_timeline(items, groups, options, height=height, key=key)

    if conflict_texts:
        toggle_label = f"⚠️ 배 간격 30m 미만 선박 경고 보기 ({len(conflict_texts)}건)"
        toggle_key = f"{key}_spacing_warning_toggle"
        show_conflicts = st.toggle(toggle_label, value=False, key=toggle_key)
        if show_conflicts:
            st.warning("배 간격 30m 미만 선박이 있습니다.", icon="⚠️")
            for text in conflict_texts:
                st.markdown(f"- {text}")

    updated = prepared.copy()
    event_payload: Optional[Dict] = None

    if isinstance(event_result, dict):
        if isinstance(event_result.get("event"), dict):
            event_payload = event_result.get("event")
        else:
            event_payload = event_result
    else:
        event_payload = None

    if isinstance(event_payload, dict):
        raw_id = event_payload.get("id") or event_payload.get("item")
        target_index = id_to_index.get(str(raw_id))

        edit_keys = {"start", "end", "group"}
        has_edit = any(k in event_payload for k in edit_keys)

        if target_index is not None and has_edit:
            if "start" in event_payload:
                snapped_start = snap_to_interval(
                    pd.to_datetime(event_payload["start"]), snap_choice
                )
                updated.loc[target_index, "gantt_start"] = snapped_start
                if "eta_plan" in updated.columns:
                    updated.loc[target_index, "eta_plan"] = snapped_start
                if "eta" in updated.columns:
                    updated.loc[target_index, "eta"] = snapped_start
            if "end" in event_payload:
                updated.loc[target_index, "etd"] = snap_to_interval(
                    pd.to_datetime(event_payload["end"]), snap_choice
                )
            if "group" in event_payload and event_payload["group"] is not None:
                updated.loc[target_index, "berth"] = str(event_payload["group"])

    if "gantt_start" in updated.columns:
        updated = updated.drop(columns=["gantt_start"], errors="ignore")

    return updated, event_payload


def apply_filters(
    df: pd.DataFrame,
    date_start: Optional[datetime],
    date_end: Optional[datetime],
    operator_filter: str,
    route_filter: str,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()

    if date_start is not None and "etd" in work.columns:
        start_ts = pd.Timestamp(date_start)
        work = work[(work["etd"].isna()) | (work["etd"] >= start_ts)]
    if date_end is not None:
        end_ts = pd.Timestamp(date_end) + pd.Timedelta(days=1)
        start_series = pd.Series(pd.NaT, index=work.index, dtype="datetime64[ns]")
        if "eta_plan" in work.columns:
            start_series = start_series.combine_first(work["eta_plan"])
        if "eta" in work.columns:
            start_series = start_series.combine_first(work["eta"])
        if not start_series.empty:
            work = work[(start_series.isna()) | (start_series <= end_ts)]

    if operator_filter:
        keyword = operator_filter.strip().upper()
        if "operator" in work.columns:
            work = work[
                work["operator"].astype(str).str.upper().str.contains(keyword, na=False)
            ]
        else:
            work = work.iloc[0:0]
    if route_filter:
        keyword = route_filter.strip().upper()
        if "route" in work.columns:
            work = work[
                work["route"].astype(str).str.upper().str.contains(keyword, na=False)
            ]
        else:
            work = work.iloc[0:0]
    return work


def update_working_state(updated_partial: pd.DataFrame) -> None:
    if "working_df" not in st.session_state:
        st.session_state["working_df"] = updated_partial
        return
    working = st.session_state["working_df"].copy()
    for idx in updated_partial.index:
        if idx in working.index:
            working.loc[idx, updated_partial.columns] = updated_partial.loc[idx]
        else:
            working.loc[idx] = updated_partial.loc[idx]
    st.session_state["working_df"] = working


def push_history(snapshot: pd.DataFrame, limit: int = 20) -> None:
    history: List[pd.DataFrame] = st.session_state.setdefault("history", [])
    history.append(snapshot)
    if len(history) > limit:
        del history[0]
    st.session_state["history"] = history


def collect_modal_row(event_payload: Optional[Dict], df: pd.DataFrame) -> Optional[pd.Series]:
    if not isinstance(event_payload, dict):
        return None
    has_edit = any(key in event_payload for key in ("start", "end", "group"))
    if has_edit:
        return None
    target_id = event_payload.get("id") or event_payload.get("item")
    if target_id is None:
        return None
    try:
        idx = int(target_id)
    except (TypeError, ValueError):
        idx = target_id
    if idx not in df.index:
        return None
    return df.loc[idx]


def render_modal(row: pd.Series) -> None:
    with st.modal(f"{row.get('vessel', '선박')} 상세 정보"):
        def section(title: str, columns: Iterable[str]) -> None:
            data = {col: [row.get(col)] for col in columns if col in row.index}
            if not data:
                return
            st.markdown(f"#### {title}")
            st.table(pd.DataFrame(data))

        section("기본", [
            "terminal_group",
            "berth",
            "vessel",
            "voyage",
            "operator",
            "mooring_type",
        ])
        section("시간", [
            "eta_plan",
            "eta",
            "work_complete",
            "etd",
            "inbound_cutoff",
        ])
        section("작업량", [
            "discharge_qty",
            "load_qty",
            "sh_qty",
            "transfer_qty",
        ])
        other_columns = [
            col
            for col in row.index
            if col
            not in {
                "terminal_group",
                "berth",
                "vessel",
                "voyage",
                "operator",
                "mooring_type",
                "eta_plan",
                "eta",
                "work_complete",
                "etd",
                "inbound_cutoff",
                "discharge_qty",
                "load_qty",
                "sh_qty",
                "transfer_qty",
            }
        ]
        if other_columns:
            section("기타", other_columns)

        row_df = row.to_frame().T
        csv_bytes = row_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "CSV로 내보내기",
            data=csv_bytes,
            file_name=f"berth_item_{row.get('vessel', 'vessel')}.csv",
            mime="text/csv",
        )
        st.markdown("이 행만 복사")
        st.code(row_df.to_csv(index=False), language="csv")


def compute_diff(original: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    if original is None or original.empty:
        return pd.DataFrame()
    if current is None or current.empty:
        return pd.DataFrame()

    common_columns = sorted(set(original.columns) & set(current.columns))
    orig_aligned = original[common_columns].copy()
    curr_aligned = current[common_columns].copy()

    orig_aligned = orig_aligned.reindex(curr_aligned.index)
    diff_mask = (orig_aligned != curr_aligned) & ~(orig_aligned.isna() & curr_aligned.isna())
    changed_rows = diff_mask.any(axis=1)

    if not changed_rows.any():
        return pd.DataFrame()
    diff_df = curr_aligned.loc[changed_rows].copy()
    return diff_df


# ------------------------------------------------------------
# Streamlit 애플리케이션 본문
# ------------------------------------------------------------
st.set_page_config(page_title="BPTC 선석 Gantt", layout="wide")
st.title("BPTC 선석 현황 Gantt")

if "raw_df" not in st.session_state:
    try:
        initial_df = fetch_bptc_dataframe()
        st.session_state["raw_df"] = initial_df.copy()
        st.session_state["working_df"] = initial_df.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
    except Exception:
        demo_df = get_demo_df()
        st.session_state["raw_df"] = demo_df.copy()
        st.session_state["working_df"] = demo_df.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
        st.warning("실제 데이터를 불러오지 못해 데모 데이터를 사용합니다.")

with st.sidebar:
    st.markdown("### 데이터 로드")
    if st.button("📡 BPTC 크롤링 새로고침", use_container_width=True):
        try:
            fetched = fetch_bptc_dataframe()
            st.session_state["raw_df"] = fetched.copy()
            st.session_state["working_df"] = fetched.copy()
            st.session_state["last_updated"] = datetime.now()
            st.session_state["history"] = []
            st.success("데이터를 갱신했습니다.")
        except Exception as exc:
            st.error(f"크롤링 실패: {exc}")
            if st.button("데모 데이터로 대체", key="demo_replace"):
                demo_df = get_demo_df()
                st.session_state["raw_df"] = demo_df.copy()
                st.session_state["working_df"] = demo_df.copy()
                st.session_state["last_updated"] = datetime.now()
                st.session_state["history"] = []

    st.markdown("---")
    today = datetime.today()
    default_start = today - timedelta(days=1)
    default_end = today + timedelta(days=5)
    date_start = st.date_input("조회 시작일", value=default_start.date())
    date_end = st.date_input("조회 종료일", value=default_end.date())
    operator_filter = st.text_input("선사 필터", value="")
    route_filter = st.text_input("항로 필터", value="")
    snap_choice = st.radio("시간 스냅", ["1h", "30m", "15m"], index=0, horizontal=True)
    timeline_days = st.slider("타임라인 표시 기간(일)", min_value=3, max_value=14, value=7, step=1)

    if st.button("↩ 되돌리기", use_container_width=True):
        history: List[pd.DataFrame] = st.session_state.get("history", [])
        if history:
            st.session_state["working_df"] = history.pop()
            st.session_state["history"] = history
            st.success("이전 상태로 복원했습니다.")
        else:
            st.info("되돌릴 내역이 없습니다.")

    st.markdown("---")
    current_df = st.session_state.get("working_df", pd.DataFrame())
    if not current_df.empty:
        csv_data = current_df.to_csv(index=False).encode("utf-8-sig")
        pickle_bytes = io.BytesIO()
        current_df.to_pickle(pickle_bytes)
        st.download_button(
            "💾 편집본 CSV 저장",
            data=csv_data,
            file_name="berth_schedule.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "💾 편집본 Pickle 저장",
            data=pickle_bytes.getvalue(),
            file_name="berth_schedule.pkl",
            mime="application/octet-stream",
            use_container_width=True,
        )

raw_df = st.session_state.get("raw_df", pd.DataFrame())
working_df = st.session_state.get("working_df", pd.DataFrame())

if st.session_state.get("last_updated"):
    st.caption(
        f"마지막 갱신: {st.session_state['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}"
    )

filtered_df = apply_filters(
    working_df,
    datetime.combine(date_start, datetime.min.time()) if date_start else None,
    datetime.combine(date_end, datetime.min.time()) if date_end else None,
    operator_filter,
    route_filter,
)

if filtered_df.empty:
    st.warning("필터 조건에 해당하는 데이터가 없습니다.")
else:
    base_date = (
        datetime.combine(date_start, datetime.min.time()) + timedelta(days=1)
        if date_start
        else datetime.today()
    )

    tabs = st.tabs(["신선대(1~5선석)", "감만(6~9선석)"])

    active_events: List[Optional[Dict]] = []

    with tabs[0]:
        updated_df, event_payload = render_berth_gantt(
            filtered_df,
            base_date=base_date,
            days=timeline_days,
            editable=True,
            snap_choice=snap_choice,
            berth_range=(1, 5),
            height="820px",
            key="sinsundae",
        )
        if event_payload:
            active_events.append(event_payload)
        if not updated_df.equals(filtered_df):
            push_history(st.session_state["working_df"].copy())
            update_working_state(updated_df)
            working_df = st.session_state["working_df"].copy()
            filtered_df = apply_filters(
                st.session_state["working_df"],
                datetime.combine(date_start, datetime.min.time()) if date_start else None,
                datetime.combine(date_end, datetime.min.time()) if date_end else None,
                operator_filter,
                route_filter,
            )

    with tabs[1]:
        df_for_gantt = filtered_df.copy()
        berth_label_pairs: List[Tuple[str, str]] = [
            ("9", "9(1)"),
            ("8", "8(2)"),
            ("7", "7(3)"),
            ("6", "6(4)"),
        ]
        berth_order = [pair[0] for pair in berth_label_pairs]
        berth_label_map = dict(berth_label_pairs)

        updated_df, event_payload = render_berth_gantt(
            df_for_gantt,
            base_date=base_date,
            days=timeline_days,
            editable=True,
            snap_choice=snap_choice,
            berth_range=(6, 9),
            berth_whitelist=berth_order,
            group_label_map=berth_label_map,
            height="820px",
            key="gamman",
        )
        if event_payload:
            active_events.append(event_payload)
        if not updated_df.equals(filtered_df):
            push_history(st.session_state["working_df"].copy())
            update_working_state(updated_df)
            working_df = st.session_state["working_df"].copy()
            filtered_df = apply_filters(
                st.session_state["working_df"],
                datetime.combine(date_start, datetime.min.time()) if date_start else None,
                datetime.combine(date_end, datetime.min.time()) if date_end else None,
                operator_filter,
                route_filter,
            )

    # 모달 렌더링
    working_df_after = st.session_state.get("working_df", pd.DataFrame())
    if active_events:
        for payload in active_events:
            modal_row = collect_modal_row(payload, working_df_after)
            if modal_row is not None:
                render_modal(modal_row)

    with st.expander("현재 테이블 보기", expanded=False):
        table_df = working_df_after.copy()
        if not table_df.empty:
            if "bitt" in table_df.columns and "bp" not in table_df.columns:
                table_df.insert(
                    min(3, len(table_df.columns)),
                    "bp",
                    table_df["bitt"],
                )
            if "f_pos" in table_df.columns and "F" not in table_df.columns:
                insert_at = (
                    table_df.columns.get_loc("bp") + 1
                    if "bp" in table_df.columns
                    else min(4, len(table_df.columns))
                )
                table_df.insert(insert_at, "F", table_df["f_pos"])
            if "e_pos" in table_df.columns and "E" not in table_df.columns:
                insert_at = (
                    table_df.columns.get_loc("F") + 1
                    if "F" in table_df.columns
                    else min(5, len(table_df.columns))
                )
                table_df.insert(insert_at, "E", table_df["e_pos"])
            length_source_col: Optional[str] = None
            if "loa_m" in table_df.columns:
                length_source_col = "loa_m"
            elif "length_m" in table_df.columns:
                length_source_col = "length_m"
            if length_source_col and "Length(m)" not in table_df.columns:
                if "E" in table_df.columns:
                    length_insert_at = table_df.columns.get_loc("E") + 1
                elif "F" in table_df.columns:
                    length_insert_at = table_df.columns.get_loc("F") + 1
                elif "bp" in table_df.columns:
                    length_insert_at = table_df.columns.get_loc("bp") + 1
                else:
                    length_insert_at = min(6, len(table_df.columns))
                table_df.insert(
                    length_insert_at,
                    "Length(m)",
                    table_df[length_source_col],
                )
        st.dataframe(table_df, use_container_width=True)

    diff_df = compute_diff(st.session_state.get("raw_df"), working_df_after)
    if not diff_df.empty:
        st.markdown("### 변경된 행")
        st.dataframe(diff_df, use_container_width=True)
    else:
        st.info("변경 사항이 없습니다.")


# ------------------------------------------------------------
# 스크립트 진입점
# ------------------------------------------------------------
if __name__ == "__main__":
    # Streamlit에서 실행될 때는 직접 호출하지 않음
    pass
