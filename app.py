# app.py
# Streamlit + vis.js 타임라인 기반 BPTC 선석 현황 보드
from __future__ import annotations

import html
import io
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from streamlit_timeline import st_timeline

from bptc_vslmsg import fetch_bptc_g_vslmsg

import numpy as np
from db import SessionLocal, get_vessel_loa_map, _normalize_berth_code as normalize_berth_code
# ------------------------------------------------------------
# 상수 정의
# ------------------------------------------------------------
BPTC_ENDPOINT = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
BPTC_FORM_PAYLOAD = {
    "ROCD": "ALL",
    "ORDER": "item3",
    "v_gu": "A",
}
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://info.bptc.co.kr/content/sw/frame/berth_status_text_frame_sw_kr.jsp",
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
}

TIME_COLUMNS = ["eta_plan", "eta", "work_complete", "etd", "inbound_cutoff"]
NUMERIC_COLUMNS = ["discharge_qty", "load_qty", "sh_qty", "transfer_qty"]

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
DEFAULT_BERTH_LENGTH_M = 300.0

BP_BASELINE_M = 1500.0
BERTH_VERTICAL_SPAN_PX = 300.0
QUARANTINE_MARKER_KEYS = ("quarantine_flag", "quarantine", "검역")
PILOT_MARKER_KEYS = ("pilot_flag", "pilotage_flag", "pilotage", "pilot", "pilot_text", "도선")

MAX_FETCH_DAYS = 31

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
def fetch_bptc_dataframe(v_time: str = "7days", start_day: Optional[str] = None) -> pd.DataFrame:
    """BPTC 텍스트 서블릿에서 테이블 전체를 크롤링."""

    form_payload = dict(BPTC_FORM_PAYLOAD)
    form_payload["v_time"] = v_time
    if start_day:
        normalized = str(start_day).replace("-", "")
        form_payload["v_day"] = normalized
        form_payload["v_date"] = normalized

    response = requests.post(
        BPTC_ENDPOINT,
        data=form_payload,
        headers=HTTP_HEADERS,
        timeout=20,
    )
    response.encoding = "euc-kr"
    if response.status_code != 200:
        raise RuntimeError(f"BPTC 요청 실패: {response.status_code}")

    soup = BeautifulSoup(response.text, "lxml")
    tables = pd.read_html(io.StringIO(str(soup)), flavor="lxml")
    if not tables:
        raise RuntimeError("테이블을 찾을 수 없습니다.")

    candidate = max(tables, key=lambda tbl: tbl.shape[1])
    df = normalize_column_names(candidate)

    if "berth" in df.columns:
        df["berth"] = df["berth"].astype(str).str.extract(r"(\d+)").iloc[:, 0]
    if "berth" in df.columns:
        df = df[~df["berth"].isna()].copy()

    for col in TIME_COLUMNS:
        if col in df.columns:
            df[col] = to_datetime(df[col])
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    if "quarantine_flag" in df.columns:
        df["quarantine_flag"] = df["quarantine_flag"].fillna("").astype(str)
    if "mooring_type" in df.columns:
        df["mooring_type"] = df["mooring_type"].fillna("").astype(str)
    if "berth" in df.columns:
        df["berth"] = df["berth"].astype(str)

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
        df = df.merge(vslmsg_compact[merge_keys + extra_cols], on=merge_keys, how="left")

        missing_mask = df.get("length_m").isna() if "length_m" in df.columns else pd.Series(False, index=df.index)
        if missing_mask.any():
            fallback = vslmsg_df.dropna(subset=["length_m"]).drop_duplicates(subset=["vessel"])
            fallback = fallback.set_index("vessel")
            for col in ["bitt", "bp_raw", "f_pos", "e_pos", "length_m", "start_meter", "end_meter"]:
                if col not in df.columns:
                    df[col] = None
                if col in fallback.columns:
                    df.loc[missing_mask, col] = df.loc[missing_mask, "vessel"].map(fallback[col])

    if "start_meter" not in df.columns and {"f_pos", "e_pos"}.issubset(df.columns):
        df["start_meter"] = df[["f_pos", "e_pos"]].min(axis=1)
        df["end_meter"] = df[["f_pos", "e_pos"]].max(axis=1)

    if "length_m" in df.columns and "loa_m" not in df.columns:
        df["loa_m"] = df["length_m"]

    return df

def attach_vessel_loa(df: pd.DataFrame) -> pd.DataFrame:
    """데이터프레임에 선박 LOA 정보를 결합한다."""

    if df is None or df.empty or "vessel" not in df.columns:
        return df

    vessels = (
        df["vessel"].dropna().astype(str).str.strip()
    )
    vessels = vessels[vessels != ""]
    if vessels.empty:
        return df

    session = SessionLocal()
    try:
        loa_map = get_vessel_loa_map(session, vessels.unique())
    finally:
        session.close()

    if not loa_map:
        return df

    enriched = df.copy()
    vessel_key = enriched["vessel"].astype(str).str.strip()
    loa_series = vessel_key.map(loa_map)

    if "loa_m" in enriched.columns:
        missing_mask = enriched["loa_m"].isna()
        enriched.loc[missing_mask, "loa_m"] = loa_series[missing_mask]
    else:
        enriched["loa_m"] = loa_series

    return enriched


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
            align-items: center;
            justify-content: center;
            gap: 2px;
            padding: 6px 8px 8px 8px;
            box-sizing: border-box;
        }}
        .berth-item-card .time-row {{
            display: flex;
            width: 100%;
            justify-content: space-between;
            font-size: 12px;
            font-weight: 700;
            color: #0f2d4c;
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
            font-weight: 600;
            color: #1f3b73;
            line-height: 1;
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
    start = row.get("start_meter")
    end = row.get("end_meter")

    if start is None or pd.isna(start):
        start = row.get("f_pos")
    if end is None or pd.isna(end):
        end = row.get("e_pos")

    def _to_float(value) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return _to_float(start), _to_float(end)


def extract_meter_range(row: pd.Series) -> Tuple[Optional[float], Optional[float]]:
    start = row.get("start_meter")
    end = row.get("end_meter")

    if start is None or pd.isna(start):
        start = row.get("f_pos")
    if end is None or pd.isna(end):
        end = row.get("e_pos")

    def _to_float(value) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return _to_float(start), _to_float(end)


def compute_item_height(row: pd.Series) -> float:
    start_meter, end_meter = extract_meter_range(row)
    if start_meter is not None and end_meter is not None:
        span = abs(end_meter - start_meter)
        if span > 0:
            return float(max(24.0, min(BERTH_VERTICAL_SPAN_PX, span)))

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
    return float(max(24.0, min(BERTH_VERTICAL_SPAN_PX, scaled)))


def compute_item_offset(row: pd.Series, item_height: float) -> float:
    start_meter, end_meter = extract_meter_range(row)
    anchor = start_meter if start_meter is not None else end_meter
    if anchor is None:
        return 0.0

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


def build_item_html(row: pd.Series) -> Tuple[str, str]:
    vessel = str(row.get("vessel") or "").strip()
    voyage = str(row.get("voyage") or "").strip()
    title = vessel if not voyage else f"{vessel} ({voyage})"

    start_text = format_time_digits(row.get("gantt_start"))
    end_text = format_time_digits(row.get("etd"))

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

    marker_top_html = (
        f"<div class='marker-text top'>{html.escape(quarantine_text)}</div>"
        if quarantine_text
        else ""
    )
    marker_bottom_html = (
        f"<div class='marker-text bottom'>{html.escape(pilot_text)}</div>"
        if pilot_text
        else ""
    )

    vessel_html = html.escape(vessel)

    html_content = f"""
    <div class='berth-item-card'>
        <div class='time-row'><span>{start_text}</span><span>{end_text}</span></div>
        {marker_top_html}
        <div class='vessel-name'>{vessel_html}</div>
        {marker_bottom_html}
        {chip_html}
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
    return html_content, tooltip

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
        "groupHeightMode": "fitItems",
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
        work["berth"] = work["berth"].apply(normalize_berth_code)
    work = attach_vessel_loa(work)
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

    if spacing_conflicts:
        conflict_texts = []
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
        st.warning(
            "배 간격 30m 미만 선박이 있습니다:\n- " + "\n- ".join(conflict_texts),
            icon="⚠️",
        )

    groups = []
    for berth in berth_order:
        label_value = (
            group_label_map.get(str(berth))
            if group_label_map is not None
            else None
        )
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
        item_height = compute_item_height(row)
        color = resolve_background_color(row)
        style = (
            f"background-color: {color};"
            f"height: {item_height:.1f}px;"
            f"min-height: {item_height:.1f}px;"
        )

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

    if height:
        effective_height = height
    else:
        row_count = max(len(groups), 1)
        effective_height = f"{max(360, int(row_count * 110))}px"

    event_result = st_timeline(items, groups, options, height=effective_height, key=key)

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


def calculate_fetch_window(
    date_start: date,
    date_end: date,
    *,
    today: Optional[date] = None,
    max_days: Optional[int] = MAX_FETCH_DAYS,
) -> Tuple[date, str, str, str]:
    today = today or datetime.today().date()
    if date_end < date_start:
        date_end = date_start

    range_days = max((date_end - date_start).days + 1, 1)
    lookback_days = max((today - date_start).days, 0)
    forward_days = max((date_end - today).days, 0)
    fetch_days = max(range_days, lookback_days + forward_days + 1)

    if max_days is not None:
        fetch_days = min(fetch_days, max_days)

    start_key = date_start.strftime("%Y%m%d")
    end_key = date_end.strftime("%Y%m%d")
    return date_end, start_key, end_key, f"{fetch_days}days"


# ------------------------------------------------------------
# Streamlit 애플리케이션 본문
# ------------------------------------------------------------
st.set_page_config(page_title="BPTC 선석 Gantt", layout="wide")
st.title("BPTC 선석 현황 Gantt")

now = datetime.now()
today_date = now.date()
default_start_date = today_date - timedelta(days=1)
default_end_date = today_date + timedelta(days=5)
_, default_start_key, default_end_key, default_v_time = calculate_fetch_window(
    default_start_date,
    default_end_date,
    today=today_date,
)

if "data_fetch_params" not in st.session_state:
    st.session_state["data_fetch_params"] = None
if "failed_fetch_params" not in st.session_state:
    st.session_state["failed_fetch_params"] = None

if "raw_df" not in st.session_state:
    try:
        initial_df = fetch_bptc_dataframe(
            v_time=default_v_time,
            start_day=default_start_key,
        )
        st.session_state["raw_df"] = initial_df.copy()
        st.session_state["working_df"] = initial_df.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
        st.session_state["data_fetch_params"] = (
            default_start_key,
            default_end_key,
            default_v_time,
        )
        st.session_state["failed_fetch_params"] = None
    except Exception:
        demo_df = get_demo_df()
        st.session_state["raw_df"] = demo_df.copy()
        st.session_state["working_df"] = demo_df.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
        st.warning("실제 데이터를 불러오지 못해 데모 데이터를 사용합니다.")
        st.session_state["data_fetch_params"] = None
        st.session_state["failed_fetch_params"] = (
            default_start_key,
            default_end_key,
            default_v_time,
        )

with st.sidebar:
    st.markdown("### 데이터 로드")
    st.markdown("---")
    date_start = st.date_input("조회 시작일", value=default_start_date)
    date_end = st.date_input("조회 종료일", value=default_end_date)
    if date_end < date_start:
        st.warning("조회 종료일은 조회 시작일보다 빠를 수 없어 시작일로 조정됩니다.")
    adjusted_end, start_key, end_key, fetch_v_time = calculate_fetch_window(
        date_start,
        date_end,
        today=today_date,
    )
    date_end = adjusted_end
    current_fetch_params = (start_key, end_key, fetch_v_time)

    def reload_data(show_success: bool = False) -> bool:
        try:
            fetched = fetch_bptc_dataframe(
                v_time=fetch_v_time,
                start_day=start_key,
            )
        except Exception as exc:
            st.error(f"크롤링 실패: {exc}")
            return False

        st.session_state["raw_df"] = fetched.copy()
        st.session_state["working_df"] = fetched.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
        st.session_state["data_fetch_params"] = current_fetch_params
        st.session_state["failed_fetch_params"] = None
        if show_success:
            st.success("데이터를 갱신했습니다.")
        return True

    if st.button("📡 BPTC 크롤링 새로고침", use_container_width=True):
        with st.spinner("조회 기간에 맞춰 데이터를 불러오는 중..."):
            st.session_state["failed_fetch_params"] = None
            if not reload_data(show_success=True):
                st.session_state["failed_fetch_params"] = current_fetch_params

    if st.button("데모 데이터로 대체", key="demo_replace", use_container_width=True):
        demo_df = get_demo_df()
        st.session_state["raw_df"] = demo_df.copy()
        st.session_state["working_df"] = demo_df.copy()
        st.session_state["last_updated"] = datetime.now()
        st.session_state["history"] = []
        st.session_state["data_fetch_params"] = current_fetch_params
        st.session_state["failed_fetch_params"] = current_fetch_params
        st.success("데모 데이터를 불러왔습니다.")

    operator_filter = st.text_input("선사 필터", value="")
    route_filter = st.text_input("항로 필터", value="")
    snap_choice = st.radio("시간 스냅", ["1h", "30m", "15m"], index=0, horizontal=True)
    timeline_days = st.slider("타임라인 표시 기간(일)", min_value=3, max_value=14, value=7, step=1)

    last_params = st.session_state.get("data_fetch_params")
    failed_params = st.session_state.get("failed_fetch_params")
    if last_params != current_fetch_params and failed_params != current_fetch_params:
        with st.spinner("조회 기간에 맞춰 데이터를 불러오는 중..."):
            if not reload_data(show_success=False):
                st.session_state["failed_fetch_params"] = current_fetch_params

    st.markdown("---")
    
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
            height=None,
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
