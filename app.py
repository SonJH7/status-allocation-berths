# app.py
# Streamlit + vis.js 타임라인 기반 BPTC 선석 현황 보드
from __future__ import annotations

import io
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from streamlit_timeline import st_timeline

import numpy as np

# ------------------------------------------------------------
# 상수 정의
# ------------------------------------------------------------
BPTC_ENDPOINT = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
BPTC_FORM_PAYLOAD = {
    "v_time": "3days",
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

AXIS_BACKGROUND_COLOR = "#e5f3ff"

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
    """BPTC 텍스트 서블릿에서 테이블 전체를 크롤링."""

    response = requests.post(
        BPTC_ENDPOINT,
        data=BPTC_FORM_PAYLOAD,
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

    return df


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
        }}
        .vis-labelset .vis-label {{
            padding-left: 52px;
        }}
        .vis-timeline .vis-item.berth-item {{
            border-radius: 12px;
            border: 1px solid rgba(0,0,0,0.25);
            font-size: 12px;
            color: #1e1e1e;
            height: 46px;
        }}
        .berth-item-content {{
            position: relative;
            width: 100%;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
            padding: 4px 8px 6px 8px;
            box-sizing: border-box;
            overflow: hidden;
        }}
        .berth-item-content .corner-tag {{
            position: absolute;
            top: 2px;
            font-size: 10px;
            font-weight: 600;
            color: #224163;
            background-color: rgba(255,255,255,0.75);
            border-radius: 6px;
            padding: 1px 4px;
        }}
        .berth-item-content .corner-tag.right {{
            right: 4px;
        }}
        .berth-item-content .corner-tag.left {{
            left: 4px;
        }}
        .berth-item-content .title {{
            text-align: center;
            font-weight: 700;
            font-size: 13px;
            color: #14233c;
            margin-bottom: 2px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .berth-item-content .ld-wrapper {{
            display: flex;
            gap: 6px;
            justify-content: center;
            align-items: center;
            flex-wrap: wrap;
            font-size: 11px;
            line-height: 1.1;
        }}
        .berth-item-content .ld-wrapper .ld-block {{
            display: flex;
            gap: 3px;
            align-items: baseline;
            background-color: rgba(255,255,255,0.6);
            border-radius: 6px;
            padding: 1px 5px;
        }}
        .berth-item-content .ld-wrapper .label {{
            font-weight: 600;
            color: #0f2d4c;
        }}
        .berth-item-content .ld-wrapper .value {{
            color: #0f2d4c;
        }}
        .berth-item-content .badge {{
            position: absolute;
            bottom: 2px;
            left: 0;
            right: 0;
            text-align: center;
            font-size: 11px;
            font-weight: 700;
            color: #0c4fb8;
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


def resolve_background_color(row: pd.Series) -> str:
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


def format_small_value(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (int, float)):
        return f"{int(value):02d}"
    text = str(value).strip()
    return text


def format_qty_value(value: object) -> str:
    if value is None or pd.isna(value):
        return "0"
    if isinstance(value, (int, float)):
        return str(int(float(value)))
    text = str(value).strip()
    return text if text else "0"


def build_item_html(row: pd.Series) -> Tuple[str, str]:
    start_tag = row.get("start_tag")
    end_tag = row.get("end_tag")

    if not start_tag:
        start_tag = row.get("eta_plan")
    if isinstance(start_tag, (datetime, pd.Timestamp)):
        start_tag = pd.Timestamp(start_tag).strftime("%d%H")
    if not start_tag:
        start_tag = row.get("load_qty")
    start_tag_text = format_small_value(start_tag)

    if not end_tag:
        end_tag = row.get("discharge_qty")
    if isinstance(end_tag, (datetime, pd.Timestamp)):
        end_tag = pd.Timestamp(end_tag).strftime("%d%H")
    end_tag_text = format_small_value(end_tag)

    voyage = str(row.get("voyage") or "").strip()
    vessel = str(row.get("vessel") or "").strip()
    title = f"{vessel}({voyage})" if voyage else vessel

    def fmt_qty(name: str, value: object) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return f"{name} 0"
        if isinstance(value, float):
            value = int(value)
        return f"{name} {value}"

    load_block = fmt_qty("적하", row.get("load_qty"))
    discharge_block = fmt_qty("양하", row.get("discharge_qty"))
    sh_block = fmt_qty("S/H", row.get("sh_qty"))
    transfer_block = fmt_qty("전배", row.get("transfer_qty"))

    badge_text = "검역" if str(row.get("quarantine_flag") or "").strip() else "도선"

    html = f"""
    <div class='berth-item-content'>
        <div class='corner-tag left'>{start_tag_text}</div>
        <div class='corner-tag right'>{end_tag_text}</div>
        <div class='title'>{title}</div>
        <div class='ld-wrapper'>
            <div class='ld-block'><span class='label'>적하</span><span class='value'>{format_qty_value(row.get('load_qty'))}</span></div>
            <div class='ld-block'><span class='label'>양하</span><span class='value'>{format_qty_value(row.get('discharge_qty'))}</span></div>
            <div class='ld-block'><span class='label'>S/H</span><span class='value'>{format_qty_value(row.get('sh_qty'))}</span></div>
            <div class='ld-block'><span class='label'>전배</span><span class='value'>{format_qty_value(row.get('transfer_qty'))}</span></div>
        </div>
        <div class='badge'>{badge_text}</div>
    </div>
    """

    tooltip_parts = [
        f"선석: {row.get('berth')}",
        f"선박: {title}",
        f"ETA: {pd.Timestamp(row.get('eta')).strftime('%Y-%m-%d %H:%M') if pd.notna(row.get('eta')) else ''}",
        f"ETD: {pd.Timestamp(row.get('etd')).strftime('%Y-%m-%d %H:%M') if pd.notna(row.get('etd')) else ''}",
        load_block,
        discharge_block,
        sh_block,
        transfer_block,
    ]
    tooltip = "<br/>".join([part for part in tooltip_parts if part])
    return html, tooltip

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
    for col in ("eta", "etd"):
        if col in work.columns:
            work[col] = to_datetime(work[col])
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
    height: str = "820px",
    key: str = "gantt",
) -> Tuple[pd.DataFrame, Optional[Dict]]:
    """df의 모든 컬럼을 보존하여 vis.js items/groups로 변환 후 렌더링."""

    ensure_timeline_css()

    prepared = prepare_dataframe(df)
    if prepared.empty:
        st.info("표시할 데이터가 없습니다.")
        return prepared, None

    base_ts = pd.Timestamp(base_date)
    view_start = base_ts.normalize() - pd.Timedelta(days=1)
    view_end = base_ts.normalize() + pd.Timedelta(days=days)

    berth_min, berth_max = berth_range
    allowed_berths = {str(b) for b in range(berth_min, berth_max + 1)}

    mask = (
        prepared["etd"].notna()
        & prepared["eta"].notna()
        & (prepared["etd"] > view_start)
        & (prepared["eta"] < view_end)
        & prepared["berth"].isin(allowed_berths)
    )
    view_df = prepared.loc[mask].copy()

    if view_df.empty:
        st.info("선택한 조건에 해당하는 선석 일정이 없습니다.")
        return prepared, None

    groups = [
        {"id": str(berth), "content": str(berth)}
        for berth in range(berth_min, berth_max + 1)
    ]

    items = []
    id_to_index: Dict[str, object] = {}

    for idx, row in view_df.iterrows():
        if pd.isna(row.get("eta")) or pd.isna(row.get("etd")):
            continue
        start = pd.Timestamp(row["eta"]).isoformat()
        end = pd.Timestamp(row["etd"]).isoformat()
        content_html, tooltip = build_item_html(row)
        item_id = str(idx)
        id_to_index[item_id] = idx

        items.append(
            {
                "id": item_id,
                "group": str(row.get("berth")),
                "start": start,
                "end": end,
                "content": content_html,
                "title": tooltip,
                "style": f"background-color: {resolve_background_color(row)};",
                "className": "berth-item",
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

    event_result = st_timeline(items, groups, options, height=height, key=key)

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
                updated.loc[target_index, "eta"] = snap_to_interval(
                    pd.to_datetime(event_payload["start"]), snap_choice
                )
            if "end" in event_payload:
                updated.loc[target_index, "etd"] = snap_to_interval(
                    pd.to_datetime(event_payload["end"]), snap_choice
                )
            if "group" in event_payload and event_payload["group"] is not None:
                updated.loc[target_index, "berth"] = str(event_payload["group"])

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
    if date_end is not None and "eta" in work.columns:
        end_ts = pd.Timestamp(date_end)
        work = work[(work["eta"].isna()) | (work["eta"] <= end_ts + pd.Timedelta(days=1))]

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
    default_end = today + timedelta(days=6)
    date_start = st.date_input("조회 시작일", value=default_start.date())
    date_end = st.date_input("조회 종료일", value=default_end.date())
    operator_filter = st.text_input("선사 필터", value="")
    route_filter = st.text_input("항로 필터", value="")
    snap_choice = st.radio("시간 스냅", ["1h", "30m", "15m"], index=0, horizontal=True)

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
    base_date = datetime.combine(date_start, datetime.min.time()) if date_start else datetime.today()

    tabs = st.tabs(["신선대(1~5선석)", "감만(6~9선석)"])

    active_events: List[Optional[Dict]] = []

    with tabs[0]:
        updated_df, event_payload = render_berth_gantt(
            filtered_df,
            base_date=base_date,
            days=7,
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
        updated_df, event_payload = render_berth_gantt(
            filtered_df,
            base_date=base_date,
            days=7,
            editable=True,
            snap_choice=snap_choice,
            berth_range=(6, 9),
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
        st.dataframe(working_df_after, use_container_width=True)

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
