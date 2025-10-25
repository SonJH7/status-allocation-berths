# plot_gantt.py

# Streamlit + vis.js 기반 "선석배정 현황(G)" Gantt 보드 렌더링 도우미

from __future__ import annotations

import html
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import streamlit as st
from streamlit_timeline import st_timeline


# ------------------------------------------------------------
# 비주얼 스타일 팔레트 & 스냅 도구
# ------------------------------------------------------------

PALETTE: Dict[str, str] = {
    "gray": "#d9d9d9",
    "cyan": "#bdefff",
    "pink": "#f8d3f1",
    "beige": "#ffe3a3",
}


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

    snapped = ts.floor(f"{minutes}min")
    return snapped


def _ensure_timeline_css(unique_key: str) -> None:
    """타임라인 꾸밈 CSS를 중복 없이 주입."""

    css_key = f"__berth_gantt_css_{unique_key}"
    if st.session_state.get(css_key):
        return

    st.markdown(
        """
        <style>
        /* 전체 폰트/경계 스타일 보정 */
        .vis-timeline .vis-item.vis-range {
            box-shadow: none;
        }
        .vis-timeline .vis-item .vis-item-content {
            padding: 0 !important;
        }
        .vis-timeline .vis-labelset::before {
            content: "선석";
            position: absolute;
            left: 12px;
            top: 6px;
            font-size: 12px;
            font-weight: 600;
            color: #1f1f1f;
        }
        .vis-timeline .vis-group:nth-child(odd) {
            background-color: rgba(17, 24, 39, 0.035);
        }
        .vis-timeline .vis-group:nth-child(even) {
            background-color: rgba(255, 255, 255, 0.0);
        }
        .vis-timeline .vis-panel.vis-left {
            min-width: 70px;
        }
        .vis-timeline .vis-labelset .vis-label .vis-inner {
            font-weight: 600;
            font-size: 13px;
            color: #111827;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.session_state[css_key] = True


def normalize_berth_label(value) -> str:
    """선석 라벨을 숫자 위주로 정규화한다 (예: "9(1)" → "9")."""

    if pd.isna(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    # 괄호 앞에 실제 선석 번호가 표기된 경우 우선적으로 사용한다.
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


def _berth_sort_key(value: str) -> Tuple[int, str]:
    if value.isdigit():
        return (0, int(value))
    return (1, value)


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """렌더링 전 컬럼/형식을 보정."""

    if df is None:
        return pd.DataFrame(columns=[
            "berth", "vessel", "eta", "etd",
            "start_tag", "end_tag", "badge", "status", "loa_m",
        ])

    work = df.copy()

    # 필수 컬럼 확보
    for col in ["start_tag", "end_tag", "badge", "status", "loa_m"]:
        if col not in work.columns:
            work[col] = None

    work["eta"] = pd.to_datetime(work["eta"], errors="coerce")
    work["etd"] = pd.to_datetime(work["etd"], errors="coerce")
    work = work.dropna(subset=["eta", "etd", "vessel", "berth"])

    work["berth"] = work["berth"].map(normalize_berth_label)
    work = work.reset_index(drop=True)
    return work


def _normalize_berth_list(values: Iterable[str] | None) -> List[str]:
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


def _build_groups(
    berths: Iterable[str],
    order: Iterable[str] | None = None,
    label_map: Dict[str, str] | None = None,
) -> List[Dict[str, str]]:
    label_map = label_map or {}

    if order:
        normalized_order = _normalize_berth_list(order)
        return [
            {"id": b, "content": label_map.get(b, b)}
            for b in normalized_order
        ]

    collected: List[str] = []
    seen = set()
    for b in berths:
        norm = normalize_berth_label(b)
        if not norm or norm in seen:
            continue
        collected.append(norm)
        seen.add(norm)

    ordered = sorted(collected, key=_berth_sort_key)
    return [{"id": b, "content": label_map.get(b, b)} for b in ordered]


def _compute_height(row: pd.Series) -> float:
    """모든 선석 블록의 높이를 고정된 범위 내 값으로 유지한다."""

    DEFAULT_HEIGHT = 40.0
    MIN_HEIGHT = 20.0
    MAX_HEIGHT = 80.0

    return max(MIN_HEIGHT, min(MAX_HEIGHT, DEFAULT_HEIGHT))


def _build_item(row: pd.Series, idx: int, editable: bool) -> Dict:
    vessel = html.escape(str(row.get("vessel", "")))
    start_tag = row.get("start_tag")
    end_tag = row.get("end_tag")
    badge = row.get("badge")

    start_tag_html = (
        f'<span style="position:absolute;top:2px;left:4px;font-size:10px;opacity:.8;">{html.escape(str(start_tag))}</span>'
        if pd.notna(start_tag) and str(start_tag) else ""
    )
    end_tag_html = (
        f'<span style="position:absolute;top:2px;right:4px;font-size:10px;opacity:.8;">{html.escape(str(end_tag))}</span>'
        if pd.notna(end_tag) and str(end_tag) else ""
    )
    badge_html = (
        f'<div style="position:absolute;bottom:2px;left:50%;transform:translateX(-50%);font-size:11px;color:#0b69ff;">{html.escape(str(badge))}</div>'
        if pd.notna(badge) and str(badge) else ""
    )

    content = (
        "<div style=\"position:relative;\">"
        f"{start_tag_html}"
        f"<div style=\"text-align:center;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\">{vessel}</div>"
        f"{end_tag_html}"
        f"{badge_html}"
        "</div>"
    )

    status = str(row.get("status")) if pd.notna(row.get("status")) else "gray"
    color = PALETTE.get(status, PALETTE["gray"])

    height = _compute_height(row)
    style = (
        f"background-color:{color};"
        "border:1px solid rgba(0,0,0,.25);"
        "border-radius:6px;"
        "font-size:12px;"
        "padding:2px 6px;"
        "line-height:16px;"
        "color:#1f2937;"
        "display:flex;"
        "align-items:center;"
        "justify-content:center;"
        f"height:{height}px;"
    )

    start = pd.to_datetime(row["eta"])
    end = pd.to_datetime(row["etd"])

    tooltip = (
        f"{vessel}<br>선석: {row['berth']}<br>ETA: {start:%m/%d %H:%M}<br>ETD: {end:%m/%d %H:%M}"
    )

    return {
        "id": str(idx),
        "group": str(row["berth"]),
        "content": content,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "editable": editable,
        "style": style,
        "title": tooltip,
    }


def _build_items(view_df: pd.DataFrame, editable: bool) -> List[Dict]:
    items: List[Dict] = []
    for idx in view_df.index:
        row = view_df.loc[idx]
        items.append(_build_item(row, idx, editable))
    return items


def _make_options(view_start: pd.Timestamp, view_end: pd.Timestamp, editable: bool) -> Dict:
    return {
        "stack": False,
        "editable": {
            "updateTime": True,                 # 드래그/리사이즈 허용
            "updateGroup": True,                # 다른 선석으로 이동 허용
            "remove": False,
            "add": False,
        } if editable else False,
        "groupEditable": False,
        "groupHeightMode": "fixed",            # 선석별 행 높이를 고정
        "groupHeight": 64,                     # 모든 선석을 동일 높이로 표시
        "min": view_start.isoformat(),
        "max": view_end.isoformat(),
        "orientation": {"axis": "top"},        # 상단에 날짜축
        "margin": {"item": 6, "axis": 12},
        "multiselect": False,
        "moveable": True,                       # 좌우 드래그로 가로 스크롤
        "zoomable": True,                       # 확대/축소 허용
        "zoomKey": "ctrlKey",                   # CTRL + 휠로 줌 (실수 방지)
        "zoomMin": 1000 * 60 * 15,              # 최소 15분 단위까지 확대
        "zoomMax": 1000 * 60 * 60 * 24 * 30,    # 최대 30일 보기
        "timeAxis": {"scale": "day", "step": 1},
        "locale": "ko",
    }

def render_berth_gantt(
    df: pd.DataFrame,
    base_date: pd.Timestamp,
    days: int = 7,
    editable: bool = True,
    snap_choice: str = "1h",
    height: str = "780px",
    key: str = "berth_gantt",
    allowed_berths: Iterable[str] | None = None,
    group_label_map: Dict[str, str] | None = None,
) -> Tuple[pd.DataFrame, Dict | None]:
    """Streamlit에서 선석 Gantt 보드를 렌더링하고 이벤트를 반영한 DataFrame을 반환."""

    base_ts = pd.Timestamp(base_date)
    df_prepared = _prepare_dataframe(df)

    if df_prepared.empty:
        st.info("표시할 선석 일정이 없습니다.")
        return df_prepared, None

    view_start = base_ts.normalize() - pd.Timedelta(days=1)
    view_end = base_ts.normalize() + pd.Timedelta(days=days)

    mask = (df_prepared["etd"] > view_start) & (df_prepared["eta"] < view_end)
    view_df = df_prepared.loc[mask].copy()

    allowed_list: List[str] | None = None
    label_map: Dict[str, str] = {}

    if group_label_map:
        for raw_key, display in group_label_map.items():
            normalized_key = normalize_berth_label(raw_key)
            if normalized_key:
                label_map[normalized_key] = display

    if allowed_berths is not None:
        allowed_list = _normalize_berth_list(allowed_berths)
        allowed_set = set(allowed_list)
        if allowed_set:
            view_df = view_df[
                view_df["berth"].map(
                    lambda x: normalize_berth_label(x) in allowed_set
                )
            ].copy()
        else:
            view_df = view_df.iloc[0:0]

    if view_df.empty:
        st.info("선택한 기간에 해당하는 일정이 없습니다.")
        return df_prepared, None

    groups = _build_groups(
        view_df["berth"].dropna(),
        order=allowed_list,
        label_map=label_map,
    )
    items = _build_items(view_df, editable)
    options = _make_options(view_start, view_end, editable)

    _ensure_timeline_css(key)

    event = st_timeline(items, groups, options, height=height, key=key)

    if isinstance(event, dict) and event.get("id") is not None:
        raw_id = event["id"]
        try:
            row_idx = int(raw_id)
        except (TypeError, ValueError):
            row_idx = raw_id

        if row_idx in view_df.index:
            if "start" in event:
                snapped = snap_to_interval(pd.to_datetime(event["start"]), snap_choice)
                view_df.loc[row_idx, "eta"] = snapped
                df_prepared.loc[row_idx, "eta"] = snapped
            if "end" in event:
                snapped = snap_to_interval(pd.to_datetime(event["end"]), snap_choice)
                view_df.loc[row_idx, "etd"] = snapped
                df_prepared.loc[row_idx, "etd"] = snapped
            if "group" in event and event["group"] is not None:
                normalized = normalize_berth_label(event["group"])
                view_df.loc[row_idx, "berth"] = normalized
                df_prepared.loc[row_idx, "berth"] = normalized

            return df_prepared.reset_index(drop=True), event

    return df_prepared.reset_index(drop=True), None


def get_demo_df(base_date: pd.Timestamp | None = None) -> pd.DataFrame:
    """스크린샷과 유사한 7일치 5선석 데모 데이터를 생성."""

    base = pd.Timestamp(base_date) if base_date is not None else pd.Timestamp.today().normalize()
    day0 = base.normalize()

    def row(berth, vessel, start_offset_h, duration_h, status, start_tag, end_tag, badge=None, loa=45):
        start = day0 + pd.Timedelta(hours=start_offset_h)
        end = start + pd.Timedelta(hours=duration_h)
        return {
            "berth": berth,
            "vessel": vessel,
            "eta": start,
            "etd": end,
            "status": status,
            "start_tag": start_tag,
            "end_tag": end_tag,
            "badge": badge,
            "loa_m": loa,
        }

    sample_rows = [
        row("1", "CKSM-18(S)", -12, 30, "cyan", "15", "11", "도선", 48),
        row("1", "KOBE STAR", 20, 18, "pink", "24", "06", "검역", 60),
        row("2", "MOON BAY", -5, 26, "beige", "09", "07", None, 52),
        row("2", "HANIL SUN", 30, 16, "gray", "05", "08", "도선", 42),
        row("3", "KARISMA", 10, 20, "cyan", "22", "03", "검역", 55),
        row("3", "BAEKDU", -18, 24, "pink", "18", "12", None, 70),
        row("4", "ORIENT GLORY", 5, 30, "beige", "11", "04", "도선", 80),
        row("4", "BLUE PEARL", 40, 22, "gray", "06", "05", None, 35),
        row("5", "TITAN", -8, 28, "cyan", "13", "09", "검역", 66),
        row("5", "SUNRISE", 32, 20, "pink", "07", "10", "도선", 58),
    ]

    df = pd.DataFrame(sample_rows)
    return df


# 기존 코드와의 호환성을 위해 alias 제공
render_gantt_g = render_berth_gantt
