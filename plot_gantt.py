# plot_gantt.py

# Streamlit + vis.js 기반 "선석배정 현황(G)" Gantt 보드 렌더링 도우미

from __future__ import annotations

import ast
import html
import json
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st
from streamlit_timeline import st_timeline

from berth_layout import (
    BERTH_VERTICAL_SPAN_PX,
    compute_item_height,
    compute_item_offset,
    normalize_berth_label,
    normalize_berth_list,
    snap_to_interval,
)


# ------------------------------------------------------------
# 비주얼 스타일 팔레트 & 스냅 도구
# ------------------------------------------------------------

PALETTE: Dict[str, str] = {
    "gray": "#d9d9d9",
    "cyan": "#bdefff",
    "pink": "#f8d3f1",
    "beige": "#ffe3a3",
}
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
    for col in [
        "start_tag",
        "end_tag",
        "badge",
        "status",
        "loa_m",
        "load_discharge",
        "load_orientation",
    ]:
        if col not in work.columns:
            work[col] = None

    work["eta"] = pd.to_datetime(work["eta"], errors="coerce")
    work["etd"] = pd.to_datetime(work["etd"], errors="coerce")
    work = work.dropna(subset=["eta", "etd", "vessel", "berth"])

    work["berth"] = work["berth"].map(normalize_berth_label)
    work = work.reset_index(drop=True)
    return work
def _build_groups(
    berths: Iterable[str],
    order: Iterable[str] | None = None,
    label_map: Dict[str, str] | None = None,
) -> List[Dict[str, str]]:
    label_map = label_map or {}

    if order:
        normalized_order = normalize_berth_list(order)
        return [
            {
                "id": b,
                "content": label_map.get(b, b),
                "style": (
                    f"height: {BERTH_VERTICAL_SPAN_PX}px; "
                    f"line-height: {BERTH_VERTICAL_SPAN_PX}px;"
                ),
            }
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
    return [
        {
            "id": b,
            "content": label_map.get(b, b),
            "style": (
                f"height: {BERTH_VERTICAL_SPAN_PX}px; "
                f"line-height: {BERTH_VERTICAL_SPAN_PX}px;"
            ),
        }
        for b in ordered
    ]


CLICK_EVENT_TYPES = {"select", "click", "itemclick", "doubleclick", "contextmenu", "tap"}

LD_COLOR_PALETTE = {
    "load": {
        "bg": "rgba(37, 99, 235, 0.12)",
        "border": "rgba(37, 99, 235, 0.35)",
        "title": "#1d4ed8",
    },
    "discharge": {
        "bg": "rgba(5, 150, 105, 0.12)",
        "border": "rgba(5, 150, 105, 0.35)",
        "title": "#047857",
    },
}


def _normalize_orientation(value, fallback: str = "horizontal") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    text = str(value).strip().lower()
    if text in {"vertical", "세로", "v"}:
        return "vertical"
    if text in {"horizontal", "가로", "h"}:
        return "horizontal"
    return fallback


def _append_entry(target: List[str], payload) -> None:
    if payload is None or (isinstance(payload, float) and pd.isna(payload)):
        return
    if isinstance(payload, (list, tuple, set)):
        for item in payload:
            _append_entry(target, item)
        return

    text = str(payload).strip()
    if text:
        target.append(text)


def _parse_load_discharge(value, orientation: str) -> tuple[str, List[str], List[str]]:
    current_orientation = orientation
    loads: List[str] = []
    discharges: List[str] = []

    if isinstance(value, dict):
        if "orientation" in value:
            current_orientation = _normalize_orientation(value.get("orientation"), current_orientation)
        if "layout" in value:
            current_orientation = _normalize_orientation(value.get("layout"), current_orientation)
        for key in ("load", "loading", "적하"):
            if key in value:
                _append_entry(loads, value[key])
        for key in ("discharge", "discharging", "unloading", "양하"):
            if key in value:
                _append_entry(discharges, value[key])
        if "items" in value and not loads and not discharges:
            nested = value["items"]
            if isinstance(nested, (list, tuple, set)):
                for item in nested:
                    nested_orientation, nested_loads, nested_discharges = _parse_load_discharge(
                        item, current_orientation
                    )
                    current_orientation = _normalize_orientation(nested_orientation, current_orientation)
                    loads.extend(nested_loads)
                    discharges.extend(nested_discharges)
        return current_orientation, loads, discharges

    if isinstance(value, (list, tuple, set)):
        for item in value:
            nested_orientation, nested_loads, nested_discharges = _parse_load_discharge(item, current_orientation)
            current_orientation = _normalize_orientation(nested_orientation, current_orientation)
            loads.extend(nested_loads)
            discharges.extend(nested_discharges)
        return current_orientation, loads, discharges

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return current_orientation, loads, discharges

        try:
            parsed = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None

        if parsed is None:
            try:
                parsed = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                parsed = None

        if isinstance(parsed, (dict, list, tuple, set)):
            return _parse_load_discharge(parsed, current_orientation)

        if "|" in text:
            prefix, suffix = text.split("|", 1)
            candidate_orientation = _normalize_orientation(prefix, current_orientation)
            if candidate_orientation != current_orientation:
                current_orientation = candidate_orientation
                text = suffix.strip()

        segments = [seg.strip() for seg in text.split(";") if seg.strip()]
        matched = False
        for segment in segments:
            if ":" in segment:
                key, payload = segment.split(":", 1)
                key = key.strip().lower()
                payload = payload.strip()
                matched = True
                if key in {"load", "loading", "적하"}:
                    _append_entry(loads, payload)
                elif key in {"discharge", "discharging", "unloading", "양하"}:
                    _append_entry(discharges, payload)
                else:
                    _append_entry(loads if not loads else discharges, payload)
            else:
                lowered = segment.lower()
                if lowered in {"load", "loading", "적하"}:
                    matched = True
                    _append_entry(loads, segment)
                elif lowered in {"discharge", "discharging", "unloading", "양하"}:
                    matched = True
                    _append_entry(discharges, segment)

        if not matched:
            _append_entry(loads if not loads else discharges, text)

        return current_orientation, loads, discharges

    _append_entry(loads if not loads else discharges, value)
    return current_orientation, loads, discharges


def _build_load_discharge_html(
    loads: List[str],
    discharges: List[str],
    orientation: str,
    *,
    compact: bool = True,
) -> str:
    direction = "row" if orientation == "horizontal" else "column"
    gap = "4px" if compact else "12px"
    margin_top = "2px" if compact else "16px"
    body_font = "10px" if compact else "13px"
    title_font = "10px" if compact else "12px"
    padding = "2px 4px" if compact else "8px 12px"
    title_margin = "1px" if compact else "6px"

    def build_section(title: str, entries: List[str], role: str) -> str:
        palette = LD_COLOR_PALETTE[role]
        if entries:
            body = "<br>".join(html.escape(item) for item in entries)
        else:
            body = "<span style=\"opacity:0.6;\">정보 없음</span>"
        return (
            "<div style=\"flex:1;min-width:0;"
            f"padding:{padding};"
            "border-radius:6px;"
            f"background-color:{palette['bg']};"
            f"border:1px solid {palette['border']};\">"
            f"<div style=\"font-size:{title_font};font-weight:700;color:{palette['title']};"
            f"margin-bottom:{title_margin};letter-spacing:0.02em;\">{title}</div>"
            f"<div style=\"font-size:{body_font};line-height:1.35;color:#111827;"
            "white-space:normal;word-break:keep-all;\">"
            f"{body}</div>"
            "</div>"
        )

    load_block = build_section("적하 (Load)", loads, "load")
    discharge_block = build_section("양하 (Discharge)", discharges, "discharge")

    return (
        f"<div style=\"display:flex;flex-direction:{direction};gap:{gap};"
        "width:100%;align-items:stretch;"
        f"margin-top:{margin_top};\">"
        f"{load_block}{discharge_block}"
        "</div>"
    )


def _resolve_load_discharge(row: pd.Series, default_orientation: str) -> tuple[str, List[str], List[str]]:
    orientation_hint = row.get("load_orientation")
    orientation = _normalize_orientation(orientation_hint, default_orientation)
    parsed_orientation, loads, discharges = _parse_load_discharge(row.get("load_discharge"), orientation)
    orientation = _normalize_orientation(parsed_orientation, orientation)
    return orientation, loads, discharges


def _format_timestamp(value, fmt: str) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "-"
    return ts.strftime(fmt)


def _format_number(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.1f}"


def _render_vessel_modal(
    row: pd.Series,
    row_idx,
    orientation: str,
    loads: List[str],
    discharges: List[str],
    timeline_key: str,
) -> None:
    vessel = str(row.get("vessel", "")).strip()
    title = f"선박 상세 정보 — {vessel}" if vessel else "선박 상세 정보"
    orientation_label = "가로" if orientation == "horizontal" else "세로"

    modal_key = f"modal_{timeline_key}_{row_idx}"
    with st.modal(title, key=modal_key):
        base_info = [
            ("선석", row.get("berth")),
            ("입항(ETA)", _format_timestamp(row.get("eta"), "%Y-%m-%d %H:%M")),
            ("출항(ETD)", _format_timestamp(row.get("etd"), "%Y-%m-%d %H:%M")),
            ("LOA (m)", _format_number(row.get("loa_m"))),
            ("시작 위치 (m)", _format_number(row.get("start_meter"))),
        ]

        details = "<br>".join(
            f"<strong>{html.escape(label)}:</strong> {html.escape(str(value)) if value not in {None, '-'} else value}"
            if value not in {None, '-'}
            else f"<strong>{html.escape(label)}:</strong> -"
            for label, value in base_info
        )
        st.markdown(details, unsafe_allow_html=True)

        badge = row.get("badge")
        if badge:
            st.markdown(
                f"<div style=\"margin-top:12px;padding:6px 10px;border-radius:6px;"
                "background-color:rgba(14,116,144,0.08);color:#0f172a;display:inline-block;\">"
                f"배지: {html.escape(str(badge))}</div>",
                unsafe_allow_html=True,
            )

        load_html = _build_load_discharge_html(loads, discharges, orientation, compact=False)
        st.markdown("---")
        st.markdown(f"**적하/양하 정보** · 표시 방향: {orientation_label}")
        st.markdown(load_html, unsafe_allow_html=True)

def _abbreviate_vessel_label(name: str, max_length: int = 12) -> str:
    """선박명을 카드 내에서 짧게 표현한다."""

    if not name:
        return ""

    text = str(name).strip()
    if len(text) <= max_length:
        return text

    # 복수 단어인 경우 머리글자를 활용한다.
    words = [segment for segment in text.replace("-", " ").split() if segment]
    if len(words) >= 2:
        acronym = "".join(word[0].upper() for word in words if word[0].isalnum())
        if 2 <= len(acronym) <= max_length:
            return acronym

    return text[: max_length - 1] + "…"


def _extract_meter_value(row: pd.Series, candidates: Iterable[str]):
    for key in candidates:
        if key not in row.index:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _format_meter_value(value) -> tuple[str, bool]:
    if value is None:
        return "-", False

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "-", False
        try:
            numeric = float(text.replace(",", ""))
        except ValueError:
            return text, False
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value), False

    if pd.isna(numeric):
        return "-", False

    if abs(numeric - round(numeric)) < 1e-6:
        return f"{int(round(numeric))}", True

    return f"{numeric:.1f}", True


def _build_fe_html(row: pd.Series) -> str:
    f_raw = _extract_meter_value(row, ("f_pos", "F_POS", "start_meter"))
    e_raw = _extract_meter_value(row, ("e_pos", "E_POS", "end_meter"))

    f_text, f_is_numeric = _format_meter_value(f_raw)
    e_text, e_is_numeric = _format_meter_value(e_raw)

    if f_text == "-" and e_text == "-":
        return ""

    def _render(label: str, text: str, numeric: bool) -> str:
        if text == "-":
            return f"<span style=\"opacity:0.65;\">{label} -</span>"
        suffix = "m" if numeric and not text.lower().endswith("m") else ""
        return f"<span>{label} {html.escape(text)}{suffix}</span>"

    return (
        "<div style=\"display:flex;justify-content:space-between;align-items:center;"
        "margin-top:6px;font-size:11px;font-weight:600;letter-spacing:0.01em;\">"
        f"{_render('F', f_text, f_is_numeric)}"
        f"{_render('E', e_text, e_is_numeric)}"
        "</div>"
    )


def _build_item(row: pd.Series, idx: int, editable: bool, default_orientation: str) -> Dict:
    vessel_label = str(row.get("vessel", ""))
    vessel_display = html.escape(_abbreviate_vessel_label(vessel_label))
    badge = row.get("badge")

    orientation, loads, discharges = _resolve_load_discharge(row, default_orientation)

    eta_text = _format_timestamp(row.get("eta"), "%H:%M")
    etd_text = _format_timestamp(row.get("etd"), "%H:%M")
    if eta_text == "-":
        start_fallback = row.get("start_tag")
        if pd.notna(start_fallback) and str(start_fallback):
            eta_text = str(start_fallback)
    if etd_text == "-":
        end_fallback = row.get("end_tag")
        if pd.notna(end_fallback) and str(end_fallback):
            etd_text = str(end_fallback)

    start_tag_html = (
        f'<span style="position:absolute;top:2px;left:4px;font-size:10px;font-weight:600;opacity:.85;">시작 {html.escape(eta_text)}</span>'
        if eta_text != "-"
        else ""
    )
    end_tag_html = (
        f'<span style="position:absolute;top:2px;right:4px;font-size:10px;font-weight:600;opacity:.85;">종료 {html.escape(etd_text)}</span>'
        if etd_text != "-"
        else ""
    )
    badge_html = (
        f'<div style="position:absolute;bottom:2px;left:50%;transform:translateX(-50%);font-size:11px;color:#0b69ff;">{html.escape(str(badge))}</div>'
        if pd.notna(badge) and str(badge)
        else ""
    )

    fe_html = _build_fe_html(row)

    content = (
        "<div style=\"position:relative;width:100%;height:100%;display:flex;flex-direction:column;justify-content:flex-start;\">"
        f"{start_tag_html}"
        f"{end_tag_html}"
        f"<div style=\"text-align:center;font-weight:700;font-size:12px;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\">{vessel_display}</div>"
        f"{fe_html}"
        f"{badge_html}"
        "</div>"
    )

    status = str(row.get("status")) if pd.notna(row.get("status")) else "gray"
    color = PALETTE.get(status, PALETTE["gray"])

    height_px = compute_item_height(row)
    offset_px = compute_item_offset(row, height_px)
    item_class = "berth-item bp-aligned"
    style = (
        f"background-color:{color};"
        "border:1px solid rgba(0,0,0,.25);"
        "border-radius:6px;"
        "font-size:12px;"
        "padding:4px 6px;"
        "line-height:1.35;"
        "color:#1f2937;"
        "display:flex;"
        "align-items:stretch;"
        "justify-content:center;"
        "overflow:hidden;"
        f"height:{height_px}px;"
        f"margin-top:{offset_px}px;"
    )

    tooltip_lines = [
        vessel_label or "선박",
        f"선석: {row.get('berth', '-')}",
        f"ETA: {_format_timestamp(row.get('eta'), '%m/%d %H:%M')}",
        f"ETD: {_format_timestamp(row.get('etd'), '%m/%d %H:%M')}",
    ]

    if loads:
        tooltip_lines.append("적하: " + ", ".join(loads))
    if discharges:
        tooltip_lines.append("양하: " + ", ".join(discharges))

    tooltip = "<br>".join(html.escape(line) for line in tooltip_lines)

    return {
        "id": str(idx),
        "group": str(row["berth"]),
        "content": content,
        "start": pd.to_datetime(row["eta"]).isoformat(),
        "end": pd.to_datetime(row["etd"]).isoformat(),
        "editable": editable,
        "style": style,
        "className": item_class,
        "title": tooltip,
        "orientation": orientation,
        "loadEntries": loads,
        "dischargeEntries": discharges,
    }


def _build_items(view_df: pd.DataFrame, editable: bool, default_orientation: str) -> List[Dict]:
    items: List[Dict] = []
    for idx in view_df.index:
        row = view_df.loc[idx]
        items.append(_build_item(row, idx, editable, default_orientation))
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
        "groupHeight": BERTH_VERTICAL_SPAN_PX,  # 선석별 높이를 300px로 적용
        "verticalScroll": True,
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
    load_discharge_orientation: str = "horizontal",
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
        allowed_list = normalize_berth_list(allowed_berths)
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
    items = _build_items(view_df, editable, load_discharge_orientation)
    options = _make_options(view_start, view_end, editable)

    _ensure_timeline_css(key)

    event = st_timeline(items, groups, options, height=height, key=key)

    modal_context: tuple[pd.Series, int | str, str, List[str], List[str]] | None = None
    event_payload: Dict | None = None

    if isinstance(event, dict):
        raw_id = event.get("id") if event.get("id") is not None else event.get("item")
        row_idx = None
        if raw_id is not None:
            try:
                row_idx = int(raw_id)
            except (TypeError, ValueError):
                row_idx = raw_id

        if row_idx is not None and row_idx in view_df.index and row_idx in df_prepared.index:
            event_payload = event
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

            event_type = str(event.get("event") or event.get("type") or "").lower()
            has_edit_keys = any(key in event for key in ("start", "end", "group"))
            should_open_modal = False

            if event_type in CLICK_EVENT_TYPES and not has_edit_keys:
                should_open_modal = True
            elif not event_type and not has_edit_keys:
                should_open_modal = True

            if should_open_modal:
                modal_row = df_prepared.loc[row_idx]
                orientation, loads, discharges = _resolve_load_discharge(modal_row, load_discharge_orientation)
                modal_context = (modal_row.copy(), row_idx, orientation, loads, discharges)

    if modal_context is not None:
        modal_row, modal_idx, modal_orientation, modal_loads, modal_discharges = modal_context
        _render_vessel_modal(modal_row, modal_idx, modal_orientation, modal_loads, modal_discharges, key)

    return df_prepared.reset_index(drop=True), event_payload


# 기존 코드와의 호환성을 위해 alias 제공
render_gantt_g = render_berth_gantt
