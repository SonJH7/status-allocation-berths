# plot_gantt.py
# BPTC G 스타일(일자 헤더 + 선석 행 + 블록)로 보이고,
# 드래그&드롭(시간 이동/길이 조정/선석 이동) + 가로 스크롤/줌(좌우 드래그/CTRL+휠)을 지원합니다.

import pandas as pd
import streamlit as st
from streamlit_timeline import st_timeline

# 단순 색상 해시(선박명 기준) - 고정 팔레트 없이 안정적 분산
def _color_from_string(s: str) -> str:
    h = abs(hash(s)) % 360
    return f"hsl({h}, 55%, 78%)"

def _build_items_and_groups(df: pd.DataFrame, editable: bool = True):
    if df.empty:
        return [], []

    gorder = sorted(df["berth"].astype(str).unique(), key=lambda x: (len(x), x))
    groups = [{"id": g, "content": g} for g in gorder]

    items = []
    for i, r in df.reset_index(drop=True).iterrows():
        start = pd.to_datetime(r["eta"])
        end = pd.to_datetime(r["etd"])
        vessel = str(r["vessel"])
        berth = str(r["berth"])

        style = (
            f"background-color:{_color_from_string(vessel)};"
            "border:1px solid rgba(0,0,0,.25);"
            "border-radius:6px;"
            "font-size:12px;padding:2px 6px;"
            "line-height:16px;"
        )

        items.append({
            "id": str(i),
            "content": vessel,        # 박스 안에 선박명 표시
            "group": berth,           # 선석(행)
            "start": start.isoformat(),
            "end": end.isoformat(),
            "editable": editable,
            "style": style,
            # tooltip (마우스 오버)
            "title": f"{vessel}<br>선석: {berth}<br>입항: {start:%m/%d %H:%M}<br>출항: {end:%m/%d %H:%M}",
        })
    return items, groups

def _make_options(view_start, view_end, editable: bool, day_scale: bool = True):
    # vis.js timeline 옵션
    opts = {
        "stack": False,                         # 같은 행(선석)에서 겹치지 않게
        "editable": {
            "updateTime": True,                 # 드래그/리사이즈 허용
            "updateGroup": True,                # 다른 선석으로 이동 허용
            "remove": False,
            "add": False
        } if editable else False,
        "groupEditable": False,
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
        "locale": "ko",
    }
    # 일자 단위 헤더(스크린샷 느낌)
    if day_scale:
        opts["timeAxis"] = {"scale": "day", "step": 1}
    return opts

def render_gantt_g(
    df: pd.DataFrame,
    base_date: pd.Timestamp,
    days: int = 7,
    editable: bool = True,
    snap_choice: str = "1h",
    height: str = "780px",
    key: str = "gantt_g",
):
    """
    df: columns = vessel, berth, eta, etd (datetime)
    base_date: 기준일(해당일 포함, 양옆 범위 함께 보여줌)
    days: base_date 이후 며칠까지 보여줄지 (헤더는 일단 day scale)
    editable: 드래그/드롭 허용 여부
    snap_choice: '1h'|'30m'|'15m' (서버측 스냅)
    """
    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return df, None

    df = df.copy()
    df["eta"] = pd.to_datetime(df["eta"], errors="coerce")
    df["etd"] = pd.to_datetime(df["etd"], errors="coerce")

    # 보기 범위: 기준일 하루 전 ~ 기준일 + days
    view_start = pd.Timestamp(base_date).normalize() - pd.Timedelta(days=1)
    view_end = pd.Timestamp(base_date).normalize() + pd.Timedelta(days=days)

    # 범위 내 교집합만
    mask = (df["etd"] > view_start) & (df["eta"] < view_end)
    vdf = df.loc[mask].reset_index(drop=True)

    # vis.js 데이터
    items, groups = _build_items_and_groups(vdf, editable=editable)
    options = _make_options(view_start, view_end, editable=editable, day_scale=True)

    # 타임라인 렌더
    evt = st_timeline(items, groups, options, height=height, key=key)

    # 이벤트 반영(서버측 스냅)
    if isinstance(evt, dict) and "id" in evt:
        from validate import snap_to_interval
        idx = int(evt["id"])
        if "start" in evt:
            vdf.loc[idx, "eta"] = snap_to_interval(pd.to_datetime(evt["start"]), snap_choice)
        if "end" in evt:
            vdf.loc[idx, "etd"] = snap_to_interval(pd.to_datetime(evt["end"]), snap_choice)
        if "group" in evt and pd.notna(evt["group"]):
            vdf.loc[idx, "berth"] = str(evt["group"])
        return vdf, evt

    return vdf, None
