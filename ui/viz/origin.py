# =========================
# ui/viz/origin.py
# =========================
import math
import pandas as pd
import streamlit as st
from streamlit_plotly_events import plotly_events

from ui.viz.common import period_str_kr, render_timeline_week
from schema import MIN_CLEARANCE_M, snap_time_5min, snap_y_30m, validate_df
from streamlit_drag_timeline import drag_timeline


def render_origin_view_static(df_origin: pd.DataFrame, title_prefix: str = ""):
    """읽기 전용(드래그 없음) 주간 그래프 비교 배치용"""
    st.subheader(f"🧭 {title_prefix} 읽기 전용 타임라인(SND / GAM)")
    tab_snd, tab_gam = st.tabs(["신항 SND", "감만 GAM"])

    def _one(terminal: str):
        df_t = df_origin[df_origin["terminal"] == terminal].reset_index(drop=True)
        if df_t.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return
        fig, (x0, x1) = render_timeline_week(df_t, terminal=terminal, title="")
        fig.update_layout(title=f"{title_prefix} {terminal} · {period_str_kr(x0, x1)}")
        fig.update_layout(width=2400, height=600)
        st.plotly_chart(fig, use_container_width=True)

    with tab_snd:
        _one("SND")
    with tab_gam:
        _one("GAM")


# ---------- 세션 상태 유틸 ----------
def _init_edit_buffers(df_norm: pd.DataFrame):
    if "edit_df" not in st.session_state:
        st.session_state["edit_df"] = df_norm.copy()
    if "orig_df_snapshot" not in st.session_state:
        st.session_state["orig_df_snapshot"] = df_norm.copy()
    if "undo_df" not in st.session_state:
        st.session_state["undo_df"] = None
    if "selected_row_id" not in st.session_state:
        st.session_state["selected_row_id"] = None
    if "edit_logs" not in st.session_state:
        st.session_state["edit_logs"] = []


def _append_log(before, after):
    st.session_state["edit_logs"].append(
        {
            "row_id": before.get("row_id"),
            "vessel": before.get("vessel", ""),
            "voyage": before.get("voyage", ""),
            "terminal": before.get("terminal", ""),
            "berth": before.get("berth", ""),
            "start_before": before.get("start"),
            "end_before": before.get("end"),
            "f_before": before.get("f"),
            "e_before": before.get("e"),
            "start_after": after.get("start"),
            "end_after": after.get("end"),
            "f_after": after.get("f"),
            "e_after": after.get("e"),
            "ts": pd.Timestamp.now(),
        }
    )


# ---------- 이동 스냅(5분/30m) ----------
def _is_finite_num(x) -> bool:
    try:
        v = float(x)
        return not (math.isnan(v) or math.isinf(v))
    except Exception:
        return False


def _ts_equal(a, b) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    return pd.Timestamp(a).value == pd.Timestamp(b).value


def _num_equal(a, b, eps=1e-6) -> bool:
    if not _is_finite_num(a) and not _is_finite_num(b):
        return True
    if not _is_finite_num(a) or not _is_finite_num(b):
        return False
    return abs(float(a) - float(b)) < eps


def _apply_move(df: pd.DataFrame, row_id: int, dmin=0, dy=0.0) -> pd.DataFrame:
    out = df.copy()
    idx_arr = out.index[out["row_id"] == row_id]
    if len(idx_arr) == 0:
        return out
    idx = idx_arr[0]
    row = out.loc[idx]

    # 기존 값
    s0, e0 = row.get("start"), row.get("end")
    f0, e1 = row.get("f"), row.get("e")

    # 정보 복제(기존값)
    s1, e2 = s0, e0
    f1, e3 = f0, e1

    changed = False

    # 시간 이동 (start/end가 유효한 경우에만)
    if dmin != 0 and (pd.notna(s0) and pd.notna(e0)):
        s1 = snap_time_5min(pd.to_datetime(s0) + pd.Timedelta(minutes=dmin))
        e2 = snap_time_5min(pd.to_datetime(e0) + pd.Timedelta(minutes=dmin))
        if (not _ts_equal(s0, s1)) or (not _ts_equal(e0, e2)):
            changed = True

    # 세로 이동 (f/e가 유효한 경우에만)
    if dy != 0 and _is_finite_num(f0) and _is_finite_num(e1):
        L = float(e1) - float(f0)
        if _is_finite_num(L) and abs(L) > 0:
            mid = (float(f0) + float(e1)) / 2.0
            new_mid = snap_y_30m(mid + float(dy))
            f1 = new_mid - abs(L) / 2.0
            e3 = new_mid + abs(L) / 2.0
            if (not _num_equal(f0, f1)) or (not _num_equal(e1, e3)):
                changed = True

    # 실제 변경 없으면 그대로 반환(로그 없음)
    if not changed:
        return out

    before = dict(row)
    out.at[idx, "start"] = s1
    out.at[idx, "end"] = e2
    out.at[idx, "f"] = f1
    out.at[idx, "e"] = e3
    after = dict(out.loc[idx])

    _append_log(before, after)  # 실제 바뀐 경우만
    st.session_state["undo_df"] = df.copy()
    return out


# ---------- Plotly 편집기 ----------
def render_origin_view(df_origin: pd.DataFrame):
    """
    - 중앙 라벨 클릭으로 선택
    - Shift+클릭: 선택된 막대를 해당 좌표로 이동(드래그 대신)
    - 변경은 st.session_state['edit_df']에 반영, 로그는 st.session_state['edit_logs']
    """
    _init_edit_buffers(df_origin)

    # 입력 데이터가 새로 들어온 경우에만 편집 버퍼를 갱신하여 UnboundLocalError 방지
    if "orig_df_snapshot" not in st.session_state or not st.session_state["orig_df_snapshot"].equals(df_origin):
        st.session_state["orig_df_snapshot"] = df_origin.copy()
        st.session_state["edit_df"] = df_origin.copy()
        st.session_state["selected_row_id"] = None
    st.subheader("🖱️ 편집 가능한 타임라인(SND / GAM)")
    st.caption("· 클릭: 선택  · Shift+클릭: 지정 위치로 이동(드롭)  · 스냅: 5분/30m")

    tab_snd, tab_gam = st.tabs(["신항 SND", "감만 GAM"])

    def _render_one(terminal: str):
        df_all = st.session_state.get("edit_df")
        if df_all is None or not isinstance(df_all, pd.DataFrame) or df_all.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return

        df_t = df_all[df_all["terminal"] == terminal].reset_index(drop=True)
        if df_t.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return

        # 그림 생성
        fig, (x0, x1) = render_timeline_week(df_t, terminal=terminal, title="")
        fig.update_layout(title=f"{terminal} · {period_str_kr(x0, x1)}", width=2400, height=600)

        events = plotly_events(
            fig,
            click_event=True,
            hover_event=True,
            select_event=False,
            override_height=600,
            override_width=2400,
            key=f"plotly-events-{terminal}",
        )

        target_event = None
        for ev in reversed(events or []):
            if ev.get("customdata") is not None:
                target_event = ev
                break
        if target_event is None and events:
            target_event = events[-1]

        if target_event:
            row_id = target_event.get("customdata")
            event_meta = target_event.get("event") or {}
            shift_pressed = bool(event_meta.get("shiftKey")) or bool(target_event.get("shiftKey"))

            if row_id is not None:
                st.session_state["selected_row_id"] = int(row_id)

            if shift_pressed and row_id is not None and target_event.get("x") is not None:
                rid = int(row_id)
                df_current = st.session_state["edit_df"]
                idx_arr = df_current.index[(df_current["row_id"] == rid) & (df_current["terminal"] == terminal)]
                if len(idx_arr):
                    idx = idx_arr[0]
                    s = pd.to_datetime(df_current.loc[idx, "start"])
                    e = pd.to_datetime(df_current.loc[idx, "end"])
                    if pd.notna(s) and pd.notna(e):
                        mid_old = s + (e - s) / 2
                        try:
                            new_x = pd.to_datetime(target_event.get("x"))
                        except Exception:
                            new_x = None

                        if new_x is not None:
                            diff_min = (new_x - mid_old).total_seconds() / 60.0
                            dmin = int(round(diff_min / 5.0) * 5)

                            f0 = df_current.loc[idx, "f"]
                            e0 = df_current.loc[idx, "e"]
                            dy = 0.0
                            y_val = target_event.get("y")
                            try:
                                if _is_finite_num(f0) and _is_finite_num(e0) and y_val is not None:
                                    mid_y_old = (float(f0) + float(e0)) / 2.0
                                    dy = float(y_val) - mid_y_old
                            except Exception:
                                dy = 0.0
                            st.session_state["edit_df"] = _apply_move(
                                st.session_state["edit_df"], rid, dmin=dmin, dy=dy
                            )

        # 간단 검증은 계속 수행하되 화면 노출은 생략
        _ = validate_df(st.session_state["edit_df"])

        # 선택 정보도 상태만 유지하고 화면에는 노출하지 않음
        _ = st.session_state.get("selected_row_id")

    with tab_snd:
        _render_one("SND")
    with tab_gam:
        _render_one("GAM")


# ---------- React 드래그 편집기 ----------
def _to_iso_ts(value):
    try:
        ts = pd.to_datetime(value)
        if pd.isna(ts):
            return None
        return ts.isoformat()
    except Exception:
        return None


def _build_drag_items(df: pd.DataFrame):
    def _safe_num(x):
        try:
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        except Exception:
            return None

    items = []
    for row in df.itertuples(index=False):
        items.append(
            {
                "row_id": getattr(row, "row_id", None),
                "vessel": getattr(row, "vessel", ""),
                "voyage": getattr(row, "voyage", ""),
                "terminal": getattr(row, "terminal", ""),
                "berth": getattr(row, "berth", None),
                "start": _to_iso_ts(getattr(row, "start", None)),
                "end": _to_iso_ts(getattr(row, "end", None)),
                "f": _safe_num(getattr(row, "f", None)),
                "e": _safe_num(getattr(row, "e", None)),
                "note": getattr(row, "note", "") or "",
                "plan_status": getattr(row, "plan_status", "") or "",
                "pilot": getattr(row, "pilot", "") or "",
            }
        )
    return items


def render_origin_view_drag(df_origin: pd.DataFrame):
    """React drag&drop 타임라인 렌더링"""
    _init_edit_buffers(df_origin)
    if "orig_df_snapshot" not in st.session_state or not st.session_state["orig_df_snapshot"].equals(df_origin):
        st.session_state["orig_df_snapshot"] = df_origin.copy()
        st.session_state["edit_df"] = df_origin.copy()
        st.session_state["selected_row_id"] = None

    st.subheader("🚢 신항/감만 React 드래그 편집기")
    st.caption("· 좌우 드래그: 5분 스냅 · 상하 드래그: 30m 스냅 · 드롭 시 한 번만 Streamlit 반영")

    df_all = st.session_state.get("edit_df")
    if df_all is None or not isinstance(df_all, pd.DataFrame) or df_all.empty:
        st.info("편집할 데이터가 없습니다. 먼저 조회/불러오기를 실행하세요.")
        return

    items = _build_drag_items(df_all)
    events = drag_timeline(items=items) or []

    if events:
        src = st.session_state.get("active_source", "crawl")
        for ev in events:
            rid = ev.get("row_id")
            if rid is None:
                continue
            dmin = int(ev.get("dmin") or 0)
            dy = float(ev.get("dy") or 0.0)
            st.session_state["edit_df"] = _apply_move(
                st.session_state["edit_df"],
                int(rid),
                dmin=dmin,
                dy=dy,
            )
            st.session_state["selected_row_id"] = int(rid)
        # 편집 버퍼를 세트별 키에도 즉시 반영해 rerun 후에도 유지
        if src == "crawl":
            st.session_state["edit_df_crawl"] = st.session_state["edit_df"].copy()
        else:
            st.session_state["edit_df_upload"] = st.session_state["edit_df"].copy()
        st.rerun()

    # 검증은 계속 수행하지만 화면에는 경고를 표시하지 않음
    _ = validate_df(st.session_state["edit_df"])
