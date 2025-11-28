# =========================
# ui/viz/origin.py
# =========================
import math  # ✅ 추가
import pandas as pd
import streamlit as st
from streamlit_plotly_events import plotly_events

from ui.viz.common import period_str_kr, render_timeline_week
from schema import MIN_CLEARANCE_M, snap_time_5min, snap_y_30m, validate_df

def render_origin_view_static(df_origin: pd.DataFrame, title_prefix: str = ""):
    """읽기 전용(드래그/키 없음) — 위/아래 비교 배치용"""
    st.subheader(f"📊 {title_prefix} 읽기 전용 타임라인 (SND / GAM)")
    tab_snd, tab_gam = st.tabs(["신선대 SND", "감만 GAM"])

    def _one(terminal: str):
        df_t = df_origin[df_origin["terminal"] == terminal].reset_index(drop=True)
        if df_t.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return
        fig, (x0, x1) = render_timeline_week(df_t, terminal=terminal, title="")
        fig.update_layout(title=f"{title_prefix} {terminal} — {period_str_kr(x0, x1)}")
        fig.update_layout(width=2400, height=600)
        st.plotly_chart(fig, use_container_width=True)

    with tab_snd: _one("SND")
    with tab_gam: _one("GAM")

# ---------- 내부 상태 유틸 ----------
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
    st.session_state["edit_logs"].append({
        "row_id": before.get("row_id"),
        "vessel": before.get("vessel",""),
        "voyage": before.get("voyage",""),
        "terminal": before.get("terminal",""),
        "berth": before.get("berth",""),
        "start_before": before.get("start"),
        "end_before": before.get("end"),
        "f_before": before.get("f"),
        "e_before": before.get("e"),
        "start_after": after.get("start"),
        "end_after": after.get("end"),
        "f_after": after.get("f"),
        "e_after": after.get("e"),
        "ts": pd.Timestamp.now()
    })


# ---------- 이동 스냅(5분/30m) ----------
def _move_time_5min(row: pd.Series, minutes: int) -> dict:
    s = snap_time_5min(row["start"] + pd.Timedelta(minutes=minutes))
    e = snap_time_5min(row["end"]   + pd.Timedelta(minutes=minutes))
    return {"start": s, "end": e}

def _move_y_30m(row: pd.Series, dy: float) -> dict:
    f0, e0 = float(row.get("f",0)), float(row.get("e",0))
    L = e0 - f0
    mid = (f0 + e0) / 2.0
    new_mid = snap_y_30m(mid + dy)
    return {"f": new_mid - abs(L)/2, "e": new_mid + abs(L)/2}

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

    # 후보 값(초기엔 기존값)
    s1, e2 = s0, e0
    f1, e3 = f0, e1

    changed = False

    # 시간 이동 (start/end가 유효할 때만)
    if dmin != 0 and (pd.notna(s0) and pd.notna(e0)):
        s1 = snap_time_5min(pd.to_datetime(s0) + pd.Timedelta(minutes=dmin))
        e2 = snap_time_5min(pd.to_datetime(e0) + pd.Timedelta(minutes=dmin))
        if (not _ts_equal(s0, s1)) or (not _ts_equal(e0, e2)):
            changed = True

    # 세로 이동 (f/e가 유효할 때만)
    if dy != 0 and _is_finite_num(f0) and _is_finite_num(e1):
        L = float(e1) - float(f0)
        if _is_finite_num(L) and abs(L) > 0:
            mid = (float(f0) + float(e1)) / 2.0
            new_mid = snap_y_30m(mid + float(dy))
            f1 = new_mid - abs(L) / 2.0
            e3 = new_mid + abs(L) / 2.0
            if (not _num_equal(f0, f1)) or (not _num_equal(e1, e3)):
                changed = True

    # 실제 변화 없으면 그대로 반환(로그 없음)
    if not changed:
        return out

    before = dict(row)
    out.at[idx, "start"] = s1
    out.at[idx, "end"] = e2
    out.at[idx, "f"] = f1
    out.at[idx, "e"] = e3
    after = dict(out.loc[idx])

    _append_log(before, after)       # ✅ 진짜 바뀐 경우에만
    st.session_state["undo_df"] = df.copy()
    return out

# ---------- 상호작용 렌더 ----------
def render_origin_view(df_origin: pd.DataFrame):
    """
    - 중앙 라벨 클릭으로 선택
    - Shift+클릭: 선택된 막대를 해당 좌표로 이동(드래그-드롭 대용)
    - 변경은 st.session_state['edit_df']에 수행, 로그는 st.session_state['edit_logs']
    """
    _init_edit_buffers(df_origin)

    # 입력 데이터가 새로 들어온 경우에만 편집 버퍼를 갱신하여 UnboundLocalError 등
    # 잔여 상태가 섞이지 않도록 한다.
    if "orig_df_snapshot" not in st.session_state or not st.session_state["orig_df_snapshot"].equals(df_origin):
        st.session_state["orig_df_snapshot"] = df_origin.copy()
        st.session_state["edit_df"] = df_origin.copy()
        st.session_state["selected_row_id"] = None
    st.subheader("📊 편집 가능한 타임라인 (SND / GAM)")
    st.caption("· 클릭: 선택  · Shift+클릭: 해당 위치로 이동(드롭)  · 스냅: 5분/30m")

    tab_snd, tab_gam = st.tabs(["신선대 SND", "감만 GAM"])

    def _render_one(terminal: str):
        df_all = st.session_state.get("edit_df")
        if df_all is None or not isinstance(df_all, pd.DataFrame) or df_all.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return

        df_t = df_all[df_all["terminal"] == terminal].reset_index(drop=True)
        if df_t.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return

        # 선택 상태 배너 자리(그래프 위)
        sel_line = st.empty()

        # 그림 생성
        fig, (x0, x1) = render_timeline_week(df_t, terminal=terminal, title="")
        fig.update_layout(title=f"{terminal} — {period_str_kr(x0, x1)}", width=2400, height=600)

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

        # 간단 검증 경고
        probs = validate_df(st.session_state["edit_df"])
        if any(p[0] == "clearance" for p in probs):
            st.warning(f"동시간대 선박 간 최소 이격 {MIN_CLEARANCE_M}m 위반 항목이 있습니다.")

        # 선택 상태 배너
        rid = st.session_state.get("selected_row_id")
        msg = "선택 없음"
        if rid is not None:
            sel = st.session_state["edit_df"]
            sel = sel[(sel["row_id"] == rid) & (sel["terminal"] == terminal)]
            if not sel.empty:
                r = sel.iloc[0]

                def _fmt(ts):
                    return pd.to_datetime(ts).strftime('%m-%d %H:%M') if pd.notna(ts) else '-'
                
                msg = (
                    f"**선택됨:** {r.get('terminal','')}-{int(r.get('berth',0))} · "
                    f"{r.get('vessel','') or '-'} · {r.get('voyage','') or '-'} · "
                    f"{_fmt(r.get('start'))} ~ {_fmt(r.get('end'))} · "
                    f"F:{float(r.get('f',0)):.0f}m → E:{float(r.get('e',0)):.0f}m"
                )
        sel_line.info(msg)


    with tab_snd:
        _render_one("SND")
    with tab_gam:
        _render_one("GAM")
