# =========================
# ui/viz/origin.py
# =========================
import streamlit as st
import pandas as pd
from streamlit_plotly_events import plotly_events
from streamlit import components
from ui.viz.common import render_timeline_week, period_str_kr
from schema import snap_time_10min, snap_y_30m, MIN_CLEARANCE_M, validate_df

def _apply_drag_week(df_filtered: pd.DataFrame, events):
    if not events:
        return df_filtered
    out = df_filtered.copy()
    for ev in events:
        idx = ev.get("pointIndex")
        if idx is None or idx < 0 or idx >= len(out):
            continue
        new_x, new_y = ev.get("x"), ev.get("y")
        if new_x is None or new_y is None:
            continue
        r = out.iloc[idx]
        s, e = r["start"], r["end"]
        if pd.isna(s) or pd.isna(e):
            continue
        mid_old = s + (e - s) / 2
        dx = pd.to_datetime(new_x) - mid_old
        out.at[idx, "start"] = snap_time_10min(s + dx)
        out.at[idx, "end"]   = snap_time_10min(e + dx)
        f0, e0 = float(r.get("f", 0)), float(r.get("e", 0))
        length = e0 - f0
        mid_y = float(new_y)
        f_new = snap_y_30m(mid_y) - abs(length)/2
        e_new = snap_y_30m(mid_y) + abs(length)/2
        out.at[idx, "f"], out.at[idx, "e"] = f_new, e_new
    probs = validate_df(out)
    if any(p[0] == "clearance" for p in probs):
        st.warning(f"동시간대 선박 간 최소 이격 {MIN_CLEARANCE_M}m 위반 항목이 있습니다.")
    return out

def _plotly_scroll(fig_html: str, height: int = 600, min_width_px: int = 2400):
    # 가로 스크롤 가능 컨테이너 (오직 이 방식만 사용)
    wrapper = f"""
    <div style="width:100%; overflow-x:auto; padding-bottom:8px;">
      <div style="width: {min_width_px}px;">
        {fig_html}
      </div>
    </div>
    """
    components.v1.html(wrapper, height=height+60, scrolling=True)


def render_origin_view(df_origin: pd.DataFrame, enable_drag: bool):
    """
    가로 스크롤 전용 시각화.
    - 드래그&드롭 편집은 지원하지 않음(Plotly HTML 임베드 방식).
    """
    st.subheader("📊 시각화 (SND / GAM) — 가로 스크롤 전용")
    st.caption("가로: 오늘 기준 24시간(KST)전부터 5일 / 라벨 4시간(00시는 날짜 표기) · 보조 그리드 10분 · 세로 30m")

    tab_snd, tab_gam = st.tabs(["신선대 SND", "감만 GAM"])

    def _render_one(terminal: str):
        df_t = df_origin[df_origin["terminal"] == terminal].reset_index(drop=False)
        if df_t.empty:
            st.info(f"{terminal} 데이터가 없습니다.")
            return

        # 공통 렌더러로 그림과 기간(x0,x1) 생성
        fig, (x0, x1) = render_timeline_week(df_t, terminal=terminal, title="")
        title = f"[사이트 데이터] {terminal} 주간 타임라인 — {period_str_kr(x0, x1)}"
        fig.update_layout(title=title)

        # 오직 가로 스크롤 버전만 출력
        html = fig.to_html(include_plotlyjs="cdn", full_html=False)
        _plotly_scroll(html, height=600, min_width_px=2400)

    with tab_snd:
        _render_one("SND")
    with tab_gam:
        _render_one("GAM")
