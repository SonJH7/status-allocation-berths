# =========================
# app.py
# =========================
import streamlit as st
import pandas as pd

from crawler import collect_berth_info
from schema import normalize_df
from ui.sidebar import build_sidebar
from ui.validation import show_validation
from ui.table import show_table
from ui.viz.origin import render_origin_view

st.set_page_config(
    page_title="부산항 선석배정 현황(사이트) · 편집/시각화",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("⚓ 부산항 선석배정 현황 — 사이트 데이터 업로드 · 검증 · 시각화")

ctrl = build_sidebar()

if "origin_df" not in st.session_state:
    st.session_state["origin_df"] = pd.DataFrame()
if "origin_raw" not in st.session_state:
    st.session_state["origin_raw"] = pd.DataFrame()

# A) 크롤러 조회
if ctrl["run_crawl"]:
    with st.spinner("크롤러로 데이터를 가져오는 중입니다..."):
        try:
            raw = collect_berth_info(add_bp=True, add_dims=ctrl["add_dims"])
            origin_df = normalize_df(raw)
            st.session_state["origin_raw"] = raw
            st.session_state["origin_df"] = origin_df
            st.success(f"조회 완료: 원본 {len(raw)}건 / 정규화 {len(origin_df)}건")
        except Exception as e:
            st.error(f"오류: {e}")

# B) 직접 파일 불러오기
if ctrl["run_load"]:
    if ctrl["origin_file"] is None:
        st.warning("먼저 CSV/XLSX 파일을 업로드하세요.")
    else:
        try:
            if ctrl["origin_file"].name.endswith(".xlsx"):
                raw = pd.read_excel(ctrl["origin_file"])
            else:
                raw = pd.read_csv(ctrl["origin_file"])
            origin_df = normalize_df(raw)
            st.session_state["origin_raw"] = raw
            st.session_state["origin_df"] = origin_df
            st.success(f"파일 불러오기 완료: 원본 {len(raw)}건 / 정규화 {len(origin_df)}건")
        except Exception as e:
            st.error(f"파일 불러오기 실패: {e}")

origin_raw = st.session_state.get("origin_raw", pd.DataFrame())
origin_df = st.session_state.get("origin_df", pd.DataFrame())

# =======================
# ① 시각화 (먼저 표시)
# =======================
if ctrl["run_viz_crawl"] or ctrl["run_viz"]:
    if origin_df is None or origin_df.empty:
        st.warning("시각화할 데이터가 없습니다. 먼저 ‘조회하기/불러오기’를 실행하세요.")
    else:
        render_origin_view(origin_df, ctrl["terminal_filter"], ctrl["enable_drag"])

# =======================
# ② 원본 & 정규화 탭 (시각화 아래)
# =======================
if (origin_raw is not None and not origin_raw.empty) or (origin_df is not None and not origin_df.empty):
    tab_raw, tab_norm = st.tabs(["📄 원본 데이터 (정규화 전)", "✅ 정규화 데이터 (검증 포함)"])

    with tab_raw:
        if origin_raw is not None and not origin_raw.empty:
            show_table(origin_raw, "📋 원본 테이블")
        else:
            st.info("원본 데이터가 없습니다. 좌측에서 ‘조회하기’ 또는 ‘불러오기’를 먼저 실행하세요.")

    with tab_norm:
        if origin_df is not None and not origin_df.empty:
            show_validation(
                "정규화 데이터",
                origin_df,
                visible=ctrl["show_validation"],
                location=ctrl["val_location"],
            )
            show_table(origin_df, "📋 정규화 테이블")
        else:
            st.info("정규화 데이터가 없습니다. 좌측에서 ‘조회하기’ 또는 ‘불러오기’를 먼저 실행하세요.")
else:
    st.info("좌측 사이드바에서 ‘조회하기’(A) 또는 ‘직접 파일 넣기→불러오기’(B)로 데이터를 불러오세요.")
