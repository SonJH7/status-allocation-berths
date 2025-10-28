# app.py
import streamlit as st
from crawler import collect_berth_info
import pandas as pd

st.set_page_config(page_title="부산항 선석배정 현황", layout="wide")
st.title("⚓ 부산항 선석배정 현황")

with st.sidebar:
    st.header("설정")
    add_dims = st.toggle("VesselFinder 길이/폭 포함 (느릴 수 있음)", value=False)
    run = st.button("조회하기 🚢")

if run:
    with st.spinner("데이터를 가져오는 중입니다..."):
        try:
            # 🔧 토글 값 전달!
            df = collect_berth_info(add_bp=True, add_dims=add_dims)
            st.session_state["df_result"] = df
        except Exception as e:
            st.error(f"오류: {e}")

if "df_result" in st.session_state:
    st.subheader("📋 조회 결과")
    st.dataframe(st.session_state["df_result"], use_container_width=True)
else:
    st.info("좌측 사이드바에서 ‘조회하기’를 눌러 데이터를 불러오세요.")
