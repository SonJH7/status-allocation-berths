# =========================
# ui/sidebar.py
# =========================
import streamlit as st

def _init_state():
    if "show_direct" not in st.session_state:
        st.session_state["show_direct"] = False

# ---------------------------------------------------------
# 사이드 바 설정
# ---------------------------------------------------------
def build_sidebar():
    _init_state()
    with st.sidebar:
        # ---------------------------------------------------------
        # 상단 타이틀/설정
        # ---------------------------------------------------------
        st.header("설정")
        st.caption("A) 크롤러로 바로 조회 · 시각화  /  B) 직접 파일 업로드 후 불러오기 · 시각화")

        # ---------------------------------------------------------
        # (A) 조회/시각화 — 크롤러 사용
        # ---------------------------------------------------------
        st.subheader("A) 크롤러 조회/시각화")
        add_dims = st.toggle("VesselFinder 길이/폭 포함 (느릴 수 있음)", value=False)
        col = st.columns(2)
        with col[0]:
            run_crawl = st.button("조회하기 🚢", use_container_width=True)
        with col[1]:
            run_viz_crawl = st.button("시각화 하기 📊", use_container_width=True)

        # ---------------------------------------------------------
        # (B) 토글 버튼: '직접 파일 넣기' 섹션 열기/닫기
        # ---------------------------------------------------------
        st.divider()
        st.subheader("B) 직접 파일 넣기")
        open_direct = st.button("직접 파일 넣기 ⤵", use_container_width=True)
        if open_direct:
            st.session_state["show_direct"] = True

        # ---------------------------------------------------------
        # (B) 업로드/시각화 — 사이트 CSV/XLSX 직접 투입
        # ---------------------------------------------------------
        origin_file = None
        run_load = False
        run_viz = False

        if st.session_state["show_direct"]:
            st.markdown("---")
            st.subheader("파일 업로드")
            origin_file = st.file_uploader("사이트 데이터 업로드 (CSV/XLSX)", type=["csv", "xlsx"])

            col1, col2 = st.columns(2)
            with col1:
                run_load = st.button("불러오기 📥", use_container_width=True)
            with col2:
                run_viz = st.button("시각화 하기 📊", use_container_width=True)

            st.caption("※ 닫으려면 아래 버튼을 클릭하세요.")
            if st.button("닫기 ⤴", use_container_width=True):
                st.session_state["show_direct"] = False

        # ---------------------------------------------------------
        # 시각화 옵션
        # ---------------------------------------------------------
        st.divider()
        st.subheader("시각화 옵션")
        enable_drag = st.toggle("드래그&드롭 편집(가로 10분 / 세로 30m 스냅)", value=True)

        # ---------------------------------------------------------
        # 유효성 경고 표시 옵션
        # ---------------------------------------------------------
        st.divider()
        st.subheader("유효성 경고 표시")
        show_validation = st.toggle("유효성 경고 보기", value=True)
        val_location = st.radio(
            "표시 위치",
            options=["본문(접기)", "사이드바(요약)"],
            index=0,
            horizontal=True,
            disabled=not show_validation,
        )

        # ---------------------------------------------------------
        # 도움말
        # ---------------------------------------------------------
        st.divider()
        st.subheader("도움말")
        st.markdown(
            "- **기간**: 오늘 기준 24시간(KST) 전부터 5일 구간입니다.\n"
            "- **라벨**: 가로축 4시간 간격(00시는 날짜 포함), 보조 그리드 10분.\n"
            "- **세로축**: SND 1500m / GAM 1400m, 30m 그리드. 굵은 선은 0·300·…·1500, 0·350·…·1400.\n"
            "- **드래그**: 가로 10분, 세로 30m 스냅. 동시간대 최소 이격 30m 검증."
        )

    # 컨트롤 값 반환
    return {
        # A) 크롤러
        "add_dims": add_dims,
        "run_crawl": run_crawl,
        "run_viz_crawl": run_viz_crawl,
        # B) 직접 업로드
        "origin_file": origin_file,
        "run_load": run_load,
        "run_viz": run_viz,
        # 공통
        "enable_drag": enable_drag,
        # 유효성 표시
        "show_validation": show_validation,
        "val_location": val_location,
    }
