# =========================
# ui/sidebar.py
# =========================
import streamlit as st


def _init_state():
    if "show_direct" not in st.session_state:
        st.session_state["show_direct"] = False
    if "active_source" not in st.session_state:
        st.session_state["active_source"] = "crawl"  # 기본값: 크롤링


# ---------------------------------------------------------
# 사이드바 레이아웃
# ---------------------------------------------------------
def build_sidebar():
    _init_state()
    with st.sidebar:
        # ---------------------------------------------------------
        # 상단 타이틀/설명
        # ---------------------------------------------------------
        st.header("설정")
        st.caption("A) 크롤링 데이터 조회 · 시각화  /  B) 파일 직접 불러오기 · 시각화")

        # ---------------------------------------------------------
        # (A) 조회/시각화 - 크롤링 세트
        # ---------------------------------------------------------
        st.subheader("A) 크롤링 조회/시각화")
        add_dims = st.toggle("VesselFinder 길이/흘수 포함 (느릴 때 꺼두기)", value=False)
        col = st.columns(2)
        with col[0]:
            run_crawl = st.button("조회하기 실행", use_container_width=True)
        with col[1]:
            run_viz_crawl = st.button("시각화 하기", use_container_width=True)

        # ---------------------------------------------------------
        # (B) 업로드 버튼: '직접 파일 열기' 섹션 열기/닫기
        # ---------------------------------------------------------
        st.divider()
        st.subheader("B) 직접 파일 열기")
        open_direct = st.button("직접 파일 열기 ▶", use_container_width=True)
        if open_direct:
            st.session_state["show_direct"] = True

        origin_file = None
        run_load = False
        run_viz = False
        if st.session_state["show_direct"]:
            st.markdown("---")
            st.subheader("파일 불러오기")
            origin_file = st.file_uploader("CSV/XLSX 데이터 불러오기", type=["csv", "xlsx"])
            col1, col2 = st.columns(2)
            with col1:
                run_load = st.button("불러오기 실행", use_container_width=True)
            with col2:
                run_viz = st.button("시각화 하기", use_container_width=True)
            st.caption("추가 업로드가 없으면 아래 버튼을 클릭하세요.")
            if st.button("닫기 ✕", use_container_width=True):
                st.session_state["show_direct"] = False

        # ---------------------------------------------------------
        # 편집/저장 컨트롤 (유효한 데이터쪽)
        # ---------------------------------------------------------
        st.divider()
        st.subheader("편집 · 저장")
        colx = st.columns([1, 1])
        with colx[0]:
            cmd_undo = st.button("되돌리기(1회)", use_container_width=True)
        with colx[1]:
            cmd_save = st.button("저장", use_container_width=True, type="primary")

        has_crawl = bool(st.session_state.get("crawl_df") is not None and not getattr(st.session_state.get("crawl_df"), "empty", True))
        has_upload = bool(st.session_state.get("upload_df") is not None and not getattr(st.session_state.get("upload_df"), "empty", True))
        active_source = st.session_state.get("active_source", "crawl")
        if has_crawl and has_upload:
            src_label = st.radio(
                "편집 대상 데이터",
                options=["크롤링", "업로드"],
                index=(0 if active_source == "crawl" else 1),
                horizontal=True,
            )
            active_source = "crawl" if src_label == "크롤링" else "upload"
            st.session_state["active_source"] = active_source

        use_react_drag = st.toggle(
            "React 드래그 편집기 사용",
            value=False,
            help="Plotly 그래프 대신 React 타임라인을 사용합니다. 드래그 후 [저장]을 눌러야 원본 테이블에 반영됩니다.",
        )

        # ---------------------------------------------------------
        # 유효성 경고 표시 옵션
        # ---------------------------------------------------------
        st.divider()
        st.subheader("유효성 경고 표시")
        show_validation = st.toggle("유효성 경고 보기", value=False)
        val_location = st.radio(
            "표시 위치",
            options=["본문(상세)", "사이드바(요약)"],
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
            "- 두 데이터가 있을 때는 **선택 데이터**만 드래그&키 이동 가능합니다 (다른 하나는 읽기 전용).\n"
            "- 그래프 편집 후 표 데이터는 **저장해야 확정**됩니다 (저장 전에는 되돌리기 가능).\n"
            "- **저장**: 원본 테이블까지 동기화\n"
            "- **초기화는 조회하기/불러오기로** 다시 받으면 원본으로 돌아갑니다.\n"
            "- React 드래그 모드에서 이동 후 [저장]을 눌러야 원본까지 반영됩니다."
        )

    return {
        "add_dims": add_dims,
        "run_crawl": run_crawl,
        "run_viz_crawl": run_viz_crawl,
        "origin_file": origin_file,
        "run_load": run_load,
        "run_viz": run_viz,
        "cmd_undo": cmd_undo,
        "cmd_save": cmd_save,
        "use_react_drag": use_react_drag,
        "show_validation": show_validation,
        "val_location": val_location,
        "active_source": active_source,
    }
