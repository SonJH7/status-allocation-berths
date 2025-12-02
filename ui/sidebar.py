# =========================
# ui/sidebar.py
# =========================
import streamlit as st


def _init_state():
    if "show_direct" not in st.session_state:
        st.session_state["show_direct"] = False
    if "active_source" not in st.session_state:
        st.session_state["active_source"] = "crawl"  # 기본: 크롤 데이터


# ---------------------------------------------------------
# 사이드바 빌더
# ---------------------------------------------------------
def build_sidebar():
    _init_state()
    with st.sidebar:
        # ---------------------------------------------------------
        # 상단 타이틀/안내
        # ---------------------------------------------------------
        st.header("설정")
        st.caption("A) 크롤 데이터 조회/시각화  /  B) 파일 업로드 조회/시각화")

        # ---------------------------------------------------------
        # (A) 조회/시각화 - 크롤 파라미터
        # ---------------------------------------------------------
        st.subheader("A) 크롤 조회/시각화")

        time_label = {"3days": "4일", "week": "일주일", "month": "한달", "term": "직접 입력"}
        time = st.radio(
            "조회기간",
            options=["3days", "week", "month", "term"],
            index=0,
            horizontal=True,
            format_func=lambda v: time_label.get(v, v),
        )

        term_start = term_end = None
        if time == "term":
            st.caption("날짜를 모두 입력하세요.")
            c1, c2 = st.columns(2)
            with c1:
                term_start = st.date_input("시작일", key="term-start")
            with c2:
                term_end = st.date_input("종료일", key="term-end")

        route_label = {"ALL": "전체", "EA": "동남아(EA)", "JP": "일본(JP)", "CN": "중국(CN)"}
        route = st.radio(
            "항로구분",
            options=["ALL", "EA", "JP", "CN"],
            index=0,
            horizontal=True,
            format_func=lambda v: route_label.get(v, v),
        )

        company = st.text_input("선사", value="", placeholder="HMM / ONE ...")

        order_label = {"item1": "출항일시", "item2": "입항예정일시", "item3": "선석"}
        order = st.radio(
            "정렬기준",
            options=["item1", "item2", "item3"],
            index=0,
            horizontal=True,
            format_func=lambda v: order_label.get(v, v),
        )

        berth_label = {"A": "전체", "S": "신선대", "G": "감만", "ALL": "전체(A+B)"}
        berth = st.radio(
            "선석구분",
            options=["A", "S", "G", "ALL"],
            index=0,
            horizontal=True,
            format_func=lambda v: berth_label.get(v, v),
        )

        with st.expander("고급/추가 옵션", expanded=False):
            add_bp = st.toggle("BP + 비고/상태 추가", value=True)
            add_dims = st.toggle("VesselFinder 길이/너비 추가 (느릴 수 있음)", value=False)
            debug = st.toggle("디버그 로그 보기", value=False)

        col = st.columns(2)
        with col[0]:
            run_crawl = st.button("조회하기 실행", use_container_width=True)
        with col[1]:
            run_viz_crawl = st.button("시각화 하기", use_container_width=True)

        # ---------------------------------------------------------
        # (B) 파일 업로드: '원본 업로드 창' 열기/닫기
        # ---------------------------------------------------------
        st.divider()
        st.subheader("B) 파일 업로드 조회")
        open_direct = st.button("원본 업로드 창", use_container_width=True)
        if open_direct:
            st.session_state["show_direct"] = True

        origin_file = None
        run_load = False
        run_viz = False
        if st.session_state["show_direct"]:
            st.markdown("---")
            st.subheader("원본 업로드")
            origin_file = st.file_uploader("CSV/XLSX 파일 업로드", type=["csv", "xlsx"])
            col1, col2 = st.columns(2)
            with col1:
                run_load = st.button("업로드 불러오기", use_container_width=True)
            with col2:
                run_viz = st.button("시각화 하기", use_container_width=True)
            st.caption("추가 업로드가 끝나면 아래 버튼을 눌러주세요.")
            if st.button("닫기 ❌", use_container_width=True):
                st.session_state["show_direct"] = False

        # ---------------------------------------------------------
        # 편집/저장 컨트롤 (검증 옵션 포함)
        # ---------------------------------------------------------
        st.divider()
        st.subheader("편집 및 저장")
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
                "편집 대상 선택",
                options=["크롤", "업로드"],
                index=(0 if active_source == "crawl" else 1),
                horizontal=True,
            )
            active_source = "crawl" if src_label == "크롤" else "upload"
            st.session_state["active_source"] = active_source

        use_react_drag = st.toggle(
            "React 드래그 뷰어 사용",
            value=False,
            help="Plotly 그래프 대신 React 타임라인으로 렌더링합니다.",
        )

        # ---------------------------------------------------------
        # 검증 옵션
        # ---------------------------------------------------------
        st.divider()
        st.subheader("검증 결과 표시")
        show_validation = st.toggle("검증 결과 보기", value=False)
        val_location = st.radio(
            "표시 위치",
            options=["본문(상단)", "사이드바(하단)"],
            index=0,
            horizontal=True,
            disabled=not show_validation,
        )

        # ---------------------------------------------------------
        # 안내
        # ---------------------------------------------------------
        st.divider()
        st.subheader("안내")
        st.markdown(
            "- 크롤/업로드 중 하나만 편집 가능합니다(다른 하나는 읽기 전용).\n"
            "- 편집 후 저장 버튼을 눌러야 반영됩니다(되돌리기 1회 지원).\n"
            "- 저장: 원본 테이블까지 동기화\n"
            "- 새로고침 시 조회/업로드를 다시 실행하세요.\n"
            "- React 뷰어에서는 [저장] 후 표 편집 상태가 초기화됩니다."
        )

    return {
        "time": time,
        "term_start": term_start,
        "term_end": term_end,
        "route": route,
        "company": company,
        "order": order,
        "berth": berth,
        "add_bp": add_bp,
        "add_dims": add_dims,
        "debug": debug,
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
