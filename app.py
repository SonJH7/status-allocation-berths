# app.py 부산항 부두배정 현황 · 데이터/크롤러 병행 · 편집/시각화
# -----------------------------------------------------------------------------
# 핵심 요약
# - 두 데이터 세트(크롤러, 업로드)를 나란히 관리하고 비교(표: 위/아래, 그래프: 좌/우).
# - 시각화는 "편집 대상(active_source)"만 사용.
# - 변경은 rerun 즉시 반영(st.rerun).
# - 조회/불러오기 직후에는 표만 보이고(시각화 숨김), "시각화하기"를 눌러야 그래프 노출(show_viz).
# -----------------------------------------------------------------------------

import streamlit as st
import pandas as pd

from crawler import collect_berth_info
from schema import normalize_df, ensure_row_id, sync_raw_with_norm
from ui.sidebar import build_sidebar
from ui.validation import show_validation
from ui.table import show_table
from ui.viz.origin import (
    render_origin_view,
    render_origin_view_static,
    render_origin_view_drag,
)


# -----------------------------------------------------------------------------
# 페이지/헤더
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="부산항 부두배정 현황(데이터) · 편집/시각화",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("⛴️ 부산항 부두배정 현황 · 데이터/크롤링 · 검증 · 시각화")


# -----------------------------------------------------------------------------
# 유틸 함수: 세션 키 보장/초기화
# -----------------------------------------------------------------------------
def _ensure_ss(key: str, default):
    """
    세션 상태(st.session_state)에 키가 없거나 None인 경우, 기본값으로 초기화합니다.
    - 모든 세션 키 초기화에 사용 (DataFrame/리스트/숫자 등)
    """
    if key not in st.session_state or st.session_state[key] is None:
        st.session_state[key] = default


def _init_all_session_keys():
    """
    앱 전역에서 사용하는 모든 세션 키를 한 번에 초기화합니다.
    - 크롤러/업로드: 원본(raw), 정규화(df), 편집버퍼(edit_df_*), 스냅샷(snapshot_*), 되돌리기(undo_*), 로그(logs_*)
    - 영역 플래그: show_viz(시각화 보이기), active_source(편집 대상)
    """
    defaults = {
        # 크롤러 세트
        "crawl_raw": pd.DataFrame(),
        "crawl_df": pd.DataFrame(),
        "edit_df_crawl": pd.DataFrame(),
        "snapshot_crawl": pd.DataFrame(),
        "undo_df_crawl": None,
        "logs_crawl": [],
        # 업로드 세트
        "upload_raw": pd.DataFrame(),
        "upload_df": pd.DataFrame(),
        "edit_df_upload": pd.DataFrame(),
        "snapshot_upload": pd.DataFrame(),
        "undo_df_upload": None,
        "logs_upload": [],
        # 플래그
        "show_viz": False,
        "active_source": "crawl",  # 기본: 크롤링
    }
    for k, v in defaults.items():
        _ensure_ss(k, v)


def _show_pending_toast():
    """
    st.rerun 후에도 알림이 보이도록 pending_toast를 소비해 토스트를 띄웁니다.
    """
    pending = st.session_state.pop("pending_toast", None)
    if pending:
        st.toast(pending.get("msg", ""), icon=pending.get("icon", "✅"))


# -----------------------------------------------------------------------------
# 핸들러: 데이터 획득(크롤링/업로드)
# -----------------------------------------------------------------------------
def handle_crawl_fetch(
    time: str,
    route: str,
    berth: str,
    company: str,
    order: str,
    add_bp: bool,
    add_dims: bool,
    debug: bool,
    term_start=None,
    term_end=None,
):
    """
    [크롤 조회] 버튼 클릭 시 호출됩니다.
    - 원본 크롤 데이터를 ensure_row_id 후 normalize_df 로 정규화, 상태(crawl_*)를 갱신
    - 시각화는 숨김(표 편집 모드) show_viz=False
    """
    year1 = month1 = day1 = year2 = month2 = day2 = None
    if time == "term":
        if not term_start or not term_end:
            st.warning("직접 입력을 선택하면 시작/종료 날짜를 모두 입력하세요.")
            return
        if term_start > term_end:
            st.warning("시작일이 종료일보다 늦을 수 없습니다.")
            return
        year1, month1, day1 = term_start.year, term_start.month, term_start.day
        year2, month2, day2 = term_end.year, term_end.month, term_end.day

    company_clean = (company or "").strip().upper()

    with st.spinner("크롤 데이터 조회 중입니다..."):
        raw = collect_berth_info(
            time=time,
            route=route,
            berth=berth,
            company=company_clean,
            order=order,
            add_bp=add_bp,
            add_dims=add_dims,
            debug=debug,
            year1=year1,
            month1=month1,
            day1=day1,
            year2=year2,
            month2=month2,
            day2=day2,
        )
        raw = ensure_row_id(raw)
        norm = ensure_row_id(normalize_df(raw))

        st.session_state["crawl_raw"] = raw.copy()
        st.session_state["crawl_df"] = norm.copy()
        st.session_state["edit_df_crawl"] = norm.copy()
        st.session_state["snapshot_crawl"] = norm.copy()
        st.session_state["undo_df_crawl"] = None
        st.session_state["logs_crawl"] = []

        st.session_state["active_source"] = "crawl"
        st.session_state["show_viz"] = False  # 조회 직후 표 모드
        st.success(f"조회 완료: 원본 {len(raw)}건 / 정규화 {len(norm)}건")

def handle_file_load(upload_file):
    """
    [불러오기] 버튼 클릭 시 호출됩니다.
    - 업로드 원본 로드(CSV/XLSX) · ensure_row_id · normalize_df · 세트(upload_*) 반영
    - 시각화는 숨김(표만 보이게) show_viz=False
    """
    if upload_file is None:
        st.warning("먼저 CSV/XLSX 파일을 업로드하세요.")
        return

    with st.spinner("파일을 불러오는 중입니다..."):
        if upload_file.name.endswith(".xlsx"):
            raw = pd.read_excel(upload_file)
        else:
            raw = pd.read_csv(upload_file)

        raw = ensure_row_id(raw)
        norm = ensure_row_id(normalize_df(raw))

        st.session_state["upload_raw"] = raw.copy()
        st.session_state["upload_df"] = norm.copy()
        st.session_state["edit_df_upload"] = norm.copy()
        st.session_state["snapshot_upload"] = norm.copy()
        st.session_state["undo_df_upload"] = None
        st.session_state["logs_upload"] = []

        st.session_state["show_viz"] = False  # 불러오기 직후엔 표만
        st.success(f"파일 불러오기 완료: 원본 {len(raw)}건 / 정규화 {len(norm)}건")


# -----------------------------------------------------------------------------
# 편집 컨텍스트 바인딩/복제
# -----------------------------------------------------------------------------
def _bind_edit_context(source: str):
    """
    편집 대상 세트(source: 'crawl'|'upload')를 공용 키로 바인딩합니다.
    - render_origin_view/drag에서 edit_df / orig_df_snapshot / undo_df / edit_logs 키를 사용하므로,
      선택된 세트의 버퍼/스냅샷/되돌리기/로그를 공용 키로 매핑합니다.
    """
    if source == "crawl":
        st.session_state["edit_df"] = st.session_state["edit_df_crawl"].copy()
        st.session_state["orig_df_snapshot"] = st.session_state["snapshot_crawl"].copy()
        st.session_state["undo_df"] = st.session_state["undo_df_crawl"]
        st.session_state["edit_logs"] = st.session_state["logs_crawl"]
    else:
        st.session_state["edit_df"] = st.session_state["edit_df_upload"].copy()
        st.session_state["orig_df_snapshot"] = st.session_state["snapshot_upload"].copy()
        st.session_state["undo_df"] = st.session_state["undo_df_upload"]
        st.session_state["edit_logs"] = st.session_state["logs_upload"]


def _persist_edit_context(source: str):
    """
    공용 편집 키를 다시 해당 세트로 복사해 둡니다.
    - 인터랙티브 시각화에서 사용자가 이동/드래그/키 조작을 하면 edit_df 값이 갱신되므로,
      그 결과를 세트별 키(edit_df_* / snapshot_* / undo_df_* / logs_*)로 되돌려 반영합니다.
    """
    if source == "crawl":
        st.session_state["edit_df_crawl"] = st.session_state["edit_df"].copy()
        st.session_state["snapshot_crawl"] = st.session_state["orig_df_snapshot"].copy()
        st.session_state["undo_df_crawl"] = st.session_state["undo_df"]
        st.session_state["logs_crawl"] = st.session_state["edit_logs"]
    else:
        st.session_state["edit_df_upload"] = st.session_state["edit_df"].copy()
        st.session_state["snapshot_upload"] = st.session_state["orig_df_snapshot"].copy()
        st.session_state["undo_df_upload"] = st.session_state["undo_df"]
        st.session_state["logs_upload"] = st.session_state["edit_logs"]


# -----------------------------------------------------------------------------
# 사이드바 액션 처리: 시각화/되돌리기/저장
# -----------------------------------------------------------------------------
def handle_sidebar_actions(ctrl: dict):
    """
    사이드바의 '시각화하기/되돌리기/저장' 액션을 처리합니다.
    - 시각화하기: show_viz=True
    - 되돌리기(1회): 편집 세트 undo 복원 + 로그 1건 삭제 + 즉시 rerun
    - 저장: 편집 세트 df 반영 + raw sync + 스냅샷/로그/undo 초기화 + show_viz=True + rerun
    """
    if ctrl.get("run_viz_crawl") or ctrl.get("run_viz"):
        st.session_state["show_viz"] = True

    if ctrl.get("cmd_undo"):
        src = ctrl["active_source"]
        if src == "crawl":
            buf = st.session_state.get("undo_df_crawl")
            if buf is not None and not getattr(buf, "empty", True):
                st.session_state["edit_df_crawl"] = buf.copy()
                st.session_state["undo_df_crawl"] = None
                if st.session_state["logs_crawl"]:
                    st.session_state["logs_crawl"].pop()
                st.session_state["pending_toast"] = {"msg": "↩️ 되돌리기 완료(크롤링 세트)", "icon": "↩️"}
                st.info("되돌리기 완료(크롤링 데이터).")
                st.rerun()
        else:
            buf = st.session_state.get("undo_df_upload")
            if buf is not None and not getattr(buf, "empty", True):
                st.session_state["edit_df_upload"] = buf.copy()
                st.session_state["undo_df_upload"] = None
                if st.session_state["logs_upload"]:
                    st.session_state["logs_upload"].pop()
                st.session_state["pending_toast"] = {"msg": "↩️ 되돌리기 완료(업로드 세트)", "icon": "↩️"}
                st.info("되돌리기 완료(업로드 데이터).")
                st.rerun()

    if ctrl.get("cmd_save"):
        src = ctrl["active_source"]
        if src == "crawl":
            st.session_state["crawl_df"] = st.session_state["edit_df_crawl"].copy()
            if not st.session_state["crawl_raw"].empty and "row_id" in st.session_state["crawl_raw"].columns:
                st.session_state["crawl_raw"] = sync_raw_with_norm(
                    st.session_state["crawl_raw"], st.session_state["crawl_df"]
                )
            st.session_state["snapshot_crawl"] = st.session_state["crawl_df"].copy()
            st.session_state["logs_crawl"] = []
            st.session_state["undo_df_crawl"] = None
            st.session_state["show_viz"] = True
            st.session_state["pending_toast"] = {"msg": "저장되었습니다 (크롤링 세트)", "icon": "💾"}
            st.success("저장 완료(크롤링 세트 반영).")
            st.rerun()
        else:
            st.session_state["upload_df"] = st.session_state["edit_df_upload"].copy()
            if not st.session_state["upload_raw"].empty and "row_id" in st.session_state["upload_raw"].columns:
                st.session_state["upload_raw"] = sync_raw_with_norm(
                    st.session_state["upload_raw"], st.session_state["upload_df"]
                )
            st.session_state["snapshot_upload"] = st.session_state["upload_df"].copy()
            st.session_state["logs_upload"] = []
            st.session_state["undo_df_upload"] = None
            st.session_state["show_viz"] = True
            st.session_state["pending_toast"] = {"msg": "저장되었습니다 (업로드 세트)", "icon": "💾"}
            st.success("저장 완료(업로드 세트 반영).")
            st.rerun()
def render_visualizations_and_validation(ctrl: dict):
    """
    메인 시각화 블록과 검증(정규화 DF 기반)을 그립니다.
    - show_viz=True일 때만 시각화 출력
    - 두 세트가 있을 때 [편집 대상(인터랙티브), 아래: 읽기 전용]으로 배치
    - 검증은 편집 대상 세트의 정규화 DF 기반으로 사이드바/본문 요약을 표시(테이블은 숨김)
    - React 드래그 토글이 켜져 있으면 Plotly 대신 React 편집기를 사용
    """
    has_crawl = not st.session_state["crawl_df"].empty
    has_upload = not st.session_state["upload_df"].empty
    use_react_drag = bool(ctrl.get("use_react_drag"))

    if not st.session_state["show_viz"]:
        return

    if not has_crawl and not has_upload:
        st.warning("시각화할 데이터가 없습니다. 먼저 조회하기/불러오기를 실행하세요.")
        return

    # 검증(정규화 DF 기반) - 편집 대상
    if ctrl.get("show_validation"):
        src = ctrl["active_source"]
        df_for_validation = st.session_state["crawl_df"] if src == "crawl" else st.session_state["upload_df"]
        if not df_for_validation.empty:
            show_validation("정규화 검증", df_for_validation, visible=True, location=ctrl["val_location"])

    def _render_editor(df_use):
        if use_react_drag:
            with st.spinner("React 드래그 편집기 로딩 중..."):
                render_origin_view_drag(df_use)
        else:
            render_origin_view(df_use)

    # 시각화(좌/우 또는 단독)
    if has_crawl and has_upload:
        st.subheader("크롤링/업로드 비교 시각화(위: 편집 대상 · 아래: 읽기 전용)")
        src = ctrl["active_source"]
        if src == "crawl":
            _bind_edit_context("crawl")
            _render_editor(st.session_state["crawl_df"])  # 인터랙티브
            _persist_edit_context("crawl")
            st.markdown("---")
            render_origin_view_static(st.session_state["upload_df"], title_prefix="업로드")
        else:
            _bind_edit_context("upload")
            _render_editor(st.session_state["upload_df"])  # 인터랙티브
            _persist_edit_context("upload")
            st.markdown("---")
            render_origin_view_static(st.session_state["crawl_df"], title_prefix="크롤링")
    else:
        # 단일 세트만 존재하는 경우
        if has_crawl:
            _bind_edit_context("crawl")
            _render_editor(st.session_state["crawl_df"])
            _persist_edit_context("crawl")
        else:
            _bind_edit_context("upload")
            _render_editor(st.session_state["upload_df"])
            _persist_edit_context("upload")


# -----------------------------------------------------------------------------
# 원본 테이블 패널(읽기/쓰기 분리, 편집 1세트만 허용)
# -----------------------------------------------------------------------------
def _render_raw_panel(source_key: str, label: str, editable: bool):
    """
    원본 테이블 1패널을 렌더링합니다.
    - editable=True (편집 허용)일 때만 '수정하기/되돌리기/저장→그래프' 버튼 표시
    - 원본 수정 시: 원본 및 정규화 동기화 후 그래프/편집버퍼/스냅샷 갱신 · 즉시 리렌더
    """
    df_raw = st.session_state[f"{source_key}_raw"]
    if df_raw.empty:
        st.info(f"{label} 원본 데이터가 없습니다.")
        return

    key_prefix = f"raw_{source_key}"
    if f"{key_prefix}_mode" not in st.session_state:
        st.session_state[f"{key_prefix}_mode"] = False
    if f"{key_prefix}_buffer" not in st.session_state:
        st.session_state[f"{key_prefix}_buffer"] = df_raw.copy()
    if f"{key_prefix}_snapshot" not in st.session_state:
        st.session_state[f"{key_prefix}_snapshot"] = df_raw.copy()

    if editable:
        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.button("수정하기", disabled=st.session_state[f"{key_prefix}_mode"], use_container_width=True, key=f"editbtn-{source_key}"):
                st.session_state[f"{key_prefix}_mode"] = True
                st.session_state[f"{key_prefix}_buffer"] = df_raw.copy()
                st.session_state[f"{key_prefix}_snapshot"] = df_raw.copy()
                # 반대쪽 원본 편집 모드 강제 해제(동시 편집 방지)
                other = "upload" if source_key == "crawl" else "crawl"
                st.session_state[f"raw_{other}_mode"] = False

        with cols[1]:
            undo_btn = st.button("되돌리기(원본)", use_container_width=True, disabled=not st.session_state[f"{key_prefix}_mode"], key=f"undobtn-{source_key}")
        with cols[2]:
            save_btn = st.button("저장→그래프", type="primary", use_container_width=True, disabled=not st.session_state[f"{key_prefix}_mode"], key=f"savebtn-{source_key}")

        if st.session_state[f"{key_prefix}_mode"]:
            st.warning("현재 **원본 테이블 편집 모드**입니다. 그래프 편집은 잠시 중지하세요.")
            edited = st.data_editor(st.session_state[f"{key_prefix}_buffer"], use_container_width=True, height=360, key=f"editor-{source_key}")

            if undo_btn:
                st.session_state[f"{key_prefix}_buffer"] = st.session_state[f"{key_prefix}_snapshot"].copy()
                st.info("원본 되돌리기 완료.")

            if save_btn:
                # 원본 반영 후 정규화 갱신 · 그래프/편집버퍼 갱신
                st.session_state[f"{source_key}_raw"] = edited.copy()
                new_norm = ensure_row_id(normalize_df(st.session_state[f"{source_key}_raw"]))
                st.session_state[f"{source_key}_df"] = new_norm.copy()
                st.session_state[f"edit_df_{source_key}"] = new_norm.copy()
                st.session_state[f"snapshot_{source_key}"] = new_norm.copy()
                # 편집/되돌리기/로그 초기화
                st.session_state[f"undo_df_{source_key}"] = None
                st.session_state[f"logs_{source_key}"] = []
                # 저장하면 시각화 켜고, 즉시 반영
                st.session_state["show_viz"] = True
                st.session_state[f"{key_prefix}_mode"] = False
                st.success(f"{label} 원본 저장 완료(그래프 갱신).")
                st.rerun()
        else:
            show_table(df_raw, f"🧾 {label} 원본")
    else:
        # 읽기 전용 패널
        show_table(df_raw, f"🧾 {label} 원본 (읽기 전용)")


def render_raw_tables(ctrl: dict):
    """
    원본 테이블 UI를 그립니다.
    - 두 세트가 있을 때/없을 때 다른 배치, 편집 대상만 수정 가능
    - 하나만 있을 때는 해당 세트만 표시(편집 허용)
    """
    has_crawl = not st.session_state["crawl_df"].empty
    has_upload = not st.session_state["upload_df"].empty

    if has_crawl and has_upload:
        st.subheader("🧾 원본 테이블 비교 (좌: 크롤링 / 우: 업로드)")
        c1, c2 = st.columns(2)
        with c1:
            _render_raw_panel("crawl", "크롤링", editable=(ctrl["active_source"] == "crawl"))
        with c2:
            _render_raw_panel("upload", "업로드", editable=(ctrl["active_source"] == "upload"))
    elif has_crawl:
        st.subheader("🧾 원본 테이블(크롤링)")
        _render_raw_panel("crawl", "크롤링", editable=True)
    elif has_upload:
        st.subheader("🧾 원본 테이블(업로드)")
        _render_raw_panel("upload", "업로드", editable=True)
    else:
        st.info("좌측 사이드바에서 '조회하기' 또는 '불러오기'를 먼저 실행하세요.")


# -----------------------------------------------------------------------------
# 실행 흐름
# -----------------------------------------------------------------------------
def main():
    """
    앱 메인 실행 함수.
    1) 사이드바 UI 및 컨트롤 수집
    2) 세션 키 초기화
    3) 조회/불러오기 처리
    4) 사이드바 액션(시각화/되돌리기/저장) 처리
    5) 시각화(좌/우 비교) + 검증 요약
    6) 원본 테이블(좌/우 비교) 렌더
    """
    ctrl = build_sidebar()
    _init_all_session_keys()
    _show_pending_toast()

    # A) 조회/불러오기
    if ctrl.get("run_crawl"):
        try:
            handle_crawl_fetch(
                time=ctrl["time"],
                route=ctrl["route"],
                berth=ctrl["berth"],
                company=ctrl["company"],
                order=ctrl["order"],
                add_bp=ctrl["add_bp"],
                add_dims=ctrl["add_dims"],
                debug=ctrl["debug"],
                term_start=ctrl["term_start"],
                term_end=ctrl["term_end"],
            )
        except Exception as e:
            st.error(f"����: {e}")

    if ctrl.get("run_load"):
        try:
            handle_file_load(ctrl["origin_file"])
        except Exception as e:
            st.error(f"파일 불러오기 실패: {e}")

    # B) 사이드바 액션 (시각화/되돌리기/저장)
    handle_sidebar_actions(ctrl)

    # C) 메인 시각화 + 검증
    render_visualizations_and_validation(ctrl)

    # D) 원본 테이블(좌/우 비교)
    render_raw_tables(ctrl)


# 진입점
if __name__ == "__main__":
    main()




