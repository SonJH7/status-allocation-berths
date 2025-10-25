# app.py
# BPTC "T" → "G" 선석배정 현황 시각화 (Streamlit + vis.js)
# License: MIT

import os
from functools import lru_cache
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import streamlit as st
from streamlit_timeline import st_timeline

from db import (
    init_db,
    SessionLocal,
    upsert_reference_data,
    create_version_with_assignments,
    list_versions,
    load_assignments_df,
    delete_all_versions,
    get_vessel_loa_map,
    set_vessels_loa,
)
from crawler import fetch_bptc_t
from validate import snap_to_interval, validate_temporal_overlaps, validate_spatial_gap
from plot_gantt import (
    render_berth_gantt,
    get_demo_df,
    normalize_berth_label,  # ✅ G형 시각화 함수 가져오기s
)

# -----------------------------------------------------------
# 기본 설정
# -----------------------------------------------------------
st.set_page_config(page_title="BPTC 선석배정 Gantt", layout="wide")

# DB 초기화
engine, Base = init_db()
session = SessionLocal()
upsert_reference_data(session)

# -----------------------------------------------------------
# 사이드바
# -----------------------------------------------------------
st.sidebar.title("⚓ BPTC 선석배정 현황(T) → Gantt")

with st.sidebar:
    st.markdown("### 🔹 데이터 소스")
    btn_crawl = st.button("📡 크롤링 실행 (BPTC T)")
    uploaded_quantum = st.file_uploader("양자 결과 CSV 업로드(optional)", type=["csv"])

    st.markdown("---")
    st.markdown("### 🔹 버전 관리")
    versions = list_versions(session)
    version_labels = [
        f"{v['id'][:8]} · {v['source']} · {v['label']} · {v['created_at']:%m-%d %H:%M}"
        for v in versions
    ]
    idx_a = st.selectbox("좌측 버전 A 선택", list(range(len(versions))), format_func=lambda i: version_labels[i] if versions else "없음")
    idx_b = st.selectbox("우측 버전 B 선택", list(range(len(versions))), format_func=lambda i: version_labels[i] if versions else "없음")

    st.markdown("---")
    st.markdown("### 🔹 스코프(편집 범위)")
    today = datetime.now()
    date_from = st.date_input("시작 날짜", value=today.date())
    time_from = st.time_input("시작 시간", value=(today - timedelta(hours=6)).time())
    scope_from = datetime.combine(date_from, time_from)

    date_to = st.date_input("끝 날짜", value=today.date() + timedelta(days=1))
    time_to = st.time_input("끝 시간", value=(today + timedelta(hours=24)).time())
    scope_to = datetime.combine(date_to, time_to)

    scope_berths = st.text_input("선석 필터 (예: B1,B2)", value="")

    st.markdown("---")
    st.markdown("### 🔹 설정")
    snap_choice = st.radio("시간 스냅 단위", ["1h", "30m", "15m"], index=0, horizontal=True)
    load_orientation_choice = st.radio(
        "적하/양하 배치 방향",
        ["가로", "세로"],
        index=0,
        horizontal=True,
    )
    load_orientation = "horizontal" if load_orientation_choice == "가로" else "vertical"
    min_gap_m = st.number_input("동시 계류 최소 이격(m)", min_value=0, value=30, step=5)

    st.markdown("---")
    do_undo = st.button("↩ 되돌리기 (Undo)", use_container_width=True)
    do_save = st.button("💾 저장 (새 버전)", type="primary", use_container_width=True)


# -----------------------------------------------------------
# 세션 상태 초기화
# -----------------------------------------------------------
if "history" not in st.session_state:
    st.session_state["history"] = []
if "working_df" not in st.session_state:
    st.session_state["working_df"] = pd.DataFrame()


@lru_cache(maxsize=1)
def load_reference_loa_map() -> dict[str, float]:
    """CSV 기준 LOA 정보를 메모리에 캐싱한다."""

    path = os.path.join(os.path.dirname(__file__), "data", "vessels_loa.csv")
    if not os.path.exists(path):
        return {}

    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    mapping: dict[str, float] = {}
    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        try:
            loa_val = float(row.get("loa_m"))
        except (TypeError, ValueError):
            continue
        mapping[name.upper()] = loa_val
    return mapping


def enrich_with_loa(source_df: pd.DataFrame) -> pd.DataFrame:
    """LOA 결측값을 DB/CSV 정보를 활용해 보강한다."""

    if source_df is None or source_df.empty:
        return source_df

    work = source_df.copy()
    if "loa_m" not in work.columns:
        work["loa_m"] = pd.NA

    missing_mask = work["loa_m"].isna()
    if not missing_mask.any():
        return work

    vessels = work.loc[missing_mask, "vessel"].dropna().astype(str)
    db_map = get_vessel_loa_map(session, vessels.tolist())
    db_case_map = {str(k).strip().casefold(): v for k, v in db_map.items()}
    csv_map = load_reference_loa_map()

    updates_for_db: dict[str, float] = {}

    for idx in work.index[missing_mask]:
        name = str(work.at[idx, "vessel"]).strip()
        if not name:
            continue

        loa_val = db_map.get(name)
        if loa_val is None:
            loa_val = db_case_map.get(name.casefold())
        if loa_val is None:
            loa_val = csv_map.get(name.upper())
            if loa_val is not None:
                updates_for_db[name] = loa_val

        if loa_val is not None:
            work.at[idx, "loa_m"] = loa_val

    if updates_for_db:
        set_vessels_loa(session, updates_for_db)

    # 최종적으로도 값이 없으면 기본값(55m)로 채워 가독성 확보
    work["loa_m"] = work["loa_m"].fillna(55.0)
    return work


def normalize_berth_column(df: pd.DataFrame) -> pd.DataFrame:
    """선석 라벨을 숫자 문자열로 정규화한다."""

    if df is None or df.empty or "berth" not in df.columns:
        return df

    work = df.copy()
    work["berth"] = work["berth"].map(normalize_berth_label)
    return work


def build_kst_label(base_label: str) -> str:
    """버전 레이블에 한국 표준시 타임스탬프를 부여한다."""

    now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    timestamp = now_kst.strftime("%Y-%m-%d %H:%M")
    base = base_label.strip()
    if base:
        return f"{base} · {timestamp} (KST)"
    return f"{timestamp} (KST)"


# -----------------------------------------------------------
# 크롤링 버튼 동작
# -----------------------------------------------------------
if btn_crawl:
    with st.spinner("BPTC T 페이지 크롤링 중..."):
        try:
            df_t = fetch_bptc_t()
        except Exception as e:
            st.error(f"❌ 크롤링 실패: {e}")
            st.stop()

        # ✅ 컬럼명 정규화 (vessel, berth, eta, etd)
        df_t.columns = [c.strip().lower() for c in df_t.columns]
        rename_map = {
            "선명": "vessel", "모선명": "vessel", "vessel": "vessel",
            "선석": "berth", "berth": "berth",
            "접안(예정)일시": "eta", "입항예정일시": "eta", "eta": "eta",
            "출항(예정)일시": "etd", "출항예정일시": "etd", "출항일시": "etd", "etd": "etd",
        }
        for k, v in rename_map.items():
            if k in df_t.columns:
                df_t = df_t.rename(columns={k: v})

        required = ["vessel", "berth", "eta", "etd"]
        if not all(c in df_t.columns for c in required):
            st.error(f"⚠️ 크롤링 결과에 필수 컬럼 누락: {df_t.columns.tolist()}")
            st.stop()

        # datetime 변환
        for c in ["eta", "etd"]:
            df_t[c] = pd.to_datetime(df_t[c], errors="coerce")

        df_t = df_t.dropna(subset=required).reset_index(drop=True)
        if df_t.empty:
            st.warning("⚠️ 선박 데이터가 비어 있습니다.")
            st.stop()

        # DB 저장
        df_t = normalize_berth_column(df_t)
        vid = create_version_with_assignments(
            session,
            df_t,
            source="crawler:bptc",
            label=build_kst_label("BPTC T 크롤링"),
        )
        st.session_state["last_df"] = df_t  # ✅ G 시각화용
        st.success(f"✅ 크롤링 완료 — 새 버전 {vid[:8]} 생성 ({len(df_t)}건)")
        st.rerun()


# -----------------------------------------------------------
# G형 시각화 (선석배정 현황)
# -----------------------------------------------------------
st.markdown("---")
st.header("📊 선석배정 현황(G) 시각화")

with st.expander("데이터 관리 (DB)"):
    st.warning("모든 선석 배정 버전과 일정 데이터가 삭제됩니다.")
    confirm_token = st.text_input("삭제하려면 DELETE 입력", key="gantt_delete_confirm")
    if st.button("🗑️ DB 선석배정 데이터 전체 삭제", type="secondary", disabled=confirm_token.strip().upper() != "DELETE"):
        deleted = delete_all_versions(session)
        if deleted:
            st.success(f"총 {deleted}개 버전을 삭제했습니다.")
        else:
            st.info("삭제할 버전이 없습니다.")
        st.session_state.pop("last_df", None)
        st.session_state.pop("history", None)
        st.session_state["working_df"] = pd.DataFrame()
        st.rerun()

colx, coly, colz = st.columns([1, 1, 1])
with colx:
    g_base = st.date_input("기준일", value=datetime.now().date())
with coly:
    g_days = st.slider("표시 일수", 3, 14, 7)
with colz:
    g_editable = st.toggle("드래그&드롭 편집", value=True)

# 표시할 데이터 결정
candidate_df = None
if "last_df" in st.session_state and not st.session_state["last_df"].empty:
    candidate_df = st.session_state["last_df"]
elif "df_left" in locals() and not df_left.empty:
    candidate_df = df_left

if candidate_df is None or len(candidate_df) == 0:
    st.info("크롤링하거나 버전을 선택하면 Gantt가 표시됩니다. 아래는 데모 데이터입니다.")
    demo_df = get_demo_df(pd.Timestamp(g_base))
    tabs = st.tabs(["신선대 (1~5선석)", "감만 (6~9선석)"])
    gamman_labels = {"9": "9(1)", "8": "8(2)", "7": "7(3)", "6": "6(4)"}
    with tabs[0]:
        render_berth_gantt(
            demo_df,
            base_date=pd.Timestamp(g_base),
            days=g_days,
            editable=False,
            snap_choice=snap_choice,
            height="720px",
            key="gantt_demo_sinseondae",
            allowed_berths=["1", "2", "3", "4", "5"],
            load_discharge_orientation=load_orientation,
        )
    with tabs[1]:
        render_berth_gantt(
            demo_df,
            base_date=pd.Timestamp(g_base),
            days=g_days,
            editable=False,
            snap_choice=snap_choice,
            height="720px",
            key="gantt_demo_gamman",
            allowed_berths=["9", "8", "7", "6"],
            group_label_map=gamman_labels,
            load_discharge_orientation=load_orientation,
        )
else:
    g_source_df = enrich_with_loa(candidate_df)
    g_source_df = normalize_berth_column(g_source_df)

    st.session_state["last_df"] = g_source_df.copy()

    tabs = st.tabs(["신선대 (1~5선석)", "감만 (6~9선석)"])
    berth_groups = {
        "sinseondae": ["1", "2", "3", "4", "5"],
        "gamman": ["9", "8", "7", "6"],
    }
    berth_labels = {
        "gamman": {"9": "9(1)", "8": "8(2)", "7": "7(3)", "6": "6(4)"},
    }

    latest_df = g_source_df
    latest_event = None

    with tabs[0]:
        latest_df, evt0 = render_berth_gantt(
            latest_df,
            base_date=pd.Timestamp(g_base),
            days=g_days,
            editable=g_editable,
            snap_choice=snap_choice,
            height="780px",
            key="gantt_main_sinseondae",
            allowed_berths=berth_groups["sinseondae"],
            load_discharge_orientation=load_orientation,
        )
        if evt0:
            latest_event = evt0
            latest_df = enrich_with_loa(latest_df)
            latest_df = normalize_berth_column(latest_df)

    with tabs[1]:
        latest_df, evt1 = render_berth_gantt(
            latest_df,
            base_date=pd.Timestamp(g_base),
            days=g_days,
            editable=g_editable,
            snap_choice=snap_choice,
            height="780px",
            key="gantt_main_gamman",
            allowed_berths=berth_groups["gamman"],
            group_label_map=berth_labels.get("gamman"),
            load_discharge_orientation=load_orientation,
        )
        if evt1:
            latest_event = evt1
            latest_df = enrich_with_loa(latest_df)
            latest_df = normalize_berth_column(latest_df)

    st.session_state["last_df"] = latest_df.copy()

    st.caption("Tip: 마우스로 **좌우 드래그**하면 가로 스크롤, **CTRL+휠**로 확대/축소할 수 있습니다.")

    if g_editable and latest_event:
        st.info("드래그 변경이 감지되었습니다. 아래 버튼으로 새 버전으로 저장할 수 있습니다.")
        if st.button("💾 Gantt 편집 내용 저장(새 버전)"):
            to_save = normalize_berth_column(latest_df)
            vid = create_version_with_assignments(
                session,
                to_save,
                source="user-edit:gantt",
                label=build_kst_label(f"Gantt편집({snap_choice})"),
            )
            st.success(f"저장 완료 — 새 버전 {vid[:8]}")
            st.rerun()


# -----------------------------------------------------------
# 버전 불러오기 (A/B 비교용)
# -----------------------------------------------------------
if versions:
    df_left = normalize_berth_column(load_assignments_df(session, versions[idx_a]["id"]))
    df_right = normalize_berth_column(load_assignments_df(session, versions[idx_b]["id"]))
else:
    st.info("버전을 먼저 생성하세요 (크롤링 또는 CSV 업로드).")
    df_left = pd.DataFrame(columns=["vessel", "berth", "eta", "etd", "loa_m", "start_meter"])
    df_right = df_left.copy()


# -----------------------------------------------------------
# 범위 필터
# -----------------------------------------------------------
def in_scope(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "eta" not in df.columns or "etd" not in df.columns:
        return pd.DataFrame(columns=df.columns)
    out = df[(df["eta"] < scope_to) & (df["etd"] > scope_from)].copy()
    if scope_berths.strip():
        keeps = [b.strip().upper() for b in scope_berths.split(",") if b.strip()]
        out = out[out["berth"].astype(str).str.upper().isin(keeps)]
    return out

left_scope = in_scope(df_left).reset_index(drop=True)
right_scope = in_scope(df_right).reset_index(drop=True)

def ensure_gantt_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Codex 사양용 Gantt 보드 컬럼을 채워 넣는다."""

    if df.empty:
        # 필요한 컬럼만 갖춘 빈 DF 반환
        cols = [
            "berth",
            "vessel",
            "eta",
            "etd",
            "loa_m",
            "start_meter",
            "start_tag",
            "end_tag",
            "badge",
            "status",
            "load_discharge",
            "load_orientation",
        ]
        return df.reindex(columns=cols)

    work = df.copy()
    work = normalize_berth_column(work)
    for col in [
        "start_tag",
        "end_tag",
        "badge",
        "status",
        "load_discharge",
        "load_orientation",
    ]:
        if col not in work.columns:
            work[col] = None

    if "loa_m" not in work.columns:
        work["loa_m"] = None

    work["status"] = work["status"].fillna("gray")

    return work


left_scope = ensure_gantt_columns(left_scope)
right_scope = ensure_gantt_columns(right_scope)


# -----------------------------------------------------------
# 좌/우 비교 뷰 (T형 편집용)
# -----------------------------------------------------------
colA, colB = st.columns(2, gap="small")

with colA:
    st.subheader("🧭 A) 편집 대상")
    if st.session_state["working_df"].empty or set(st.session_state["working_df"].columns) != set(left_scope.columns):
        st.session_state["working_df"] = left_scope.copy()

    scope_delta = scope_to - scope_from
    scope_days = max(1, int(scope_delta.total_seconds() // (24 * 3600)) + 1)
    scope_base = pd.Timestamp(scope_from)

    prev_df = st.session_state["working_df"].copy()
    updated_df, timeline_eventA = render_berth_gantt(
        st.session_state["working_df"],
        base_date=scope_base,
        days=scope_days,
        editable=True,
        snap_choice=snap_choice,
        height="560px",
        key="timeline_left",
        load_discharge_orientation=load_orientation,
    )

    if timeline_eventA:
        st.session_state["history"].append(prev_df)

    st.session_state["working_df"] = ensure_gantt_columns(updated_df)
    with st.expander("자세히 보기 / LOA·start_meter 편집"):
        st.session_state["working_df"] = st.data_editor(
            st.session_state["working_df"],
            column_config={
                "eta": st.column_config.DatetimeColumn("입항(ETA)"),
                "etd": st.column_config.DatetimeColumn("출항(ETD)"),
                "loa_m": st.column_config.NumberColumn("LOA(m)", min_value=0, step=1),
                "start_meter": st.column_config.NumberColumn("시작 위치(m)", min_value=0, step=1),
            },
            width="stretch",
            num_rows="dynamic",
            key="editorA",
        )
        st.session_state["working_df"] = ensure_gantt_columns(st.session_state["working_df"])

    if not st.session_state["working_df"].empty:
        wdf = st.session_state["working_df"].copy()
        wdf["eta"] = wdf["eta"].map(lambda x: snap_to_interval(x, snap_choice))
        wdf["etd"] = wdf["etd"].map(lambda x: snap_to_interval(x, snap_choice))
        v1 = validate_temporal_overlaps(wdf)
        v2 = validate_spatial_gap(wdf, min_gap_m=min_gap_m)
        if v1 or v2:
            st.error("🚫 제약 위반:\n- " + "\n- ".join(v1 + v2))
        else:
            st.success("✅ 제약 위반 없음")

    if do_undo and st.session_state["history"]:
        st.session_state["working_df"] = st.session_state["history"].pop()

    if do_save:
        to_save = normalize_berth_column(st.session_state["working_df"])
        vid = create_version_with_assignments(
            session,
            to_save,
            source="user-edit",
            label=build_kst_label(f"수정본({snap_choice})"),
        )
        st.success(f"💾 저장 완료 — 새 버전 {vid[:8]}")
        st.session_state["history"].clear()
        st.rerun()

with colB:
    st.subheader("📊 B) 비교 대상 (읽기 전용)")
    _ = render_berth_gantt(
        right_scope,
        base_date=pd.Timestamp(scope_from),
        days=scope_days,
        editable=False,
        snap_choice=snap_choice,
        height="560px",
        key="timeline_right",
        load_discharge_orientation=load_orientation,
    )

st.caption("🔸 외부(BPTC) 시스템에는 쓰기 요청을 하지 않으며, 사내 DB 사본만 관리합니다.")
