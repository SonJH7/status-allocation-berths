# =========================
# ui/validation.py
# =========================
import re
from typing import Any

import pandas as pd
import streamlit as st

from schema import validate_df

_GROUP_KEY_RE = re.compile(r"^(SND|GAM)-(\d+)$")


# ---------------------------------------------------------
# 내부 유틸: 사람이 읽기 좋은 유효성 결과 테이블 만들기
# ---------------------------------------------------------

def _safe_text(x: Any, default: str = "-") -> str:
    if x is None:
        return default
    try:
        if pd.isna(x):
            return default
    except Exception:
        pass
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "nat"}:
        return default
    return s


def _safe_int_text(x: Any, default: str = "-") -> str:
    try:
        if x is None or pd.isna(x):
            return default
        return str(int(float(x)))
    except Exception:
        return _safe_text(x, default)


def _resolve_row(df: pd.DataFrame, row_key: Any) -> pd.Series | None:
    """validate_df가 돌려준 row_or_key를 실제 행으로 최대한 안전하게 매핑한다."""
    if df is None or df.empty:
        return None

    # 1) 인덱스 라벨 직접 조회
    try:
        if row_key in df.index:
            row = df.loc[row_key]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            return row
    except Exception:
        pass

    # 2) 정수형이면 위치 기반 보조 조회
    try:
        ikey = int(row_key)
        if 0 <= ikey < len(df):
            row = df.iloc[ikey]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            return row
    except Exception:
        pass

    return None


def _rows_for_group(df: pd.DataFrame, group_key: str) -> pd.DataFrame:
    """예: 'SND-3' 과 같이 berth 단위 문제에 해당하는 행들을 찾는다."""
    m = _GROUP_KEY_RE.match(str(group_key or ""))
    if not m or df is None or df.empty:
        return pd.DataFrame()
    terminal = m.group(1)
    berth = int(m.group(2))
    out = df[(df.get("terminal") == terminal) & (pd.to_numeric(df.get("berth"), errors="coerce") == berth)]
    return out.reset_index(drop=False).rename(columns={"index": "row_idx"})


def _join_head(values: pd.Series, limit: int = 3) -> str:
    vals = []
    for v in values.tolist():
        s = _safe_text(v, default="")
        if s:
            vals.append(s)
        if len(vals) >= limit:
            break
    return ", ".join(vals) if vals else "-"


def _problem_row_to_dict(df: pd.DataFrame, problem: tuple) -> dict:
    row_or_key, field, msg = problem

    row = _resolve_row(df, row_or_key)
    if row is not None:
        terminal = _safe_text(row.get("terminal"))
        berth_txt = _safe_int_text(row.get("berth"))
        vessel = _safe_text(row.get("vessel"))
        voyage = _safe_text(row.get("voyage"))
        row_idx_txt = _safe_text(row_or_key)
        return {
            "대상": f"row {row_idx_txt} · {vessel} · {terminal}-{berth_txt}",
            "row_idx": row_idx_txt,
            "row_id": _safe_int_text(row.get("row_id")),
            "선박명": vessel,
            "모선항차": voyage,
            "terminal": terminal,
            "berth": berth_txt,
            "항목": _safe_text(field),
            "메시지": _safe_text(msg),
        }

    # berth/terminal 그룹 문제(clearance 등)
    group_rows = _rows_for_group(df, field)
    if not group_rows.empty:
        terminal = _safe_text(group_rows.iloc[0].get("terminal"))
        berth_txt = _safe_int_text(group_rows.iloc[0].get("berth"))
        count = len(group_rows)
        return {
            "대상": f"group {terminal}-{berth_txt} · {count}건", 
            "row_idx": _join_head(group_rows["row_idx"]),
            "row_id": _join_head(group_rows.get("row_id", pd.Series(dtype=object))),
            "선박명": _join_head(group_rows.get("vessel", pd.Series(dtype=object))),
            "모선항차": _join_head(group_rows.get("voyage", pd.Series(dtype=object))),
            "terminal": terminal,
            "berth": berth_txt,
            "항목": _safe_text(row_or_key),
            "메시지": _safe_text(msg),
        }

    # 마지막 fallback
    return {
        "대상": _safe_text(row_or_key),
        "row_idx": _safe_text(row_or_key),
        "row_id": "-",
        "선박명": "-",
        "모선항차": "-",
        "terminal": "-",
        "berth": "-",
        "항목": _safe_text(field),
        "메시지": _safe_text(msg),
    }


def _build_validation_frame(df: pd.DataFrame, probs: list[tuple]) -> pd.DataFrame:
    rows = [_problem_row_to_dict(df, p) for p in probs]
    if not rows:
        return pd.DataFrame(columns=["대상", "row_idx", "row_id", "선박명", "모선항차", "terminal", "berth", "항목", "메시지"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------
# 유효성 표시
#  - visible=False 이면 아무것도 렌더하지 않음(데이터는 반환)
#  - location="본문(접기)" → 본문 expander에 상세
#  - location="사이드바(요약)" → 사이드바에 개수와 일부만
# ---------------------------------------------------------
def show_validation(name: str, df: pd.DataFrame, visible: bool = True, location: str = "본문(접기)"):
    probs = validate_df(df)
    pretty = _build_validation_frame(df, probs)

    if not visible:
        return probs

    if location == "사이드바(요약)":
        with st.sidebar:
            if probs:
                st.error(f"{name} 경고 {len(probs)}건")
                st.dataframe(pretty.head(10), height=260, use_container_width=True, hide_index=True)
                st.caption("상세는 본문의 유효성 경고 섹션에서 확인하세요.")
            else:
                st.success(f"{name} 검증 통과")
        return probs

    if probs:
        with st.expander(f"⚠ {name} 유효성 경고 {len(probs)}건 보기", expanded=False):
            st.dataframe(pretty, height=320, use_container_width=True, hide_index=True)
    else:
        st.success(f"{name} 검증 통과")
    return probs
