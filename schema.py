# =========================
# schema.py
# =========================
import re
import numpy as np
import pandas as pd

# ---------------------------------------------------------
# 상수 정의
# ---------------------------------------------------------
SND_BERTHS = set(range(1, 6))    # 신선대 1~5
GAM_BERTHS = set(range(6, 10))   # 감만 6~9
Y_GRID_M = 30                    # 세로(선석 내 m) 스냅 단위
MIN_CLEARANCE_M = 30             # 선박 간 최소 이격(m)
TIME_GRID_MIN = 10               # 가로(시간) 스냅 단위(분)

TERMINAL_LAYOUT = {
    "SND": {"y_max": 1500.0, "step": 300.0, "berths": [1, 2, 3, 4, 5]},
    # Plotly 읽기전용 뷰의 라벨 순서와 동일하게 상단→하단 = 9,8,7,6
    "GAM": {"y_max": 1400.0, "step": 350.0, "berths": [9, 8, 7, 6]},
}

# ---------------------------------------------------------
# 한글 원본 → 표준 컬럼 매핑 (요청한 컬럼만 사용)
#   - 입항 예정일시, 작업완료 일시, 모선항차, 선박명, 구분, 선석, bp, f, e
# ---------------------------------------------------------
KOR_MAP = {
    "입항 예정일시": "start",
    "입항예정일시": "start",
    "출항일시": "end",
    "출항 일시": "end",
    "모선항차": "voyage",
    "선박명": "vessel",
    "구분": "stype",
    "선석": "berth",
    "bp": "bp",
    "f": "f",
    "e": "e",
    "접안": "berthing",
    "검역": "quarantine",
    "도선": "pilot",
}

# 표준 출력 순서
STD_ORDER = ["terminal", "berth", "vessel", "voyage", "start", "end", "stype", "bp", "f", "e", "berthing", "quarantine", "pilot", "y_m"]


# ---------------------------------------------------------
# 유틸
# ---------------------------------------------------------
def terminal_layout(terminal: str) -> dict:
    return TERMINAL_LAYOUT.get(str(terminal or "").upper(), {})


def _infer_terminal_from_berth(b: int) -> str:
    """선석 번호만 보고 터미널 추론: 1~5=SND, 6~9=GAM, 그 외는 빈값"""
    try:
        bi = int(b)
    except Exception:
        return ""
    if 1 <= bi <= 5:
        return "SND"
    if 6 <= bi <= 9:
        return "GAM"
    return ""


def infer_berth_from_y(terminal: str, y_m) -> int | None:
    layout = terminal_layout(terminal)
    if not layout:
        return None
    try:
        y = float(y_m)
    except Exception:
        return None
    if np.isnan(y):
        return None
    y = min(max(y, 0.0), float(layout["y_max"]) - 1e-6)
    idx = int(y // float(layout["step"]))
    idx = max(0, min(idx, len(layout["berths"]) - 1))
    return int(layout["berths"][idx])


def berth_band_bounds(terminal: str, berth: int) -> tuple[float, float] | tuple[None, None]:
    layout = terminal_layout(terminal)
    if not layout:
        return (None, None)
    berths = list(layout["berths"])
    try:
        idx = berths.index(int(berth))
    except Exception:
        return (None, None)
    step = float(layout["step"])
    return (idx * step, (idx + 1) * step)


def _safe_float(x):
    try:
        v = float(x)
        return None if np.isnan(v) else v
    except Exception:
        return None


def row_center_y(row) -> float | None:
    f = _safe_float(row.get("f")) if isinstance(row, dict) else _safe_float(getattr(row, "f", None))
    e = _safe_float(row.get("e")) if isinstance(row, dict) else _safe_float(getattr(row, "e", None))
    if f is not None and e is not None:
        return (f + e) / 2.0
    y = _safe_float(row.get("y_m")) if isinstance(row, dict) else _safe_float(getattr(row, "y_m", None))
    if y is not None:
        return y
    bp = _safe_float(row.get("bp")) if isinstance(row, dict) else _safe_float(getattr(row, "bp", None))
    return bp


def row_span(row) -> tuple[float, float] | tuple[None, None]:
    f = _safe_float(row.get("f")) if isinstance(row, dict) else _safe_float(getattr(row, "f", None))
    e = _safe_float(row.get("e")) if isinstance(row, dict) else _safe_float(getattr(row, "e", None))
    if f is not None and e is not None:
        return (min(f, e), max(f, e))
    y = row_center_y(row)
    if y is None:
        return (None, None)
    return (y, y)


def span_gap_m(a0, a1, b0, b1) -> float | None:
    try:
        a0, a1 = sorted((float(a0), float(a1)))
        b0, b1 = sorted((float(b0), float(b1)))
    except Exception:
        return None
    if a1 < b0:
        return b0 - a1
    if b1 < a0:
        return a0 - b1
    # 음수면 겹친 길이(절댓값), 0이면 접촉
    return -min(a1, b1) + max(a0, b0)


def _coerce_datetime(x):
    """
    관대한 날짜 파서:
      - None/NaN/빈문자/대시류/N/A → NaT
      - 엑셀 직렬값(정수/실수, 대략 20000~80000 범위) → to_datetime(origin='1899-12-30', unit='D')
      - 그 외 문자열 → 구분자/한글단위 정리 후 pandas.to_datetime(errors='coerce')
      - Timestamp는 그대로 반환
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NaT
    if isinstance(x, pd.Timestamp):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s == "" or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return pd.NaT
        s_norm = re.sub(r"[./]", "-", s)
        s_norm = re.sub(r"[년월일시분초]", "-", s_norm)
        s_norm = re.sub(r"\s+", " ", s_norm).strip("- ").strip()
        ts = pd.to_datetime(s_norm, errors="coerce", utc=False)
        return ts if not pd.isna(ts) else pd.NaT
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        xf = float(x)
        if 20000 <= xf <= 80000:
            try:
                return pd.to_datetime(xf, origin="1899-12-30", unit="D")
            except Exception:
                return pd.NaT
        return pd.NaT
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return pd.NaT


# ---------------------------------------------------------
# 정규화
# ---------------------------------------------------------
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    원본 테이블을 표준 스키마로 통일
      - 컬럼명 표준화(KOR_MAP)
      - berth 정수화, terminal 추론
      - 문자열 기본 처리(vessel/voyage/stype 등)
      - start/end 관대 파싱(_coerce_datetime)
      - bp/f/e 숫자화
      - y_m은 f/e 중심값이 있으면 그 값을, 없으면 bp를 사용
      - STD_ORDER만 노출
    """
    out = df.copy()

    rename_map = {k: v for k, v in KOR_MAP.items() if k in out.columns}
    out.rename(columns=rename_map, inplace=True)

    if "berth" in out:
        out["berth"] = (
            out["berth"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .astype(float)
            .fillna(0)
            .astype(int)
        )
    else:
        out["berth"] = 0

    out["terminal"] = out["berth"].apply(_infer_terminal_from_berth)

    for col in ["vessel", "voyage", "stype", "remark", "berthing", "quarantine", "pilot"]:
        if col not in out:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    if "start" in out:
        out["start"] = out["start"].apply(_coerce_datetime)
    else:
        out["start"] = pd.Series(pd.NaT, index=out.index)

    if "end" in out:
        out["end"] = out["end"].apply(_coerce_datetime)
    else:
        out["end"] = pd.Series(pd.NaT, index=out.index)

    for col in ["bp", "f", "e"]:
        if col not in out:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    mids = (out["f"].astype(float) + out["e"].astype(float)) / 2.0
    out["y_m"] = mids.where(~(out["f"].isna() | out["e"].isna()), out["bp"]).fillna(0).astype(float)

    std = [c for c in STD_ORDER if c in out.columns]
    extras = [c for c in ["Length(m)", "Beam(m)", "note", "plan_status"] if c in out.columns]
    out = out[std + extras]
    return out


# ---------------------------------------------------------
# 검증
# ---------------------------------------------------------
def _overlap(a_start, a_end, b_start, b_end) -> bool:
    if pd.isna(a_start) or pd.isna(a_end) or pd.isna(b_start) or pd.isna(b_end):
        return False
    return not (a_end <= b_start or b_end <= a_start)


def validate_df(df: pd.DataFrame) -> list[tuple]:
    """
    유효성 검사:
      - terminal in {SND,GAM}
      - 선석 범위(SND:1~5, GAM:6~9)
      - start < end
      - 위치(y_m)와 berth 일관성
      - 동일 (terminal, berth) / 시간 중첩 상태에서 선체 구간(f~e) 간격 >= MIN_CLEARANCE_M
    """
    problems: list[tuple] = []

    for i, r in df.iterrows():
        t = r.get("terminal")
        b = int(r.get("berth", 0))
        s = r.get("start")
        e = r.get("end")
        layout = terminal_layout(t)
        lo, hi = row_span(r)
        mid = row_center_y(r)

        if t not in {"SND", "GAM"}:
            problems.append((i, "terminal", "터미널 값 오류(SND/GAM)"))
        if t == "SND" and b not in SND_BERTHS:
            problems.append((i, "berth", "신선대 선석 범위(1~5) 위반"))
        if t == "GAM" and b not in GAM_BERTHS:
            problems.append((i, "berth", "감만 선석 범위(6~9) 위반"))
        if pd.isna(s) or pd.isna(e) or s >= e:
            problems.append((i, "time", "시작/종료 시간 오류"))
        if layout and lo is not None and hi is not None:
            if lo < 0 or hi > float(layout["y_max"]):
                problems.append((i, "position", f"선체 위치가 터미널 범위(0~{int(layout['y_max'])}m)를 벗어남"))
        if layout and mid is not None and b:
            inferred = infer_berth_from_y(t, mid)
            if inferred is not None and int(inferred) != b:
                problems.append((i, "berth", f"현재 위치(y={mid:.1f}m) 기준 선석은 {inferred}인데 데이터는 {b}로 저장됨"))

    keyed = df.reset_index().rename(columns={"index": "_row_idx"})
    for (t, b), g in keyed.groupby(["terminal", "berth"], dropna=False):
        g = g.sort_values("start").reset_index(drop=True)
        n = len(g)
        for i in range(n):
            for j in range(i + 1, n):
                if not _overlap(g.loc[i, "start"], g.loc[i, "end"], g.loc[j, "start"], g.loc[j, "end"]):
                    continue
                a0, a1 = row_span(g.loc[i])
                b0, b1 = row_span(g.loc[j])
                gap = span_gap_m(a0, a1, b0, b1)
                if gap is None:
                    continue
                if gap < MIN_CLEARANCE_M:
                    row_key = f"{t}-{int(b) if pd.notna(b) else b}"
                    if gap < 0:
                        problems.append((
                            "clearance",
                            row_key,
                            f"동시간대 선체 구간이 겹침 (겹침 {abs(gap):.1f}m)",
                        ))
                    else:
                        problems.append((
                            "clearance",
                            row_key,
                            f"동시간대 선박 간 최소 이격 {MIN_CLEARANCE_M}m 위반 (실제 {gap:.1f}m)",
                        ))
    return problems


# ---------------------------------------------------------
# 스냅(시간 5분 / 세로 30m)
# ---------------------------------------------------------
def snap_time_5min(ts: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(ts):
        return ts
    base = ts.replace(second=0, microsecond=0)
    minutes = base.hour * 60 + base.minute
    snapped = round(minutes / 5) * 5
    h, m = divmod(snapped, 60)
    day_offset, h = divmod(h, 24)
    return (
        base.normalize()
        + pd.to_timedelta(day_offset, "D")
        + pd.to_timedelta(h, "H")
        + pd.to_timedelta(m, "m")
    )


def snap_y_30m(y_m: float) -> float:
    try:
        y_m = float(y_m)
    except Exception:
        return 0.0
    return round(y_m / Y_GRID_M) * Y_GRID_M


# ===== (추가) row_id 보장 =====
def ensure_row_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "row_id" not in out.columns:
        out.insert(0, "row_id", range(len(out)))
    return out


# ===== (추가) 정규화 ↔ 원본 동기화 =====
def sync_raw_with_norm(raw_df: pd.DataFrame, norm_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or norm_df is None:
        return raw_df
    if "row_id" not in raw_df.columns or "row_id" not in norm_df.columns:
        return raw_df.copy()

    out = raw_df.copy()
    inv = {}
    for k, v in KOR_MAP.items():
        inv.setdefault(v, []).append(k)

    std_cols = ["start", "end", "voyage", "vessel", "stype", "berth", "bp", "f", "e", "berthing", "quarantine", "pilot"]
    g = norm_df.set_index("row_id")
    for rid, row in g.iterrows():
        if rid not in out["row_id"].values:
            continue
        mask = out["row_id"] == rid
        for std_col in std_cols:
            if std_col not in row.index:
                continue
            val = row.get(std_col)
            for kor_col in inv.get(std_col, []):
                if kor_col in out.columns:
                    out.loc[mask, kor_col] = val
                    break
    return out
