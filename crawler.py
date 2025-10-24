# crawler.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO

URL = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko,en;q=0.9",
}

def fetch_bptc_t():
    """CJ KBCT 선석배정 현황(T) 테이블 파싱"""
    form = {
        "v_time": "3days",   # 최근 4일
        "ROCD": "ALL",       # 전체 항로
        "v_gu": "A",         # 전체 선석
        "ORDER": "item3",    # 정렬 기준: 선석
    }

    try:
        r = requests.post(URL, data=form, headers=HEADERS, timeout=30)
        r.encoding = "euc-kr"
    except Exception as e:
        print("⚠️ 요청 실패:", e)
        return pd.DataFrame(columns=["vessel","berth","eta","etd"])

    soup = BeautifulSoup(r.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        print("⚠️ No <table> found in response.")
        return pd.DataFrame(columns=["vessel","berth","eta","etd"])

    # ✅ pandas.read_html 권장 방식 (FutureWarning 방지)
    dfs = pd.read_html(StringIO(str(soup)), flavor="lxml", encoding="euc-kr")

    # 가장 열이 많은 테이블 선택
    df = max(dfs, key=lambda d: len(d.columns))
    df.columns = [str(c).strip() for c in df.columns]

    # ✅ 컬럼명 한글 → 영문 통일
    rename_map = {
        "선박명": "vessel",
        "모선항차": "vessel",
        "선석": "berth",
        "입항예정일시": "eta",
        "입항 예정일시": "eta",
        "출항일시": "etd",
        "출항(예정)일시": "etd",
        "출항 예정일시": "etd",
        "접안": "berth",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    # ✅ 필요한 컬럼만 추출
    keep_cols = [c for c in ["vessel","berth","eta","etd"] if c in df.columns]
    df = df[keep_cols].copy()

    # ✅ 날짜 변환
    for c in ["eta","etd"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # ✅ 결측 제거
    df = df.dropna(subset=["vessel","berth"]).reset_index(drop=True)

    print(f"✅ 크롤링 성공: {len(df)}건, 컬럼: {df.columns.tolist()}")
    return df
