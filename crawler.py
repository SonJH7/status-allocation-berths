# crawler.py
from __future__ import annotations

from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
DATA_URL = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
FRAME_URL = (
    "https://info.bptc.co.kr/content/sw/frame/berth_status_text_frame_sw_kr.jsp"
    "?p_id=BETX_SH_KR&snb_num=2&snb_div=service"
)

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _parse_cookie_str(cookie_str: str) -> cookiejar.CookieJar:
    jar = requests.cookies.RequestsCookieJar()
    for chunk in cookie_str.split(";"):
        if not chunk.strip():
            continue
        if "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        jar.set(name.strip(), value.strip())
    return jar


def _init_session(
    timeout: float = 30.0,
    *,
    trust_env: bool | None = None,
    cookies: cookiejar.CookieJar | Mapping[str, str] | str | None = None,
    proxies: Mapping[str, str] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> requests.Session:
    """브라우저와 유사한 세션을 선행 페이지와 함께 구성한다."""

    sess = requests.Session()
    if trust_env is not None:
        sess.trust_env = trust_env

    sess.headers.update(BASE_HEADERS)
    if extra_headers:
        sess.headers.update(extra_headers)

    if cookies:
        if isinstance(cookies, str):
            sess.cookies.update(_parse_cookie_str(cookies))
        else:
            sess.cookies.update(cookies)

    if proxies:
        sess.proxies.update(proxies)

    try:
        resp = sess.get(FRAME_URL, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:
        # 사전 프레임 접근이 실패해도 POST 자체는 시도한다.
        print("⚠️ 초기 프레임 로드 실패:", exc)

    return sess


def _load_env_proxies() -> Mapping[str, str] | None:
    proxy = os.getenv("BPTC_PROXY")
    if proxy:
        return {"http": proxy, "https": proxy}

    http_proxy = os.getenv("BPTC_HTTP_PROXY")
    https_proxy = os.getenv("BPTC_HTTPS_PROXY")
    if not http_proxy and not https_proxy:
        return None
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    return proxies or None


def fetch_bptc_t(
    v_time: str = "3days",
    route: str = "ALL",
    operator: str | None = None,
    order: str = "item3",
    berth_group: str = "A",
    timeout: float = 30.0,
    *,
    trust_env: bool | None = None,
    cookies: cookiejar.CookieJar | Mapping[str, str] | str | None = None,
    proxies: Mapping[str, str] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """CJ KBCT 선석배정 현황(T) 테이블을 크롤링한다."""

    form = {
        "v_time": v_time,
        "ROCD": route,
        "ORDER": order,
        "v_gu": berth_group,
    }
    if operator:
        form["v_oper_cd"] = operator.upper()

    env_cookie = os.getenv("BPTC_COOKIE")
    if cookies is None and env_cookie:
        cookies = env_cookie

    if proxies is None:
        proxies = _load_env_proxies()

    sess = _init_session(
        timeout=timeout,
        trust_env=trust_env,
        cookies=cookies,
        proxies=proxies,
        extra_headers=extra_headers,
    )
    headers = {
        **BASE_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://info.bptc.co.kr",
        "Referer": FRAME_URL,
        "Sec-Fetch-Dest": "iframe",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    try:
        resp = sess.post(DATA_URL, data=form, headers=headers, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = "euc-kr"
    except Exception as exc:
        print("⚠️ 요청 실패:", exc)
        return pd.DataFrame(columns=["vessel", "berth", "eta", "etd"])

    soup = BeautifulSoup(resp.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        print("⚠️ No <table> found in response.")
        return pd.DataFrame(columns=["vessel", "berth", "eta", "etd"])

    dfs = pd.read_html(StringIO(str(soup)), flavor="lxml", encoding="euc-kr")

    df = max(dfs, key=lambda d: len(d.columns))
    df.columns = [str(c).strip() for c in df.columns]

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
    keep_cols = [c for c in ["vessel", "berth", "eta", "etd"] if c in df.columns]
    df = df[keep_cols].copy()

    # ✅ 날짜 변환
    for c in ["eta", "etd"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # ✅ 결측 제거
    df = df.dropna(subset=["vessel", "berth"]).reset_index(drop=True)

    print(f"✅ 크롤링 성공: {len(df)}건, 컬럼: {df.columns.tolist()}")
    return df

__all__ = ["fetch_bptc_t"]