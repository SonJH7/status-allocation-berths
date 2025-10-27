# crawler.py
from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
import os
from http import cookiejar
from pathlib import Path
import ast
import re
from typing import Mapping

import pandas as pd
import requests
from bs4 import BeautifulSoup
DATA_URL = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
FRAME_URL = (
    "https://info.bptc.co.kr/content/sw/frame/berth_status_text_frame_sw_kr.jsp"
    "?p_id=BETX_SH_KR&snb_num=2&snb_div=service"
)
G_PAGE_URL = "https://info.bptc.co.kr/content/sw/jsp/berth_g_sw_kr.jsp"

SECURITY_WALL_MARKERS = tuple(
    marker.lower()
    for marker in (
        "cache-control header is missing or empty",
        "x-content-type-options",
        "incorrect use of autocomplete attribute",
        "buttons must have discernible text",
        "aria attribute is not allowed",
        "form elements must have labels",
        "ids used in aria and labels must be unique",
        "set-cookie header doesn't have the 'secure' directive",
        "set-cookie header doesn't have the 'httponly' directive",
    )
)

SECURITY_WALL_HINT_PHRASES = (
    "A 'cache-control' header is missing or empty",
    "A 'set-cookie' header doesn't have the 'secure' directive",
    "A 'set-cookie' header doesn't have the 'httponly' directive",
    "Response should include 'x-content-type-options' header",
    "Buttons must have discernible text",
    "Elements must only use allowed ARIA attributes",
    "Form elements must have labels",
    "IDs used in ARIA and labels must be unique",
    "Incorrect use of autocomplete attribute",
)

def _extract_security_wall_hints(html: str) -> list[str]:
    """반환된 보안 안내 페이지에서 핵심 점검 항목 텍스트를 추출한다."""

    soup = BeautifulSoup(html, "lxml")
    text_lines: list[str] = []
    for raw in soup.get_text("\n", strip=True).splitlines():
        cleaned = " ".join(raw.split())
        if cleaned:
            text_lines.append(cleaned)

    found: list[str] = []
    for phrase in SECURITY_WALL_HINT_PHRASES:
        lowered = phrase.lower()
        for line in text_lines:
            if lowered in line.lower() and line not in found:
                found.append(line)
                break

    return found[:10]


DIAG_DIR = Path("data") / "diagnostics"


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
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def _dump_debug_artifact(prefix: str, payload: str, *, suffix: str = ".html") -> Path | None:
    """진단을 위해 응답 원본을 저장한다."""

    try:
        DIAG_DIR.mkdir(parents=True, exist_ok=True)
        path = DIAG_DIR / f"{prefix}{suffix}"
        path.write_text(payload, encoding="utf-8", errors="ignore")
        return path
    except Exception:
        return None


def _ensure_not_security_wall(
    resp: requests.Response,
    *,
    dump_prefix: str | None = None,
) -> None:
    """보안 게이트(취약점 검사 안내 페이지) 응답을 탐지해 예외로 승격한다."""

    lower_body = resp.text.lower()
    if any(marker in lower_body for marker in SECURITY_WALL_MARKERS):
        hints = _extract_security_wall_hints(resp.text)
        details = "\n- " + "\n- ".join(hints) if hints else ""
        artifact_note = ""
        if dump_prefix:
            artifact = _dump_debug_artifact(f"{dump_prefix}_security_wall", resp.text)
            if artifact:
                artifact_note = f"\n원본 응답: {artifact}"

        raise RuntimeError(
            "BPTC 서버가 보안 점검 안내 페이지로 응답했습니다. "
            "브라우저에서 info.bptc.co.kr 페이지를 먼저 열어 reCAPTCHA/보안 절차를 통과한 뒤, "
            "필요하다면 BPTC_COOKIE 환경변수에 세션 쿠키를 설정하세요." + details + artifact_note
        )




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
        if not resp.encoding:
            resp.encoding = resp.apparent_encoding or "utf-8"
        _ensure_not_security_wall(resp, dump_prefix="frame")
    except RuntimeError:
        raise
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
        _ensure_not_security_wall(resp, dump_prefix="post")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"BPTC 요청 실패: {exc}") from exc

    soup = BeautifulSoup(resp.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        artifact = _dump_debug_artifact("post_no_table", resp.text)
        hint = f" (응답 원본: {artifact})" if artifact else ""
        print(f"⚠️ No <table> found in response.{hint}")
        return pd.DataFrame(columns=["vessel", "berth", "eta", "etd"])

    try:
        dfs = pd.read_html(StringIO(str(soup)), flavor="lxml", encoding="euc-kr")
    except ValueError as exc:
        artifact = _dump_debug_artifact("post_parse_error", resp.text)
        hint = f" 응답 원본: {artifact}" if artifact else ""
        raise RuntimeError("BPTC 테이블 파싱에 실패했습니다." + hint) from exc
    df = max(dfs, key=lambda d: len(d.columns))

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(part).strip() for part in col if str(part).strip() and str(part).strip() != "nan")
            for col in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # 중복 컬럼은 뒤쪽 값을 우선시한다.
    df = df.loc[:, ~df.columns.duplicated(keep="last")]

    alias_map = {
        "vessel": ("선박명", "모선항차", "모선", "본선"),
        "berth": ("선석", "접안", "접안(선석)"),
        "eta": (
            "입항예정일시",
            "입항 예정일시",
            "입항(예정)일시",
            "입항 예정",
            "입항",
        ),
        "etd": (
            "출항일시",
            "출항(예정)일시",
            "출항 예정일시",
            "출항 예정",
            "출항",
        ),
    }
    normalized: dict[str, pd.Series] = {}
    for canonical, candidates in alias_map.items():
        for candidate in candidates:
            if candidate in df.columns:
                normalized[canonical] = df[candidate]
                break

    missing = [key for key in ("vessel", "berth", "eta", "etd") if key not in normalized]
    if missing:
        artifact = _dump_debug_artifact("post_missing_columns", resp.text)
        hint = f" (응답 원본: {artifact})" if artifact else ""
        raise RuntimeError(
            "BPTC 응답에서 필수 컬럼을 찾을 수 없습니다: "
            + ", ".join(missing)
            + hint
        )

    df = pd.DataFrame(normalized)
    # ✅ 날짜 변환
    for c in ["eta", "etd"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # ✅ 결측 제거
    # 빈 문자열/하이픈 치환 후 결측 제거
    df = df.replace({"": pd.NA, "-": pd.NA})
    df = df.dropna(subset=["vessel", "berth", "eta", "etd"], how="any").reset_index(drop=True)

    # 시간 순/선석 순 정렬로 일관성 유지
    if {"berth", "eta"}.issubset(df.columns):
        df = df.sort_values(["berth", "eta"], kind="stable").reset_index(drop=True)

    print(f"✅ 크롤링 성공: {len(df)}건, 컬럼: {df.columns.tolist()}")
    return df

def _to_float(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_css_numeric(style: str) -> dict[str, float | str | None]:
    out: dict[str, float | str | None] = {}
    if not style:
        return out

    for key in ("left", "top", "width", "height"):
        match = re.search(rf"{key}\s*:\s*([-0-9.]+)px", style, flags=re.IGNORECASE)
        if match:
            out[f"{key}_px"] = _to_float(match.group(1))

    color_match = re.search(r"background-color\s*:\s*([^;]+)", style, flags=re.IGNORECASE)
    if color_match:
        out["background_color"] = color_match.group(1).strip()

    return out


def _parse_vslmsg_args(href: str | None) -> tuple[str, ...] | None:
    if not href:
        return None
    href = href.strip()
    prefix = "javascript:VslMsg("
    if not href.startswith(prefix) or not href.endswith(")"):
        return None
    body = href[len(prefix):-1]
    try:
        return ast.literal_eval("(" + body + ")")
    except (ValueError, SyntaxError):
        return None


def _extract_time_cells(section: BeautifulSoup | None) -> dict[str, str | None]:
    info: dict[str, str | None] = {"eta_display": None, "etd_display": None, "mid_display": None}
    if section is None:
        return info

    table = section.find("table")
    if table:
        cells = table.find_all("td")
        if cells:
            info["eta_display"] = cells[0].get_text(strip=True) or None
        if len(cells) >= 3:
            info["etd_display"] = cells[2].get_text(strip=True) or None
        if len(cells) >= 2:
            info["mid_display"] = cells[1].get_text(strip=True) or None

    return info


def _collect_info_lines(section: BeautifulSoup | None) -> tuple[str | None, list[str]]:
    if section is None:
        return None, []
    lines = [" ".join(chunk.split()) for chunk in section.stripped_strings]
    lines = [line for line in lines if line]
    if not lines:
        return None, []
    return lines[0], lines[1:]


@dataclass
class BerthGData:
    """선석배정(G) 페이지의 블록/캘린더 정보."""

    blocks: pd.DataFrame
    calendar: pd.DataFrame


def parse_berth_g_html(html: str) -> BerthGData:
    """`berth_g_sw_kr.jsp` HTML에서 블록/캘린더 정보를 추출한다."""

    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("section#layer1 > a[href^='javascript:VslMsg']")

    block_rows: list[dict] = []
    for idx, anchor in enumerate(anchors):
        msg_args = _parse_vslmsg_args(anchor.get("href"))
        block_section = anchor.find("section", id=re.compile(r"^vsl", re.IGNORECASE))
        info_section = anchor.find("section", id=re.compile(r"^vinf", re.IGNORECASE))

        row: dict[str, object] = {
            "index": idx,
            "block_id": block_section.get("id") if block_section else None,
            "info_id": info_section.get("id") if info_section else None,
            "href": anchor.get("href"),
        }
        row.update(_parse_css_numeric(block_section.get("style", "")) if block_section else {})
        row.update(_extract_time_cells(block_section))

        info_label, extra_lines = _collect_info_lines(info_section)
        row["info_label"] = info_label
        row["info_lines"] = extra_lines

        if msg_args:
            names = [
                "mode",
                "service_code",
                "voyage_year",
                "berth_code",
                "start_hint",
                "end_hint",
                "draft_hint",
                "alignment",
                "operator",
                "vessel_name",
                "remark",
                "memo",
            ]
            for name, value in zip(names, msg_args):
                row[name] = value
            row["voyage_year"] = _to_int(row.get("voyage_year"))
            row["berth_code"] = row.get("berth_code")
            row["start_hint"] = _to_int(row.get("start_hint"))
            row["end_hint"] = _to_int(row.get("end_hint"))
            row["draft_hint"] = _to_int(row.get("draft_hint"))

        block_rows.append(row)

    blocks_df = pd.DataFrame(block_rows)

    calendar_rows: list[dict[str, object]] = []
    calendar_table = soup.select_one("table.Calendar")
    if calendar_table:
        headers = [th.get_text(strip=True) for th in calendar_table.select("thead th")]
        date_headers = headers[1:]
        for tr in calendar_table.select("tbody tr"):
            cells = tr.find_all("td")
            if not cells:
                continue
            berth_label = cells[0].get_text(" ", strip=True)
            for idx_cell, cell in enumerate(cells[1:]):
                calendar_rows.append(
                    {
                        "berth_label": berth_label,
                        "date_label": date_headers[idx_cell] if idx_cell < len(date_headers) else None,
                        "cell_text": cell.get_text(strip=True) or None,
                        "cell_class": " ".join(cell.get("class", [])),
                    }
                )

    calendar_df = pd.DataFrame(calendar_rows)

    return BerthGData(blocks=blocks_df, calendar=calendar_df)


def fetch_bptc_g(
    *,
    params: Mapping[str, str] | None = None,
    timeout: float = 30.0,
    trust_env: bool | None = None,
    cookies: cookiejar.CookieJar | Mapping[str, str] | str | None = None,
    proxies: Mapping[str, str] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> BerthGData:
    """선석배정 현황(G) 페이지 전체 HTML을 크롤링해 파싱한다."""

    default_params = {
        "p_id": "BEGR_SH_KR",
        "snb_num": "2",
        "snb_div": "service",
        "pop_ok": "Y",
    }
    if params:
        default_params.update(params)

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
        "Referer": FRAME_URL,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    try:
        resp = sess.get(G_PAGE_URL, params=default_params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "euc-kr"
        _ensure_not_security_wall(resp, dump_prefix="g_page")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"BPTC G 페이지 요청 실패: {exc}") from exc

    return parse_berth_g_html(resp.text)


__all__ = ["fetch_bptc_t", "fetch_bptc_g", "parse_berth_g_html", "BerthGData"]