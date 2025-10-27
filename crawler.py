# crawler.py
from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from http import cookiejar
from typing import Mapping

import pandas as pd
import requests
from bs4 import BeautifulSoup

from crawling.main import collect_berth_info

G_PAGE_URL = "https://info.bptc.co.kr/content/sw/jsp/berth_g_sw_kr.jsp"
G_REFERER = (
    "https://info.bptc.co.kr/content/sw/frame/berth_g_frame_sw_kr.jsp"
    "?p_id=BEGR_SH_KR&snb_num=2&snb_div=service"
)
DEFAULT_HEADERS = {
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
    "Referer": G_REFERER,
}


def _to_cookie_jar(
    cookies: cookiejar.CookieJar | Mapping[str, str] | str | None,
) -> cookiejar.CookieJar | None:
    if cookies is None:
        return None
    if isinstance(cookies, cookiejar.CookieJar):
        return cookies
    jar = requests.cookies.RequestsCookieJar()
    if isinstance(cookies, str):
        parts = [chunk.strip() for chunk in cookies.split(";") if chunk.strip()]
        for part in parts:
            if "=" in part:
                name, value = part.split("=", 1)
                jar.set(name.strip(), value.strip())
    else:
        for name, value in cookies.items():
            jar.set(name, value)
    return jar


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
    debug: bool = False,
) -> pd.DataFrame:
    """crawling 패키지의 수집 로직을 이용해 선석배정 데이터를 반환한다."""

    try:
        df = collect_berth_info(time=v_time, route=route, berth=berth_group, debug=debug)
    except Exception as exc:  # pragma: no cover - 외부 API 오류 래핑
        raise RuntimeError(f"crawling.collect_berth_info 호출 실패: {exc}") from exc

    if not isinstance(df, pd.DataFrame):
        raise RuntimeError("collect_berth_info가 DataFrame을 반환하지 않았습니다.")

    if df.empty:
        return df

    df = df.copy()

    rename_map = {
        "Length(m)": "length_m",
        "Beam(m)": "beam_m",
        "f": "f_pos",
        "e": "e_pos",
    }
    existing_map = {src: dst for src, dst in rename_map.items() if src in df.columns}
    if existing_map:
        df = df.rename(columns=existing_map)

    numeric_cols = ["bp", "f_pos", "e_pos", "length_m", "beam_m"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if operator and "선사" in df.columns:
        target = operator.strip().upper()
        df = df[df["선사"].astype(str).str.upper() == target]

    if order and order.lower() == "item3" and "입항 예정일시" in df.columns:
        df = df.sort_values("입항 예정일시", kind="stable")

    def _format_bp(row: pd.Series) -> str | None:
        bp_val = row.get("bp")
        f_val = row.get("f_pos")
        e_val = row.get("e_pos")
        if pd.isna(bp_val) and pd.isna(f_val) and pd.isna(e_val):
            return None
        parts: list[str] = []
        if pd.notna(bp_val):
            try:
                parts.append(str(int(float(bp_val))))
            except (TypeError, ValueError):
                parts.append(str(bp_val))
        detail: list[str] = []
        if pd.notna(f_val):
            detail.append(f"F: {int(float(f_val))}")
        if pd.notna(e_val):
            detail.append(f"E: {int(float(e_val))}")
        if detail:
            parts.append(f"( {', '.join(detail)} )")
        return " ".join(parts) if parts else None

    if {"bp", "f_pos", "e_pos"}.intersection(df.columns):
        bp_text = df.apply(_format_bp, axis=1)
        df["bp_raw"] = bp_text
        df["bitt"] = bp_text

    if {"f_pos", "e_pos"}.issubset(df.columns):
        df["start_meter"] = df[["f_pos", "e_pos"]].min(axis=1)
        df["end_meter"] = df[["f_pos", "e_pos"]].max(axis=1)

    df = df.replace({"": pd.NA})
    df = df.reset_index(drop=True)
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
    body = href[len(prefix) : -1]
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

    headers = dict(DEFAULT_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    request_cookies = _to_cookie_jar(cookies)

    response = requests.get(
        G_PAGE_URL,
        params=default_params,
        headers=headers,
        timeout=timeout,
        cookies=request_cookies,
        proxies=proxies,
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "euc-kr"
    return parse_berth_g_html(response.text)


__all__ = ["fetch_bptc_t", "fetch_bptc_g", "parse_berth_g_html", "BerthGData"]

