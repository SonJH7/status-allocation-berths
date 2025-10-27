"""BPTC 선석배정현황(G) 페이지의 VslMsg() 파라미터 파싱 도구."""

from __future__ import annotations

import re
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

G_PAGE_URL = (
    "https://info.bptc.co.kr/content/sw/jsp/berth_g_sw_kr.jsp"
    "?p_id=BEGR_SH_KR&snb_num=2&snb_div=service&pop_ok=Y"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

# 정규식 컴파일
_VSLMSG_PATTERN = re.compile(r"onclick\\s*=\\s*\"VslMsg\\((.*?)\\)\"")
_ARG_PATTERN = re.compile(r"'((?:\\\\'|[^'])*)'")
_F_PATTERN = re.compile(r"F\s*[:=]\s*(-?\d+)")
_E_PATTERN = re.compile(r"E\s*[:=]\s*(-?\d+)")


def _clean_arg(value: str) -> str:
    return value.replace("\\'", "'").strip()


def _parse_args(raw: str) -> Optional[List[str]]:
    matches = _ARG_PATTERN.findall(raw)
    if matches:
        return [_clean_arg(m) for m in matches]
    # 따옴표 없이 전달된 인자를 대비한 보조 파싱
    tentative = [chunk.strip() for chunk in raw.split(",")]
    if len(tentative) >= 12:
        return [chunk.strip("' ") for chunk in tentative[:12]]
    return None


def _parse_bp(bp_raw: str) -> tuple[Optional[int], Optional[int]]:
    if not bp_raw:
        return None, None
    text = bp_raw.replace(" ", "")
    f_match = _F_PATTERN.search(text)
    e_match = _E_PATTERN.search(text)
    f_val = int(f_match.group(1)) if f_match else None
    e_val = int(e_match.group(1)) if e_match else None
    return f_val, e_val


def fetch_bptc_g_vslmsg(timeout: float = 30.0) -> pd.DataFrame:
    """BPTC 선석배정현황(G) 페이지에서 VslMsg 인자를 파싱해 DataFrame 반환."""

    response = requests.get(G_PAGE_URL, headers=HEADERS, timeout=timeout)
    response.encoding = "euc-kr"
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    anchors = soup.find_all("a", onclick=_VSLMSG_PATTERN)

    records: List[dict] = []
    for anchor in anchors:
        onclick_attr = anchor.get("onclick", "")
        match = _VSLMSG_PATTERN.search(onclick_attr)
        if not match:
            continue
        raw_args = match.group(1)
        args = _parse_args(raw_args)
        if not args or len(args) < 12:
            continue
        (
            ps_id,
            ship_cd,
            call_yy,
            call_no,
            loc_cnt,
            dis_cnt,
            sft_cnt,
            plan_cd,
            oper_cd,
            ship_nm,
            bitt,
            member_section,
        ) = args[:12]
        f_pos, e_pos = _parse_bp(bitt)
        length = abs(e_pos - f_pos) if (f_pos is not None and e_pos is not None) else None

        records.append(
            {
                "PS_ID": ps_id,
                "ship_cd": ship_cd,
                "call_yy": call_yy,
                "call_no": call_no,
                "loc_cnt": loc_cnt,
                "dis_cnt": dis_cnt,
                "sft_cnt": sft_cnt,
                "plan_cd": plan_cd,
                "oper_cd": oper_cd,
                "ship_nm": ship_nm,
                "bitt": bitt,
                "member_section": member_section,
                "f_pos": f_pos,
                "e_pos": e_pos,
                "length_m": length,
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["vessel"] = df["ship_nm"].astype(str).str.strip()
    df["voyage"] = (
        df["ship_cd"].astype(str).str.strip()
        + "-"
        + df["call_yy"].astype(str).str.strip()
        + "-"
        + df["call_no"].astype(str).str.strip()
    )
    df["bp_raw"] = df["bitt"].astype(str).str.replace(" ", "")

    numeric_cols = ["loc_cnt", "dis_cnt", "sft_cnt", "f_pos", "e_pos", "length_m"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[
        [
            "vessel",
            "voyage",
            "oper_cd",
            "loc_cnt",
            "dis_cnt",
            "sft_cnt",
            "plan_cd",
            "bp_raw",
            "f_pos",
            "e_pos",
            "length_m",
        ]
    ].rename(columns={"oper_cd": "operator"})


__all__ = ["fetch_bptc_g_vslmsg"]
