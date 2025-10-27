import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re


def get_berth_status(time="3days", route="ALL", berth="A"):
    """
    신선대감만터미널 선석배정 현황 조회
    
    Args:
        time: 조회기간 (기본값: "3days")
        route: 항로구분 (기본값: "ALL")
        berth: 터미널 구분 - 신선대(A), 감만(B) (기본값: "A")
    
    Returns:
        pandas.DataFrame: 선석배정 현황 데이터
    """
    url = "https://info.bptc.co.kr/Berth_status_text_servlet_sw_kr"
    
    payload = {
        "v_time": time,
        "ROCD": route,
        "v_oper_cd": "",
        "ORDER": "item1",
        "v_gu": berth,
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://info.bptc.co.kr/content/sw/frame/berth_status_text_frame_sw_kr.jsp?p_id=BETX_SH_KR&snb_num=2&snb_div=service",
    }
    
    # 요청
    res = requests.post(url, data=payload, headers=headers)
    res.encoding = "euc-kr"
    
    soup = BeautifulSoup(res.text, "html.parser")
    
    # 테이블 찾기
    table = soup.find("table")
    if not table:
        return pd.DataFrame()
    
    headers_list = [th.get_text(strip=True) for th in table.find_all("th")]
    
    rows = []
    for tr in table.find_all("tr")[1:]:  # 첫 번째는 헤더
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cols:
            rows.append(cols)
    
    df = pd.DataFrame(rows, columns=headers_list)
    return df


def get_all_bp_data(date=None):
    """
    한 번의 요청으로 모든 BP 데이터를 딕셔너리로 가져오기
    
    Args:
        date: 조회할 날짜. None이면 오늘 날짜 사용
    
    Returns:
        dict: {(ship_cd, call_no): bitt} 형식의 딕셔너리
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    
    url = "https://info.bptc.co.kr/content/sw/jsp/berth_g_sw_kr.jsp"
    
    params = {
        "p_id": "BEGR_SH_KR",
        "snb_num": "2",
        "pop_ok": "Y",
        "PAR": "",
        "v_dt": date,
        "sub": "+%C8%AE+%C0%CE+"  # '조회' URL 인코딩
    }
    
    headers = {
        "Referer": "https://info.bptc.co.kr/content/sw/frame/berth_g_frame_sw_kr.jsp?p_id=BEGR_SH_KR&snb_num=2&snb_div=service",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    res = requests.get(url, params=params, headers=headers)
    res.encoding = "euc-kr"
    
    soup = BeautifulSoup(res.text, "html.parser")
    
    bp_dict = {}
    
    # 모든 layer1 섹션 찾기 (dl class="over"와 일반 dl 모두에서)
    layer1_sections = soup.find_all("section", id="layer1")
    
    if not layer1_sections:
        print("경고: layer1을 찾을 수 없습니다.")
        return bp_dict
    
    # 각 layer1 섹션에서 a 태그들을 수집
    for layer1 in layer1_sections:
        a_tags = layer1.find_all("a")
        
        for a_tag in a_tags:
            href = a_tag.get("href", "")
            
            # VslMsg(PS_ID,ship_cd,call_yy,call_no,loc_cnt,dis_cnt,sft_cnt,plan_cd,oper_cd,ship_nm,bitt,member_section) 패턴 매칭
            match = re.search(r"VslMsg\('([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)'\)", href)
            
            if match:
                PS_ID = match.group(1)
                ship_cd = match.group(2)
                call_yy = match.group(3)
                call_no = match.group(4)
                loc_cnt = match.group(5)
                dis_cnt = match.group(6)
                sft_cnt = match.group(7)
                plan_cd = match.group(8)
                oper_cd = match.group(9)
                ship_nm = match.group(10)
                bitt = match.group(11)
                member_section = match.group(12)
                
                # 딕셔너리에 저장: key는 (ship_cd, call_no)
                bp_dict[(ship_cd, call_no)] = bitt
    
    return bp_dict


def parse_bp(bp_str):
    """
    BP 문자열을 파싱해서 BP, F, E 값을 추출
    
    예: "110 ( F: 1, E: 142)" -> BP=110, F=1, E=142
    
    Args:
        bp_str: BP 문자열
    
    Returns:
        tuple: (bp, f, e) 또는 (None, None, None)
    """
    if not bp_str or pd.isna(bp_str):
        return (None, None, None)
    
    # 정규식으로 파싱: "정수 ( F: 정수, E: 정수)"
    match = re.search(r"(\d+)\s*\(\s*F:\s*(\d+)\s*,\s*E:\s*(\d+)\)", bp_str)
    
    if match:
        bp = int(match.group(1))
        f = int(match.group(2))
        e = int(match.group(3))
        return (bp, f, e)
    
    return (None, None, None)


def add_bp_to_dataframe(df, date=None):
    """
    DataFrame의 모선항차 정보를 기반으로 BP 정보를 추가
    
    Args:
        df: 모선항차 정보가 포함된 DataFrame (모선항차 컬럼 필요)
        date: 조회할 날짜. None이면 오늘 날짜 사용
    
    Returns:
        pandas.DataFrame: BP 컬럼이 추가된 DataFrame
    """
    if "모선항차" not in df.columns:
        print("경고: DataFrame에 '모선항차' 컬럼이 없습니다.")
        return df
    
    # 한 번의 요청으로 전체 BP 데이터 가져오기
    bp_dict = get_all_bp_data(date)
    
    bp_list = []
    f_list = []
    e_list = []
    failed_ships = []  # 실패한 선박 추적
    total = len(df)
    
    for idx, row in df.iterrows():
        mocen = str(row["모선항차"]) if pd.notna(row["모선항차"]) else ""
        
        # 모선항차 형식: "NSVY-44" -> ship_cd="NSVY", call_no="44"
        parts = mocen.split("-")
        if len(parts) >= 2:
            ship_cd = parts[0]
            call_no = parts[1]  # 두 번째 부분이 call_no
            
            bp_str = bp_dict.get((ship_cd, call_no))
            
            # BP 문자열 파싱: "110 ( F: 1, E: 142)" -> bp=110, f=1, e=142
            bp, f, e = parse_bp(bp_str)
            
            bp_list.append(bp)
            f_list.append(f)
            e_list.append(e)
            
            # BP 정보가 없는 경우 실패 목록에 추가
            if bp is None and f is None and e is None:
                failed_ships.append(mocen)
        else:
            print(f"  경고: 모선항차 형식이 올바르지 않습니다: {mocen}")
            failed_ships.append(mocen)
            bp_list.append(None)
            f_list.append(None)
            e_list.append(None)
    
    # 실패한 선박이 있는 경우 출력
    if failed_ships:
        print(f"\n⚠️ BP 정보 조회 실패 ({len(failed_ships)}척):")
        for mocen in failed_ships:
            print(f"  - {mocen}")
    
    df["bp"] = bp_list
    df["f"] = f_list
    df["e"] = e_list
    return df


if __name__ == "__main__":
    # 테스트 실행
    df = get_berth_status()
    print(df.head())
