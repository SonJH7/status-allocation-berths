import time
import re
import requests
import pandas as pd
from urllib.parse import quote_plus
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.vesselfinder.com/",
})

# 캐시(동일선박 중복 요청 방지)
_dims_cache = {}


def get_vessel_dimensions(name: str, debug=False):
    """
    VesselFinder에서 선박의 Length와 Beam 정보를 가져옴
    
    Args:
        name: 선박명
        debug: 디버그 모드 (True일 경우 실제 응답을 출력)
    
    Returns:
        tuple: (length, beam) - 검색 실패시 (None, None)
    """
    key = name.strip().lower()
    if key in _dims_cache:
        return _dims_cache[key]
    
    try:
        if debug:
            print(f"\n=== 디버깅: {name} ===")
            print(f"[1단계] VesselFinder 검색 페이지에서 직접 크기 정보 추출...")
        
        # 검색 페이지 접근
        search_url = f"https://www.vesselfinder.com/vessels?name={quote_plus(name)}"
        
        if debug:
            print(f"  검색 URL: {search_url}")
        
        response = session.get(search_url, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        length = None
        beam = None
        
        # 검색 결과 테이블에서 v6 클래스를 가진 td 찾기 (길이/폭 정보)
        v6_cells = soup.find_all('td', class_='v6')
        
        for cell in v6_cells:
            text = cell.get_text(strip=True)
            # "133 / 19" 형식 파싱
            match = re.search(r'(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', text)
            if match:
                length = float(match.group(1))
                beam = float(match.group(2))
                if debug:
                    print(f"  추출된 정보: {text}")
                break
        
        if debug:
            print(f"  추출된 Length: {length} m")
            print(f"  추출된 Beam: {beam} m")
        
        if length and beam:
            result = (length, beam)
            _dims_cache[key] = result
            return result
        else:
            _dims_cache[key] = (None, None)
            return (None, None)
        
    except Exception as e:
        print(f"Error fetching dimensions for {name}: {e}")
        _dims_cache[key] = (None, None)
        return (None, None)
    
    finally:
        # API 부하 방지를 위한 딜레이
        time.sleep(0.4)


def enrich_with_length_beam(df: pd.DataFrame, ship_name_column="선박명", debug=False):
    """
    DataFrame에 선박의 Length와 Beam 정보를 추가
    
    Args:
        df: 선석배정 현황 DataFrame
        ship_name_column: 선박명이 있는 열 이름
        debug: 디버그 모드 (첫 번째 선박의 응답 상세 출력)
    
    Returns:
        pandas.DataFrame: Length와 Beam 정보가 추가된 DataFrame
    """
    out = df.copy()
    
    if ship_name_column not in out.columns:
        print(f"경고: '{ship_name_column}' 열이 존재하지 않습니다.")
        out["length"] = None
        out["beam"] = None
        return out
    
    Ls, Bs = [], []
    failed_ships = []  # 실패한 선박 추적
    total = len(out)
    
    for idx, ship_name in enumerate(out[ship_name_column].astype(str).fillna(""), 1):
        # 진행도만 표시
        print(f"진행 중: {idx}/{total}", end="\r")
        
        # 첫 번째 선박만 디버그 모드로 실행
        is_debug = debug and (idx == 1)
        L, B = get_vessel_dimensions(ship_name, debug=is_debug)
        Ls.append(L)
        Bs.append(B)
        
        # 실패한 선박 기록 (L이나 B가 None인 경우)
        if L is None or B is None:
            failed_ships.append(ship_name)
    
    print(f"진행 중: {total}/{total}\n")  # 마지막 진행도 출력
    
    # 실패한 선박이 있는 경우 출력
    if failed_ships:
        print(f"\n⚠️ VesselFinder 검색 실패 ({len(failed_ships)}척):")
        for ship in failed_ships:
            print(f"  - {ship}")
    
    out["Length(m)"] = Ls
    out["Beam(m)"] = Bs
    
    return out
