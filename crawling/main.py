"""
부산항 선석배정 현황 및 선박 정보 수집 프로그램
- 신선대감만터미널 선석배정 현황 조회
- VesselFinder API를 통한 선박 길이 및 폭 정보 추가
"""

import pandas as pd

try:  # 패키지로 임포트될 때
    from .bpt import get_berth_status, add_bp_to_dataframe
    from .vsfinder import enrich_with_length_beam
except ImportError:  # pragma: no cover - 단독 실행 시 폴백
    from bpt import get_berth_status, add_bp_to_dataframe
    from vsfinder import enrich_with_length_beam


def collect_berth_info(time="3days", route="ALL", berth="A", debug=False):
    """
    신선대감만터미널 선석배정 현황을 조회하고 선박 정보를 추가
    
    Args:
        time: 조회기간 (기본값: "3days")
        route: 항로구분 (기본값: "ALL")
        berth: 터미널 구분 - 신선대(A), 감만(B) (기본값: "A")
        debug: 디버그 모드 (True일 경우 API 응답 상세 출력)
    
    Returns:
        pandas.DataFrame: 선석배정 현황 및 선박 정보가 포함된 DataFrame
    """
    print("=" * 60)
    print("부산항 선석배정 현황 정보 수집 시작")
    print("=" * 60)
    
    # 1. 선석배정 현황 조회
    print("\n[1단계] 신선대감만터미널 선석배정 현황 조회 중...")
    df = get_berth_status(time=time, route=route, berth=berth)
    
    if df.empty:
        print("경고: 데이터를 가져올 수 없습니다.")
        return df
    
    print(f"✓ {len(df)}건의 선박 정보를 조회했습니다.")
    print(f"\n컬럼: {list(df.columns)}")
    print("\n데이터 미리보기:")
    print(df.head())
    
    # 2. VesselFinder API를 통한 선박 정보 추가
    print("\n" + "=" * 60)
    print("[2단계] VesselFinder API를 통한 선박 크기 정보 조회 중...")
    if debug:
        print("디버그 모드: 첫 번째 선박의 API 응답 상세 출력")
    print("=" * 60)
    
    df_enriched = enrich_with_length_beam(df, ship_name_column="선박명", debug=debug)
    
    # 3. BP(Bitt) 정보 추가
    print("\n" + "=" * 60)
    print("[3단계] BP(Bitt) 정보 조회 중...")
    print("=" * 60)
    
    df_enriched = add_bp_to_dataframe(df_enriched)
    
    print("\n" + "=" * 60)
    print("수집 완료!")
    print("=" * 60)
    
    return df_enriched


def save_to_excel(df: pd.DataFrame, filename="부산항_선석배정현황.xlsx"):
    """
    DataFrame을 Excel 파일로 저장
    
    Args:
        df: 저장할 DataFrame
        filename: 저장할 파일명
    """
    try:
        # engine을 지정하지 않으면 자동 선택 (openpyxl 또는 xlsxwriter)
        df.to_excel(filename, index=False)
        print(f"\n✓ 데이터가 '{filename}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"\n❌ Excel 저장 실패: {e}")
        # CSV로 대체 저장
        csv_filename = filename.replace('.xlsx', '.csv')
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"✓ 데이터가 '{csv_filename}' 파일로 저장되었습니다.")


if __name__ == "__main__":
    import sys
    
    # 명령줄 인자로 디버그 모드 활성화
    debug_mode = "--debug" in sys.argv or "-d" in sys.argv
    
    if debug_mode:
        print("🔍 디버그 모드로 실행합니다.\n")
    
    # 기본 실행: 신선대 터미널 3일치 데이터 조회
    df_result = collect_berth_info(debug=debug_mode)
    
    if not df_result.empty:
        print("\n" + "=" * 60)
        print("최종 결과:")
        print("=" * 60)
        print(df_result)
        
        # 결과 저장
        save_to_excel(df_result)
    else:
        print("\n❌ 데이터를 가져올 수 없습니다.")

