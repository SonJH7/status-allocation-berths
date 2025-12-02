# 크롤러 테스트 스크립트
from crawler import collect_berth_info
from datetime import datetime

print("=" * 60)
print("크롤러 테스트")
print("=" * 60)

# 테스트 1: 기본 (4일, 전체)
print("\n[테스트 1] 기본 - 4일, 전체")
df1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df1)}건")
if not df1.empty:
    print(df1.head(3))

# 테스트 2: 일주일
print("\n[테스트 2] 일주일")
df2 = collect_berth_info(
    time="week",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df2)}건")

# 테스트 3: 한달
print("\n[테스트 3] 한달")
df3 = collect_berth_info(
    time="month",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df3)}건")

# 테스트 4: 직접입력 (오늘 하루)
print("\n[테스트 4] 직접입력 - 2025년 11월 28일")
df4 = collect_berth_info(
    time="term",
    route="ALL",
    berth="A",
    year1=2025, month1=11, day1=28,
    year2=2025, month2=11, day2=28,
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df4)}건")

# 테스트 5: 동남아 항로만
print("\n[테스트 5] 동남아 항로")
df5 = collect_berth_info(
    time="3days",
    route="EA",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df5)}건")

# 테스트 6: 신선대만
print("\n[테스트 6] 신선대만")
df6 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="S",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df6)}건")

# 테스트 7: 선사 검색 (예: HAS)
print("\n[테스트 7] 선사 검색 - HAS")
df7 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    company="HAS",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df7)}건")
if not df7.empty:
    print("\n=== 전체 결과 ===")
    # pandas 출력 옵션 설정 (모든 행과 열 표시)
    import pandas as pd
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df7)
else:
    print("⚠️ 검색 결과가 없습니다.")

print("\n" + "=" * 60)
print("테스트 완료!")
print("=" * 60)

