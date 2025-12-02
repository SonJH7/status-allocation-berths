# 크롤러 테스트 스크립트 - 페이로드별 테스트
from crawler import collect_berth_info
import pandas as pd

# pandas 출력 옵션 설정
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

print("=" * 80)
print("크롤러 테스트 - 페이로드별")
print("=" * 80)

# =============================================================================
# 1. v_time (조회기간) 테스트
# =============================================================================
print("\n" + "=" * 80)
print("1. v_time (조회기간) 테스트")
print("=" * 80)

# 1-1. 3days (4일)
print("\n[1-1] v_time=3days (4일)")
df_1_1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_1_1)}건")

# 1-2. week (일주일)
print("\n[1-2] v_time=week (일주일)")
df_1_2 = collect_berth_info(
    time="week",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_1_2)}건")

# 1-3. month (한달)
print("\n[1-3] v_time=month (한달)")
df_1_3 = collect_berth_info(
    time="month",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_1_3)}건")

# 1-4. term (직접입력) - 2025년 11월 28일 하루
print("\n[1-4] v_time=term (직접입력: 2025-11-28)")
df_1_4 = collect_berth_info(
    time="term",
    route="ALL",
    berth="A",
    year1=2025, month1=11, day1=28,
    year2=2025, month2=11, day2=28,
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_1_4)}건")

# 1-5. term (직접입력) - 일주일 범위
print("\n[1-5] v_time=term (직접입력: 2025-11-28 ~ 2025-12-05)")
df_1_5 = collect_berth_info(
    time="term",
    route="ALL",
    berth="A",
    year1=2025, month1=11, day1=28,
    year2=2025, month2=12, day2=5,
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_1_5)}건")

# =============================================================================
# 2. ROCD (항로구분) 테스트
# =============================================================================
print("\n" + "=" * 80)
print("2. ROCD (항로구분) 테스트")
print("=" * 80)

# 2-1. ALL (전체)
print("\n[2-1] ROCD=ALL (전체)")
df_2_1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_2_1)}건")

# 2-2. EA (동남아)
print("\n[2-2] ROCD=EA (동남아)")
df_2_2 = collect_berth_info(
    time="3days",
    route="EA",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_2_2)}건")

# 2-3. JP (일본)
print("\n[2-3] ROCD=JP (일본)")
df_2_3 = collect_berth_info(
    time="3days",
    route="JP",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_2_3)}건")

# 2-4. CN (중국)
print("\n[2-4] ROCD=CN (중국)")
df_2_4 = collect_berth_info(
    time="3days",
    route="CN",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_2_4)}건")

# =============================================================================
# 3. v_gu (선석구분) 테스트
# =============================================================================
print("\n" + "=" * 80)
print("3. v_gu (선석구분) 테스트")
print("=" * 80)

# 3-1. A (전체)
print("\n[3-1] v_gu=A (전체)")
df_3_1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_3_1)}건")

# 3-2. S (신선대)
print("\n[3-2] v_gu=S (신선대)")
df_3_2 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="S",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_3_2)}건")

# 3-3. G (감만)
print("\n[3-3] v_gu=G (감만)")
df_3_3 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="G",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_3_3)}건")

# =============================================================================
# 4. v_oper_cd (선사 검색) 테스트
# =============================================================================
print("\n" + "=" * 80)
print("4. v_oper_cd (선사 검색) 테스트")
print("=" * 80)

# 4-1. 선사 검색 없음 (전체)
print("\n[4-1] v_oper_cd='' (전체)")
df_4_1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    company="",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_4_1)}건")

# 4-2. HMM 검색
print("\n[4-2] v_oper_cd='HMM'")
df_4_2 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    company="HMM",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_4_2)}건")
if not df_4_2.empty:
    print(df_4_2.head(3))

# 4-3. ONE 검색
print("\n[4-3] v_oper_cd='ONE'")
df_4_3 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    company="ONE",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_4_3)}건")
if not df_4_3.empty:
    print(df_4_3.head(3))

# 4-4. HAS 검색 (전체 결과 출력)
print("\n[4-4] v_oper_cd='HAS' (전체 결과)")
df_4_4 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    company="HAS",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_4_4)}건")
if not df_4_4.empty:
    print("\n=== 전체 데이터 ===")
    print(df_4_4)
else:
    print("⚠️ 검색 결과 없음")

# =============================================================================
# 5. ORDER (정렬기준) 테스트
# =============================================================================
print("\n" + "=" * 80)
print("5. ORDER (정렬기준) 테스트")
print("=" * 80)

# 5-1. item1 (출항일시)
print("\n[5-1] ORDER=item1 (출항일시)")
df_5_1 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    order="item1",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_5_1)}건")

# 5-2. item2 (입항예정일시)
print("\n[5-2] ORDER=item2 (입항예정일시)")
df_5_2 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    order="item2",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_5_2)}건")

# 5-3. item3 (선석)
print("\n[5-3] ORDER=item3 (선석)")
df_5_3 = collect_berth_info(
    time="3days",
    route="ALL",
    berth="A",
    order="item3",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_5_3)}건")

# =============================================================================
# 6. 복합 조건 테스트
# =============================================================================
print("\n" + "=" * 80)
print("6. 복합 조건 테스트")
print("=" * 80)

# 6-1. 일주일 + 동남아 + 신선대 + 출항일시 정렬
print("\n[6-1] week + EA + S + item1")
df_6_1 = collect_berth_info(
    time="week",
    route="EA",
    berth="S",
    order="item1",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_6_1)}건")

# 6-2. 직접입력 + 일본 + 감만 + HMM 검색
print("\n[6-2] term(2025-11-28~12-05) + JP + G + HMM")
df_6_2 = collect_berth_info(
    time="term",
    route="JP",
    berth="G",
    company="HMM",
    order="item1",
    year1=2025, month1=11, day1=28,
    year2=2025, month2=12, day2=5,
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_6_2)}건")
if not df_6_2.empty:
    print(df_6_2)

# 6-3. 한달 + 중국 + 전체선석 + 입항예정일시 정렬
print("\n[6-3] month + CN + A + item2")
df_6_3 = collect_berth_info(
    time="month",
    route="CN",
    berth="A",
    order="item2",
    add_bp=False,
    add_dims=False
)
print(f"✅ 결과: {len(df_6_3)}건")

print("\n" + "=" * 80)
print("테스트 완료!")
print("=" * 80)
