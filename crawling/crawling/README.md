# 부산항 선석배정 현황 정보 수집 프로그램

신선대감만터미널 선석배정 현황을 조회하고, 각 선박의 Length와 Beam 정보를 자동으로 추가하는 프로그램입니다.

## 📁 파일 구조

- **bpt.py**: 신선대감만터미널 선석배정 현황 조회
- **vsfinder.py**: VesselFinder에서 선박 크기 정보 검색 및 추출
- **main.py**: 전체 프로세스를 통합 실행
- **test_simple.py**: 간단한 테스트 (1척)

## 🚀 실행 방법

### 전체 실행
```bash
python main.py
```
또는 `실행_부산항정보수집.bat` 더블 클릭

### 디버그 모드
```bash
python main.py --debug
```
또는 `실행_디버그모드.bat` 더블 클릭

### 테스트
```bash
python test_simple.py
```
또는 `테스트.bat` 더블 클릭

## 🔄 작동 흐름

1. **bpt.py**: 신선대감만터미널 선석배정 현황 조회
2. **vsfinder.py**: 
   - VesselFinder 검색 페이지 접근
   - 검색 결과 테이블에서 `<td class="v6">`의 Length/Beam 정보 추출
   - "133 / 19" 형식 파싱
3. **main.py**: 결과를 Excel 파일로 저장

## 📊 출력 파일

- **부산항_선석배정현황.xlsx**: 선석배정 현황 + Length & Beam 정보

## 📦 필요한 패키지

```bash
pip install -r requirements.txt
```

## ⚙️ 커스터마이징

### 선석배정 조회 기간 변경
`main.py` 수정:
```python
df_result = collect_berth_info(time="7days")  # 7일치 데이터
```

### 터미널 구분
- 신선대: `berth="A"`
- 감만: `berth="B"`

## 🐛 문제 해결

### 일부 선박 정보가 누락되는 경우
VesselFinder에서 검색되지 않는 선박입니다. 선박명이 정확한지 확인하세요.

### API 응답 지연
최소 0.4초 간격으로 요청하도록 구현되어 있습니다.
