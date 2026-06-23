# 2. Universe 정의

## 목적

본 프로젝트의 Universe는 AI 모델이 매일 예측하고, Ranking하고, 최종 포트폴리오 후보로 사용할 종목 집합을 의미한다.

이 시스템의 기본 Universe는 다음 두 지수의 구성 종목으로 정의한다.

```text
KOSPI200 + KOSDAQ150
```

즉, 약 350개 종목을 대상으로 한다.

---

## 기본 Universe

```text
Universe v1.0 = KOSPI200 구성 종목 + KOSDAQ150 구성 종목
```

### 구성

| 구분        | 설명                   |  종목 수 |
| --------- | -------------------- | ----: |
| KOSPI200  | KOSPI 대표 대형주 지수      |   200 |
| KOSDAQ150 | KOSDAQ 대표 대형주/성장주 지수 |   150 |
| 합계        | 전체 투자 대상             | 약 350 |

---

## Universe 선정 이유

### 1. 충분한 유동성

KOSPI200과 KOSDAQ150은 각 시장에서 상대적으로 거래가 활발한 종목으로 구성된다.

이는 실제 운용에서 중요하다.

```text
유동성이 낮은 종목
  ↓
체결 어려움
  ↓
슬리피지 증가
  ↓
백테스트와 실전 성과 괴리 발생
```

따라서 초기 버전에서는 유동성이 높은 대표 종목군으로 제한한다.

---

### 2. 데이터 품질

KOSPI200 + KOSDAQ150 종목은 가격, 거래량, 거래대금, 업종, 시가총액 등 데이터 품질이 상대적으로 좋다.

데이터 누락이나 비정상 거래가 많은 소형주는 v1.0에서는 제외한다.

---

### 3. 모델 학습에 적절한 규모

약 350개 종목을 대상으로 10년치 일봉 데이터를 사용하면 다음 정도의 데이터가 생성된다.

```text
350개 종목 × 약 2,500거래일 = 약 875,000 rows
```

이는 LightGBM, XGBoost 기반 모델을 학습하기에 적절한 규모이다.

---

### 4. 실전 운용 가능성

이 Universe는 실제 매매 관점에서 다음 장점이 있다.

```text
- 거래대금이 충분한 종목이 많다.
- 슬리피지 추정이 비교적 안정적이다.
- 상장폐지 위험이 상대적으로 낮다.
- 기관/외국인 수급 영향이 반영되기 쉽다.
- 미국시장, 환율, 업종지수와의 연동성이 높다.
```

---

## Universe 구성 시점

Universe는 반드시 **예측 시점 기준으로 알 수 있는 정보만 사용**해야 한다.

즉, T일을 예측할 때 T일 이후에 편입된 종목을 과거에 포함해서는 안 된다.

---

## 권장 방식

v1.0에서는 구현 단순성을 위해 다음 방식을 사용한다.

```text
현재 기준 KOSPI200 + KOSDAQ150 구성 종목을 기본 Universe로 사용한다.
```

단, 백테스트 리포트에는 다음 한계를 명시한다.

```text
현재 구성 종목 기준 Universe를 과거 전체 기간에 적용하면
생존편향 및 지수편입편향이 발생할 수 있다.
```

---

## 향후 개선 방식

v2.0에서는 날짜별 지수 구성 종목 이력을 사용한다.

```text
각 날짜 T-1 기준
실제로 KOSPI200 또는 KOSDAQ150에 편입되어 있던 종목만 Universe에 포함
```

이 방식은 더 정확하지만, 지수 구성 이력 데이터 확보가 필요하다.

---

## Universe 필터

기본 Universe에 포함되더라도, 실제 매매 대상에서는 다음 필터를 적용한다.

### 1. 거래정지 종목 제외

```text
거래정지
관리종목
상장폐지 예정
정리매매
```

해당 종목은 제외한다.

---

### 2. 유동성 필터

최근 20거래일 평균 거래대금 기준 필터를 적용한다.

```text
trading_value_ma20 >= 최소 거래대금 기준
```

v1.0 기본값:

```text
최근 20일 평균 거래대금 50억원 이상
```

---

### 3. 가격 필터

극단적으로 낮은 가격의 종목은 제외한다.

```text
close(T-1) >= 1,000원
```

이유:

```text
- 호가 단위 영향 증가
- 급등락/작전성 움직임 가능성 증가
- 슬리피지 확대 가능성
```

---

### 4. 결측치 필터

모델 입력에 필요한 핵심 Feature가 부족한 종목은 제외한다.

예:

```text
- 최근 60거래일 이상 가격 데이터 없음
- 거래량 데이터 결측
- 업종 정보 결측
- 시가총액 정보 결측
```

---

### 5. 신규 상장 종목 필터

상장 후 데이터가 충분하지 않은 종목은 제외한다.

v1.0 기준:

```text
상장 후 최소 120거래일 이상 데이터가 존재해야 한다.
```

---

## 최종 Daily Universe

매일 T-1일 장 종료 후 다음 순서로 Daily Universe를 생성한다.

```text
1. KOSPI200 + KOSDAQ150 구성 종목 로드
2. 거래정지/관리종목 제외
3. 최근 20일 평균 거래대금 필터 적용
4. 가격 필터 적용
5. 최소 데이터 길이 필터 적용
6. 핵심 Feature 결측치 필터 적용
7. 최종 Daily Universe 확정
```

---

## Daily Universe 예시

```text
Date: 2026-06-11

Base Universe:
KOSPI200 + KOSDAQ150 = 350개

필터 적용:
- 거래정지 제외
- 거래대금 부족 제외
- 가격 1,000원 미만 제외
- 데이터 부족 제외

Final Daily Universe:
약 320~350개 종목
```

---

## Universe Metadata

각 종목은 다음 메타데이터를 가져야 한다.

```text
ticker
name
market_type
sector
industry
market_cap
market_cap_rank
index_membership
```

예:

```text
ticker: 005930
name: 삼성전자
market_type: KOSPI
sector: 반도체
industry: 메모리반도체
market_cap_rank: 1
index_membership: KOSPI200
```

---

## Identity Feature로 사용할 항목

Universe Metadata 중 일부는 모델 Feature로 사용한다.

```text
sector
market_type
market_cap_group
```

v1.0에서는 `stock_code` 자체는 Feature로 사용하지 않는다.

이유:

```text
stock_code를 categorical feature로 넣으면
모델이 특정 종목의 과거 패턴을 외울 가능성이 있다.
```

따라서 v1.0에서는 일반화 성능을 높이기 위해 다음 Feature만 사용한다.

```text
sector
market_type
market_cap_group
```

---

## market_cap_group 정의

시가총액 순위를 기준으로 그룹을 만든다.

```text
Top20
Top50
Top100
Top200
Others
```

예:

```text
market_cap_rank <= 20      → Top20
market_cap_rank <= 50      → Top50
market_cap_rank <= 100     → Top100
market_cap_rank <= 200     → Top200
그 외                         → Others
```

---

## Universe 관련 데이터 누수 방지 규칙

```text
1. T일 이후에 편입된 종목을 T일 이전 Universe에 포함하지 않는다.
2. 상장폐지된 종목을 과거 백테스트에서 임의로 제거하면 안 된다.
3. 현재 구성 종목만 과거 전체에 적용할 경우 생존편향이 있음을 리포트에 명시한다.
4. 거래대금 필터는 반드시 T-1일까지의 데이터로 계산한다.
5. 가격 필터도 반드시 T-1일 종가 기준으로 적용한다.
6. Feature 결측 여부도 T-1 기준으로 판단한다.
```

---

## v1.0 구현 기준

v1.0에서는 다음 기준으로 구현한다.

```text
Base Universe:
KOSPI200 + KOSDAQ150 현재 구성 종목

Daily Filter:
- 거래정지 제외
- 최근 20일 평균 거래대금 50억원 이상
- T-1 종가 1,000원 이상
- 최소 120거래일 이상 데이터 보유
- 핵심 Feature 결측치 없음

Identity Feature:
- sector
- market_type
- market_cap_group

주의:
현재 구성 종목 기준 백테스트이므로 생존편향 가능성을 리포트에 명시한다.
```

---

## 향후 v2.0 개선 과제

```text
1. 날짜별 KOSPI200 구성 종목 이력 반영
2. 날짜별 KOSDAQ150 구성 종목 이력 반영
3. 상장폐지 종목 데이터 포함
4. 과거 관리종목/거래정지 이력 반영
5. 업종 분류 변경 이력 반영
6. 시가총액 순위의 날짜별 재계산
```
