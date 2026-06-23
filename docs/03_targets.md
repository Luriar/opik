# 3. Target 정의

## 목적

본 프로젝트는 가격 자체를 예측하는 것이 아니라,

**T일의 상대적인 강도(Ranking), 시가 수익률(Gap), 장중 수익률(Intraday Return)을 예측하는 것을 목표로 한다.**

모든 Target은 수익률(Ratio) 형태로 정의하며, 절대 가격은 Target으로 사용하지 않는다.

---

# Target 구성

본 프로젝트는 다음 3개의 Target을 사용한다.

| Model   | Target             | 목적            |
| ------- | ------------------ | ------------- |
| Model 1 | target_rank_return | 강한 종목 Ranking |
| Model 2 | target_gap         | T일 시가 수익률 예측  |
| Model 3 | target_intraday    | T일 장중 수익률 예측  |

---

# Model 1 Target

## target_rank_return

### 목적

350개 종목 중 T일에 상대적으로 가장 강한 종목을 찾는다.

Ranking Model의 학습 Target으로 사용한다.

---

## 공식

```python
target_rank_return

=

close(T)

/

close(T-1)

-

1
```

---

## 의미

전일 종가 대비 당일 종가 수익률이다.

예를 들어

| Close(T-1) | Close(T) | Target |
| ---------- | -------- | ------ |
| 100        | 105      | +5%    |
| 100        | 98       | -2%    |

---

## Ranking 생성

매일 모든 Universe 종목에 대해

```text
target_rank_return
```

을 계산하고

이를 기준으로

```text
1위
2위
...
350위
```

순위를 생성한다.

모델은 이 순위를 직접 예측하는 것이 아니라,

상대적인 강도를 나타내는 Score를 학습한다.

---

# Model 2 Target

## target_gap

### 목적

T일 시가(Open)가

전일 종가(Close(T-1)) 대비 얼마나 변할지를 예측한다.

---

## 공식

```python
target_gap

=

open(T)

/

close(T-1)

-

1
```

---

## 의미

야간 뉴스,

미국시장,

환율,

글로벌 Macro 환경이 반영된 Overnight Return이다.

---

## 예

| Close(T-1) | Open(T) | target_gap |
| ---------- | ------- | ---------- |
| 100        | 102     | +2%        |
| 100        | 98      | -2%        |

---

## Predicted Open

모델 출력

```text
pred_gap
```

을 이용하여

```python
pred_open

=

close(T-1)

×

(1+pred_gap)
```

을 계산한다.

---

# Model 3 Target

## target_intraday

### 목적

T일 시가에서 종가까지의 장중 수익률을 예측한다.

---

## 공식

```python
target_intraday

=

close(T)

/

open(T)

-

1
```

---

## 의미

장 시작 이후

종가까지의 움직임을 나타낸다.

---

## 예

| Open(T) | Close(T) | target_intraday |
| ------- | -------- | --------------- |
| 100     | 103      | +3%             |
| 100     | 97       | -3%             |

---

## Predicted Close

모델 출력

```text
pred_intraday
```

을 이용하여

```python
pred_close

=

pred_open

×

(1+pred_intraday)
```

을 계산한다.

---

# 최종 예측 가격

최종 예측 가격은

Model2와 Model3를 결합하여 계산한다.

```text
Close(T-1)

↓

Pred Gap

↓

Pred Open

↓

Pred Intraday

↓

Pred Close
```

---

## 공식

```python
pred_open

=

close(T-1)

×

(1+pred_gap)
```

```python
pred_close

=

pred_open

×

(1+pred_intraday)
```

---

# Target 생성 시점

Target은 반드시

T일 데이터로 생성한다.

Feature는 반드시

T-1일까지의 데이터만 사용한다.

```text
Feature

↓

T-1까지

-----------------------

Target

↓

T
```

---

# 데이터 누수 방지 규칙

다음 값은 Target 생성에는 사용할 수 있으나,

Feature 생성에는 절대로 사용할 수 없다.

```text
Open(T)

High(T)

Low(T)

Close(T)

Volume(T)
```

---

모든 Feature는

```text
T-1일까지
```

생성되어야 한다.

---

# Target Scaling

Target은 추가적인 Standard Scaling 또는 MinMax Scaling을 수행하지 않는다.

Raw Return 형태를 그대로 사용한다.

```python
target_gap

=

0.015

(+1.5%)
```

```python
target_intraday

=

-0.008

(-0.8%)
```

---

# Target 단위

모든 Target은 Ratio(수익률) 형태를 사용한다.

```text
+0.015

=

+1.5%

---------------------

-0.008

=

-0.8%
```

절대 가격은 Target으로 사용하지 않는다.

---

# 프로젝트 기본 원칙

본 프로젝트는

```text
가격 예측 시스템

(X)
```

이 아니라

```text
Ranking + Return Prediction System

(O)
```

이다.

---

## 최종 구조

```text
Model1

target_rank_return

↓

Ranking Score

↓

Top20 후보

-----------------------------

Model2

target_gap

↓

Pred Open

-----------------------------

Model3

target_intraday

↓

Pred Close

-----------------------------

Portfolio Optimizer

↓

최종 Top10 종목 선정
```

---

# 구현 원칙

```text
1. 모든 Target은 Return(Ratio) 형태를 사용한다.

2. 모든 Feature는 T-1일까지 생성한다.

3. Target 생성 시 T일 데이터를 사용한다.

4. Target은 절대 Feature 생성 과정에 사용하지 않는다.

5. Ranking Model이 프로젝트의 핵심 모델이다.

6. Gap Model과 Intraday Model은 Ranking Model을 보조하는 역할을 수행한다.
```
