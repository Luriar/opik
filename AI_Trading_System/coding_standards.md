# CODING_STANDARDS.md

# AI Trading System v1.0 Coding Standards

---

# 1. Purpose

본 문서는 AI Trading System v1.0의 코드 작성 표준을 정의한다.

목표는 다음이다.

```text
Readable
Maintainable
Testable
Reproducible
Leakage-Free
Production-Ready
```

---

# 2. Core Coding Philosophy

항상 다음 우선순위를 따른다.

```text
Correctness
>
Readability
>
Testability
>
Performance
```

성능 최적화는 테스트와 정확성이 확보된 이후에만 수행한다.

---

# 3. Python Version

기본 Python 버전:

```text
Python 3.12+
```

모든 코드는 Python 3.12 이상에서 동작해야 한다.

---

# 4. Code Style

기본 스타일:

```text
PEP8
Type Hints
Docstrings
Small Functions
Config Driven
```

권장 도구:

```text
ruff
black
mypy
pytest
```

---

# 5. Naming Convention

## Files

```text
snake_case.py
```

Good:

```text
feature_generator.py
walk_forward.py
portfolio_engine.py
```

Bad:

```text
FeatureGenerator.py
walkForward.py
portfolioEngine.py
```

---

## Functions

```text
snake_case
```

Good:

```python
generate_features()
calculate_expected_return()
run_backtest()
```

---

## Classes

```text
PascalCase
```

Good:

```python
FeatureGenerator
WalkForwardRunner
PortfolioEngine
```

---

## Constants

```text
UPPER_SNAKE_CASE
```

Good:

```python
TARGET_COLUMNS
DEFAULT_RANDOM_SEED
FORBIDDEN_FEATURE_PATTERNS
```

---

## Variables

```text
snake_case
```

Good:

```python
feature_df
target_df
prediction_df
```

---

# 6. Function Design

함수는 하나의 책임만 가진다.

Good:

```python
def calculate_expected_return(
    pred_gap: float,
    pred_intraday: float,
) -> float:
    return (1 + pred_gap) * (1 + pred_intraday) - 1
```

Bad:

```python
def run_everything():
    ...
```

---

# 7. Function Length

권장:

```text
1 function <= 50 lines
```

50줄을 초과하면 다음을 검토한다.

```text
Can this be split?
Can this be tested separately?
Does this function have multiple responsibilities?
```

---

# 8. Type Hints

모든 public function은 type hint를 가져야 한다.

Good:

```python
def build_features(
    price_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    ...
```

Bad:

```python
def build_features(price_df, config):
    ...
```

---

# 9. Docstring

모든 public function과 class에는 docstring을 작성한다.

형식:

```python
def calculate_trade_return(
    open_price: float,
    close_price: float,
    buy_cost: float,
    sell_cost: float,
) -> float:
    """
    Calculate net trade return after transaction costs.

    Parameters
    ----------
    open_price:
        Buy price.
    close_price:
        Sell price.
    buy_cost:
        Buy transaction cost ratio.
    sell_cost:
        Sell transaction cost ratio.

    Returns
    -------
    float
        Net return.
    """
```

---

# 10. Configuration Rule

하드코딩 금지.

Bad:

```python
portfolio_size = 10
buy_cost = 0.0015
```

Good:

```python
portfolio_size = config["portfolio"]["portfolio_size"]
buy_cost = config["transaction_cost"]["buy_cost"]
```

모든 설정값은 `configs/*.yaml`에서 읽는다.

---

# 11. DataFrame Rules

## Required Columns Check

모든 주요 함수는 입력 DataFrame의 필수 컬럼을 검증한다.

```python
def validate_required_columns(
    df: pd.DataFrame,
    required_columns: set[str],
) -> None:
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
```

---

## Avoid In-place Mutation

가능하면 원본 DataFrame을 직접 변경하지 않는다.

Good:

```python
df = df.copy()
df["return_5d"] = ...
return df
```

Bad:

```python
input_df["return_5d"] = ...
return input_df
```

---

## Sorting Rule

시계열 계산 전 반드시 정렬한다.

```python
df = df.sort_values(["ticker", "date"])
```

---

# 12. Data Leakage Coding Rule

Feature 생성 시 다음 원칙을 따른다.

```text
shift(1) before rolling
T-1 Feature only
No T-day OHLCV
No Target columns
```

Good:

```python
ma20 = close.shift(1).rolling(20).mean()
```

Bad:

```python
ma20 = close.rolling(20).mean()
```

---

# 13. Feature Engineering Rules

모든 Feature 함수는 다음 패턴을 따른다.

```python
def add_price_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["ticker", "date"])

    g = df.groupby("ticker")

    df["return_5d"] = (
        g["close"].shift(1) / g["close"].shift(6) - 1
    )

    return df
```

---

# 14. Target Generation Rule

Target 생성은 Feature 생성과 분리한다.

Good:

```text
src/targets/build_targets.py
```

Bad:

```text
Feature 생성 함수 안에서 target 생성
```

---

# 15. Model Coding Rule

모델 학습 함수는 다음을 분리한다.

```text
Dataset Build
Model Init
Training
Evaluation
Saving
```

한 함수에서 모두 처리하지 않는다.

---

# 16. Walk-forward Rule

Random split 금지.

Bad:

```python
train_test_split(df, shuffle=True)
```

Good:

```python
train_df = df[df["date"] <= train_end]
valid_df = df[(df["date"] >= valid_start) & (df["date"] <= valid_end)]
test_df = df[(df["date"] >= test_start) & (df["date"] <= test_end)]
```

---

# 17. Backtest Coding Rule

Portfolio selection에는 prediction column만 사용한다.

허용:

```text
ranking_score
pred_gap
pred_intraday
expected_return
```

금지:

```text
target_gap
target_intraday
actual_return
close_T
```

---

# 18. Logging Standard

모든 pipeline step은 로그를 남긴다.

필수 필드:

```text
timestamp
run_id
step
status
message
```

권장 형식:

```python
logger.info(
    "feature_generation_completed",
    extra={
        "run_id": run_id,
        "step": "feature_generation",
        "status": "success",
        "rows": len(feature_df),
    },
)
```

---

# 19. Exception Handling

예외를 조용히 무시하지 않는다.

Bad:

```python
try:
    ...
except Exception:
    pass
```

Good:

```python
try:
    ...
except Exception as exc:
    logger.exception("feature_generation_failed")
    raise
```

---

# 20. Test Rule

모든 주요 모듈은 대응되는 테스트 파일을 가져야 한다.

```text
src/features/

↓

tests/test_feature_generation.py

--------------------

src/backtest/

↓

tests/test_backtest.py
```

---

# 21. Test Naming

테스트 함수 이름은 명확해야 한다.

Good:

```python
def test_rolling_feature_uses_lagged_data():
    ...
```

Bad:

```python
def test_feature():
    ...
```

---

# 22. Test Coverage Priority

가장 중요한 테스트 순서:

```text
Data Leakage
Feature Generation
Target Generation
Walk-forward
Backtest
Portfolio
Execution
```

---

# 23. Reproducibility

모든 학습은 random seed를 고정한다.

```python
random_state = config["model"]["common"]["random_seed"]
```

결과 저장 시 metadata를 함께 저장한다.

```text
model_version
feature_version
config_version
train_period
validation_period
test_period
random_seed
```

---

# 24. Output Rule

모든 output은 명확한 경로에 저장한다.

```text
outputs/
  models/
  predictions/
  backtests/
  portfolio/
  execution/
  reports/
  logs/
```

---

# 25. File Format

권장 저장 형식:

```text
parquet  for dataset
json     for metadata
csv      for manual review
txt      for LightGBM model
log      for logs
```

---

# 26. Dependency Rule

불필요한 dependency를 추가하지 않는다.

기본 dependency:

```text
pandas
numpy
lightgbm
scikit-learn
pyyaml
pytest
pyarrow
```

추가 dependency는 명확한 이유가 있어야 한다.

---

# 27. Performance Rule

초기 구현에서는 성능 최적화보다 정확성을 우선한다.

최적화는 다음 이후에 수행한다.

```text
Tests pass
No leakage
Backtest reproducible
```

---

# 28. Pandas Performance Guideline

가능하면 vectorized operation을 사용한다.

Good:

```python
df["return_5d"] = df.groupby("ticker")["close"].shift(1) / ...
```

Bad:

```python
for row in df.itertuples():
    ...
```

단, 명확성이 더 중요할 경우 작은 loop는 허용한다.

---

# 29. LightGBM Rule

LightGBM 학습 시 categorical feature는 명시한다.

```python
model.fit(
    X_train,
    y_train,
    categorical_feature=categorical_features,
)
```

`date`, `ticker`, `target_*` 컬럼은 학습 Feature에서 제외한다.

---

# 30. Security Rule

API Key, 계정 정보, 비밀번호는 코드에 저장하지 않는다.

사용:

```text
.env
environment variables
```

커밋 금지:

```text
.env
API keys
broker credentials
account number
```

---

# 31. Review Checklist

코드 작성 후 다음을 확인한다.

```text
[ ] Type hints exist
[ ] Docstrings exist
[ ] No hardcoding
[ ] Config driven
[ ] No data leakage
[ ] Tests added
[ ] Tests pass
[ ] Logging exists
[ ] Exceptions are not swallowed
[ ] Outputs saved to correct path
```

---

# 32. Final Rule

Whenever there is a conflict, choose:

```text
Explicit
>
Implicit

Simple
>
Clever

Tested
>
Untested

Leakage-Free
>
High Backtest Return
```
