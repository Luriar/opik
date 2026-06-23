# prompts/phase1_data_layer.md

```text
# ==========================================================
# AI Trading System v1.0
#
# Phase 1 : Data Layer
# ==========================================================

Implement Phase 1 ONLY.

Before coding, read:

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md
CODING_STANDARDS.md
PROJECT_TASKS.md
PROJECT_STATUS.md

Reference:

docs/01_system_architecture.md
docs/02_universe.md
docs/06_data_leakage_rules.md

configs/feature.yaml
configs/validation.yaml

tests/test_data_loader.py
tests/test_universe.py

------------------------------------------------------------

Implement

src/data/

    data_loader.py
    macro_loader.py
    universe.py
    validator.py
    calendar.py

------------------------------------------------------------

Data Loader Requirements

Implement reusable loaders for:

- Korean OHLCV data
- Macro data
- US market data
- FX data
- Commodity data
- Metadata / identity data

Expected OHLCV columns:

date
ticker
open
high
low
close
volume

Expected Macro columns:

date
nasdaq_close
sox_close
sp500_close
vix_close
usdkrw
wti_close

All date columns must be datetime.

All ticker columns must be string.

Data must be sorted by:

ticker
date

Duplicate date + ticker rows must be rejected.

------------------------------------------------------------

Validator Requirements

Implement checks for:

- Required columns
- Duplicate date/ticker
- Positive price values
- Non-negative volume
- high >= low
- high >= open / close
- low <= open / close
- Macro values are positive
- Date column is datetime
- Ticker column is string

Raise clear ValueError on validation failure.

------------------------------------------------------------

Universe Requirements

Implement Daily Universe generation.

Base universe:

KOSPI200 + KOSDAQ150

v1.0 may use current constituents, but must preserve the warning about survivorship bias.

Filters:

- Common stock only
- Remove ETF
- Remove ETN
- Remove SPAC
- Remove preferred stock
- Remove REIT
- Remove trading halt
- Remove management issue
- Liquidity filter using trading_value_ma20
- Universe size limit if configured

Required universe columns:

date
ticker
market
security_type
trading_value_ma20

------------------------------------------------------------

Calendar Requirements

Implement basic trading calendar utilities:

- sort trading dates
- get previous trading date
- get next trading date
- validate date coverage

------------------------------------------------------------

Rules

- Follow CODING_STANDARDS.md
- Type hints
- Docstrings
- Logging
- Config driven
- Do not implement Feature Engine
- Do not implement Models
- Do not implement Portfolio, Backtest, or Execution
- Do not use future data
- Do not silently ignore invalid rows unless explicitly configured

------------------------------------------------------------

Run

pytest \
tests/test_config.py \
tests/test_project_structure.py \
tests/test_project_health.py \
tests/test_data_loader.py \
tests/test_universe.py

Fix every failure.

------------------------------------------------------------

Update PROJECT_STATUS.md

Mark Phase 1 as complete only if:

- Data loader tests pass
- Universe tests pass
- Phase 0 tests still pass
- Data validation is implemented
- Daily universe generation is implemented

Do NOT implement future phases.
```
