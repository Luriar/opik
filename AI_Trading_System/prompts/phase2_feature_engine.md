# prompts/phase2_feature_engine.md

```text
# ==========================================================
# AI Trading System v1.0
#
# Phase 2 : Feature Engine
# ==========================================================

Implement Phase 2 ONLY.

Before coding, read:

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md
CODING_STANDARDS.md
PROJECT_TASKS.md
PROJECT_STATUS.md

Reference:

docs/05_feature_library.md
docs/06_data_leakage_rules.md

configs/feature.yaml

tests/test_feature_generation.py
tests/test_data_leakage.py

------------------------------------------------------------

Implement

src/features/

    price_features.py
    momentum_features.py
    volume_features.py
    volatility_features.py
    candlestick_features.py
    breakout_features.py
    technical_features.py
    cross_sectional_features.py
    macro_features.py
    identity_features.py
    feature_builder.py

------------------------------------------------------------

Feature Groups

Implement features defined in configs/feature.yaml.

Price:

- return_1d
- return_3d
- return_5d
- return_20d
- return_60d
- close_ma5_ratio
- close_ma20_ratio
- close_ma60_ratio
- close_position

Momentum:

- momentum_5d
- momentum_20d
- momentum_diff
- momentum_accel
- relative_return_5d_vs_market
- relative_return_20d_vs_market
- relative_return_20d_vs_sector
- momentum_rank_pct
- return_rank_pct

Volume:

- volume_change_1d
- relative_trading_value
- trading_value_rank_pct

Volatility:

- volatility_5d
- volatility_20d
- intraday_range_5d
- atr_percent
- volatility_rank_pct

Candlestick:

- body
- upper_shadow
- lower_shadow
- body_ratio
- close_position

Breakout:

- high_20d
- low_20d
- close_to_20d_high
- close_to_20d_low
- breakout_strength
- breakout_rank_pct

Technical:

- rsi14
- rsi_change_5d
- rsi_rank_pct
- macd_hist_ratio
- macd_rank_pct
- bb_position
- bb_width
- bb_position_change_5d
- bb_position_rank_pct
- atr_rank_pct

Cross-sectional:

- return_5d_rank_pct
- return_20d_rank_pct
- momentum_rank_pct
- momentum_20d_rank_pct
- momentum_diff_rank_pct
- trading_value_rank_pct
- volume_change_rank_pct
- volatility_rank_pct
- atr_rank_pct
- bb_width_rank_pct
- breakout_rank_pct
- low_rebound_rank_pct
- relative_return_5d_rank_pct
- sector_relative_rank_pct

Macro:

- nasdaq_return_1d
- sox_return_1d
- sp500_return_1d
- vix_change_1d
- usdkrw_return_1d
- wti_return_1d

Identity:

- sector
- market_type
- market_cap_group

------------------------------------------------------------

Data Leakage Rules

Every feature must satisfy:

Feature Date < Target Date

Use T-1 data only.

Always apply:

shift(1) before rolling()

Good:

close.shift(1).rolling(20).mean()

Bad:

close.rolling(20).mean()

Do not use:

Open(T)
High(T)
Low(T)
Close(T)
Volume(T)
target_rank_return
target_gap
target_intraday
actual_return

Cross-sectional features must use:

df.groupby("date")[feature].rank(pct=True)

Never rank over the full dataset.

------------------------------------------------------------

Feature Builder Requirements

Implement feature_builder.py to:

- Load feature config
- Apply enabled feature groups
- Validate required columns
- Generate feature metadata
- Exclude forbidden columns
- Preserve date and ticker
- Return final feature DataFrame
- Save feature list if configured

------------------------------------------------------------

Rules

- Follow CODING_STANDARDS.md
- Type hints
- Docstrings
- Logging
- Config driven
- No target generation inside feature functions
- No model training
- No portfolio/backtest/execution logic
- Do not implement future phases

------------------------------------------------------------

Run

pytest \
tests/test_config.py \
tests/test_project_structure.py \
tests/test_project_health.py \
tests/test_data_loader.py \
tests/test_universe.py \
tests/test_feature_generation.py \
tests/test_data_leakage.py

Fix every failure.

------------------------------------------------------------

Update PROJECT_STATUS.md

Mark Phase 2 as complete only if:

- Feature generation tests pass
- Data leakage tests pass
- Phase 0 and Phase 1 tests still pass
- Feature metadata is generated
- No target columns are included as features

Do NOT implement future phases.
```
