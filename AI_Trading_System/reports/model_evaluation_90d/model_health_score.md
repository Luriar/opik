# Model Health Score

| Dimension | Grade | Evidence |
|---|---|---|
| Prediction Quality | C | Daily mean Ranking IC 0.0042; Gap direction accuracy 0.5433; Intraday direction accuracy 0.4885 |
| Ranking Stability | C | Mean daily IC 0.0042; positive IC days 56.67% |
| Portfolio Stability | B | Sharpe 2.20; MDD -24.93%; win rate 55.56% |
| Feature Stability | B | Top20 feature list generated from average importances across evaluation folds |
| Overall Production Readiness | C | Strong portfolio window, but ranking stability remains modest |

## Interpretation

- `A+`: exceptional and stable across prediction, ranking, portfolio, and feature dimensions.
- `A`: production-strong with manageable monitoring requirements.
- `B`: usable with monitoring; meaningful strengths but some instability.
- `C`: caution; evidence is mixed or weak in key model dimensions.
- `D`: not production-ready without remediation.

## Final Assessment

Overall Production Readiness: **C**