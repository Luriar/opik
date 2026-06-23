# 150-Day Walk-Forward Evaluation Summary

## Evaluation Window

- 90 Trading Days: 2026-01-30 to 2026-06-17
- Prediction rows: 31319
- Top10 rows: 900

## Rolling Train

- Rolling Train: 150 Trading Days
- Production code/config unchanged. This is an evaluation-only window override.

## Comparison: 150D vs 250D

| Metric               |        250D |        150D |   Difference | Improved(Y/N)   |
|:---------------------|------------:|------------:|-------------:|:----------------|
| Ranking RMSE         |  0.0532602  |  0.0569229  |   0.00366272 | N               |
| Ranking IC           |  0.00419463 |  0.00144636 |  -0.00274827 | N               |
| Gap RMSE             |  0.0303628  |  0.0330147  |   0.00265188 | N               |
| Intraday RMSE        |  0.0432952  |  0.0448303  |   0.00153513 | N               |
| Top10 Average Return |  0.00615186 |  0.0021932  |  -0.00395866 | N               |
| Portfolio CAGR       |  2.66861    |  0.358781   |  -2.30983    | N               |
| Sharpe               |  2.20113    |  0.786846   |  -1.41429    | N               |
| Maximum Drawdown     | -0.249273   | -0.225313   |   0.0239601  | Y               |
| Win Rate             |  0.555556   |  0.5        |  -0.0555556  | N               |

## Advantages

- Faster retraining than 250D due to fewer training rows.
- More responsive to recent market regime changes in principle.

## Disadvantages

- Lower ranking metrics in this run.
- Much weaker Top10 portfolio performance than 250D.
- Higher model error for Ranking, Gap, and Intraday in this evaluation window.

## Performance Change

- Ranking IC changed from 0.004195 to 0.001446.
- Portfolio CAGR changed from 2.668614 to 0.358781.
- Sharpe changed from 2.201132 to 0.786846.
- MDD changed from -0.249273 to -0.225313.

## Recommendations

- Do not replace the 250D production rolling window with 150D based on this evaluation.
- Keep 250D as the current production default unless a broader multi-window study over longer periods shows otherwise.
- If shorter windows are explored, test 180D/200D and include costs/slippage before any production change.