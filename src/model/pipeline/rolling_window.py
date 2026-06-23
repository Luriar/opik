"""Pure rolling-window helpers for daily model training."""

from __future__ import annotations

import pandas as pd


FORBIDDEN_MODEL_COLUMNS: set[str] = {
    "date",
    "ticker",
    "ticker_name",
    "feature_date",
    "target_date",
    "prediction_horizon",
    "prev_close",
    "target_ranking",
    "target_gap",
    "target_intraday",
    "sector",
    "market_type",
    "market_cap_group",
}


def select_rolling_train_window(
    training_dataset: pd.DataFrame,
    prediction_date: str | pd.Timestamp,
    rolling_train_days: int = 250,
) -> tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp, list[pd.Timestamp], int]:
    """Select exactly the most recent N feature dates before prediction_date."""
    if "feature_date" not in training_dataset.columns:
        raise ValueError("training_dataset must include feature_date")
    data = training_dataset.copy()
    data["feature_date"] = pd.to_datetime(data["feature_date"])
    cutoff = pd.Timestamp(prediction_date)
    eligible_dates = (
        data.loc[data["feature_date"] < cutoff, "feature_date"]
        .drop_duplicates()
        .sort_values()
        .to_list()
    )
    if len(eligible_dates) < rolling_train_days:
        raise ValueError(
            f"Need at least {rolling_train_days} unique feature_date values before "
            f"{cutoff.date()}, found {len(eligible_dates)}"
        )
    selected_dates = eligible_dates[-rolling_train_days:]
    train_df = data[data["feature_date"].isin(selected_dates)].copy()
    train_df = train_df.sort_values(["feature_date", "ticker"] if "ticker" in train_df.columns else ["feature_date"])
    return (
        train_df.reset_index(drop=True),
        pd.Timestamp(selected_dates[0]),
        pd.Timestamp(selected_dates[-1]),
        [pd.Timestamp(item) for item in selected_dates],
        int(len(train_df)),
    )


def get_model_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return model feature columns excluding audit, target, and identity columns."""
    return [
        column
        for column in df.columns
        if column not in FORBIDDEN_MODEL_COLUMNS and not column.startswith("target_")
    ]
