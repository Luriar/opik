"""Daily training dataset update helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.model.pipeline.config import DailyUpdateConfig
from src.model.pipeline.daily_context import DailyRunContext
from src.model.pipeline.rolling_window import get_model_feature_columns, select_rolling_train_window


TARGET_COLUMNS = {"target_ranking", "target_gap", "target_intraday"}
AUDIT_COLUMNS = {"date", "ticker", "feature_date", "target_date", "prediction_horizon", "prev_close"}


@dataclass(frozen=True)
class TrainingUpdateResult:
    """Summary of one daily training dataset update/check."""

    training_rows_added: int
    training_rows_replaced: int
    daily_training_snapshot_path: str | None
    training_update_mode: str
    target_feature_dates_added: list[str]
    leakage_violations: int
    forbidden_model_features_found: list[str]
    rolling_train_start_date: str | None
    rolling_train_end_date: str | None
    rolling_train_unique_dates: int
    rolling_train_rows: int
    selected_feature_count: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_training_update(
    config: DailyUpdateConfig,
    context: DailyRunContext,
    dry_run: bool,
    force: bool,
) -> TrainingUpdateResult:
    """Run the Part 3B training dataset update/check step."""
    update_date = pd.Timestamp(context.update_date).normalize()
    prediction_date = pd.Timestamp(context.prediction_date).normalize()
    feature_path = config.resolve_path("feature_file")
    ohlcv_path = config.resolve_path("clean_ohlcv_file")
    training_path = config.resolve_path("training_dataset_file")

    features = read_feature_file(feature_path)
    clean_ohlcv = read_ohlcv(ohlcv_path)
    existing_training = read_training_dataset(training_path)

    target_rows = build_target_available_rows(features, clean_ohlcv, update_date)
    changed_rows = _rows_that_would_change(existing_training, target_rows, force=force)
    combined, rows_added, rows_replaced, mode = safe_append_training_rows(
        existing_training,
        target_rows,
        force=force,
    )
    leakage_violations = count_leakage_violations(changed_rows)
    forbidden_features = find_forbidden_model_features(combined)

    snapshot_path: str | None = None
    if dry_run:
        mode = f"dry_run_{mode}"
    else:
        write_training_dataset(training_path, combined)
        snapshot_path = write_daily_training_snapshot(config, update_date, changed_rows)

    rolling_start: str | None = None
    rolling_end: str | None = None
    rolling_unique_dates = 0
    rolling_rows = 0
    selected_feature_count = len(get_model_feature_columns(combined))
    warnings: list[str] = []
    errors: list[str] = []
    try:
        _, start, end, unique_dates, row_count = select_rolling_train_window(
            combined,
            prediction_date,
            config.rolling_train_days,
        )
        rolling_start = start.date().isoformat()
        rolling_end = end.date().isoformat()
        rolling_unique_dates = len(unique_dates)
        rolling_rows = int(row_count)
    except ValueError as exc:
        errors.append(str(exc))

    return TrainingUpdateResult(
        training_rows_added=int(rows_added),
        training_rows_replaced=int(rows_replaced),
        daily_training_snapshot_path=snapshot_path,
        training_update_mode=mode,
        target_feature_dates_added=[
            pd.Timestamp(item).date().isoformat()
            for item in sorted(pd.to_datetime(changed_rows["feature_date"]).dropna().unique())
        ]
        if "feature_date" in changed_rows.columns and not changed_rows.empty
        else [],
        leakage_violations=int(leakage_violations),
        forbidden_model_features_found=forbidden_features,
        rolling_train_start_date=rolling_start,
        rolling_train_end_date=rolling_end,
        rolling_train_unique_dates=int(rolling_unique_dates),
        rolling_train_rows=int(rolling_rows),
        selected_feature_count=int(selected_feature_count),
        warnings=warnings,
        errors=errors,
    )


def read_feature_file(path: Path) -> pd.DataFrame:
    """Read optimized feature parquet."""
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    return data.sort_values(["date", "ticker"]).reset_index(drop=True)


def read_ohlcv(path: Path) -> pd.DataFrame:
    """Read clean OHLCV parquet."""
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    return data.sort_values(["ticker", "date"]).reset_index(drop=True)


def read_training_dataset(path: Path) -> pd.DataFrame:
    """Read existing training dataset or return empty frame."""
    if not path.exists():
        return pd.DataFrame(columns=["date", "ticker", "feature_date", "target_date"])
    data = pd.read_parquet(path)
    return normalize_training_rows(data)


def build_target_available_rows(
    features: pd.DataFrame,
    clean_ohlcv: pd.DataFrame,
    update_date: pd.Timestamp,
) -> pd.DataFrame:
    """Create training rows whose next-trading-day target is available."""
    feature_data = features.copy()
    feature_data["date"] = pd.to_datetime(feature_data["date"]).dt.normalize()
    feature_data["ticker"] = feature_data["ticker"].astype(str).str.zfill(6)

    price_data = clean_ohlcv.copy()
    price_data["date"] = pd.to_datetime(price_data["date"]).dt.normalize()
    price_data["ticker"] = price_data["ticker"].astype(str).str.zfill(6)
    price_data = price_data.sort_values(["ticker", "date"]).reset_index(drop=True)
    price_data["feature_date"] = price_data["date"]
    price_data["target_date"] = price_data.groupby("ticker")["date"].shift(-1)
    price_data["prev_close"] = price_data["close"]
    price_data["target_open"] = price_data.groupby("ticker")["open"].shift(-1)
    price_data["target_close"] = price_data.groupby("ticker")["close"].shift(-1)

    target_base = price_data[
        ["ticker", "feature_date", "target_date", "prev_close", "target_open", "target_close"]
    ].dropna(subset=["target_date", "prev_close", "target_open", "target_close"])
    target_base = target_base[target_base["target_date"].le(update_date)].copy()
    if target_base.empty:
        return pd.DataFrame(columns=[*feature_data.columns, "feature_date", "target_date", "prediction_horizon", "prev_close", *TARGET_COLUMNS])

    merged = feature_data.merge(
        target_base,
        left_on=["date", "ticker"],
        right_on=["feature_date", "ticker"],
        how="inner",
        validate="one_to_one",
    )
    if merged.empty:
        return merged

    merged["target_ranking"] = merged["target_close"] / merged["prev_close"] - 1
    merged["target_gap"] = merged["target_open"] / merged["prev_close"] - 1
    merged["target_intraday"] = merged["target_close"] / merged["target_open"] - 1
    merged["prediction_horizon"] = 1
    merged["date"] = merged["target_date"]
    merged = merged.drop(columns=["target_open", "target_close"])

    if (merged["feature_date"] >= merged["target_date"]).any():
        raise ValueError("feature_date must be earlier than target_date")
    return normalize_training_rows(merged)


def safe_append_training_rows(
    existing_training: pd.DataFrame,
    new_rows: pd.DataFrame,
    force: bool,
) -> tuple[pd.DataFrame, int, int, str]:
    """Append or replace training rows without duplicate feature_date/ticker keys."""
    existing = normalize_training_rows(existing_training)
    new_data = normalize_training_rows(new_rows)
    if new_data.empty:
        return existing, 0, 0, "no_target_available"

    affected_dates = set(pd.to_datetime(new_data["feature_date"]).dt.normalize())
    key_columns = ["feature_date", "ticker"]
    if force:
        same_date_mask = existing["feature_date"].isin(affected_dates) if not existing.empty else pd.Series(dtype=bool)
        rows_replaced = int(same_date_mask.sum()) if not existing.empty else 0
        base = existing.loc[~same_date_mask].copy() if not existing.empty else existing.copy()
        combined = pd.concat([base, new_data], ignore_index=True)
        rows_added = len(new_data)
        mode = "replace"
    else:
        existing_keys = set(zip(existing["feature_date"], existing["ticker"], strict=False)) if not existing.empty else set()
        keep_mask = [(row.feature_date, row.ticker) not in existing_keys for row in new_data.itertuples(index=False)]
        append_rows = new_data.loc[keep_mask].copy()
        combined = pd.concat([existing, append_rows], ignore_index=True)
        rows_added = len(append_rows)
        rows_replaced = 0
        mode = "append" if rows_added else "existing"

    combined = normalize_training_rows(combined)
    if combined.duplicated(subset=key_columns).any() or combined.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Duplicate training date/ticker rows after append")
    return combined, int(rows_added), int(rows_replaced), mode


def count_leakage_violations(rows: pd.DataFrame) -> int:
    """Count feature_date/target_date leakage violations."""
    if rows.empty or {"feature_date", "target_date"} - set(rows.columns):
        return 0
    feature_dates = pd.to_datetime(rows["feature_date"])
    target_dates = pd.to_datetime(rows["target_date"])
    return int((feature_dates >= target_dates).sum())


def find_forbidden_model_features(training_dataset: pd.DataFrame) -> list[str]:
    """Return forbidden audit/target columns if model feature selection leaks them."""
    selected = set(get_model_feature_columns(training_dataset))
    forbidden = AUDIT_COLUMNS | TARGET_COLUMNS | {column for column in training_dataset.columns if column.startswith("target_")}
    return sorted(selected & forbidden)


def normalize_training_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize training row date and ticker columns."""
    if df.empty:
        return df.copy()
    result = df.copy()
    for column in ["date", "feature_date", "target_date"]:
        if column in result.columns:
            result[column] = pd.to_datetime(result[column]).dt.normalize()
    if "ticker" in result.columns:
        result["ticker"] = result["ticker"].astype(str).str.zfill(6)
    sort_columns = ["feature_date", "ticker"] if "feature_date" in result.columns and "ticker" in result.columns else ["date", "ticker"]
    return result.sort_values(sort_columns).reset_index(drop=True)


def write_training_dataset(path: Path, rows: pd.DataFrame) -> None:
    """Write training dataset parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    normalize_training_rows(rows).to_parquet(path, index=False)


def write_daily_training_snapshot(
    config: DailyUpdateConfig,
    update_date: pd.Timestamp,
    rows: pd.DataFrame,
) -> str:
    """Write daily training CSV snapshot."""
    compact = update_date.strftime("%Y%m%d")
    path = config.resolve_path("daily_training_dir") / f"training_rows_{compact}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    normalize_training_rows(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)


def _rows_that_would_change(existing_training: pd.DataFrame, new_rows: pd.DataFrame, force: bool) -> pd.DataFrame:
    if new_rows.empty:
        return new_rows.copy()
    existing = normalize_training_rows(existing_training)
    new_data = normalize_training_rows(new_rows)
    if force:
        return new_data.copy()
    existing_keys = set(zip(existing["feature_date"], existing["ticker"], strict=False)) if not existing.empty else set()
    keep_mask = [(row.feature_date, row.ticker) not in existing_keys for row in new_data.itertuples(index=False)]
    return new_data.loc[keep_mask].copy()
