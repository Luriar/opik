"""Daily optimized feature-store update helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.features.feature_builder import build_features
from src.pipeline.config import DailyUpdateConfig
from src.pipeline.daily_context import DailyRunContext


IDENTITY_COLUMNS = {"sector", "market_type", "market_cap_group"}
BASE_DUPLICATES_TO_REMOVE = {"momentum_5d", "momentum_20d"}
TARGET_PREFIX = "target_"
REQUIRED_PRODUCTION_FEATURES = ("sox_return_1d",)


@dataclass(frozen=True)
class FeatureUpdateResult:
    """Summary of one daily optimized feature update/check."""

    feature_rows_added: int
    feature_rows_replaced: int
    daily_feature_snapshot_path: str | None
    feature_update_mode: str
    feature_update_date: str
    feature_missing_count: int
    feature_column_count: int
    feature_ticker_count: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_feature_update(
    config: DailyUpdateConfig,
    context: DailyRunContext,
    dry_run: bool,
    force: bool,
) -> FeatureUpdateResult:
    """Run the Part 3A feature update/check step."""
    update_date = pd.Timestamp(context.update_date).normalize()
    clean_ohlcv = read_ohlcv(config.resolve_path("clean_ohlcv_file"))
    macro = read_macro(config.resolve_path("macro_file"))
    feature_path = config.resolve_path("feature_file")
    existing_features = read_feature_file(feature_path)

    context_ohlcv = select_feature_context(clean_ohlcv, update_date, lookback_dates=120)
    daily_features = build_daily_features(context_ohlcv, macro, update_date)
    daily_features = optimize_feature_frame(daily_features)

    rows_added = 0
    rows_replaced = 0
    mode = "dry_run"
    snapshot_path: str | None = None
    if dry_run:
        rows_added, rows_replaced = preview_feature_append(existing_features, daily_features, update_date, force)
    else:
        validate_required_feature_availability(daily_features)
        updated, rows_added, rows_replaced, mode = safe_append_features(
            existing_features,
            daily_features,
            update_date,
            force,
        )
        write_feature_file(feature_path, updated)
        snapshot_path = write_daily_feature_snapshot(config, update_date, daily_features)

    if not dry_run:
        mode_value = mode
    else:
        mode_value = "dry_run_replace" if rows_replaced else "dry_run_append"

    return FeatureUpdateResult(
        feature_rows_added=int(rows_added),
        feature_rows_replaced=int(rows_replaced),
        daily_feature_snapshot_path=snapshot_path,
        feature_update_mode=mode_value,
        feature_update_date=update_date.date().isoformat(),
        feature_missing_count=int(daily_features.isna().sum().sum()),
        feature_column_count=int(len([col for col in daily_features.columns if col not in {"date", "ticker"}])),
        feature_ticker_count=int(daily_features["ticker"].nunique()) if "ticker" in daily_features.columns else 0,
    )


def validate_required_feature_availability(feature_df: pd.DataFrame) -> None:
    """Reject required production features that are absent or null for every row."""
    for feature in REQUIRED_PRODUCTION_FEATURES:
        if feature not in feature_df.columns or pd.to_numeric(feature_df[feature], errors="coerce").isna().all():
            raise ValueError(
                "SOX close exists but sox_return_1d cannot be computed yet because "
                "prior SOX history is missing."
            )


def read_ohlcv(path: Path) -> pd.DataFrame:
    """Read clean OHLCV parquet."""
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    return data.sort_values(["date", "ticker"]).reset_index(drop=True)


def read_macro(path: Path) -> pd.DataFrame:
    """Read macro parquet."""
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    provenance_columns = [
        column for column in data.columns
        if column.startswith("actual_") or column.startswith("expected_")
    ]
    if provenance_columns:
        data = data.drop(columns=provenance_columns)
    return data.sort_values("date").reset_index(drop=True)


def read_feature_file(path: Path) -> pd.DataFrame:
    """Read existing feature parquet or return empty frame."""
    if not path.exists():
        return pd.DataFrame(columns=["date", "ticker"])
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    return data.sort_values(["date", "ticker"]).reset_index(drop=True)


def select_feature_context(
    clean_ohlcv: pd.DataFrame,
    update_date: pd.Timestamp,
    lookback_dates: int = 120,
) -> pd.DataFrame:
    """Select last N trading dates before update_date plus update_date."""
    dates = clean_ohlcv.loc[clean_ohlcv["date"].le(update_date), "date"].drop_duplicates().sort_values()
    selected_dates = dates.tail(lookback_dates + 1)
    if selected_dates.empty or selected_dates.max() != update_date:
        raise ValueError(f"No clean OHLCV rows available for update_date {update_date.date()}")
    return clean_ohlcv[clean_ohlcv["date"].isin(selected_dates)].copy()


def build_daily_features(context_ohlcv: pd.DataFrame, macro: pd.DataFrame, update_date: pd.Timestamp) -> pd.DataFrame:
    """Build features using existing Phase 2 FeatureBuilder and keep update_date rows."""
    merged = context_ohlcv.merge(macro, on="date", how="left", validate="many_to_one")
    built = build_features(merged.sort_values(["ticker", "date"]).reset_index(drop=True)).features
    built["date"] = pd.to_datetime(built["date"]).dt.normalize()
    built["ticker"] = built["ticker"].astype(str).str.zfill(6)
    daily = built[built["date"].eq(update_date)].copy()
    if daily.empty:
        raise ValueError(f"FeatureBuilder produced no rows for update_date {update_date.date()}")
    return daily.sort_values(["date", "ticker"]).reset_index(drop=True)


def optimize_feature_frame(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Apply existing full-universe feature optimization policy."""
    optimized = feature_df.copy()
    drop_columns = [column for column in sorted(BASE_DUPLICATES_TO_REMOVE | IDENTITY_COLUMNS) if column in optimized.columns]
    target_columns = [column for column in optimized.columns if column.startswith(TARGET_PREFIX)]
    drop_columns.extend(target_columns)
    if drop_columns:
        optimized = optimized.drop(columns=drop_columns)

    rank_remove = duplicated_rank_features_to_remove(optimized)
    if rank_remove:
        optimized = optimized.drop(columns=sorted(rank_remove))

    if {"date", "ticker"} - set(optimized.columns):
        raise ValueError("Optimized feature frame must preserve date and ticker")
    return optimized.sort_values(["date", "ticker"]).reset_index(drop=True)


def duplicated_rank_features_to_remove(feature_df: pd.DataFrame) -> set[str]:
    """Detect clearly duplicated rank features and choose redundant names to remove."""
    rank_columns = [
        column
        for column in feature_df.select_dtypes(include="number").columns
        if "rank_pct" in column
    ]
    preferred_order = [
        "return_5d_rank_pct",
        "return_20d_rank_pct",
        "momentum_20d_rank_pct",
        "trading_value_rank_pct",
        "volatility_rank_pct",
        "atr_rank_pct",
        "bb_width_rank_pct",
        "breakout_rank_pct",
        "relative_return_5d_rank_pct",
        "sector_relative_rank_pct",
    ]
    preference = {name: idx for idx, name in enumerate(preferred_order)}
    remove: set[str] = set()
    for idx, left in enumerate(rank_columns):
        if left in remove:
            continue
        for right in rank_columns[idx + 1 :]:
            if right in remove:
                continue
            pair = feature_df[[left, right]].dropna()
            if pair.empty:
                continue
            corr = pair[left].corr(pair[right])
            if pair[left].equals(pair[right]) or (pd.notna(corr) and corr >= 0.999999):
                keep, drop = sorted(
                    [left, right],
                    key=lambda item: (preference.get(item, 999), len(item), item),
                )
                _ = keep
                remove.add(drop)
    return remove


def safe_append_features(
    existing_features: pd.DataFrame,
    daily_features: pd.DataFrame,
    update_date: pd.Timestamp,
    force: bool,
) -> tuple[pd.DataFrame, int, int, str]:
    """Append or replace daily features without duplicate date/ticker rows."""
    existing = normalize_features(existing_features)
    daily = normalize_features(daily_features)
    existing_same_date = existing[existing["date"].eq(update_date)]
    existing_keys = set(zip(existing["date"], existing["ticker"], strict=False))

    if force:
        base = existing[~existing["date"].eq(update_date)].copy()
        updated = pd.concat([base, daily], ignore_index=True)
        rows_added = len(daily)
        rows_replaced = len(existing_same_date)
        mode = "replace"
    else:
        keep_mask = [
            (row.date, row.ticker) not in existing_keys
            for row in daily.itertuples(index=False)
        ]
        append_rows = daily.loc[keep_mask].copy()
        updated = pd.concat([existing, append_rows], ignore_index=True)
        rows_added = len(append_rows)
        rows_replaced = 0
        mode = "append" if rows_added else "existing"

    updated = normalize_features(updated)
    if updated.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Duplicate date/ticker rows after feature append")
    return updated, int(rows_added), int(rows_replaced), mode


def preview_feature_append(
    existing_features: pd.DataFrame,
    daily_features: pd.DataFrame,
    update_date: pd.Timestamp,
    force: bool,
) -> tuple[int, int]:
    """Return rows that would be added/replaced."""
    _, rows_added, rows_replaced, _ = safe_append_features(existing_features, daily_features, update_date, force)
    return rows_added, rows_replaced


def normalize_features(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize feature key columns and sort."""
    if df.empty:
        return df.copy()
    result = df.copy()
    result["date"] = pd.to_datetime(result["date"]).dt.normalize()
    result["ticker"] = result["ticker"].astype(str).str.zfill(6)
    return result.sort_values(["date", "ticker"]).reset_index(drop=True)


def write_feature_file(path: Path, features: pd.DataFrame) -> None:
    """Write feature parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    normalize_features(features).to_parquet(path, index=False)


def write_daily_feature_snapshot(
    config: DailyUpdateConfig,
    update_date: pd.Timestamp,
    daily_features: pd.DataFrame,
) -> str:
    """Write daily feature CSV snapshot."""
    compact = update_date.strftime("%Y%m%d")
    path = config.resolve_path("daily_feature_dir") / f"features_{compact}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    normalize_features(daily_features).to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)
