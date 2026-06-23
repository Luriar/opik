"""Feature builder orchestration for Phase 2."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.features.breakout_features import add_breakout_features
from src.features.candlestick_features import add_candlestick_features
from src.features.cross_sectional_features import add_cross_sectional_features
from src.features.identity_features import IDENTITY_COLUMNS, add_identity_features
from src.features.macro_features import MACRO_RETURN_COLUMNS, add_macro_features
from src.features.momentum_features import add_momentum_features
from src.features.price_features import add_price_features
from src.features.technical_features import add_technical_features
from src.features.volume_features import add_volume_features
from src.features.volatility_features import add_volatility_features
from src.utils.config_loader import load_yaml_config
from src.utils.paths import build_project_paths


TARGET_COLUMNS: set[str] = {"target_rank_return", "target_gap", "target_intraday"}
RAW_MACRO_COLUMNS: set[str] = set(MACRO_RETURN_COLUMNS.values())
BASE_REQUIRED_COLUMNS: tuple[str, ...] = ("date", "ticker", "open", "high", "low", "close", "volume")
GROUP_BUILDERS = {
    "price": add_price_features,
    "momentum": add_momentum_features,
    "volume": add_volume_features,
    "volatility": add_volatility_features,
    "candlestick": add_candlestick_features,
    "breakout": add_breakout_features,
    "technical": add_technical_features,
    "cross_sectional": add_cross_sectional_features,
    "macro": add_macro_features,
}


@dataclass(frozen=True)
class FeatureBuildResult:
    """Feature dataset and metadata returned by the builder."""

    features: pd.DataFrame
    metadata: dict[str, Any]


def load_feature_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load the feature configuration root."""
    path = Path(config_path) if config_path is not None else build_project_paths().configs / "feature.yaml"
    data = load_yaml_config(path)
    if "feature" not in data:
        raise ValueError("feature.yaml must contain a 'feature' root key")
    return data["feature"]


def validate_feature_input(df: pd.DataFrame) -> None:
    """Validate minimum raw columns for feature generation."""
    missing = set(BASE_REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required feature input columns: {sorted(missing)}")
    overlap = TARGET_COLUMNS & set(df.columns)
    if overlap:
        raise ValueError(f"Target columns are forbidden in feature input: {sorted(overlap)}")


def get_enabled_feature_groups(config: dict[str, Any]) -> list[str]:
    """Return enabled feature groups in deterministic build order."""
    order = [
        "price",
        "momentum",
        "volume",
        "volatility",
        "candlestick",
        "breakout",
        "technical",
        "cross_sectional",
        "macro",
        "identity",
    ]
    return [group for group in order if config.get(group, {}).get("enabled", False)]


def generate_feature_metadata(
    feature_df: pd.DataFrame,
    feature_columns: list[str],
    config: dict[str, Any],
    enabled_groups: list[str],
) -> dict[str, Any]:
    """Create metadata for reproducibility and audit."""
    return {
        "feature_version": config.get("version", "unknown"),
        "enabled_groups": enabled_groups,
        "feature_count": len(feature_columns),
        "feature_columns": feature_columns,
        "row_count": int(len(feature_df)),
        "date_min": str(feature_df["date"].min()) if not feature_df.empty else None,
        "date_max": str(feature_df["date"].max()) if not feature_df.empty else None,
    }


def build_features(
    df: pd.DataFrame,
    config_path: str | Path | None = None,
    save_metadata: bool = False,
) -> FeatureBuildResult:
    """Build the Phase 2 feature dataset from raw OHLCV and optional macro/identity columns."""
    config = load_feature_config(config_path)
    validate_feature_input(df)

    result = df.copy()
    enabled_groups = get_enabled_feature_groups(config)
    for group_name in enabled_groups:
        if group_name == "identity":
            fill_value = config.get("missing_value", {}).get("fill_identity_missing", "unknown")
            result = add_identity_features(result, fill_value=fill_value)
            continue
        builder = GROUP_BUILDERS.get(group_name)
        if builder is not None:
            result = builder(result)

    excluded = set(config.get("excluded_columns", [])) | TARGET_COLUMNS | RAW_MACRO_COLUMNS
    preserve = ["date", "ticker"]
    feature_columns = [
        column
        for column in result.columns
        if column not in excluded and column not in preserve
    ]
    feature_df = result.loc[:, preserve + feature_columns].copy()

    final_feature_columns = [column for column in feature_df.columns if column not in preserve]
    metadata = generate_feature_metadata(feature_df, final_feature_columns, config, enabled_groups)
    if save_metadata:
        _save_feature_metadata(metadata, config)

    return FeatureBuildResult(features=feature_df, metadata=metadata)


def _save_feature_metadata(metadata: dict[str, Any], config: dict[str, Any]) -> None:
    """Persist feature metadata and feature list when configured."""
    output_config = config.get("output", {})
    metadata_path = build_project_paths().root / output_config.get(
        "feature_metadata_path", "outputs/features/feature_metadata.json"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    if output_config.get("save_feature_list", False):
        feature_list_path = metadata_path.with_name("feature_list.txt")
        feature_list_path.write_text("\n".join(metadata["feature_columns"]), encoding="utf-8")
