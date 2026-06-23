"""Build the full-universe real dataset from provided KOSPI200/KOSDAQ150 files."""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("MPLCONFIGDIR", str(PROJECT_ROOT / "data" / "raw" / ".mplconfig"))

from src.features.feature_builder import build_features  # noqa: E402


START_DATE = "2023-06-15"
END_DATE = "2026-06-14"
START_COMPACT = "20230615"
END_COMPACT = "20260614"

KOSPI200_INPUT = PROJECT_ROOT / "data" / "metadata" / "kospi200_260616.csv"
KOSDAQ150_INPUT = PROJECT_ROOT / "data" / "metadata" / "kosdaq150_260616.csv"
KOSPI200_INPUT_CANDIDATES = [
    KOSPI200_INPUT,
    PROJECT_ROOT / "data" / "metadata" / "kospi200_20260616.csv",
    PROJECT_ROOT / "data" / "metadata" / "kospi200_20260616.csv.csv",
]
KOSDAQ150_INPUT_CANDIDATES = [
    KOSDAQ150_INPUT,
    PROJECT_ROOT / "data" / "metadata" / "kosdaq150_20260616.csv",
    PROJECT_ROOT / "data" / "metadata" / "kosdaq150_20260616.csv.csv",
]
TICKER_NAMES_CSV = PROJECT_ROOT / "data" / "metadata" / "ticker_names.csv"
FULL_UNIVERSE_CSV = PROJECT_ROOT / "data" / "metadata" / "full_universe_260616.csv"

RAW_OHLCV_PARQUET = PROJECT_ROOT / "data" / "raw" / "kr_stock" / f"ohlcv_full_universe_{START_COMPACT}_{END_COMPACT}.parquet"
RAW_OHLCV_CSV = PROJECT_ROOT / "data" / "raw" / "kr_stock" / f"ohlcv_full_universe_{START_COMPACT}_{END_COMPACT}.csv"
RAW_MACRO_PARQUET = PROJECT_ROOT / "data" / "raw" / "macro" / f"macro_{START_COMPACT}_{END_COMPACT}.parquet"
RAW_MACRO_CSV = PROJECT_ROOT / "data" / "raw" / "macro" / f"macro_{START_COMPACT}_{END_COMPACT}.csv"

CLEAN_OHLCV_PARQUET = PROJECT_ROOT / "data" / "processed" / "kr_stock" / f"ohlcv_full_universe_clean_{START_COMPACT}_{END_COMPACT}.parquet"
CLEAN_OHLCV_CSV = PROJECT_ROOT / "data" / "processed" / "kr_stock" / f"ohlcv_full_universe_clean_{START_COMPACT}_{END_COMPACT}.csv"
CLEAN_MACRO_PARQUET = PROJECT_ROOT / "data" / "processed" / "macro" / f"macro_clean_{START_COMPACT}_{END_COMPACT}.parquet"

FEATURE_PARQUET = PROJECT_ROOT / "data" / "features" / "full_universe_features_optimized.parquet"
FEATURE_CSV = PROJECT_ROOT / "data" / "features" / "full_universe_features_optimized.csv"

TRAINING_PARQUET = PROJECT_ROOT / "data" / "processed" / "full_universe_training_dataset.parquet"
TRAINING_CSV = PROJECT_ROOT / "data" / "processed" / "full_universe_training_dataset.csv"
TRAINING_METADATA_JSON = PROJECT_ROOT / "data" / "processed" / "full_universe_training_metadata.json"
REPORT_MD = PROJECT_ROOT / "reports" / "full_universe_dataset_build_report.md"

SAMSUNG = "005930"
TARGET_COLUMNS = ["target_ranking", "target_gap", "target_intraday"]
AUDIT_COLUMNS = ["date", "ticker", "feature_date", "target_date", "prediction_horizon", "prev_close"]
FORBIDDEN_FEATURE_COLUMNS = set(AUDIT_COLUMNS) | set(TARGET_COLUMNS) | {"target_rank_return"}
IDENTITY_COLUMNS = {"sector", "market_type", "market_cap_group"}
BASE_DUPLICATES_TO_REMOVE = {"momentum_5d", "momentum_20d"}


def resolve_universe_input(candidates: list[Path]) -> Path:
    """Return the first available universe input path from accepted names."""
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        "Missing required universe input. Checked: "
        f"{[str(path) for path in candidates]}"
    )


def load_universe_file(path: Path, source_index: str) -> pd.DataFrame:
    """Load one provided universe CSV with ticker and Korean stock name columns."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required universe input: {path}")
    data = read_korean_csv(path)
    if data.shape[1] < 2:
        raise ValueError(f"{path} must contain at least ticker and ticker name columns")
    result = data.iloc[:, :2].copy()
    result.columns = ["ticker", "ticker_name"]
    result["ticker"] = result["ticker"].astype(str).str.extract(r"(\d+)")[0].str.zfill(6)
    result["ticker_name"] = result["ticker_name"].fillna("").astype(str).str.strip()
    result = result.dropna(subset=["ticker"])
    result = result[result["ticker"].str.fullmatch(r"\d{6}")]
    result = result[result["ticker_name"].ne("")]
    result["source_index"] = source_index
    return result.loc[:, ["ticker", "ticker_name", "source_index"]].drop_duplicates()


def read_korean_csv(path: Path) -> pd.DataFrame:
    """Read a Korean CSV using common encodings."""
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, dtype=str, header=None, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        f"Could not decode {path} with utf-8-sig, cp949, or euc-kr: {last_error}",
    )


def build_universe_mapping() -> pd.DataFrame:
    """Create ticker_names.csv and full_universe_260616.csv from provided inputs."""
    kospi_path = resolve_universe_input(KOSPI200_INPUT_CANDIDATES)
    kosdaq_path = resolve_universe_input(KOSDAQ150_INPUT_CANDIDATES)
    kospi = load_universe_file(kospi_path, "KOSPI200")
    kosdaq = load_universe_file(kosdaq_path, "KOSDAQ150")
    combined = pd.concat([kospi, kosdaq], ignore_index=True)
    combined = combined.sort_values(["ticker", "source_index"]).reset_index(drop=True)
    combined["source_index"] = combined.groupby(["ticker", "ticker_name"])["source_index"].transform(
        lambda values: "+".join(sorted(set(values)))
    )
    combined = combined.drop_duplicates(subset=["ticker", "ticker_name"]).sort_values("ticker")
    if SAMSUNG not in set(combined["ticker"]):
        raise ValueError("Samsung Electronics ticker 005930 is missing from provided universe files")

    TICKER_NAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.loc[:, ["ticker", "ticker_name"]].drop_duplicates("ticker").to_csv(
        TICKER_NAMES_CSV,
        index=False,
        encoding="utf-8-sig",
    )
    combined.to_csv(FULL_UNIVERSE_CSV, index=False, encoding="utf-8-sig")
    return combined


def download_ohlcv(tickers: list[str]) -> pd.DataFrame:
    """Download full-universe OHLCV from pykrx."""
    from pykrx import stock

    frames: list[pd.DataFrame] = []
    for idx, ticker in enumerate(tickers, start=1):
        print(f"[{idx:03d}/{len(tickers):03d}] Downloading OHLCV: {ticker}")
        try:
            data = stock.get_market_ohlcv_by_date(START_COMPACT, END_COMPACT, ticker)
        except Exception as exc:
            print(f"  skipped {ticker}: {exc}")
            continue
        if data.empty:
            print(f"  skipped {ticker}: empty data")
            continue
        frames.append(normalize_krx_ohlcv(data, ticker))
        sleep(0.05)

    if not frames:
        raise RuntimeError("No OHLCV data downloaded for full universe")
    ohlcv = pd.concat(frames, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    if ohlcv.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Raw full-universe OHLCV contains duplicate date/ticker rows")
    RAW_OHLCV_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    ohlcv.to_parquet(RAW_OHLCV_PARQUET, index=False)
    ohlcv.to_csv(RAW_OHLCV_CSV, index=False, encoding="utf-8-sig")
    return ohlcv


def normalize_krx_ohlcv(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Normalize pykrx OHLCV output to project columns."""
    result = data.reset_index().copy()
    rename_map = {
        "Date": "date",
        "날짜": "date",
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
        "거래대금": "trading_value",
    }
    result = result.rename(columns=rename_map)
    required = ["date", "open", "high", "low", "close", "volume"]
    missing = set(required) - set(result.columns)
    if missing:
        raise ValueError(f"Missing pykrx OHLCV columns for {ticker}: {sorted(missing)}")
    if "trading_value" not in result.columns:
        result["trading_value"] = result["close"] * result["volume"]
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = str(ticker).zfill(6)
    return result.loc[:, ["date", "ticker", "open", "high", "low", "close", "volume", "trading_value"]]


def ensure_macro() -> pd.DataFrame:
    """Use existing macro parquet or download macro data if missing."""
    if RAW_MACRO_PARQUET.exists():
        macro = pd.read_parquet(RAW_MACRO_PARQUET)
        macro["date"] = pd.to_datetime(macro["date"])
        return macro
    macro = download_macro()
    RAW_MACRO_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    macro.to_parquet(RAW_MACRO_PARQUET, index=False)
    macro.to_csv(RAW_MACRO_CSV, index=False, encoding="utf-8-sig")
    return macro


def download_macro() -> pd.DataFrame:
    """Download macro/global data through yfinance."""
    import yfinance as yf

    ticker_map = {
        "nasdaq_close": "^IXIC",
        "sox_close": "^SOX",
        "sp500_close": "^GSPC",
        "vix_close": "^VIX",
        "usdkrw": "KRW=X",
        "wti_close": "CL=F",
    }
    frames: list[pd.DataFrame] = []
    for output_column, yf_ticker in ticker_map.items():
        print(f"Downloading macro series {output_column}: {yf_ticker}")
        data = yf.download(
            yf_ticker,
            start=START_DATE,
            end="2026-06-15",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
        if data.empty:
            raise RuntimeError(f"No macro data returned for {yf_ticker}")
        close = data["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        frame = close.rename(output_column).reset_index().rename(columns={"Date": "date"})
        frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
        frames.append(frame.loc[:, ["date", output_column]])

    result = frames[0]
    for frame in frames[1:]:
        result = result.merge(frame, on="date", how="outer")
    result = result.sort_values("date").reset_index(drop=True)
    return result[(result["date"] >= START_DATE) & (result["date"] <= END_DATE)].reset_index(drop=True)


def clean_ohlcv(ohlcv: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove invalid OHLCV rows without forward-filling prices."""
    data = ohlcv.copy()
    for column in ["date"]:
        data[column] = pd.to_datetime(data[column])
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    valid = (
        data["open"].gt(0)
        & data["high"].gt(0)
        & data["low"].gt(0)
        & data["close"].gt(0)
        & data["high"].ge(data["low"])
        & data["high"].ge(data["open"])
        & data["high"].ge(data["close"])
        & data["low"].le(data["open"])
        & data["low"].le(data["close"])
    )
    cleaned = data.loc[valid].sort_values(["ticker", "date"]).reset_index(drop=True)
    CLEAN_OHLCV_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(CLEAN_OHLCV_PARQUET, index=False)
    cleaned.to_csv(CLEAN_OHLCV_CSV, index=False, encoding="utf-8-sig")
    return cleaned, int((~valid).sum())


def clean_macro(macro: pd.DataFrame) -> pd.DataFrame:
    """Sort and forward-fill macro columns using only prior observations."""
    cleaned = macro.copy()
    cleaned["date"] = pd.to_datetime(cleaned["date"])
    cleaned = cleaned.sort_values("date").reset_index(drop=True)
    macro_columns = [column for column in cleaned.columns if column != "date"]
    cleaned[macro_columns] = cleaned[macro_columns].ffill()
    CLEAN_MACRO_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(CLEAN_MACRO_PARQUET, index=False)
    return cleaned


def optimize_features(feature_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Apply the same duplicate/rank/identity feature optimization policy."""
    optimized = feature_df.copy()
    removed: list[str] = []

    for column in sorted(BASE_DUPLICATES_TO_REMOVE | IDENTITY_COLUMNS):
        if column in optimized.columns:
            optimized = optimized.drop(columns=[column])
            removed.append(column)

    rank_columns = [
        column
        for column in optimized.select_dtypes(include="number").columns
        if "rank_pct" in column and column not in {"date", "ticker"}
    ]
    rank_remove: set[str] = set()
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
    for idx, left in enumerate(rank_columns):
        if left in rank_remove:
            continue
        for right in rank_columns[idx + 1 :]:
            if right in rank_remove:
                continue
            pair = optimized[[left, right]].dropna()
            if pair.empty:
                continue
            exactly_equal = bool(pair[left].equals(pair[right]))
            corr = pair[left].corr(pair[right])
            if exactly_equal or (pd.notna(corr) and corr >= 0.999999):
                keep, drop = sorted(
                    [left, right],
                    key=lambda item: (preference.get(item, 999), len(item), item),
                )
                rank_remove.add(drop)

    if rank_remove:
        optimized = optimized.drop(columns=sorted(rank_remove))
        removed.extend(sorted(rank_remove))

    target_like = [column for column in optimized.columns if column.startswith("target_")]
    if target_like:
        optimized = optimized.drop(columns=target_like)
        removed.extend(target_like)

    optimized = optimized.sort_values(["ticker", "date"]).reset_index(drop=True)
    FEATURE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    optimized.to_parquet(FEATURE_PARQUET, index=False)
    optimized.to_csv(FEATURE_CSV, index=False, encoding="utf-8-sig")
    return optimized, removed


def build_feature_store(clean_ohlcv_df: pd.DataFrame, clean_macro_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Merge cleaned data and generate optimized features with existing FeatureBuilder."""
    merged = clean_ohlcv_df.merge(clean_macro_df, on="date", how="left", validate="many_to_one")
    merged = merged.sort_values(["ticker", "date"]).reset_index(drop=True)
    built = build_features(merged).features
    if {"date", "ticker"} - set(built.columns):
        raise ValueError("FeatureBuilder output must preserve date and ticker")
    return optimize_features(built)


def build_target_frame(clean_ohlcv_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate future targets and previous-close audit values."""
    data = clean_ohlcv_df.sort_values(["ticker", "date"]).reset_index(drop=True).copy()
    group = data.groupby("ticker", sort=False)
    data["feature_date"] = group["date"].shift(1)
    data["prev_close"] = group["close"].shift(1)
    data["target_date"] = data["date"]
    data["prediction_horizon"] = (data["target_date"] - data["feature_date"]).dt.days
    data["target_ranking"] = data["close"] / data["prev_close"] - 1
    data["target_gap"] = data["open"] / data["prev_close"] - 1
    data["target_intraday"] = data["close"] / data["open"] - 1
    return data.loc[:, ["date", "ticker", *AUDIT_COLUMNS[2:], *TARGET_COLUMNS]]


def build_training_dataset(features: pd.DataFrame, clean_ohlcv_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Merge optimized features with targets and remove rows lacking targets."""
    targets = build_target_frame(clean_ohlcv_df)
    merged = features.merge(targets, on=["date", "ticker"], how="left", validate="one_to_one")
    before = len(merged)
    cleaned = merged.dropna(subset=["feature_date", "target_date", "prediction_horizon", "prev_close", *TARGET_COLUMNS]).copy()
    cleaned = cleaned[cleaned["feature_date"] < cleaned["target_date"]].copy()
    cleaned = cleaned.sort_values(["ticker", "date"]).reset_index(drop=True)
    TRAINING_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(TRAINING_PARQUET, index=False)
    cleaned.to_csv(TRAINING_CSV, index=False, encoding="utf-8-sig")
    return cleaned, int(before - len(cleaned))


def model_feature_columns(training_df: pd.DataFrame) -> list[str]:
    """Return model feature columns after excluding audit and target columns."""
    return [column for column in training_df.columns if column not in FORBIDDEN_FEATURE_COLUMNS]


def build_metadata(
    universe: pd.DataFrame,
    raw_ohlcv: pd.DataFrame,
    clean_ohlcv_df: pd.DataFrame,
    invalid_rows_removed: int,
    features: pd.DataFrame,
    removed_features: list[str],
    training: pd.DataFrame,
    removed_target_rows: int,
) -> dict[str, Any]:
    """Create serializable build metadata."""
    feature_columns = model_feature_columns(training)
    leakage_violations = training[~(training["feature_date"] < training["target_date"])]
    metadata = {
        "created_at": datetime.now(UTC).isoformat(),
        "input_files": {
            "kospi200": str(KOSPI200_INPUT),
            "kosdaq150": str(KOSDAQ150_INPUT),
            "macro": str(RAW_MACRO_PARQUET),
        },
        "output_files": {
            "ticker_names": str(TICKER_NAMES_CSV),
            "full_universe": str(FULL_UNIVERSE_CSV),
            "raw_ohlcv_parquet": str(RAW_OHLCV_PARQUET),
            "raw_ohlcv_csv": str(RAW_OHLCV_CSV),
            "clean_ohlcv_parquet": str(CLEAN_OHLCV_PARQUET),
            "clean_ohlcv_csv": str(CLEAN_OHLCV_CSV),
            "clean_macro_parquet": str(CLEAN_MACRO_PARQUET),
            "features_parquet": str(FEATURE_PARQUET),
            "features_csv": str(FEATURE_CSV),
            "training_parquet": str(TRAINING_PARQUET),
            "training_csv": str(TRAINING_CSV),
            "training_metadata": str(TRAINING_METADATA_JSON),
            "report": str(REPORT_MD),
        },
        "universe_count": int(universe["ticker"].nunique()),
        "source_index_counts": {str(k): int(v) for k, v in universe["source_index"].value_counts().items()},
        "raw_ohlcv_row_count": int(len(raw_ohlcv)),
        "cleaned_ohlcv_row_count": int(len(clean_ohlcv_df)),
        "invalid_ohlcv_rows_removed": int(invalid_rows_removed),
        "feature_shape": list(features.shape),
        "feature_count": len([column for column in features.columns if column not in {"date", "ticker"}]),
        "removed_features": removed_features,
        "training_dataset_shape": list(training.shape),
        "training_feature_count": len(feature_columns),
        "target_count": len(TARGET_COLUMNS),
        "removed_target_rows": int(removed_target_rows),
        "unique_tickers_training": int(training["ticker"].nunique()),
        "ticker_005930_exists": bool(training["ticker"].eq(SAMSUNG).any()),
        "min_feature_date": training["feature_date"].min().date().isoformat(),
        "max_feature_date": training["feature_date"].max().date().isoformat(),
        "min_target_date": training["target_date"].min().date().isoformat(),
        "max_target_date": training["target_date"].max().date().isoformat(),
        "leakage_check": {
            "feature_date_lt_target_date": bool(leakage_violations.empty),
            "violation_count": int(len(leakage_violations)),
            "prev_close_is_model_feature": "prev_close" in feature_columns,
            "target_columns_as_model_features": sorted(set(TARGET_COLUMNS) & set(feature_columns)),
        },
        "missing_values_training": {
            column: int(value)
            for column, value in training.isna().sum().sort_values(ascending=False).items()
            if int(value) > 0
        },
    }
    return metadata


def render_report(metadata: dict[str, Any]) -> str:
    """Render the Markdown build report."""
    lines = [
        "# Full Universe Dataset Build Report",
        "",
        f"- Created at: `{metadata['created_at']}`",
        f"- Universe count: `{metadata['universe_count']}`",
        f"- Raw OHLCV rows: `{metadata['raw_ohlcv_row_count']}`",
        f"- Cleaned OHLCV rows: `{metadata['cleaned_ohlcv_row_count']}`",
        f"- Invalid OHLCV rows removed: `{metadata['invalid_ohlcv_rows_removed']}`",
        f"- Feature shape: `{tuple(metadata['feature_shape'])}`",
        f"- Feature count: `{metadata['feature_count']}`",
        f"- Training dataset shape: `{tuple(metadata['training_dataset_shape'])}`",
        f"- Training feature count: `{metadata['training_feature_count']}`",
        f"- Target count: `{metadata['target_count']}`",
        f"- Removed target rows: `{metadata['removed_target_rows']}`",
        f"- 005930 exists: `{metadata['ticker_005930_exists']}`",
        f"- Leakage check passed: `{metadata['leakage_check']['feature_date_lt_target_date']}`",
        f"- Leakage violation count: `{metadata['leakage_check']['violation_count']}`",
        f"- prev_close is model feature: `{metadata['leakage_check']['prev_close_is_model_feature']}`",
        f"- Target columns as model features: `{metadata['leakage_check']['target_columns_as_model_features']}`",
        "",
        "## Removed Features",
    ]
    if metadata["removed_features"]:
        lines.extend(f"- `{column}`" for column in metadata["removed_features"])
    else:
        lines.append("- None")
    lines.extend(["", "## Output Files"])
    lines.extend(f"- `{path}`" for path in metadata["output_files"].values())
    return "\n".join(lines) + "\n"


def main() -> None:
    """Build the full-universe real dataset end to end."""
    print("Building full-universe ticker mapping...")
    universe = build_universe_mapping()
    tickers = sorted(universe["ticker"].unique())
    print(f"Universe count: {len(tickers)}")
    print(f"005930 exists in universe: {SAMSUNG in tickers}")

    print("Downloading full-universe OHLCV...")
    raw_ohlcv = download_ohlcv(tickers)
    print(f"Raw OHLCV shape: {raw_ohlcv.shape}")

    print("Loading or downloading macro data...")
    macro = ensure_macro()

    print("Cleaning OHLCV and macro...")
    cleaned_ohlcv, invalid_rows_removed = clean_ohlcv(raw_ohlcv)
    cleaned_macro = clean_macro(macro)
    print(f"Cleaned OHLCV shape: {cleaned_ohlcv.shape}")
    print(f"Cleaned macro shape: {cleaned_macro.shape}")

    print("Generating and optimizing full-universe features...")
    features, removed_features = build_feature_store(cleaned_ohlcv, cleaned_macro)
    print(f"Optimized feature shape: {features.shape}")

    print("Creating full-universe training dataset...")
    training, removed_target_rows = build_training_dataset(features, cleaned_ohlcv)
    metadata = build_metadata(
        universe,
        raw_ohlcv,
        cleaned_ohlcv,
        invalid_rows_removed,
        features,
        removed_features,
        training,
        removed_target_rows,
    )
    if not metadata["leakage_check"]["feature_date_lt_target_date"]:
        raise ValueError("Leakage check failed: feature_date must be before target_date")
    if metadata["leakage_check"]["prev_close_is_model_feature"]:
        raise ValueError("prev_close must not be a model feature")
    if metadata["leakage_check"]["target_columns_as_model_features"]:
        raise ValueError("target columns must not be model features")

    TRAINING_METADATA_JSON.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text(render_report(metadata), encoding="utf-8")

    print("Full universe dataset build complete")
    print(f"Universe count: {metadata['universe_count']}")
    print(f"OHLCV row count: {metadata['raw_ohlcv_row_count']}")
    print(f"Cleaned row count: {metadata['cleaned_ohlcv_row_count']}")
    print(f"Feature shape: {tuple(metadata['feature_shape'])}")
    print(f"Feature count: {metadata['feature_count']}")
    print(f"Training dataset shape: {tuple(metadata['training_dataset_shape'])}")
    print(f"005930 exists: {metadata['ticker_005930_exists']}")
    print(f"Leakage check: {metadata['leakage_check']}")


if __name__ == "__main__":
    main()
