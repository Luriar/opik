"""OPIK Model Prediction Script — Phase 2 integration.

Runs daily model training + prediction generation + S3 Gold upload.
Called by Airflow DAG at 06:00 KST daily, after US market close.

Usage:
    python scripts/model/run_prediction.py --date 20260619
    python scripts/model/run_prediction.py --date 20260619 --project-root /path/to/data
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Ensure OPIK project root is on sys.path
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parents[2]  # scripts/model -> scripts -> opik root
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.model.pipeline.config import load_daily_update_config, ensure_daily_directories
from src.model.pipeline.daily_context import build_daily_run_context
from src.model.pipeline.daily_model import train_daily_models
from src.model.pipeline.daily_prediction import generate_daily_predictions


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run model training + prediction + S3 upload.")
    parser.add_argument(
        "--date", required=True,
        help="Prediction date as YYYYMMDD.",
    )
    parser.add_argument(
        "--config", default="configs/daily_update.yaml",
        help="Path to daily update YAML config.",
    )
    parser.add_argument(
        "--project-root", default=None,
        help="Override project root for data paths (default: auto-detect via pyproject.toml).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without writing files.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    try:
        pred_date = datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print(f"ERROR: --date must be YYYYMMDD, got '{args.date}'", file=sys.stderr)
        return 2

    # Override project root if specified (for data path resolution)
    if args.project_root:
        import src.model.utils.paths as _paths_mod
        data_root = Path(args.project_root).resolve()
        _paths_mod.get_project_root = lambda start=None: data_root
        print(f"Project root overridden: {data_root}")

    config = load_daily_update_config(args.config)
    print(f"Config: {config.config_path}")
    print(f"Project root: {config.project_root}")

    # Build date context (skip download phases — model pipeline only)
    # We use build_daily_run_context for date calculation, but skip OHLCV download.
    # The key dates are: update_date (last trading day), prediction_date (next trading day).
    from src.model.pipeline.daily_context import DailyRunContext

    as_of_date = pred_date.strftime("%Y-%m-%d")

    # Simplified context: model pipeline only needs prediction_date for training window.
    # download-related flags are always False since this pipeline doesn't download.
    context = DailyRunContext(
        run_date=as_of_date,
        as_of_date=as_of_date,
        latest_clean_data_date=as_of_date,
        target_update_date=as_of_date,
        update_date=as_of_date,
        prediction_date=as_of_date,
        dry_run=args.dry_run,
        skip_download=True,
        force=False,
        warnings=[],
    )

    print(f"Update date: {context.update_date}")
    print(f"Prediction date: {context.prediction_date}")

    # Ensure output directories exist
    ensure_daily_directories(config, dry_run=args.dry_run)

    if args.dry_run:
        print("DRY RUN — skipping model training and prediction writes")
        return 0

    # Step 1: Train models
    print("Training daily models...")
    model_result = train_daily_models(
        config=config,
        prediction_date=context.prediction_date,
    )
    print(f"Models trained: {model_result.model_output_dir}")
    print(f"Features: {len(model_result.feature_columns)}")
    print(f"Train window: {model_result.train_start_date} -> {model_result.train_end_date}")
    print(f"Train rows: {model_result.rolling_train_rows}")

    # Step 2: Generate predictions (includes S3 Gold upload)
    print("Generating predictions...")
    prediction_result = generate_daily_predictions(
        config=config,
        model_bundle=model_result.model_bundle,
        update_date=context.update_date,
        prediction_date=context.prediction_date,
        train_start_date=model_result.train_start_date,
        train_end_date=model_result.train_end_date,
        rolling_train_days=model_result.rolling_train_days,
    )
    print(f"Predictions: {prediction_result.prediction_rows} rows")
    print(f"Local CSV: {prediction_result.prediction_output_csv}")
    print(f"Local Parquet: {prediction_result.prediction_output_parquet}")
    print(f"S3 Gold: s3://s3-opik-bucket/{prediction_result.s3_key}")

    print("Model pipeline completed successfully.")
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        return run(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
