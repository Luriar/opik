"""OPIK Daily OHLCV Collection DAG

Downloads Korean stock OHLCV data every trading day at 06:00 KST,
before the model prediction DAG runs at 06:30 KST.

Uses FinanceDataReader (primary) with pykrx fallback to ensure
fresh market data is available for the model training window.

Output: Appends to data/raw/kr_stock/ohlcv_full_universe_*.parquet
         and data/processed/kr_stock/ohlcv_full_universe_clean_*.parquet
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

logger = logging.getLogger("opik.ohlcv_dag")

OPIK_ROOT = "/opt/airflow/opik"


def _download_daily_ohlcv(**context):
    """Download OHLCV for the previous trading day via FinanceDataReader.

    FinanceDataReader provides faster batch OHLCV than per-ticker pykrx,
    and supports KOSPI + KOSDAQ universe in a single call.
    """
    import sys
    import os
    sys.path.insert(0, OPIK_ROOT)

    exec_date = context["execution_date"]
    target_date = exec_date.strftime("%Y%m%d")

    import FinanceDataReader as fdr
    import pandas as pd
    from pathlib import Path

    # Load ticker universe
    universe_path = Path(OPIK_ROOT) / "data" / "metadata" / "full_universe_260616.csv"
    if not universe_path.exists():
        logger.warning(f"Universe file not found: {universe_path}, using KOSPI+KOSDAQ default")
        # Fallback: download all KOSPI + KOSDAQ
        kospi = fdr.StockListing("KOSPI")
        kosdaq = fdr.StockListing("KOSDAQ")
        universe = pd.concat([kospi, kosdaq], ignore_index=True)
    else:
        universe = pd.read_csv(universe_path, dtype={"ticker": str})

    tickers = universe["ticker"].dropna().unique().tolist()
    logger.info(f"Downloading OHLCV for {target_date}, {len(tickers)} tickers")

    # FinanceDataReader batch download
    all_data = []
    failed = []
    for ticker in tickers:
        try:
            df = fdr.DataReader(ticker, exec_date.strftime("%Y-%m-%d"), exec_date.strftime("%Y-%m-%d"))
            if not df.empty:
                df = df.reset_index()
                df["ticker"] = str(ticker).zfill(6)
                df = df.rename(columns={
                    "Date": "date", "Open": "open", "High": "high",
                    "Low": "low", "Close": "close", "Volume": "volume",
                })
                if "trading_value" not in df.columns:
                    df["trading_value"] = df["close"] * df["volume"]
                cols = ["date", "ticker", "open", "high", "low", "close", "volume", "trading_value"]
                all_data.append(df[[c for c in cols if c in df.columns]])
        except Exception as e:
            failed.append(str(ticker))
            continue

    if not all_data:
        logger.error(f"No OHLCV data downloaded for {target_date} from FinanceDataReader")
        # Fallback: try pykrx
        logger.info("Falling back to pykrx per-ticker download")
        try:
            from pykrx import stock
            compact = target_date
            for ticker in tickers[:50]:  # Limit fallback to 50 tickers
                try:
                    raw = stock.get_market_ohlcv_by_date(compact, compact, ticker)
                    if not raw.empty:
                        raw = raw.reset_index()
                        raw["ticker"] = str(ticker).zfill(6)
                        raw = raw.rename(columns={
                            "날짜": "date", "시가": "open", "고가": "high",
                            "저가": "low", "종가": "close", "거래량": "volume",
                        })
                        raw["trading_value"] = raw["close"] * raw["volume"]
                        cols = ["date", "ticker", "open", "high", "low", "close", "volume", "trading_value"]
                        all_data.append(raw[[c for c in cols if c in raw.columns]])
                except Exception:
                    continue
        except ImportError:
            logger.error("pykrx not available for fallback")

    if not all_data:
        logger.error(f"CRITICAL: No OHLCV data obtained for {target_date}")
        return {"date": target_date, "tickers": 0, "rows": 0, "failed": len(failed)}

    result_df = pd.concat(all_data, ignore_index=True)
    result_df["date"] = pd.to_datetime(result_df["date"]).dt.normalize()
    result_df["ticker"] = result_df["ticker"].astype(str).str.zfill(6)
    for col in ["open", "high", "low", "close", "volume", "trading_value"]:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

    # Append to raw OHLCV file
    raw_path = Path(OPIK_ROOT) / "data" / "raw" / "kr_stock" / "ohlcv_full_universe_20230615_20260614.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_path.exists():
        existing = pd.read_parquet(raw_path)
        combined = pd.concat([existing, result_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    else:
        combined = result_df
    combined.to_parquet(raw_path, index=False)
    logger.info(f"Raw OHLCV: {len(combined)} total rows, +{len(result_df)} new")

    # Filter valid rows for clean file
    valid = result_df[
        (result_df["open"] > 0) & (result_df["high"] > 0)
        & (result_df["low"] > 0) & (result_df["close"] > 0)
        & (result_df["high"] >= result_df["low"])
    ].copy()

    clean_path = Path(OPIK_ROOT) / "data" / "processed" / "kr_stock" / "ohlcv_full_universe_clean_20230615_20260614.parquet"
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    if clean_path.exists():
        existing_clean = pd.read_parquet(clean_path)
        combined_clean = pd.concat([existing_clean, valid], ignore_index=True)
        combined_clean = combined_clean.drop_duplicates(subset=["date", "ticker"], keep="last")
    else:
        combined_clean = valid
    combined_clean.to_parquet(clean_path, index=False)
    logger.info(f"Clean OHLCV: {len(combined_clean)} total rows, +{len(valid)} new")

    return {
        "date": target_date,
        "tickers": int(result_df["ticker"].nunique()),
        "raw_rows": len(result_df),
        "clean_rows": len(valid),
        "total_raw": len(combined),
        "total_clean": len(combined_clean),
        "failed": len(failed),
    }


default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="daily_ohlcv_collection",
    default_args=default_args,
    schedule="0 6 * * 1-5",
    start_date=pendulum.datetime(2026, 6, 22, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["opik", "market-data", "ohlcv"],
    description="Daily Korean stock OHLCV download (06:00 KST, Mon-Fri) via FinanceDataReader",
) as dag:

    download_ohlcv = PythonOperator(
        task_id="download_daily_ohlcv",
        python_callable=_download_daily_ohlcv,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
    )

    download_ohlcv
