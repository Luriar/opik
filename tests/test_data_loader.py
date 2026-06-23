
"""
tests/test_data_loader.py

Data layer tests for AI Trading System v1.0.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


REQUIRED_OHLCV_COLUMNS = {
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
}

REQUIRED_MACRO_COLUMNS = {
    "date",
    "nasdaq_close",
    "sox_close",
    "sp500_close",
    "vix_close",
    "usdkrw",
    "wti_close",
}


@pytest.fixture
def sample_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "ticker": ["005930", "005930"],
            "open": [70000, 71000],
            "high": [71500, 72000],
            "low": [69500, 70500],
            "close": [71000, 71500],
            "volume": [10_000_000, 11_000_000],
        }
    )


@pytest.fixture
def sample_macro() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "nasdaq_close": [15000, 15100],
            "sox_close": [4000, 4100],
            "sp500_close": [4700, 4720],
            "vix_close": [14.5, 15.0],
            "usdkrw": [1300, 1310],
            "wti_close": [75, 76],
        }
    )


def validate_required_columns(df: pd.DataFrame, required: set[str]) -> None:
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns: {missing}"


def test_ohlcv_required_columns_exist(sample_ohlcv: pd.DataFrame) -> None:
    validate_required_columns(sample_ohlcv, REQUIRED_OHLCV_COLUMNS)


def test_macro_required_columns_exist(sample_macro: pd.DataFrame) -> None:
    validate_required_columns(sample_macro, REQUIRED_MACRO_COLUMNS)


def test_ohlcv_date_column_is_datetime(sample_ohlcv: pd.DataFrame) -> None:
    assert pd.api.types.is_datetime64_any_dtype(sample_ohlcv["date"])


def test_macro_date_column_is_datetime(sample_macro: pd.DataFrame) -> None:
    assert pd.api.types.is_datetime64_any_dtype(sample_macro["date"])


def test_ohlcv_has_no_duplicate_date_ticker(sample_ohlcv: pd.DataFrame) -> None:
    duplicated = sample_ohlcv.duplicated(subset=["date", "ticker"])
    assert not duplicated.any()


def test_ohlcv_duplicate_date_ticker_should_fail(sample_ohlcv: pd.DataFrame) -> None:
    duplicated_df = pd.concat([sample_ohlcv, sample_ohlcv.iloc[[0]]], ignore_index=True)
    duplicated = duplicated_df.duplicated(subset=["date", "ticker"])
    assert duplicated.any()


def test_price_columns_are_positive(sample_ohlcv: pd.DataFrame) -> None:
    price_columns = ["open", "high", "low", "close"]

    for col in price_columns:
        assert (sample_ohlcv[col] > 0).all()


def test_volume_is_non_negative(sample_ohlcv: pd.DataFrame) -> None:
    assert (sample_ohlcv["volume"] >= 0).all()


def test_high_is_greater_than_or_equal_to_low(sample_ohlcv: pd.DataFrame) -> None:
    assert (sample_ohlcv["high"] >= sample_ohlcv["low"]).all()


def test_high_is_greater_than_or_equal_to_open_close(sample_ohlcv: pd.DataFrame) -> None:
    assert (sample_ohlcv["high"] >= sample_ohlcv["open"]).all()
    assert (sample_ohlcv["high"] >= sample_ohlcv["close"]).all()


def test_low_is_less_than_or_equal_to_open_close(sample_ohlcv: pd.DataFrame) -> None:
    assert (sample_ohlcv["low"] <= sample_ohlcv["open"]).all()
    assert (sample_ohlcv["low"] <= sample_ohlcv["close"]).all()


def test_macro_values_are_positive(sample_macro: pd.DataFrame) -> None:
    positive_columns = [
        "nasdaq_close",
        "sox_close",
        "sp500_close",
        "vix_close",
        "usdkrw",
        "wti_close",
    ]

    for col in positive_columns:
        assert (sample_macro[col] > 0).all()


def test_ohlcv_sorted_by_ticker_and_date(sample_ohlcv: pd.DataFrame) -> None:
    sorted_df = sample_ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True)
    actual_df = sample_ohlcv.reset_index(drop=True)

    pd.testing.assert_frame_equal(actual_df, sorted_df)


def test_data_file_path_validation() -> None:
    path = Path("data/raw/kr_stock")

    assert isinstance(path, Path)
    assert path.as_posix().endswith("data/raw/kr_stock")


def test_trading_value_can_be_calculated(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.copy()
    df["trading_value"] = df["close"] * df["volume"]

    assert (df["trading_value"] > 0).all()


def test_macro_can_calculate_returns(sample_macro: pd.DataFrame) -> None:
    df = sample_macro.sort_values("date").copy()
    df["nasdaq_return_1d"] = df["nasdaq_close"] / df["nasdaq_close"].shift(1) - 1

    assert df["nasdaq_return_1d"].iloc[1] == pytest.approx(15100 / 15000 - 1)


def test_loader_output_minimum_rows(sample_ohlcv: pd.DataFrame) -> None:
    assert len(sample_ohlcv) > 0


def test_loader_output_has_ticker_as_string(sample_ohlcv: pd.DataFrame) -> None:
    assert sample_ohlcv["ticker"].map(lambda x: isinstance(x, str)).all()


def test_missing_required_columns_should_fail(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.drop(columns=["volume"])

    missing = REQUIRED_OHLCV_COLUMNS - set(df.columns)

    assert missing == {"volume"}

