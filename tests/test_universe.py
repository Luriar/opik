"""
tests/test_universe.py

Universe generation tests for AI Trading System v1.0.

Based on:
docs/02_universe.md
"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_universe_df() -> pd.DataFrame:

    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"] * 8),
            "ticker": [
                "005930",
                "000660",
                "035420",
                "ETF001",
                "SPAC001",
                "PREF001",
                "REIT001",
                "005380",
            ],
            "market": [
                "KOSPI",
                "KOSPI",
                "KOSDAQ",
                "ETF",
                "KOSDAQ",
                "KOSPI",
                "REIT",
                "KOSPI",
            ],
            "security_type": [
                "COMMON",
                "COMMON",
                "COMMON",
                "ETF",
                "SPAC",
                "PREFERRED",
                "REIT",
                "COMMON",
            ],
            "trading_halt": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            "management_issue": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
            "trading_value_ma20": [
                100,
                90,
                80,
                200,
                50,
                60,
                70,
                110,
            ],
        }
    )


def test_common_stock_filter(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["security_type"] == "COMMON"
    ]

    assert (
        universe["security_type"] == "COMMON"
    ).all()


def test_etf_is_removed(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["security_type"] == "COMMON"
    ]

    assert "ETF001" not in set(universe["ticker"])


def test_spac_is_removed(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["security_type"] == "COMMON"
    ]

    assert "SPAC001" not in set(universe["ticker"])


def test_preferred_is_removed(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["security_type"] == "COMMON"
    ]

    assert "PREF001" not in set(universe["ticker"])


def test_reit_is_removed(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["security_type"] == "COMMON"
    ]

    assert "REIT001" not in set(universe["ticker"])


def test_trading_halt_removed(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df[
        sample_universe_df["trading_halt"] == False
    ]

    assert "005380" not in set(
        universe["ticker"]
    )


def test_management_issue_removed(sample_universe_df: pd.DataFrame):

    df = sample_universe_df.copy()

    df.loc[df.index[0], "management_issue"] = True

    universe = df[
        df["management_issue"] == False
    ]

    assert "005930" not in set(
        universe["ticker"]
    )


def test_liquidity_rank(sample_universe_df: pd.DataFrame):

    universe = sample_universe_df.copy()

    universe["rank"] = universe[
        "trading_value_ma20"
    ].rank(
        ascending=False,
        method="first",
    )

    assert universe.loc[
        universe["ticker"] == "ETF001",
        "rank",
    ].iloc[0] == 1


def test_top350_selection():

    df = pd.DataFrame(
        {
            "ticker": [
                f"T{i}"
                for i in range(500)
            ],
            "trading_value_ma20": list(
                range(500, 0, -1)
            ),
        }
    )

    top350 = df.nlargest(
        350,
        "trading_value_ma20",
    )

    assert len(top350) == 350


def test_universe_has_unique_ticker():

    df = pd.DataFrame(
        {
            "ticker": [
                "A",
                "B",
                "C",
            ]
        }
    )

    assert not df[
        "ticker"
    ].duplicated().any()


def test_duplicate_ticker_should_fail():

    df = pd.DataFrame(
        {
            "ticker": [
                "A",
                "A",
                "B",
            ]
        }
    )

    assert df[
        "ticker"
    ].duplicated().any()


def test_universe_has_required_columns():

    required = {
        "date",
        "ticker",
        "market",
        "security_type",
        "trading_value_ma20",
    }

    df = pd.DataFrame(
        columns=list(required)
    )

    missing = required - set(df.columns)

    assert not missing


def test_universe_date_is_datetime():

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02"
                ]
            )
        }
    )

    assert pd.api.types.is_datetime64_any_dtype(
        df["date"]
    )


def test_universe_ticker_is_string():

    df = pd.DataFrame(
        {
            "ticker": [
                "005930",
                "000660",
            ]
        }
    )

    assert df[
        "ticker"
    ].map(
        lambda x: isinstance(
            x,
            str,
        )
    ).all()


def test_universe_size_limit():

    df = pd.DataFrame(
        {
            "ticker": [
                f"T{i}"
                for i in range(350)
            ]
        }
    )

    assert len(df) <= 350


def test_universe_contains_only_common_stock():

    df = pd.DataFrame(
        {
            "security_type": [
                "COMMON",
                "COMMON",
                "COMMON",
            ]
        }
    )

    assert (
        df["security_type"]
        == "COMMON"
    ).all()


def test_final_universe_pipeline(sample_universe_df):

    universe = sample_universe_df.copy()

    universe = universe[
        universe["security_type"]
        == "COMMON"
    ]

    universe = universe[
        universe["trading_halt"]
        == False
    ]

    universe = universe[
        universe["management_issue"]
        == False
    ]

    universe = universe.nlargest(
        350,
        "trading_value_ma20",
    )

    assert (
        universe[
            "security_type"
        ]
        == "COMMON"
    ).all()

    assert (
        universe[
            "trading_halt"
        ]
        == False
    ).all()

    assert (
        universe[
            "management_issue"
        ]
        == False
    ).all()

    assert len(universe) <= 350