"""Download real Korean OHLCV and macro market data."""

from __future__ import annotations

import sys
from dataclasses import dataclass
import os
from pathlib import Path
from time import sleep

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("MPLCONFIGDIR", str(PROJECT_ROOT / "data" / "raw" / ".mplconfig"))


START_DATE = "2023-06-15"
END_DATE = "2026-06-14"
START_COMPACT = "20230615"
END_COMPACT = "20260614"

KR_STOCK_DIR = PROJECT_ROOT / "data" / "raw" / "kr_stock"
MACRO_DIR = PROJECT_ROOT / "data" / "raw" / "macro"
KR_PARQUET = KR_STOCK_DIR / f"ohlcv_{START_COMPACT}_{END_COMPACT}.parquet"
KR_CSV = KR_STOCK_DIR / f"ohlcv_{START_COMPACT}_{END_COMPACT}.csv"
MACRO_PARQUET = MACRO_DIR / f"macro_{START_COMPACT}_{END_COMPACT}.parquet"
MACRO_CSV = MACRO_DIR / f"macro_{START_COMPACT}_{END_COMPACT}.csv"

KOSPI200_INDEX_CODE = "1028"
KOSDAQ150_INDEX_CODE = "2203"
SAMSUNG_ELECTRONICS = "005930"
FALLBACK_UNIVERSE_TICKERS = sorted(
    {
        # KOSPI large/liquid names, including Samsung Electronics.
        "000270",
        "000660",
        "000810",
        "003550",
        "003670",
        "005380",
        "005490",
        "005930",
        "006400",
        "009150",
        "009540",
        "009830",
        "010130",
        "010140",
        "010950",
        "011070",
        "011200",
        "012330",
        "015760",
        "017670",
        "018260",
        "021240",
        "024110",
        "028260",
        "030200",
        "032830",
        "033780",
        "034020",
        "034730",
        "035420",
        "035720",
        "036570",
        "042660",
        "047810",
        "051910",
        "055550",
        "066570",
        "068270",
        "086280",
        "086790",
        "090430",
        "096770",
        "105560",
        "138040",
        "161390",
        "180640",
        "207940",
        "251270",
        "267260",
        "271560",
        "316140",
        "326030",
        "373220",
        # KOSDAQ large/liquid names.
        "005290",
        "025900",
        "028300",
        "033640",
        "035900",
        "036540",
        "036930",
        "039030",
        "041510",
        "058470",
        "060250",
        "064550",
        "067310",
        "068760",
        "078600",
        "084850",
        "085660",
        "086520",
        "089030",
        "091990",
        "095340",
        "095660",
        "099190",
        "101490",
        "108860",
        "112040",
        "121600",
        "122870",
        "131970",
        "140860",
        "145020",
        "182400",
        "195940",
        "196170",
        "206650",
        "214150",
        "214370",
        "215200",
        "222800",
        "240810",
        "247540",
        "253450",
        "263750",
        "277810",
        "290650",
        "293490",
        "348370",
        "357780",
        "403870",
    }
)


@dataclass(frozen=True)
class DownloadSummary:
    """Summary statistics for downloaded datasets."""

    row_count: int
    unique_ticker_count: int | None
    min_date: str | None
    max_date: str | None
    samsung_exists: bool | None
    missing_values: dict[str, int]


def _require_dependencies() -> None:
    """Fail with a helpful message if market-data dependencies are unavailable."""
    missing: list[str] = []
    for package in ("pykrx", "yfinance"):
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    if missing:
        raise ImportError(
            "Missing required market-data packages: "
            f"{', '.join(missing)}. Install them with `python -m pip install {' '.join(missing)}`."
        )


def get_universe_tickers() -> list[str]:
    """Return KOSPI200 + KOSDAQ150 tickers, forcing Samsung Electronics when available."""
    from pykrx import stock

    tickers: set[str] = set()
    for index_code in (KOSPI200_INDEX_CODE, KOSDAQ150_INDEX_CODE):
        try:
            constituents = stock.get_index_portfolio_deposit_file(index_code, END_COMPACT)
            tickers.update(_coerce_ticker_list(constituents))
        except Exception as exc:
            print(f"Index constituent lookup failed for {index_code}: {exc}")

    if not tickers:
        print(
            "pykrx returned no index constituents; using fallback liquid KOSPI/KOSDAQ seed universe."
        )
        tickers.update(FALLBACK_UNIVERSE_TICKERS)

    all_tickers = set(_coerce_ticker_list(stock.get_market_ticker_list(END_COMPACT, market="ALL")))
    if not all_tickers or SAMSUNG_ELECTRONICS in all_tickers:
        tickers.add(SAMSUNG_ELECTRONICS)

    return sorted(tickers)


def download_korean_ohlcv(tickers: list[str]) -> pd.DataFrame:
    """Download OHLCV for each ticker from pykrx."""
    from pykrx import stock

    frames: list[pd.DataFrame] = []
    total = len(tickers)
    for idx, ticker in enumerate(tickers, start=1):
        print(f"[{idx:03d}/{total:03d}] Downloading Korean OHLCV: {ticker}")
        try:
            data = stock.get_market_ohlcv_by_date(START_COMPACT, END_COMPACT, ticker)
        except Exception as exc:
            print(f"  skipped {ticker}: {exc}")
            continue
        if data.empty:
            print(f"  skipped {ticker}: empty data")
            continue
        normalized = _normalize_krx_ohlcv(data, ticker)
        frames.append(normalized)
        sleep(0.05)

    if not frames:
        raise RuntimeError("No Korean OHLCV data was downloaded")
    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)
    duplicate_mask = result.duplicated(subset=["date", "ticker"])
    if duplicate_mask.any():
        raise ValueError("Downloaded Korean OHLCV contains duplicate date/ticker rows")
    return result


def download_macro_data() -> pd.DataFrame:
    """Download macro/global data from yfinance."""
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
        close = _extract_close_series(data, output_column)
        frames.append(close)

    result = frames[0]
    for frame in frames[1:]:
        result = result.merge(frame, on="date", how="outer")
    result = result.sort_values("date").reset_index(drop=True)
    result = result[(result["date"] >= START_DATE) & (result["date"] <= END_DATE)]
    return result.reset_index(drop=True)


def save_outputs(ohlcv: pd.DataFrame, macro: pd.DataFrame) -> None:
    """Save downloaded datasets to CSV and parquet."""
    KR_STOCK_DIR.mkdir(parents=True, exist_ok=True)
    MACRO_DIR.mkdir(parents=True, exist_ok=True)

    ohlcv.to_csv(KR_CSV, index=False)
    ohlcv.to_parquet(KR_PARQUET, index=False)
    macro.to_csv(MACRO_CSV, index=False)
    macro.to_parquet(MACRO_PARQUET, index=False)


def summarize_ohlcv(ohlcv: pd.DataFrame) -> DownloadSummary:
    """Build summary for Korean OHLCV output."""
    return DownloadSummary(
        row_count=int(len(ohlcv)),
        unique_ticker_count=int(ohlcv["ticker"].nunique()),
        min_date=_date_to_string(ohlcv["date"].min()),
        max_date=_date_to_string(ohlcv["date"].max()),
        samsung_exists=bool((ohlcv["ticker"] == SAMSUNG_ELECTRONICS).any()),
        missing_values={column: int(value) for column, value in ohlcv.isna().sum().items()},
    )


def summarize_macro(macro: pd.DataFrame) -> DownloadSummary:
    """Build summary for macro output."""
    return DownloadSummary(
        row_count=int(len(macro)),
        unique_ticker_count=None,
        min_date=_date_to_string(macro["date"].min()),
        max_date=_date_to_string(macro["date"].max()),
        samsung_exists=None,
        missing_values={column: int(value) for column, value in macro.isna().sum().items()},
    )


def print_summary(name: str, summary: DownloadSummary) -> None:
    """Print a download summary."""
    print(f"\n{name} summary")
    print(f"row_count: {summary.row_count}")
    if summary.unique_ticker_count is not None:
        print(f"unique_ticker_count: {summary.unique_ticker_count}")
    print(f"min_date: {summary.min_date}")
    print(f"max_date: {summary.max_date}")
    if summary.samsung_exists is not None:
        print(f"ticker_005930_exists: {summary.samsung_exists}")
    print("missing_value_summary:")
    for column, missing_count in summary.missing_values.items():
        print(f"  {column}: {missing_count}")


def _normalize_krx_ohlcv(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    result = data.reset_index().copy()
    rename_map = {
        "날짜": "date",
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
        "거래대금": "trading_value",
        "Date": "date",
    }
    result = result.rename(columns=rename_map)
    required = ["date", "open", "high", "low", "close", "volume", "trading_value"]
    missing = set(required) - set(result.columns)
    if missing == {"trading_value"}:
        result["trading_value"] = result["close"] * result["volume"]
    elif missing:
        raise ValueError(f"Missing KRX OHLCV columns for {ticker}: {sorted(missing)}")
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = str(ticker).zfill(6)
    return result.loc[:, ["date", "ticker", "open", "high", "low", "close", "volume", "trading_value"]]


def _extract_close_series(data: pd.DataFrame, output_column: str) -> pd.DataFrame:
    close = data["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    result = close.rename(output_column).reset_index()
    result = result.rename(columns={"Date": "date"})
    result["date"] = pd.to_datetime(result["date"]).dt.tz_localize(None)
    return result.loc[:, ["date", output_column]]


def _coerce_ticker_list(value: object) -> list[str]:
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return []
        for column in ("티커", "ticker", "종목코드"):
            if column in value.columns:
                return [str(item).zfill(6) for item in value[column].dropna().tolist()]
        return [str(item).zfill(6) for item in value.index.dropna().tolist()]
    if isinstance(value, pd.Index):
        return [str(item).zfill(6) for item in value.dropna().tolist()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).zfill(6) for item in value if pd.notna(item)]
    return []


def _date_to_string(value: object) -> str | None:
    if pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def main() -> None:
    """Download and save real market datasets."""
    _require_dependencies()
    print(f"Downloading real market data from {START_DATE} to {END_DATE}")
    tickers = get_universe_tickers()
    print(f"Universe ticker count: {len(tickers)}")
    print(f"Samsung Electronics in universe: {SAMSUNG_ELECTRONICS in tickers}")

    ohlcv = download_korean_ohlcv(tickers)
    macro = download_macro_data()
    save_outputs(ohlcv, macro)

    print(f"\nSaved Korean OHLCV CSV: {KR_CSV}")
    print(f"Saved Korean OHLCV parquet: {KR_PARQUET}")
    print(f"Saved macro CSV: {MACRO_CSV}")
    print(f"Saved macro parquet: {MACRO_PARQUET}")

    print_summary("Korean OHLCV", summarize_ohlcv(ohlcv))
    print_summary("Macro", summarize_macro(macro))


if __name__ == "__main__":
    main()
