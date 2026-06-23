"""Create human-readable rolling validation prediction reports."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "outputs" / "walk_forward_real_rolling" / "predictions.csv"
TICKER_NAME_PATH = PROJECT_ROOT / "data" / "metadata" / "ticker_names.csv"
REPORT_DIR = PROJECT_ROOT / "reports"
READABLE_CSV = REPORT_DIR / "validation_predictions_readable.csv"
READABLE_XLSX = REPORT_DIR / "validation_predictions_readable.xlsx"
TOP10_CSV = REPORT_DIR / "validation_predictions_top10_by_date.csv"
TOP10_XLSX = REPORT_DIR / "validation_predictions_top10_by_date.xlsx"
SUMMARY_MD = REPORT_DIR / "validation_predictions_readable_summary.md"

READABLE_COLUMNS = [
    "date",
    "ticker",
    "ticker_name",
    "ranking_score",
    "ranking_score_pct",
    "expected_return",
    "expected_return_pct",
    "pred_gap",
    "pred_gap_pct",
    "pred_intraday",
    "pred_intraday_pct",
    "prev_close",
    "pred_open_price",
    "pred_close_price",
    "target_gap",
    "target_gap_pct",
    "target_intraday",
    "target_intraday_pct",
    "target_ranking",
    "model_version",
    "train_start_date",
    "train_end_date",
    "prediction_date",
]

PCT_COLUMNS = {
    "ranking_score": "ranking_score_pct",
    "expected_return": "expected_return_pct",
    "pred_gap": "pred_gap_pct",
    "pred_intraday": "pred_intraday_pct",
    "target_gap": "target_gap_pct",
    "target_intraday": "target_intraday_pct",
}


def format_pct(value: float | int | None) -> str:
    """Format a decimal value as a human-readable percentage."""
    if pd.isna(value):
        return ""
    return f"{float(value) * 100:.2f}%"


def load_predictions() -> pd.DataFrame:
    """Load rolling walk-forward predictions without modifying the source file."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing prediction file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    for column in ["date", "train_start_date", "train_end_date", "prediction_date"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    return df


def load_offline_ticker_names() -> dict[str, str]:
    """Load local ticker name mappings when available."""
    if not TICKER_NAME_PATH.exists():
        return {}
    mapping = pd.read_csv(TICKER_NAME_PATH, dtype={"ticker": str, "ticker_name": str})
    required = {"ticker", "ticker_name"}
    missing = required - set(mapping.columns)
    if missing:
        raise ValueError(f"ticker_names.csv is missing columns: {sorted(missing)}")
    mapping["ticker"] = mapping["ticker"].astype(str).str.zfill(6)
    mapping["ticker_name"] = mapping["ticker_name"].fillna("").astype(str).str.strip()
    mapping = mapping[mapping["ticker_name"].ne("")]
    return dict(zip(mapping["ticker"], mapping["ticker_name"], strict=False))


def ticker_name_lookup(tickers: list[str]) -> tuple[dict[str, str], list[str], list[str]]:
    """Map ticker names offline first, then try pykrx for unresolved tickers."""
    offline_names = load_offline_ticker_names()
    names: dict[str, str] = {}
    failed: list[str] = []
    offline_mapped = sorted(set(tickers) & set(offline_names))

    for ticker in tickers:
        if ticker in offline_names:
            names[ticker] = offline_names[ticker]

    missing_tickers = [ticker for ticker in tickers if ticker not in names]
    if not missing_tickers:
        return names, failed, offline_mapped

    try:
        from pykrx import stock
    except Exception:
        names.update({ticker: "UNKNOWN" for ticker in missing_tickers})
        return names, missing_tickers, offline_mapped

    for ticker in missing_tickers:
        try:
            name = stock.get_market_ticker_name(ticker)
        except Exception:
            name = ""
        if not name:
            names[ticker] = "UNKNOWN"
            failed.append(ticker)
        else:
            names[ticker] = name
    return names, failed, offline_mapped


def make_readable(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Add ticker names, percentage columns, sorting, and column order."""
    tickers = sorted(df["ticker"].unique())
    names, failed, offline_mapped = ticker_name_lookup(tickers)

    readable = df.copy()
    readable["ticker_name"] = readable["ticker"].map(names).fillna("UNKNOWN")
    for source, target in PCT_COLUMNS.items():
        readable[target] = readable[source].map(format_pct)
    readable = readable.sort_values(
        ["date", "ranking_score"],
        ascending=[True, False],
    ).reset_index(drop=True)
    return readable.loc[:, READABLE_COLUMNS].copy(), failed, offline_mapped


def make_top10(readable: pd.DataFrame) -> pd.DataFrame:
    """Select the daily Top10 rows by ranking score."""
    top10 = readable.copy()
    top10["rank"] = top10.groupby("date")["ranking_score"].rank(
        method="first",
        ascending=False,
    ).astype(int)
    top10 = top10[top10["rank"] <= 10].copy()
    columns = ["date", "rank"] + [column for column in READABLE_COLUMNS if column != "date"]
    return top10.loc[:, columns].sort_values(["date", "rank"]).reset_index(drop=True)


def save_xlsx(path: Path, df: pd.DataFrame, sheet_name: str) -> None:
    """Save a simple, filterable workbook."""
    with pd.ExcelWriter(path) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
        for column_cells in worksheet.columns:
            header = str(column_cells[0].value)
            sample_lengths = [len(str(cell.value)) for cell in column_cells[:200] if cell.value is not None]
            width = min(max([len(header), *sample_lengths]) + 2, 28)
            worksheet.column_dimensions[column_cells[0].column_letter].width = width


def render_summary(
    readable: pd.DataFrame,
    top10: pd.DataFrame,
    failed_tickers: list[str],
    offline_mapped: list[str],
) -> str:
    """Render summary Markdown."""
    top20 = (
        top10.groupby(["ticker", "ticker_name"])
        .size()
        .reset_index(name="top10_appearances")
        .sort_values(["top10_appearances", "ticker"], ascending=[False, True])
        .head(20)
    )
    failed_text = ", ".join(failed_tickers) if failed_tickers else "None"
    offline_text = ", ".join(offline_mapped) if offline_mapped else "None"
    unknown_unique = sorted(readable.loc[readable["ticker_name"].eq("UNKNOWN"), "ticker"].unique())
    unknown_text = ", ".join(unknown_unique) if unknown_unique else "None"
    top20_table = top20.to_markdown(index=False)
    return "\n".join(
        [
            "# Validation Predictions Readable Summary",
            "",
            f"- Total rows: {len(readable)}",
            f"- Date range: {readable['date'].min().date()} to {readable['date'].max().date()}",
            f"- Unique dates: {readable['date'].nunique()}",
            f"- Unique tickers: {readable['ticker'].nunique()}",
            f"- Known offline mapped tickers: {offline_text}",
            f"- Unknown ticker name rows: {int(readable['ticker_name'].eq('UNKNOWN').sum())}",
            f"- Unknown unique tickers: {len(unknown_unique)}",
            f"- Unknown ticker list: {unknown_text}",
            f"- Failed ticker lookups: {failed_text}",
            f"- 005930 exists: {bool(readable['ticker'].eq('005930').any())}",
            f"- 005930 ticker_name: {readable.loc[readable['ticker'].eq('005930'), 'ticker_name'].iloc[0] if readable['ticker'].eq('005930').any() else 'MISSING'}",
            f"- Top10 rows: {len(top10)}",
            f"- Average expected_return of Top10: {top10['expected_return'].mean():.8f}",
            f"- Average ranking_score of Top10: {top10['ranking_score'].mean():.8f}",
            "",
            "## Top 20 Daily Top10 Tickers",
            "",
            top20_table,
            "",
        ]
    )


def main() -> None:
    """Create readable prediction report files."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    print("Loading rolling walk-forward predictions...")
    predictions = load_predictions()
    print("Adding ticker names and formatted percentage columns...")
    readable, failed_tickers, offline_mapped = make_readable(predictions)
    top10 = make_top10(readable)

    print("Saving CSV and XLSX reports...")
    readable.to_csv(READABLE_CSV, index=False, encoding="utf-8-sig")
    top10.to_csv(TOP10_CSV, index=False, encoding="utf-8-sig")
    save_xlsx(READABLE_XLSX, readable, "ValidationPredictions")
    save_xlsx(TOP10_XLSX, top10, "Top10ByDate")
    SUMMARY_MD.write_text(
        render_summary(readable, top10, failed_tickers, offline_mapped),
        encoding="utf-8",
    )

    print("Readable validation report complete")
    print(f"Total rows: {len(readable)}")
    print(f"Top10 rows: {len(top10)}")
    print(f"Unknown ticker row count: {int(readable['ticker_name'].eq('UNKNOWN').sum())}")
    print(f"Unknown unique ticker count: {readable.loc[readable['ticker_name'].eq('UNKNOWN'), 'ticker'].nunique()}")
    print(f"Known offline mapped tickers: {offline_mapped}")
    print(f"Failed ticker count: {len(failed_tickers)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Failed to create readable validation report: {exc}", file=sys.stderr)
        raise
