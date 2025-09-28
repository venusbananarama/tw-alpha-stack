\
import argparse, os
import pandas as pd
from pathlib import Path

def run(merged_parquet: str, out_dir: str):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(merged_parquet)
    # Basic columns expected
    expected = ["date","open","high","low","close","adj_close","volume","symbol"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Cast
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol","date"]).reset_index(drop=True)

    # Duplicates
    dup = df.duplicated(subset=["symbol","date"]).sum()

    # Per-symbol summary
    g = df.groupby("symbol", sort=False)
    summary = g.agg(
        first_date=("date","min"),
        last_date=("date","max"),
        rows=("date","size"),
        null_close=("close", lambda s: int(s.isna().sum())),
        null_volume=("volume", lambda s: int(s.isna().sum())),
        min_close=("close","min"),
        max_close=("close","max"),
        avg_dollar_volume=("close", lambda s: float((s * g.obj.loc[s.index, "volume"]).mean()))
    ).reset_index()

    # Save
    summary_path = out_dir / "qc_per_symbol.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    # Cross-day gap estimation (rough): count date gaps per symbol by comparing day diff>1B
    df["date_lag"] = df.groupby("symbol")["date"].shift(1)
    df["bday_gap"] = (df["date"] - df["date_lag"]).dt.days.fillna(1)
    gap_cnt = df.loc[df["bday_gap"] > 4].groupby("symbol")["bday_gap"].count().rename("gap_count").reset_index()
    gap_cnt_path = out_dir / "qc_gap_counts.csv"
    gap_cnt.to_csv(gap_cnt_path, index=False, encoding="utf-8-sig")

    meta_path = out_dir / "qc_meta.txt"
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(f"Symbols: {summary.shape[0]}\n")
        f.write(f"Total rows: {len(df):,}\n")
        f.write(f"Duplicate (symbol,date): {dup}\n")
        f.write(f"Date range: {df['date'].min()} → {df['date'].max()}\n")

    print(f"[QC] Wrote:\n- {summary_path}\n- {gap_cnt_path}\n- {meta_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="merged parquet path")
    ap.add_argument("--out-dir", required=True, help="output folder")
    args = ap.parse_args()
    run(args.file, args.out_dir)
