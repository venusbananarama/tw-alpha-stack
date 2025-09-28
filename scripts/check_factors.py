import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="Path to factors parquet")
    ap.add_argument("--factor", default="composite_score", help="Factor column to check")
    args = ap.parse_args()

    df = pd.read_parquet(args.file)
    df["date"] = pd.to_datetime(df["date"])

    print(f"[INFO] rows={len(df):,}, symbols={df['symbol'].nunique():,}")
    print(f"[INFO] date range: {df['date'].min().date()} → {df['date'].max().date()}")

    if args.factor not in df.columns:
        print(f"[WARN] factor '{args.factor}' not found in columns.")
    else:
        nonnull = df[args.factor].notna().sum()
        ratio = nonnull / len(df)
        print(f"[INFO] factor '{args.factor}': non-null {nonnull:,} ({ratio:.2%})")

        # Check last 12 months coverage by month-end
        month_end = (
            df.groupby(df["date"].dt.to_period("M"))["date"]
              .max().sort_values()
        )
        tail_dates = month_end.tail(12)
        coverage = []
        for d in tail_dates:
            day = df[df["date"]==d]
            ok = day[args.factor].notna().mean() if len(day) else 0.0
            coverage.append((str(d.date()), round(float(ok),4)))
        print("[INFO] recent 12 rebalance dates coverage (share of non-null on the day):")
        for d, c in coverage:
            print(f"  {d}: {c:.2%}")

if __name__ == "__main__":
    main()
