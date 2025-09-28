
# Compute alpha factors and a composite score from OHLCV parquet.
import argparse
from pathlib import Path
import json
import pandas as pd
import numpy as np
import yaml
import os, sys

# make local imports work when running as a script
HERE = os.path.dirname(__file__)
if HERE not in sys.path:
    sys.path.append(HERE)

from factors_core import ensure_schema, add_returns, momentum_lookback, volatility, liquidity, composite_from_config

def load_config(path: str | None):
    if path is None:
        # default weights
        return {
            "factors": {"mom_252_21": 1.0, "vol_20": -0.5, "liq_20": 0.3},
            "zscore_clip": 5.0,
            "neutralize_by": None,
            "output_columns": ["date","symbol","ret","mom_252_21","vol_20","liq_20","composite_score"],
        }
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser(description="Compute alpha factors parquet from OHLCV parquet.")
    ap.add_argument("--file", required=True, help="Path to ohlcv_daily_all.parquet")
    ap.add_argument("--out", required=True, help="Path to output factors parquet")
    ap.add_argument("--config", default=None, help="YAML with factor weights and options")
    ap.add_argument("--topn-csv", default=None, help="Optional: write monthly TopN picks CSV")
    ap.add_argument("--topn", type=int, default=50, help="N for Top picks if --topn-csv is set")
    args = ap.parse_args()

    df = pd.read_parquet(args.file)
    df = ensure_schema(df)
    df = add_returns(df)

    # Compute base factors
    df = momentum_lookback(df, lookback=252, skip=21, col="adj_close", outname="mom_252_21")
    df = volatility(df, window=20, ret_col="ret", outname="vol_20")
    df = liquidity(df, window=20, outname="liq_20")

    cfg = load_config(args.config)
    weights = cfg.get("factors", {})
    clip = float(cfg.get("zscore_clip", 5.0))

    # Composite
    df["composite_score"] = composite_from_config(df, weights, clip=clip)

    out_cols = cfg.get("output_columns", ["date","symbol","ret","mom_252_21","vol_20","liq_20","composite_score"])
    out = df[out_cols].dropna().reset_index(drop=True)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, index=False)
    print(f"[INFO] Wrote factors → {args.out}, rows={len(out):,}")

    # Optional monthly TopN preview
    if args.topn_csv:
        picks = (
            out.assign(month=out["date"].values.astype("datetime64[M]"))
               .sort_values(["date","composite_score"], ascending=[True, False])
               .groupby("month")
               .head(args.topn)[["month","date","symbol","composite_score"]]
        )
        Path(args.topn_csv).parent.mkdir(parents=True, exist_ok=True)
        picks.to_csv(args.topn_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] Wrote monthly Top{args.topn} picks → {args.topn_csv}")

if __name__ == "__main__":
    main()
