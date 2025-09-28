#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_report_safe.py
-------------------
Robust report generator for AlphaCity backtest outputs.

Inputs:
- --nav-csv: CSV with columns [date, nav] (date in YYYY-MM-DD or parseable format)
- --benchmark-csv (optional): CSV with [date, nav or price]. If "price" provided, it will be rebased to 1 at start.
- --out-dir: directory to write outputs (PNG plots, metrics CSV/JSON, cleaned NAV)

Outputs:
- nav_clean.csv
- nav_plot.png
- drawdown_plot.png
- metrics.json / metrics.csv
- relative_plot.png (if benchmark provided)

Behavior:
- Cleans NaN/inf/-inf, forward-fills short gaps, drops leading/trailing invalids.
- Detects frequency (daily/weekly/monthly) from dates for annualization (fallback daily=252).
- Does not set custom colors (matplotlib default), generates one figure per chart.
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import json 

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--nav-csv", required=True, help="Path to NAV csv with columns [date, nav]")
    p.add_argument("--benchmark-csv", default=None, help="Optional benchmark csv with [date, nav] or [date, price]")
    p.add_argument("--out-dir", required=True, help="Output directory")
    p.add_argument("--rf", type=float, default=0.0, help="Annual risk-free rate (e.g., 0.02 for 2%)")
    p.add_argument("--ffill-limit", type=int, default=3, help="Max consecutive NaNs to forward-fill within NAV")
    return p.parse_args()

def _read_nav(path: Path):
    df = pd.read_csv(path)
    # try common column names
    cols = {c.lower(): c for c in df.columns}
    date_col = cols.get("date") or cols.get("datetime")
    if date_col is None:
        raise ValueError("NAV CSV must have a 'date' column")
    nav_col = cols.get("nav") or cols.get("equity") or cols.get("value")
    if nav_col is None:
        # sometimes returns are given; try to rebuild
        if "ret" in cols:
            date = pd.to_datetime(df[date_col])
            ret = pd.to_numeric(df[cols["ret"]], errors="coerce").fillna(0.0)
            nav = (1.0 + ret).cumprod()
            return pd.DataFrame({"date": date, "nav": nav})
        raise ValueError("NAV CSV must have a 'nav' column (or 'ret' to reconstruct).")
    date = pd.to_datetime(df[date_col])
    nav  = pd.to_numeric(df[nav_col], errors="coerce")
    return pd.DataFrame({"date": date, "nav": nav})

def _clean_nav(df: pd.DataFrame, ffill_limit:int=3) -> pd.DataFrame:
    x = df.copy()
    # remove inf/-inf
    x["nav"] = x["nav"].replace([np.inf, -np.inf], np.nan)
    # sort & drop duplicates
    x = x.sort_values("date").drop_duplicates("date")
    # forward fill short gaps
    x["nav"] = x["nav"].ffill(limit=ffill_limit)
    # drop still-missing
    x = x.dropna(subset=["nav"])
    # enforce positive
    x = x[x["nav"] > 0]
    x = x.reset_index(drop=True)
    return x

def _infer_annualization_factor(dates: pd.Series) -> int:
    # Try to infer from median spacing
    if len(dates) < 3:
        return 252
    s = dates.sort_values().diff().median()
    if pd.isna(s):
        return 252
    days = s / pd.Timedelta(days=1)
    if days <= 1.5:
        return 252  # daily
    if days <= 8:
        return 52   # weekly
    if days <= 20:
        return 12   # monthly-ish (trading months ~21 days, but annualize as 12)
    return 252  # fallback

def _max_drawdown(nav: pd.Series) -> float:
    peaks = nav.cummax()
    dd = (nav / peaks) - 1.0
    return dd.min()

def _compute_metrics(nav: pd.Series, dates: pd.Series, ann_factor:int, rf_annual: float=0.0):
    # Convert NAV to returns
    ret = nav.pct_change().fillna(0.0)
    # convert annual RF to per-period RF
    rf_period = (1 + rf_annual) ** (1/ann_factor) - 1
    excess = ret - rf_period
    vol = excess.std(ddof=1) * np.sqrt(ann_factor)
    sharpe = (excess.mean() * ann_factor) / vol if vol > 0 else np.nan
    mdd = _max_drawdown(nav)
    total_return = nav.iloc[-1] / nav.iloc[0] - 1.0 if len(nav) >= 2 else np.nan
    cagr = (nav.iloc[-1] / nav.iloc[0]) ** (ann_factor * 1.0 / max(len(nav),1)) - 1.0 if len(nav) >= 2 else np.nan
    return {
        "start": str(pd.to_datetime(dates.iloc[0]).date()) if len(dates) else None,
        "end": str(pd.to_datetime(dates.iloc[-1]).date()) if len(dates) else None,
        "periods": int(len(nav)),
        "ann_factor": int(ann_factor),
        "total_return": float(total_return),
        "CAGR": float(cagr),
        "MaxDD": float(mdd),
        "Sharpe": float(sharpe),
    }

def _align_benchmark(port: pd.DataFrame, bench_path: Path):
    b = pd.read_csv(bench_path)
    cols = {c.lower(): c for c in b.columns}
    date_col = cols.get("date") or cols.get("datetime")
    if date_col is None:
        raise ValueError("Benchmark CSV must have a 'date' column")
    nav_col = cols.get("nav")
    price_col = cols.get("price") or cols.get("close")
    if nav_col is None and price_col is None:
        raise ValueError("Benchmark CSV must have 'nav' or 'price' column")
    date = pd.to_datetime(b[date_col])
    if nav_col is not None:
        nav = pd.to_numeric(b[nav_col], errors="coerce")
    else:
        price = pd.to_numeric(b[price_col], errors="coerce")
        # rebase to 1
        first = price.dropna().iloc[0]
        nav = price / first
    bench = pd.DataFrame({"date": date, "bench_nav": nav}).dropna()
    # align to portfolio dates
    merged = port.merge(bench, on="date", how="inner")
    return merged

def _plot_nav(df: pd.DataFrame, out_path: Path, title:str):
    fig = plt.figure()
    plt.plot(df["date"], df["nav"])
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("NAV")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def _plot_drawdown(df: pd.DataFrame, out_path: Path, title:str):
    nav = df["nav"].values
    peaks = np.maximum.accumulate(nav)
    dd = nav / peaks - 1.0
    fig = plt.figure()
    plt.plot(df["date"], dd)
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Drawdown")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def _plot_relative(df: pd.DataFrame, out_path: Path, title:str):
    rel = df["nav"].values / df["bench_nav"].values
    fig = plt.figure()
    plt.plot(df["date"], rel)
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Relative (Portfolio / Benchmark)")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def main():
    args = _parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = _read_nav(Path(args.nav_csv))
    clean = _clean_nav(raw, ffill_limit=args.ffill_limit)

    if clean.empty:
        print("Error: NAV data is empty after cleaning.", file=sys.stderr)
        sys.exit(2)

    ann_factor = _infer_annualization_factor(clean["date"])
    metrics = _compute_metrics(clean["nav"], clean["date"], ann_factor, rf_annual=args.rf)

    # write cleaned nav
    nav_clean_path = out_dir / "nav_clean.csv"
    clean.to_csv(nav_clean_path, index=False)

    # plots
    _plot_nav(clean, out_dir/"nav_plot.png", "Portfolio NAV")
    _plot_drawdown(clean, out_dir/"drawdown_plot.png", "Portfolio Drawdown")

    results = {"portfolio": metrics}

    # benchmark part
    if args.benchmark_csv:
        try:
            merged = _align_benchmark(clean, Path(args.benchmark_csv))
            if not merged.empty:
                _plot_relative(merged, out_dir/"relative_plot.png", "Relative to Benchmark")
                ann_b = _infer_annualization_factor(merged["date"])
                bench_metrics = _compute_metrics(merged["bench_nav"], merged["date"], ann_b, rf_annual=args.rf)
                results["benchmark"] = bench_metrics
                # simple relative stats
                rel = merged["nav"].values / merged["bench_nav"].values
                rel_total = float(rel[-1] / rel[0] - 1.0) if len(rel) >= 2 else np.nan
                results["relative_total"] = rel_total
        except Exception as e:
            print(f"Warning: benchmark processing failed: {e}", file=sys.stderr)

    # save metrics
    (out_dir/"metrics.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    # also CSV (flat)
    flat_rows = []
    for k, v in results.items():
        if isinstance(v, dict):
            row = {"scope": k, **v}
            flat_rows.append(row)
        else:
            flat_rows.append({"scope": k, "value": v})
    pd.DataFrame(flat_rows).to_csv(out_dir/"metrics.csv", index=False)

    print(f"Done. Wrote outputs to: {out_dir}")

if __name__ == "__main__":
    main()
