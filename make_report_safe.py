#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_report_safe.py
-------------------
Robust report generator for AlphaCity backtest outputs.

Inputs:
- --nav-csv: CSV with date + nav columns (nav / nav_net / nav_gross) or ret columns.
- --bench-csv (optional): CSV with [date, nav or price]. If "price" provided, it will be rebased to 1 at start.
- --out-dir: directory to write outputs (PNG plots, metrics CSV/JSON, cleaned NAV)

Outputs:
- nav_clean.csv
- nav_plot.png
- drawdown_plot.png
- metrics.json / metrics.csv
- report.html
- report_summary.json
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
from typing import Dict, Tuple

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--nav-csv", required=True, help="Path to NAV csv with columns [date, nav]")
    p.add_argument("--bench-csv", dest="bench_csv", default=None, help="Optional benchmark csv with [date, nav] or [date, price]")
    p.add_argument("--benchmark-csv", dest="bench_csv", default=None, help="Alias of --bench-csv")
    p.add_argument("--out-dir", required=True, help="Output directory")
    p.add_argument("--title", default="Backtest Report", help="Report title")
    p.add_argument("--cost-tag", default=None, help="Cost scenario tag")
    p.add_argument("--rf", type=float, default=0.0, help="Annual risk-free rate (e.g., 0.02 for 2%%)")
    p.add_argument("--ffill-limit", type=int, default=3, help="Max consecutive NaNs to forward-fill within NAV")
    return p.parse_args()

def _read_nav_frame(path: Path) -> Tuple[pd.Series, Dict[str, pd.Series]]:
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    date_col = cols.get("date") or cols.get("datetime")
    if date_col is None:
        raise ValueError("NAV CSV must have a 'date' column")
    date = pd.to_datetime(df[date_col], errors="coerce")

    nav_series: Dict[str, pd.Series] = {}
    if "nav_net" in cols:
        nav_series["net"] = pd.to_numeric(df[cols["nav_net"]], errors="coerce")
    if "nav_gross" in cols:
        nav_series["gross"] = pd.to_numeric(df[cols["nav_gross"]], errors="coerce")
    if "nav" in cols:
        nav_series["nav"] = pd.to_numeric(df[cols["nav"]], errors="coerce")
    if not nav_series and "equity" in cols:
        nav_series["nav"] = pd.to_numeric(df[cols["equity"]], errors="coerce")
    if not nav_series and "value" in cols:
        nav_series["nav"] = pd.to_numeric(df[cols["value"]], errors="coerce")

    if not nav_series:
        if "ret_net" in cols:
            ret = pd.to_numeric(df[cols["ret_net"]], errors="coerce").fillna(0.0)
            nav_series["net"] = (1.0 + ret).cumprod()
        if "ret_gross" in cols:
            ret = pd.to_numeric(df[cols["ret_gross"]], errors="coerce").fillna(0.0)
            nav_series["gross"] = (1.0 + ret).cumprod()
        if "ret" in cols:
            ret = pd.to_numeric(df[cols["ret"]], errors="coerce").fillna(0.0)
            nav_series["nav"] = (1.0 + ret).cumprod()

    if not nav_series:
        raise ValueError("NAV CSV must have nav/nav_net/nav_gross or ret columns.")
    return date, nav_series

def _clean_series(date: pd.Series, nav: pd.Series, ffill_limit: int = 3) -> pd.DataFrame:
    x = pd.DataFrame({"date": date, "nav": nav})
    x["nav"] = x["nav"].replace([np.inf, -np.inf], np.nan)
    x = x.sort_values("date").drop_duplicates("date")
    x["nav"] = x["nav"].ffill(limit=ffill_limit)
    x = x.dropna(subset=["nav"])
    x = x[x["nav"] > 0]
    return x.reset_index(drop=True)

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

    date, nav_map = _read_nav_frame(Path(args.nav_csv))
    clean_map: Dict[str, pd.DataFrame] = {}
    for key, series in nav_map.items():
        clean_map[key] = _clean_series(date, series, ffill_limit=args.ffill_limit)

    primary_key = "net" if "net" in clean_map else ("nav" if "nav" in clean_map else "gross")
    clean = clean_map[primary_key]

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

    results = {"portfolio": metrics, "scope": primary_key}
    for key, frame in clean_map.items():
        if key == primary_key or frame.empty:
            continue
        ann_k = _infer_annualization_factor(frame["date"])
        results[f"portfolio_{key}"] = _compute_metrics(frame["nav"], frame["date"], ann_k, rf_annual=args.rf)

    # benchmark part
    if args.bench_csv:
        try:
            merged = _align_benchmark(clean, Path(args.bench_csv))
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
    metrics_csv_path = out_dir / "metrics.csv"
    pd.DataFrame(flat_rows).to_csv(metrics_csv_path, index=False)

    report_summary = {
        "title": args.title,
        "cost_tag": args.cost_tag,
        "primary_scope": primary_key,
        "metrics": results,
    }
    (out_dir / "report_summary.json").write_text(json.dumps(report_summary, indent=2), encoding="utf-8")

    report_html = out_dir / "report.html"
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{args.title}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1 {{ margin-bottom: 8px; }}
    table {{ border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 10px; }}
    .meta {{ color: #555; }}
    img {{ max-width: 100%; margin-top: 12px; }}
  </style>
</head>
<body>
  <h1>{args.title}</h1>
  <div class="meta">Cost tag: {args.cost_tag or "n/a"}</div>
  <div class="meta">Primary scope: {primary_key}</div>
  <h2>Metrics</h2>
  {pd.DataFrame(flat_rows).to_html(index=False)}
  <h2>Charts</h2>
  <div><img src="nav_plot.png" alt="NAV plot"/></div>
  <div><img src="drawdown_plot.png" alt="Drawdown plot"/></div>
  {"<div><img src='relative_plot.png' alt='Relative plot'/></div>" if args.bench_csv else ""}
</body>
</html>
"""
    report_html.write_text(html, encoding="utf-8")

    report_tables = out_dir / "report_tables"
    report_tables.mkdir(parents=True, exist_ok=True)
    (report_tables / "metrics.csv").write_text(metrics_csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Done. Wrote outputs to: {out_dir}")

if __name__ == "__main__":
    main()
