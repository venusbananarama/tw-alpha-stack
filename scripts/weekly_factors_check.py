from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

def _to_md(df):
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


def parse_args():
    ap = argparse.ArgumentParser(description="Weekly factor health check (final fix)")
    ap.add_argument("--factors", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--start", type=str, default="")
    ap.add_argument("--end", type=str, default="")
    ap.add_argument("--factors-path", type=str, required=True)
    ap.add_argument("--config", type=str, default="")
    return ap.parse_args()

def load_cfg(cfg_path: str | None):
    base = dict(
        date_col="date",
        symbol_col="symbol",
        price_col="close",
        score_col="composite_score",
        weekly_anchor="FRI",
        neutralize_by=None,
        winsor=(0.01, 0.99),
        topN=50,
    )
    if cfg_path:
        with open(cfg_path, "r", encoding="utf-8") as f:
            user = yaml.safe_load(f) or {}
        base.update(user)
    return base

def winsorize(s: pd.Series, lo=0.01, hi=0.99):
    if s.dropna().empty:
        return s
    qlo, qhi = s.quantile([lo, hi])
    return s.clip(qlo, qhi)

def compute_weekly_ic(df: pd.DataFrame, factor_cols: list[str], date_col: str, symbol_col: str, price_col: str) -> pd.DataFrame:
    df = df.sort_values([symbol_col, date_col]).copy()
    if price_col == "ret":
        df["ret1"] = df[price_col]
    else:
        if price_col not in df.columns:
            raise ValueError(f"price_col='{price_col}' not in columns.")
        df["ret1"] = df.groupby(symbol_col)[price_col].pct_change()
    # add week column
    df["week"] = pd.to_datetime(df[date_col]) + pd.offsets.Week(weekday=4)
    # compute next-week returns correctly using DataFrame groupby
    df["week_ret"] = df.groupby([symbol_col, "week"])["ret1"].transform(lambda x: (1 + x).prod() - 1.0)
    wk = df.groupby([symbol_col, "week"], as_index=False).last()
    wk["next_week"] = wk["week"] + pd.offsets.Week()
    ret_next = wk[[symbol_col, "week", "week_ret"]].rename(columns={"week": "next_week", "week_ret": "fwd_ret"})
    merged = wk.merge(ret_next, on=[symbol_col, "next_week"], how="left")
    out = []
    for week, g in merged.groupby("week", sort=True):
        row = {"week": week}
        for fac in factor_cols:
            if fac not in g.columns:
                continue
            x = g[fac]; y = g["fwd_ret"]
            mask = x.notna() & y.notna()
            if mask.sum() < 10:
                row[fac] = np.nan
            else:
                row[fac] = x[mask].rank().corr(y[mask].rank(), method="spearman")
        out.append(row)
    return pd.DataFrame(out).sort_values("week")

def main():
    args = parse_args()
    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    cfg = load_cfg(args.config or None)

    print("=== DEBUG CONFIG (Final Fix) ===")
    print("Config path:", args.config)
    print("Loaded config:", cfg)

    df = pd.read_parquet(args.factors_path)
    print("DataFrame cols:", df.columns.tolist())

    factor_cols = args.factors.strip().split()
    date_col = cfg["date_col"]; symbol_col = cfg["symbol_col"]
    price_col = cfg["price_col"]

    if args.start:
        df = df[df[date_col] >= args.start]
    if args.end:
        df = df[df[date_col] <= args.end]

    if symbol_col not in df.columns:
        raise SystemExit(f"[ERROR] symbol_col='{symbol_col}' 不在資料欄位: {df.columns.tolist()}")

    for fac in factor_cols:
        if fac not in df.columns:
            print(f"[WARN] Factor '{fac}' not found in dataframe columns", file=sys.stderr)
    factor_cols = [f for f in factor_cols if f in df.columns]
    if not factor_cols:
        raise SystemExit("No valid factors found.")

    if cfg.get("winsor"):
        lo, hi = cfg["winsor"]
        for fac in factor_cols:
            df[fac] = df.groupby(symbol_col)[fac].transform(lambda s: winsorize(s, lo, hi))

    ic = compute_weekly_ic(df, factor_cols, date_col, symbol_col, price_col)
    ic_path = outdir / "weekly_ic.csv"; ic.to_csv(ic_path, index=False)

    summary = ic[factor_cols].agg(["mean","std","median","count"]).T
    summary["t_stat"] = summary["mean"] / (summary["std"] / np.sqrt(summary["count"].clip(lower=1)))
    summary_path = outdir / "summary.csv"; summary.to_csv(summary_path)

    md = ["# Weekly Factor Health Check",
          f"- Period: {args.start or df[date_col].min()} → {args.end or df[date_col].max()}",
          f"- Factors: {', '.join(factor_cols)}",
          "",
          "## Summary (Spearman IC)",
          _to_md(summary),
          "",
          "## Sample (last 10 weeks)",
          ic.tail(10).to_markdown(index=False),
          ""]
    (outdir / "REPORT.md").write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote: {ic_path}, {summary_path}, REPORT.md")

if __name__ == "__main__":
    main()

