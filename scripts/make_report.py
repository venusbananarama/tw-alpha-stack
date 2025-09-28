# -*- coding: utf-8 -*-
from __future__ import annotations

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # 確保能找到 reports/

import argparse
from datetime import datetime
import pandas as pd
import numpy as np

from reports.reporting import (
    to_nav_from_returns, to_returns_from_close, drawdown,
    PerfStats, plot_nav_vs_bench, plot_drawdown, plot_rolling_sharpe,
    read_series, write_series_csv, try_jinja_render, save_stats_json,
)

def parse_args():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--returns-csv", type=str, help="策略報酬 CSV 路徑")
    g.add_argument("--nav-csv", type=str, help="策略 NAV CSV 路徑")
    p.add_argument("--date-col", type=str, default="date")
    p.add_argument("--value-col", type=str, default=None, help="策略數值欄位，returns 或 nav")
    p.add_argument("--benchmark-csv", type=str, default=None, help="Benchmark CSV（可省略）")
    p.add_argument("--bench-type", type=str, default="close", choices=["returns", "nav", "close"])
    p.add_argument("--bench-date-col", type=str, default="date")
    p.add_argument("--bench-value-col", type=str, default=None)
    p.add_argument("--out-dir", type=str, required=True)
    p.add_argument("--freq", type=str, default="D", help="D/W/M, 影響年化換算")
    p.add_argument("--rf", type=float, default=0.0, help="年化無風險利率（Sharpe 用）")
    p.add_argument("--title", type=str, default="回測報告")
    p.add_argument("--name", type=str, default=None, help="報表檔名前綴")
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    prefix = (args.name or "report").replace(" ", "_")

    if args.returns_csv:
        s = read_series(args.returns_csv, args.date_col, args.value_col or "ret")
        nav = to_nav_from_returns(s, start_nav=1.0)
    else:
        nav = read_series(args.nav_csv, args.date_col, args.value_col or "nav").rename("nav")

        # 🚨 保險清理：移除 inf/-inf/NaN，避免報表崩潰
        nav = pd.to_numeric(nav, errors="coerce")
        nav = nav.replace([np.inf, -np.inf], np.nan).dropna()
        if len(nav) == 0:
            raise ValueError("NAV 全部都是空值，請檢查回測輸出！")

        # 可選：強制起始 NAV = 1.0
        # nav = nav / nav.iloc[0]

    bench_nav, bench_stats = None, None
    if args.benchmark_csv:
        braw = read_series(args.benchmark_csv, args.bench_date_col, args.bench_value_col or ("ret" if args.bench_type=="returns" else "close"))
        if args.bench_type == "returns":
            bench_nav = to_nav_from_returns(braw, start_nav=1.0)
        elif args.bench_type == "nav":
            bench_nav = braw.rename("nav")
        else:
            bench_nav = to_nav_from_returns(to_returns_from_close(braw), start_nav=1.0)

    returns = nav.pct_change().fillna(0.0).rename("ret")
    stats = PerfStats.compute(returns, rf=args.rf, freq=args.freq)

    write_series_csv(nav, os.path.join(args.out_dir, f"{prefix}_nav.csv"))
    write_series_csv(drawdown(nav), os.path.join(args.out_dir, f"{prefix}_drawdown.csv"))
    save_stats_json(stats, os.path.join(args.out_dir, f"{prefix}_metrics_strategy.json"))

    if bench_nav is not None:
        write_series_csv(bench_nav, os.path.join(args.out_dir, f"{prefix}_bench_nav.csv"))
        bret = bench_nav.pct_change().fillna(0.0)
        bench_stats = PerfStats.compute(bret, rf=args.rf, freq=args.freq)
        save_stats_json(bench_stats, os.path.join(args.out_dir, f"{prefix}_metrics_benchmark.json"))

    plot_drawdown(nav, os.path.join(args.out_dir, f"{prefix}_drawdown.png"))
    if bench_nav is not None:
        plot_nav_vs_bench(nav, bench_nav, os.path.join(args.out_dir, f"{prefix}_nav_vs_bench.png"), title=args.title)
    else:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10,5))
        (nav / nav.iloc[0]).plot(label="Strategy")
        plt.title(args.title); plt.xlabel("Date"); plt.ylabel("Indexed NAV")
        plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(args.out_dir, f"{prefix}_nav.png"), dpi=160)
        plt.close()

    plot_rolling_sharpe(returns, os.path.join(args.out_dir, f"{prefix}_rolling_sharpe.png"), window=63, freq=args.freq)

    out_md = os.path.join(args.out_dir, f"{prefix}_report.md")
    context = {
        "title": args.title,
        "strategy": stats.__dict__,
        "benchmark": bench_stats.__dict__ if bench_stats else None,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "images": {
            "drawdown": f"{prefix}_drawdown.png",
            "nav_or_vs": f"{prefix}_nav_vs_bench.png" if bench_nav is not None else f"{prefix}_nav.png",
            "rolling_sharpe": f"{prefix}_rolling_sharpe.png",
        }
    }
    template_path = os.path.join(os.path.dirname(__file__), "..", "templates", "report_template.md.j2")
    from reports.reporting import generate_markdown_report, try_jinja_render
    if os.path.exists(template_path) and try_jinja_render(context, template_path, out_md):
        pass
    else:
        generate_markdown_report(stats, bench_stats, out_md, extra={"產出時間": context["generated_at"]}, title=args.title)
    print(f"[OK] 報表已產出於: {args.out_dir}")

if __name__ == "__main__":
    main()
