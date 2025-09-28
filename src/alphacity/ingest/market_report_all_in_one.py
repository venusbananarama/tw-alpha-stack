#!/usr/bin/env python
# -*- coding: utf-8 -*-
# All-in-one market report generator (improved).
# - Robust groupby apply (no FutureWarning)
# - Safer chained assignment
# - Optional charts with headless backend
# - Top-N sheets (returns & turnover)
# - Optional board mapping merge
#
# Usage:
#   python market_report_all_in_one.py --file PATH --out PATH [--board-csv PATH] [--detail-sample "2330.TW,2317.TW"] [--topn 100] [--with-charts]

import argparse
import os
from pathlib import Path
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True, help="Merged OHLCV parquet path")
    p.add_argument("--out", required=True, help="Output Excel path")
    p.add_argument("--board-csv", default=None, help="symbol→board mapping CSV")
    p.add_argument("--detail-sample", default="", help="Comma separated symbols for detail charts")
    p.add_argument("--topn", type=int, default=100, help="Top N rows for ranking sheets")
    p.add_argument("--with-charts", action="store_true", help="Embed PNG charts in Excel")
    return p.parse_args()

def ensure_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    for c in ["open","high","low","close","adj_close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str)
    return df

def compute_symbol_stats(df: pd.DataFrame) -> pd.DataFrame:
    def per_symbol(g: pd.DataFrame) -> pd.Series:
        g = g.sort_values("date").reset_index(drop=True)
        ret1 = g["close"].pct_change()
        def last_ret(n):
            if len(g) <= n or pd.isna(g["close"].iloc[-n-1] if len(g)>n else np.nan):
                return np.nan
            return (g["close"].iloc[-1] / g["close"].iloc[-1 - n]) - 1.0
        vol20 = ret1.rolling(20).std().iloc[-1] * np.sqrt(252) if len(ret1) >= 20 else np.nan
        turnover = g["close"] * g["volume"]
        dvol20 = turnover.rolling(20).mean().iloc[-1] if len(turnover) >= 20 else np.nan
        out = pd.Series({
            "first_date": g["date"].min(),
            "last_date": g["date"].max(),
            "nobs": len(g),
            "last_close": g["close"].iloc[-1] if len(g) else np.nan,
            "ret_1d": last_ret(1),
            "ret_5d": last_ret(5),
            "ret_20d": last_ret(20),
            "ret_60d": last_ret(60),
            "vol_20d_annual": vol20,
            "turnover20": dvol20,
            "avg_volume20": g["volume"].rolling(20).mean().iloc[-1] if len(g)>=20 else np.nan,
        })
        return out

    stats = (
        df.sort_values(["symbol","date"])
          .groupby("symbol", group_keys=False)
          .apply(per_symbol)
          .reset_index()
    )
    return stats

def attach_board(stats: pd.DataFrame, board_csv: str|None) -> pd.DataFrame:
    if not board_csv:
        return stats
    if not os.path.exists(board_csv):
        print(f"[WARN] 找不到 board 對照表：{board_csv}，將略過板塊彙總。")
        return stats
    m = pd.read_csv(board_csv)
    if "symbol" not in m.columns or "board" not in m.columns:
        print("[WARN] board CSV 缺少必要欄位(symbol/board)，略過。")
        return stats
    merged = stats.merge(m[["symbol","board"]], on="symbol", how="left")
    merged["board"] = merged["board"].fillna("Unknown")
    return merged

def board_summary(stats_with_board: pd.DataFrame) -> pd.DataFrame:
    if "board" not in stats_with_board.columns:
        return pd.DataFrame()
    agg = (stats_with_board
           .groupby("board", dropna=False)
           .agg(
                symbols=("symbol","nunique"),
                m_ret1d=("ret_1d","mean"),
                m_ret5d=("ret_5d","mean"),
                m_ret20d=("ret_20d","mean"),
                m_vol20=("vol_20d_annual","mean"),
                m_turnover20=("turnover20","mean"),
           )
           .reset_index()
          )
    return agg

def choose_top(stats: pd.DataFrame, col: str, n: int) -> pd.DataFrame:
    if col not in stats.columns:
        return pd.DataFrame(columns=stats.columns)
    return stats.sort_values(col, ascending=False).head(n).reset_index(drop=True)

def chart_symbol(df: pd.DataFrame, symbol: str, outdir: Path) -> Path|None:
    g = df[df["symbol"] == symbol].sort_values("date")
    if g.empty: return None
    fig = plt.figure(figsize=(7, 3.2), dpi=120)
    ax = plt.gca()
    ax.plot(g["date"], g["close"], label="Close")
    ax.plot(g["date"], g["close"].rolling(20).mean(), label="MA20", linewidth=1)
    ax.set_title(symbol)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=8)
    fig.autofmt_xdate()
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"{symbol}.png"
    plt.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path

def main():
    args = parse_args()
    f = args.file
    out = args.out
    print(f"[INFO] 讀取 {f} ...")
    df = pd.read_parquet(f)
    if "symbol" not in df.columns:
        raise KeyError("'symbol' 欄位不存在。請確認合併檔格式。")
    df = ensure_dtypes(df)

    n_symbols = df["symbol"].nunique()
    n_rows = len(df)
    start, end = df["date"].min(), df["date"].max()
    print(f"[INFO] 股票數: {n_symbols}, 總筆數: {n_rows}, 期間: {start.date()} → {end.date()}")

    print("[INFO] 計算報酬、波動與成交額指標 ...")
    stats = compute_symbol_stats(df)

    stats_b = attach_board(stats, args.board_csv)
    bsum = board_summary(stats_b)

    topn = max(1, int(args.topn))
    tops = {
        "topN_ret_1d": choose_top(stats_b, "ret_1d", topn),
        "topN_ret_20d": choose_top(stats_b, "ret_20d", topn),
        "topN_turnover20": choose_top(stats_b, "turnover20", topn),
    }

    sample_syms = [s.strip() for s in args.detail_sample.split(",") if s.strip()] if args.detail_sample else []
    img_dir = Path(out).with_suffix("").parent / "_charts_tmp"
    sym2img = {}
    if args.with_charts and sample_syms:
        for s in sample_syms:
            p = chart_symbol(df, s, img_dir)
            if p: sym2img[s] = p

    print(f"[INFO] 輸出 Excel → {out}")
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        ov = pd.DataFrame({
            "metric": ["symbols","rows","start","end"],
            "value": [n_symbols, n_rows, start, end]
        })
        ov.to_excel(xw, index=False, sheet_name="overview")
        stats_b.sort_values(["symbol"]).to_excel(xw, index=False, sheet_name="per_symbol")
        if not bsum.empty:
            bsum.to_excel(xw, index=False, sheet_name="board_stats")
        for name, df_t in tops.items():
            if not df_t.empty:
                df_t.to_excel(xw, index=False, sheet_name=name)

        if sym2img:
            ws = xw.book.add_worksheet("sample_details")
            row = 1
            ws.write(0,0,"symbol")
            ws.write(0,1,"chart")
            for sym, imgp in sym2img.items():
                ws.write(row, 0, sym)
                ws.insert_image(row, 1, str(imgp), {"x_scale":0.75, "y_scale":0.75})
                row += 20

    print(f"[INFO] 完成：{out}")
    try:
        for p in Path(img_dir).glob("*.png"):
            p.unlink()
        Path(img_dir).rmdir()
    except Exception:
        pass

if __name__ == "__main__":
    main()