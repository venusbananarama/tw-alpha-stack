
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
make_watchlists.py
從合併後的 ohlcv_daily_all.parquet 產出各式觀察清單（CSV）。

功能：
- 以每檔最後交易日為快照，計算 1D/5D/20D/60D/YTD 報酬、20D均量、20D均成交額、
  52週高低與距離、量爆比、成交額爆量比、波動(20D ATR proxy)。
- 產出：
  top_gainers.csv, top_losers.csv, volume_spikes.csv, turnover_spikes.csv,
  near_52w_high.csv, near_52w_low.csv, momentum_20d.csv, low_volatility.csv
使用：
python make_watchlists.py \
  --merged-path "G:\AI\datahub\ohlcv_daily_all.parquet" \
  --out-root "G:\AI\datahub\reports\watchlists" \
  --topn 100 \
  --min-avg20-vol 200000 \
  --min-avg20-turnover 5e8 \
  --volume-spike-th 2.0 \
  --turnover-spike-th 2.0
"""
import argparse
import os
from datetime import datetime
import numpy as np
import pandas as pd

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c) for c in df.columns]
    lower = [c.lower() for c in cols]
    need = {"date","open","high","low","close","adj_close","volume","symbol"}
    # 已經符合
    if need.issubset(set(lower)):
        # 直接標準化大小寫
        mapping = {c: c.lower() for c in df.columns}
        return df.rename(columns=mapping)
    # 嘗試以包含字串方式對應（處理像 "('date','')" 這種）
    mapping = {}
    for want in need:
        matched = None
        for c in df.columns:
            cl = str(c).lower()
            if want in cl:
                # 避免把 'close' 吃到 'adj_close'
                if want == "close" and "adj_close" in cl:
                    continue
                matched = c
                break
        if matched is not None:
            mapping[matched] = want
    out = df.rename(columns=mapping)
    missing = need - set([str(c).lower() for c in out.columns])
    if missing:
        raise ValueError(f"Missing required columns after normalize: {missing}")
    return out

def compute_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol","date"])
    # per-symbol計算
    def per_symbol(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        # 報酬
        g["ret_1d"]  = g["close"].pct_change(1)
        g["ret_5d"]  = g["close"].pct_change(5)
        g["ret_20d"] = g["close"].pct_change(20)
        g["ret_60d"] = g["close"].pct_change(60)
        # YTD
        y = g["date"].dt.year
        first_of_year = g.groupby(y)["close"].transform("first")
        g["ret_ytd"] = g["close"] / first_of_year - 1.0
        # 均量 / 均成交額
        g["avg20_vol"] = g["volume"].rolling(20, min_periods=1).mean()
        g["turnover"] = g["close"] * g["volume"]
        g["avg20_turnover"] = g["turnover"].rolling(20, min_periods=1).mean()
        # 量爆比與成交額爆量比
        g["vol_spike"] = g["volume"] / g["avg20_vol"].replace(0, np.nan)
        g["turnover_spike"] = g["turnover"] / g["avg20_turnover"].replace(0, np.nan)
        # 52週高低 (252 交易日近似)
        g["hi_52w"] = g["high"].rolling(252, min_periods=1).max()
        g["lo_52w"] = g["low"].rolling(252, min_periods=1).min()
        g["dist_52w_hi"] = (g["hi_52w"] - g["close"]) / g["hi_52w"].replace(0, np.nan)
        g["dist_52w_lo"] = (g["close"] - g["lo_52w"]) / g["lo_52w"].replace(0, np.nan)
        # 20D ATR proxy（簡化）：(high-low).rolling(20).mean()
        g["atr20_proxy"] = (g["high"] - g["low"]).rolling(20, min_periods=1).mean()
        g["atr20_pct"] = g["atr20_proxy"] / g["close"].replace(0, np.nan)
        return g.iloc[[-1]]  # snapshot 最後一筆
    out = df.groupby("symbol", as_index=False, group_keys=False).apply(per_symbol)
    # 去除無限與nan
    for c in ["ret_1d","ret_5d","ret_20d","ret_60d","ret_ytd",
              "avg20_vol","avg20_turnover","vol_spike","turnover_spike",
              "hi_52w","lo_52w","dist_52w_hi","dist_52w_lo","atr20_proxy","atr20_pct"]:
        if c in out.columns:
            out[c] = out[c].replace([np.inf, -np.inf], np.nan)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--merged-path", required=True, help="合併後 parquet 路徑")
    ap.add_argument("--out-root", required=True, help="輸出資料夾")
    ap.add_argument("--topn", type=int, default=100)
    ap.add_argument("--min-avg20-vol", type=float, default=0, help="過濾：20D均量 ≥ 此值")
    ap.add_argument("--min-avg20-turnover", type=float, default=0, help="過濾：20D均成交額 ≥ 此值")
    ap.add_argument("--volume-spike-th", type=float, default=2.0)
    ap.add_argument("--turnover-spike-th", type=float, default=2.0)
    args = ap.parse_args()

    os.makedirs(args.out_root, exist_ok=True)
    print(f"[INFO] Loading {args.merged_path} ...")
    df = pd.read_parquet(args.merged_path)
    snap = compute_snapshot(df)

    # 基礎過濾（若指定）
    base = snap.copy()
    if args.min_avg20_vol > 0:
        base = base[base["avg20_vol"] >= args.min_avg20_vol]
    if args.min_avg20_turnover > 0:
        base = base[base["avg20_turnover"] >= args.min_avg20_turnover]

    # 產出各清單
    def save(df_, name):
        path = os.path.join(args.out_root, name)
        df_.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Wrote {name} ({len(df_)} rows)")

    # Top 漲/跌
    g = base.sort_values("ret_1d", ascending=False).head(args.topn)
    save(g[["symbol","date","close","ret_1d","ret_5d","ret_20d","avg20_vol","avg20_turnover"]], "top_gainers.csv")

    l = base.sort_values("ret_1d", ascending=True).head(args.topn)
    save(l[["symbol","date","close","ret_1d","ret_5d","ret_20d","avg20_vol","avg20_turnover"]], "top_losers.csv")

    # 量/成交額爆量
    vs = base.sort_values("vol_spike", ascending=False)
    vs = vs[vs["vol_spike"] >= args.volume_spike_th].head(args.topn)
    save(vs[["symbol","date","close","volume","avg20_vol","vol_spike"]], "volume_spikes.csv")

    ts = base.sort_values("turnover_spike", ascending=False)
    ts = ts[ts["turnover_spike"] >= args.turnover_spike_th].head(args.topn)
    save(ts[["symbol","date","close","turnover","avg20_turnover","turnover_spike"]], "turnover_spikes.csv")

    # 接近 52W 高/低
    hi = base.sort_values("dist_52w_hi", ascending=True)
    hi = hi[hi["dist_52w_hi"].notna()].head(args.topn)
    save(hi[["symbol","date","close","hi_52w","dist_52w_hi","ret_20d","ret_60d"]], "near_52w_high.csv")

    lo = base.sort_values("dist_52w_lo", ascending=True)
    lo = lo[lo["dist_52w_lo"].notna()].head(args.topn)
    save(lo[["symbol","date","close","lo_52w","dist_52w_lo","ret_20d","ret_60d"]], "near_52w_low.csv")

    # 動能與低波動
    mom20 = base.sort_values("ret_20d", ascending=False).head(args.topn)
    save(mom20[["symbol","date","close","ret_1d","ret_5d","ret_20d","ret_60d"]], "momentum_20d.csv")

    lowvol = base.sort_values("atr20_pct", ascending=True).head(args.topn)
    save(lowvol[["symbol","date","close","atr20_pct","ret_20d","avg20_turnover"]], "low_volatility.csv")

    # 一併輸出快照總表
    save_cols = ["symbol","date","close","open","high","low",
                 "ret_1d","ret_5d","ret_20d","ret_60d","ret_ytd",
                 "avg20_vol","avg20_turnover","vol_spike","turnover_spike",
                 "hi_52w","lo_52w","dist_52w_hi","dist_52w_lo","atr20_proxy","atr20_pct"]
    snap_out = snap[[c for c in save_cols if c in snap.columns]].copy()
    save(snap_out, "snapshot_all.csv")

    print("[INFO] Done.")
if __name__ == "__main__":
    main()
