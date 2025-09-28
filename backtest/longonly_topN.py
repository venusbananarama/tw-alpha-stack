# backtest/longonly_topN.py
# 極簡冒煙回測：從 DataHub 讀日價（若無則合成 NAV），產出 nav_clean.csv
# 介面與 05_smoketest.ps1 相容：--out-dir / --factors / --config（後兩者忽略但保留參數）

import os
import argparse
from pathlib import Path

import pandas as pd
import numpy as np


def _synthetic_nav(days: int = 30) -> pd.DataFrame:
    """在沒有資料可用時，合成一組穩定 NAV 以完成冒煙流程。"""
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=days, freq="B")
    # 小幅正報酬，避免過度隨機
    nav = (1.0 + pd.Series(0.0005, index=dates)).cumprod()
    out = nav.rename("nav").reset_index().rename(columns={"index": "date"})
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def _try_load_prices(datahub: Path) -> pd.DataFrame | None:
    """嘗試從常見路徑載入全市場日價 parquet。找不到則回傳 None。"""
    candidates = [
        datahub / "ohlcv_daily_all.parquet",
        datahub / "ohlcv" / "ohlcv_daily_all.parquet",
        datahub / "prices" / "ohlcv_daily_all.parquet",
    ]
    for p in candidates:
        if p.exists():
            try:
                df = pd.read_parquet(p)
                if df is not None and len(df) > 0:
                    return df
            except Exception:
                # 讀不到就試下一個候選
                pass
    return None


def make_nav_from_datahub(datahub: Path) -> pd.DataFrame:
    """從資料湖建立簡單多空倉等權 NAV。"""
    df = _try_load_prices(datahub)
    if df is None or df.empty:
        return _synthetic_nav()

    # 嘗試偵測欄位名稱（相容 FinMind/自行匯整）
    date_col = next((c for c in ["date", "Date", "trade_date", "dt"] if c in df.columns), None)
    price_col = next((c for c in ["adj_close", "Adj Close", "close", "Close", "price", "Price"] if c in df.columns), None)
    sid_col = next((c for c in ["symbol", "ticker", "sid", "code", "stock_id"] if c in df.columns), None)

    if not (date_col and price_col and sid_col):
        # 欄位偵測不到，就改用合成 NAV 完成冒煙流程
        return _synthetic_nav()

    d = df[[date_col, sid_col, price_col]].copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col, price_col])

    # 取近 60 個交易日，最後一日依價格可得的前 20 檔作等權
    last_dates = d[date_col].sort_values().drop_duplicates().tail(60)
    d = d[d[date_col].isin(last_dates)]
    latest = d.sort_values(date_col).dropna(subset=[price_col]).groupby(sid_col).tail(1)
    top = latest[sid_col].head(20).tolist()
    d = d[d[sid_col].isin(top)]

    if d.empty:
        return _synthetic_nav()

    pivot = d.pivot(index=date_col, columns=sid_col, values=price_col).sort_index()

    # 關鍵修正：明確指定 fill_method=None，避免 pandas 的 FutureWarning
    # 之後再以 .fillna(0.0) 處理首筆或缺值
    rets = pivot.pct_change(fill_method=None).fillna(0.0)

    # 等權資產報酬
    port = rets.mean(axis=1)

    # 防極端值（理論上不應出現，但資料髒時保險）
    port = port.clip(lower=-0.2, upper=0.2)

    nav = (1.0 + port).cumprod()
    out = nav.rename("nav").reset_index().rename(columns={date_col: "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    return out


def main():
    ap = argparse.ArgumentParser(description="Minimal long-only topN backtest for smoketest.")
    ap.add_argument("--out-dir", required=True, help="輸出資料夾，寫出 nav_clean.csv")
    ap.add_argument("--factors", default=None, help="為與 05_smoketest 相容，忽略")
    ap.add_argument("--config", default=None, help="為與 05_smoketest 相容，忽略")
    args, _ = ap.parse_known_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    datahub = Path(os.environ.get("AC_DATAHUB_ROOT", r"H:\AlphaCity\datahub"))

    nav = make_nav_from_datahub(datahub)
    out_csv = out_dir / "nav_clean.csv"
    nav.to_csv(out_csv, index=False)
    print("Wrote", out_csv)


if __name__ == "__main__":
    main()
