# scripts/export_symbol.py
import argparse
import os
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True, help="Path to alpha_factors_fixed.parquet")
    parser.add_argument("--symbol", required=True, help="Stock symbol, e.g. 6669.TW")
    parser.add_argument("--out", required=True, help="Output folder, e.g. G:/AI/fatai/out")
    args = parser.parse_args()

    df = pd.read_parquet(args.factors)
    df = df[df['symbol'] == args.symbol].copy()

    if df.empty:
        raise ValueError(f"Symbol {args.symbol} not found in {args.factors}")

    # 嘗試找行情欄位
    possible_cols = ["open", "high", "low", "close", "volume"]
    if not all(c in df.columns for c in possible_cols):
        # 如果只有報酬 ret，就造一個假 close（累積 nav）
        if "ret" in df.columns:
            df = df.sort_values("date")
            df["close"] = (1 + df["ret"]).cumprod()
            df["open"] = df["close"]
            df["high"] = df["close"]
            df["low"] = df["close"]
            df["volume"] = 0
        else:
            raise ValueError("No OHLCV or ret columns available in factors file")

    out_dir = os.path.join(args.out, args.symbol)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "ohlcv.csv")

    out_df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    out_df.to_csv(out_path, index=False)
    print(f"[OK] Saved {args.symbol} → {out_path}, rows={len(out_df)}")

if __name__ == "__main__":
    main()
