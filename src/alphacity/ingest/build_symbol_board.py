# -*- coding: utf-8 -*-
"""
Build or update a Taiwan symbol → board mapping CSV from a merged parquet.
"""
import argparse
import os
import sys
import pandas as pd

def guess_board(code: str) -> str:
    # Basic heuristics for Taiwan tickers; safe defaults.
    # - ETF: starts with '00' (e.g., 0050, 0056, 00878 ...)
    # - TDR/Foreign: starts with '91'
    # - Emerging (興櫃): starts with '9' (e.g., 9958) and length == 4
    # - Other: length > 4 or non-digit codes
    # - MainBoard: fallback for typical 4-digit stocks
    if not code.isdigit():
        return "Other"
    if code.startswith("00"):
        return "ETF"
    if code.startswith("91"):
        return "TDR"
    if len(code) > 4:
        return "Other"
    if len(code) == 4 and code.startswith("9"):
        return "Emerging"
    return "MainBoard"

def main():
    p = argparse.ArgumentParser(description="Build or update symbol_board.csv from a merged parquet.")
    p.add_argument("--from-parquet", required=True, help="Path to ohlcv_daily_all.parquet")
    p.add_argument("--out", required=True, help="Output CSV path, e.g. G:/AI/datahub/metadata/symbol_board.csv")
    p.add_argument("--force", action="store_true", help="Overwrite instead of incremental merge")
    args = p.parse_args()

    if not os.path.exists(args.from_parquet):
        print(f"[ERROR] parquet not found: {args.from_parquet}")
        sys.exit(2)

    print(f"[INFO] Reading symbols from {args.from_parquet} ...")
    df = pd.read_parquet(args.from_parquet, columns=["symbol"]).dropna()
    syms = sorted(df["symbol"].astype(str).unique())
    print(f"[INFO] Unique symbols: {len(syms)}")

    columns = ["symbol", "code", "board", "industry", "name", "note"]
    if os.path.exists(args.out) and not args.force:
        print(f"[INFO] Found existing mapping: {args.out} (incremental update)")
        existing = pd.read_csv(args.out, dtype=str, encoding="utf-8-sig")
        for c in columns:
            if c not in existing.columns:
                existing[c] = ""
        existing["symbol"] = existing["symbol"].astype(str)
        exist_set = set(existing["symbol"].tolist())
        new_rows = []
        for s in syms:
            if s not in exist_set:
                code = s.split(".")[0]
                new_rows.append({
                    "symbol": s,
                    "code": code,
                    "board": guess_board(code),
                    "industry": "",
                    "name": "",
                    "note": "auto-added",
                })
        if new_rows:
            print(f"[INFO] Adding {len(new_rows)} new symbols.")
            add_df = pd.DataFrame(new_rows, columns=columns)
            out_df = pd.concat([existing[columns], add_df], ignore_index=True)
        else:
            print("[INFO] No new symbols to add.")
            out_df = existing[columns]
    else:
        print("[INFO] Creating fresh mapping ...")
        rows = []
        for s in syms:
            code = s.split(".")[0]
            rows.append({
                "symbol": s,
                "code": code,
                "board": guess_board(code),
                "industry": "",
                "name": "",
                "note": "heuristic",
            })
        out_df = pd.DataFrame(rows, columns=columns)

    # Sort for human-friendly editing
    out_df["code_num"] = pd.to_numeric(out_df["code"], errors="coerce")
    out_df = out_df.sort_values(["board", "code_num", "symbol"], na_position="last")
    out_df = out_df.drop(columns=["code_num"])

    # Ensure utf-8-sig for Excel-friendly BOM
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_df.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"[INFO] Wrote mapping → {args.out}")
    # Summary
    print("[INFO] Board counts:")
    try:
        print(out_df["board"].value_counts(dropna=False).to_string())
    except Exception:
        pass

if __name__ == "__main__":
    main()
