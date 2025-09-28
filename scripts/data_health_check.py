
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data_health_check.py
對合併後資料做健康檢查並輸出報告。
輸出：
- data_health_summary.csv  (每檔概況)
- data_health_issues.csv   (只列出有問題的檔)
"""
import argparse
import os
import numpy as np
import pandas as pd

REQ_COLS = {"date","open","high","low","close","adj_close","volume","symbol"}

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c) for c in df.columns]
    lower = [c.lower() for c in cols]
    if REQ_COLS.issubset(set(lower)):
        mapping = {c: c.lower() for c in df.columns}
        return df.rename(columns=mapping)
    mapping = {}
    for want in REQ_COLS:
        matched = None
        for c in df.columns:
            cl = str(c).lower()
            if want in cl:
                if want == "close" and "adj_close" in cl:
                    continue
                matched = c
                break
        if matched is not None:
            mapping[matched] = want
    out = df.rename(columns=mapping)
    missing = REQ_COLS - set([str(c).lower() for c in out.columns])
    if missing:
        raise ValueError(f"Missing required columns after normalize: {missing}")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--merged-path", required=True)
    ap.add_argument("--out-root", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_root, exist_ok=True)
    df = pd.read_parquet(args.merged_path)
    df = _normalize_columns(df)
    df["date"] = pd.to_datetime(df["date"])

    def per_symbol(g: pd.DataFrame):
        g = g.sort_values("date")
        rows = len(g)
        start = g["date"].iloc[0]
        end = g["date"].iloc[-1]
        dup_dates = g["date"].duplicated().sum()
        na_rows = int(g[["open","high","low","close","adj_close","volume"]].isna().any(axis=1).sum())
        neg_price = int((g[["open","high","low","close","adj_close"]] < 0).any(axis=1).sum())
        zero_vol = int((g["volume"] == 0).sum())
        non_monotonic = int((g["date"].diff().dt.days.fillna(1) < 0).any())
        return pd.Series({
            "rows": rows,
            "start_date": start,
            "end_date": end,
            "dup_dates": dup_dates,
            "na_rows": na_rows,
            "neg_price_rows": neg_price,
            "zero_volume_rows": zero_vol,
            "non_monotonic_date": non_monotonic
        })

    summ = df.groupby("symbol").apply(per_symbol).reset_index()
    summ = summ.sort_values("symbol")
    summ_path = os.path.join(args.out_root, "data_health_summary.csv")
    summ.to_csv(summ_path, index=False, encoding="utf-8-sig")

    issues = summ.query("dup_dates>0 or na_rows>0 or neg_price_rows>0 or non_monotonic_date>0")
    issues_path = os.path.join(args.out_root, "data_health_issues.csv")
    issues.to_csv(issues_path, index=False, encoding="utf-8-sig")

    print(f"[INFO] Wrote {summ_path} ({len(summ)} rows)")
    print(f"[INFO] Wrote {issues_path} ({len(issues)} rows)")
    print("[INFO] Done.")
if __name__ == "__main__":
    main()
