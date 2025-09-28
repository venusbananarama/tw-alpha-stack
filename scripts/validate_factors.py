#!/usr/bin/env python
# scripts/validate_factors.py
import argparse, os, sys, pandas as pd, json
def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--factors-path", required=True)
    p.add_argument("--want", type=str, default="composite_score")
    p.add_argument("--out", type=str, default="")
    return p.parse_args()
def main():
    a = parse(); df = pd.read_parquet(a.factors_path)
    have = set(df.columns)
    want = [x.strip() for x in a.want.replace(","," ").split() if x.strip()]
    missing = [w for w in want if w not in have]
    result = {"path": a.factors_path, "ncols": len(df.columns), "nrows": int(len(df)), "want": want, "missing": missing}
    txt = json.dumps(result, ensure_ascii=False, indent=2)
    print(txt)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f: f.write(txt + "\n")
if __name__ == "__main__":
    main()
