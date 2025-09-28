#!/usr/bin/env python
# scripts/plot_nav.py
import argparse, os, sys
def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--nav", required=True, help="path to nav.csv")
    p.add_argument("--out", default="", help="output png path (default next to nav.csv)")
    return p.parse_args()
def main():
    a = parse()
    try:
        import pandas as pd
        df = pd.read_csv(a.nav)
    except Exception as ex:
        print("[fatal] failed to read nav:", ex); sys.exit(1)
    out = a.out or os.path.join(os.path.dirname(a.nav), "nav.png")
    try:
        import matplotlib.pyplot as plt
        fig = plt.figure()
        plt.plot(df["date"], df["nav"])
        plt.title("NAV")
        plt.xlabel("date"); plt.ylabel("nav")
        fig.autofmt_xdate()
        plt.tight_layout()
        plt.savefig(out, dpi=150)
        print("[ok] wrote plot ->", out)
    except Exception as ex:
        print("[warn] matplotlib not available, skipping plot:", ex)
        print("Install with: pip install matplotlib")
        sys.exit(2)
if __name__ == "__main__":
    main()
