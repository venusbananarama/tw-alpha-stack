import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument("--summary", required=True)
ap.add_argument("--outdir", default=None)
a = ap.parse_args()

df = pd.read_csv(a.summary)
outdir = Path(a.outdir) if a.outdir else Path(a.summary).parent
outdir.mkdir(parents=True, exist_ok=True)

plt.figure()
for fac, g in df.groupby("factor"):
    plt.scatter(g["CAGR"], g["Sharpe"], label=str(fac))
plt.xlabel("CAGR"); plt.ylabel("Sharpe"); plt.title("Sharpe vs CAGR"); plt.legend()
plt.tight_layout(); plt.savefig(outdir / "scatter_sharpe_cagr.png", dpi=160); plt.close()

df.sort_values("Sharpe", ascending=False).to_csv(outdir / "summary_sorted_by_sharpe.csv", index=False)
print("[OK] Charts saved to", outdir)
