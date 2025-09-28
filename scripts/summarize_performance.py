import argparse, json
from pathlib import Path
import pandas as pd

def compute_from_nav(nav_csv: Path):
    nav = pd.read_csv(nav_csv, index_col=0).iloc[:,0]
    r = nav.pct_change().dropna()
    if len(r)==0:
        return {"CAGR":0,"AnnVol":0,"Sharpe":0,"MaxDD":0,"HitRatio":0,"Length":0}
    ANN = 252
    cagr = float((nav.iloc[-1] / nav.iloc[0]) ** (ANN/len(nav)) - 1)
    ann_vol = float(r.std() * (ANN ** 0.5))
    sharpe = float((r.mean()/r.std() * (ANN ** 0.5)) if r.std()!=0 else 0.0)
    dd = float((nav / nav.cummax() - 1).min())
    hit = float((r>0).mean())
    return {"CAGR":cagr,"AnnVol":ann_vol,"Sharpe":sharpe,"MaxDD":dd,"HitRatio":hit,"Length":int(len(nav))}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out = Path(args.out_dir)
    perf_json = out / "performance.json"
    if perf_json.exists():
        print(perf_json.read_text(encoding="utf-8"))
    else:
        nav_csv = out / "nav.csv"
        if not nav_csv.exists():
            print("[ERROR] nav.csv not found; run backtest first.")
            return
        perf = compute_from_nav(nav_csv)
        print(json.dumps(perf, indent=2))

if __name__ == "__main__":
    main()
