import argparse
import json
from pathlib import Path
import pandas as pd
from dataclasses import asdict
from datetime import datetime

# Import core backtest pieces
from backtest.core import BTConfig, backtest_topN

DEFAULTS = {
    "topN": 50,
    "rebalance": "M",
    "fees": 0.0005,
    "slippage": 0.0005,
    "delay": 1,
    "start": None,
    "end": None,
    "factor": "composite_score",
}

def load_cfg(path: str | None) -> dict:
    cfg = DEFAULTS.copy()
    if path:
        # Support both YAML and JSON for simplicity
        if path.lower().endswith((".yml", ".yaml")):
            import yaml  # requires PyYAML
            with open(path, "r", encoding="utf-8") as f:
                user = yaml.safe_load(f) or {}
        else:
            with open(path, "r", encoding="utf-8") as f:
                user = json.load(f) or {}
        # merge (only known keys)
        for k in list(user.keys()):
            if k not in cfg:
                # ignore unknown keys
                continue
        cfg.update({k: user[k] for k in cfg.keys() if k in user})
    return cfg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factors", required=True, help="Path to factors parquet")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--config", default=None, help="YAML/JSON config")
    ap.add_argument("--factor", default=None, help="Factor column to rank (overrides config)")
    ap.add_argument("--start", default=None, help="Override start date (YYYY-MM-DD)")
    ap.add_argument("--end", default=None, help="Override end date (YYYY-MM-DD)")
    args = ap.parse_args()

    cfg_dict = load_cfg(args.config)
    if args.factor:
        cfg_dict["factor"] = args.factor
    if args.start:
        cfg_dict["start"] = args.start
    if args.end:
        cfg_dict["end"] = args.end

    # Load factors
    fac = pd.read_parquet(args.factors)
    # sanity columns
    required = {"date", "symbol", "ret", cfg_dict["factor"]}
    missing = required - set(fac.columns)
    if missing:
        raise ValueError(f"missing columns in factors parquet: {missing}")

    fac["date"] = pd.to_datetime(fac["date"])

    # If start/end not set, default to data range
    if not cfg_dict["start"]:
        cfg_dict["start"] = fac["date"].min().strftime("%Y-%m-%d")
    if not cfg_dict["end"]:
        cfg_dict["end"] = fac["date"].max().strftime("%Y-%m-%d")

    # Build BTConfig (only supported keys)
    bt_kwargs = {k: cfg_dict[k] for k in ("topN","rebalance","fees","slippage","delay","start","end")}
    cfg = BTConfig(**bt_kwargs)
    # NEW: pass factor column via dynamic attribute (supported by patched core.py v3+)
    cfg.factor_col = cfg_dict["factor"]

    nav, pos = backtest_topN(fac, cfg)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    nav.to_csv(out_dir / "nav.csv", header=["nav"])
    pos.to_csv(out_dir / "positions.csv", index=False)

    # Build performance using core metrics if available
    # Recompute here for robustness:
    def _metrics(nav: pd.Series) -> dict:
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

    perf = _metrics(nav)
    perf["start"] = cfg.start
    perf["end"] = cfg.end
    perf["topN"] = cfg.topN
    perf["rebalance"] = cfg.rebalance
    perf["fees"] = cfg.fees
    perf["slippage"] = cfg.slippage
    perf["delay"] = cfg.delay
    perf["factor"] = cfg.factor_col

    (out_dir / "performance.json").write_text(json.dumps(perf, indent=2), encoding="utf-8")
    print("[OK] Backtest finished.")
    print(json.dumps(perf, indent=2))

if __name__ == "__main__":
    main()
