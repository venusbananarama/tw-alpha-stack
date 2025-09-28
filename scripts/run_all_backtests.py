#!/usr/bin/env python3
"""
run_all_backtests.py
One-shot pipeline:
  1) sanity-check environment
  2) run grid of backtests (factor x topN x rebalance) by generating temp YAML per run
  3) collect metrics -> summary.csv
  4) produce charts + sorted CSV
Outputs and logs land in --outdir
"""
from __future__ import annotations
import argparse, json, subprocess, sys, shutil, traceback
from pathlib import Path
from datetime import datetime

def log(msg: str):
    print(msg, flush=True)

def ensure_modules(mods: list[str]) -> dict[str, tuple[bool, str]]:
    out = {}
    for m in mods:
        try:
            mod = __import__(m)
            ver = getattr(mod, "__version__", "?")
            out[m] = (True, str(ver))
        except Exception as e:
            out[m] = (False, str(e))
    return out

def load_yaml(path: Path) -> dict:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def dump_yaml(data: dict, path: Path):
    import yaml
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

def run_one(factors: Path, base_cfg: Path, outdir: Path, factor: str, topn: int, rebalance: str) -> dict | None:
    # 1) Make subfolder
    sub = outdir / f"{factor}_N{topn}_{rebalance}"
    sub.mkdir(parents=True, exist_ok=True)

    # 2) Load base YAML & patch params
    cfg = load_yaml(base_cfg)
    # Standard BTConfig fields we know (topN, rebalance, fees, slippage, delay, start, end)
    cfg["topN"] = int(topn)
    cfg["rebalance"] = str(rebalance).upper()

    # 3) Write temp cfg into subfolder
    tmp_cfg = sub / "config.generated.yaml"
    dump_yaml(cfg, tmp_cfg)

    # 4) Call backtest with current Python interpreter for environment consistency
    cmd = [
        sys.executable, "-m", "backtest.longonly_topN",
        "--factors", str(factors),
        "--out-dir", str(sub),
        "--config", str(tmp_cfg),
        "--factor", factor,
    ]
    log("[RUN] " + " ".join(cmd))
    try:
        cp = subprocess.run(cmd, check=True, capture_output=True, text=True)
        # Mirror stdout to a log file
        (sub / "run_stdout.log").write_text(cp.stdout, encoding="utf-8")
        (sub / "run_stderr.log").write_text(cp.stderr, encoding="utf-8")
    except subprocess.CalledProcessError as e:
        (sub / "run_failed.log").write_text(e.stderr or str(e), encoding="utf-8")
        log(f"[ERROR] run failed for {factor} N{topn} {rebalance}: {e}")
        return None

    # 5) Read metrics
    perf_file = sub / "performance.json"
    if perf_file.exists():
        try:
            perf = json.loads(perf_file.read_text(encoding="utf-8"))
            perf.update({"factor": factor, "topN": topn, "rebalance": rebalance})
            return perf
        except Exception as e:
            log(f"[WARN] failed to parse performance.json for {factor} N{topn} {rebalance}: {e}")
    else:
        log(f"[WARN] missing performance.json for {factor} N{topn} {rebalance}")
    return None

def analyze(summary_csv: Path, outdir: Path):
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv(summary_csv)
    # Scatter chart: Sharpe vs CAGR
    plt.figure()
    for fac, g in df.groupby("factor"):
        plt.scatter(g["CAGR"], g["Sharpe"], label=str(fac))
    plt.xlabel("CAGR"); plt.ylabel("Sharpe"); plt.title("Sharpe vs CAGR")
    plt.legend(); plt.tight_layout()
    (outdir / "scatter_sharpe_cagr.png").parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outdir / "scatter_sharpe_cagr.png", dpi=160)
    plt.close()

    # Sorted table by Sharpe
    df.sort_values("Sharpe", ascending=False).to_csv(outdir / "summary_sorted_by_sharpe.csv", index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factors", required=True, help="Path to factors parquet")
    ap.add_argument("--config", required=True, help="Path to base YAML config (will be cloned per run)")
    ap.add_argument("--outdir", required=True, help="Output folder for all results")
    ap.add_argument("--factorset", nargs="+", default=["composite_score","mom_252_21","vol_20"])
    ap.add_argument("--topn", nargs="+", type=int, default=[20,50,100])
    ap.add_argument("--rebalance", nargs="+", default=["M","W"])
    ap.add_argument("--skip-charts", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    log_path = outdir / "run_all.log"

    def write_log(s): 
        with open(log_path, "a", encoding="utf-8") as f: f.write(s + "\n")

    write_log(f"=== run_all_backtests start @ {datetime.now().isoformat()} ===")
    write_log(f"Python: {sys.executable}")

    # Sanity check modules up-front
    mods = ensure_modules(["pandas","numpy","yaml","pyarrow","matplotlib","openpyxl"])
    for k,(ok,ver) in mods.items():
        write_log(f"[ENV] {k}: {'OK '+ver if ok else 'MISS -> '+ver}")
        if not ok and k == "yaml":
            print("[FATAL] PyYAML not available. Install with: pip install pyyaml", file=sys.stderr)
            sys.exit(2)

    # Run grid
    rows = []
    for fac in args.factorset:
        for n in args.topn:
            for r in args.rebalance:
                perf = run_one(Path(args.factors), Path(args.config), outdir, fac, n, r)
                if perf: rows.append(perf)

    # Save summary
    import pandas as pd
    if rows:
        df = pd.DataFrame(rows)
        summary_csv = outdir / "summary.csv"
        df.to_csv(summary_csv, index=False)
        log(f"[OK] Summary saved to {summary_csv}")
        write_log(f"[OK] Summary saved to {summary_csv}")
        if not args.skip_charts:
            analyze(summary_csv, outdir)
            log(f"[OK] Charts saved to {outdir}")
            write_log(f"[OK] Charts saved to {outdir}")
        try:
            log(df.sort_values("Sharpe", ascending=False).head().to_string(index=False))
        except Exception:
            pass
    else:
        log("[WARN] No runs succeeded; nothing to summarize.")

    write_log(f"=== run_all_backtests end @ {datetime.now().isoformat()} ===")

if __name__ == "__main__":
    main()
