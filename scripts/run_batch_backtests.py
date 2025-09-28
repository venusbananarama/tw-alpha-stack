#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_batch_backtests.py
----------------------
Minimal-but-solid batch runner for AlphaCity backtests.

It expands a grid of parameters and launches your existing backtest script
(e.g., backtest/longonly_topN.py) via subprocess to avoid import dependency
issues. After each run, it optionally calls make_report_safe.py to compute
and collect metrics, then produces a summary CSV.

Usage example:
python run_batch_backtests.py ^
  --backtest-cmd "python backtest/longonly_topN.py --factors {factors} --out-dir {out_dir} --config {config}" ^
  --grid-yaml configs/batch_grid_example.yaml ^
  --reports yes ^
  --report-script make_report_safe.py ^
  --report-benchmark-csv "" ^
  --out-root G:/AI/datahub/alpha/backtests/grid_runs

Notes:
- You can use placeholders in --backtest-cmd:
  {factors} {out_dir} {config} {extra}
- Each grid item can define factors/extra/config overrides.
- Basic retry provided to avoid single-run failures killing the batch.
"""
import argparse
import itertools
import json
import os
import subprocess
import sys
from pathlib import Path
import time
import uuid

import pandas as pd
import yaml

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--grid-yaml", required=True, help="YAML defining parameter grid")
    p.add_argument("--backtest-cmd", required=True, help="Backtest command template with placeholders")
    p.add_argument("--out-root", required=True, help="Root output directory for runs")
    p.add_argument("--reports", choices=["yes","no"], default="yes", help="Generate reports & summary")
    p.add_argument("--report-script", default="make_report_safe.py", help="Path to make_report_safe.py")
    p.add_argument("--report-benchmark-csv", default="", help="Optional benchmark csv for relative plots")
    p.add_argument("--max-retries", type=int, default=1, help="Retries per job on failure")
    p.add_argument("--sleep-seconds", type=float, default=0.2, help="Sleep between launches to be gentle on IO")
    p.add_argument("--timeout-seconds", type=int, default=0, help="Per-run timeout (0=none)")
    return p.parse_args()

def _load_grid(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Expected structure:
    # base:
    #   config: "configs/backtest_topN_fixed.yaml"
    #   factors: ["composite_score", "mom_252_21", "vol_20"]
    #   topN: [30, 50]
    #   fees_bps: [2.5, 10]
    #   tax_bps: [30]
    #   extra: ["", "--neutralize_by industry"]
    # overrides: [ {name: "quick", topN:[10]} ]
    return cfg

def _expand_grid(cfg: dict):
    base = cfg.get("base", {})
    # keys to expand as grid (list-like values)
    keys = [k for k,v in base.items() if isinstance(v, (list, tuple))]
    values = [base[k] for k in keys]

    # produce cartesian product
    for combo in itertools.product(*values):
        item = dict(base)  # shallow copy
        for k, val in zip(keys, combo):
            item[k] = val
        yield item

def _safe_name(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_=+." else "_" for ch in s)[:120]

def _run_cmd(cmd: str, timeout: int=0):
    try:
        if timeout and timeout > 0:
            p = subprocess.run(cmd, shell=True, timeout=timeout)
        else:
            p = subprocess.run(cmd, shell=True)
        return p.returncode
    except subprocess.TimeoutExpired:
        return 124

def main():
    args = _parse_args()
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    grid_cfg = _load_grid(Path(args.grid_yaml))
    base_config = grid_cfg.get("base", {}).get("config", "")
    summary_rows = []

    for item in _expand_grid(grid_cfg):
        # Compose run-specific out_dir & placeholders
        name_bits = [
            f"fac={item.get('factors')}",
            f"topN={item.get('topN')}",
            f"fees={item.get('fees_bps')}",
            f"tax={item.get('tax_bps')}",
        ]
        run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
        run_dir = out_root / _safe_name("__".join(name_bits)) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        placeholders = {
            "factors": item.get("factors"),
            "out_dir": str(run_dir).replace("\\", "/"),
            "config": item.get("config", base_config),
            "extra": item.get("extra", ""),
        }
        cmd = args.backtest_cmd.format(**placeholders)

        # Basic retry
        tries = 0
        ret = -1
        while tries <= args.max_retries:
            print(f"[RUN] {cmd}")
            ret = _run_cmd(cmd, timeout=args.timeout_seconds)
            if ret == 0:
                break
            tries += 1
            print(f"[WARN] run failed (code={ret}), retry {tries}/{args.max_retries}")
            time.sleep(1)

        record = {
            "run_dir": str(run_dir),
            "status": "ok" if ret == 0 else f"fail({ret})",
            "cmd": cmd,
            **{k: item.get(k) for k in ["factors","topN","fees_bps","tax_bps","neutralize_by","rebalance"] if k in item}
        }

        # Optionally make report + read metrics
        if args.reports == "yes" and ret == 0:
            nav_csv = run_dir / "nav.csv"
            if nav_csv.exists():
                report_cmd = [
                    sys.executable, args.report_script,
                    "--nav-csv", str(nav_csv),
                    "--out-dir", str(run_dir)
                ]
                if args.report_benchmark_csv:
                    report_cmd += ["--benchmark-csv", args.report_benchmark_csv]
                print("[REPORT]", " ".join(report_cmd))
                r = subprocess.run(report_cmd)
                if r.returncode == 0:
                    mpath = run_dir / "metrics.json"
                    if mpath.exists():
                        try:
                            metrics = json.loads(mpath.read_text(encoding="utf-8"))
                            # flatten
                            port = metrics.get("portfolio", {})
                            bench = metrics.get("benchmark", {})
                            record.update({f"port_{k}": v for k,v in port.items()})
                            if bench:
                                record.update({f"bench_{k}": v for k,v in bench.items()})
                            if "relative_total" in metrics:
                                record["relative_total"] = metrics["relative_total"]
                        except Exception as e:
                            print(f"[WARN] failed to parse metrics.json: {e}")
            else:
                print(f"[WARN] nav.csv not found in {run_dir}")

        summary_rows.append(record)
        time.sleep(args.sleep_seconds)

    # Write summary
    df = pd.DataFrame(summary_rows)
    df.to_csv(out_root / "batch_summary.csv", index=False)
    print(f"Done. Summary -> {out_root/'batch_summary.csv'}")

if __name__ == "__main__":
    main()
