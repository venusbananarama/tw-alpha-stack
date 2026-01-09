#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch grid runner for TopN backtests (P2-MVP).
"""
from __future__ import annotations

import argparse
import copy
import itertools
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import yaml  # type: ignore


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--grid-yaml", required=True, help="Grid config YAML")
    p.add_argument("--backtest-cmd", required=True, help="Backtest command template")
    p.add_argument("--out-root", required=True, help="Output root directory")
    p.add_argument("--reports", default="no", choices=["yes", "no"], help="Whether to run reports")
    p.add_argument("--report-script", default="make_report_safe.py", help="Report script path")
    p.add_argument("--max-workers", type=int, default=1, help="Reserved for future parallelism")
    p.add_argument("--fail-fast", default="no", choices=["yes", "no"], help="Stop on first failure")
    return p.parse_args()


def _load_yaml(path: Path) -> Dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Grid config must be a mapping: {path}")
    return raw


def _set_nested(d: Dict, dotted_key: str, value) -> None:
    parts = dotted_key.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _iter_grid(grid: Dict[str, Iterable]) -> List[Dict[str, object]]:
    keys = list(grid.keys())
    values = [list(grid[k]) if isinstance(grid[k], list) else [grid[k]] for k in keys]
    combos = []
    for vals in itertools.product(*values):
        combos.append({k: v for k, v in zip(keys, vals)})
    return combos


def _build_cost_tag(cfg: Dict) -> str:
    costs = cfg.get("costs", {})
    fees = costs.get("fees_bps")
    tax = costs.get("tax_bps")
    slip = costs.get("slip_bps")
    parts = []
    if fees is not None:
        parts.append(f"fees={fees}")
    if tax is not None:
        parts.append(f"tax={tax}")
    if slip is not None:
        parts.append(f"slip={slip}")
    return "_".join(parts) if parts else "costs=none"


def _run_cmd(cmd_str: str) -> subprocess.CompletedProcess:
    cmd_list = shlex.split(cmd_str, posix=(os.name != "nt"))
    return subprocess.run(cmd_list, capture_output=True, text=True)


def _read_metrics(path: Path) -> Tuple[Dict, Dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    metrics = raw.get("metrics", {}) if isinstance(raw, dict) else {}
    costs = raw.get("costs", {}) if isinstance(raw, dict) else {}
    return metrics, costs


def _summarize(df: pd.DataFrame, metric_col: str) -> Dict:
    if df.empty or metric_col not in df.columns:
        return {}
    data = df.dropna(subset=[metric_col]).sort_values(metric_col, ascending=False)
    if data.empty:
        return {}
    median_idx = int(len(data) // 2)
    best = data.iloc[0].to_dict()
    worst = data.iloc[-1].to_dict()
    median = data.iloc[median_idx].to_dict()
    return {"best": best, "median": median, "worst": worst}


def main() -> int:
    try:
        args = _parse_args()
        grid_cfg = _load_yaml(Path(args.grid_yaml))

        base_config = grid_cfg.get("base_config") or grid_cfg.get("base", {}).get("config")
        if not base_config:
            raise SystemExit("grid_yaml.base_config is required.")
        base_config_path = Path(base_config)
        base_cfg = yaml.safe_load(base_config_path.read_text(encoding="utf-8")) or {}

        inputs_cfg = grid_cfg.get("inputs", {})
        factors_path = inputs_cfg.get("factors_path") or grid_cfg.get("base", {}).get("factors")
        if not factors_path:
            raise SystemExit("grid_yaml.inputs.factors_path is required.")

        grid = grid_cfg.get("grid") or {}
        if not isinstance(grid, dict) or not grid:
            raise SystemExit("grid_yaml.grid must be a non-empty mapping.")

        reporting_cfg = grid_cfg.get("reporting", {})
        reports_enabled = args.reports == "yes" or bool(reporting_cfg.get("enabled", False))
        report_root = Path(reporting_cfg.get("out_dir") or Path(args.out_root) / "reports")

        out_root = Path(args.out_root)
        out_root.mkdir(parents=True, exist_ok=True)

        combos = _iter_grid(grid)
        results_rows: List[Dict] = []
        failures: List[Dict] = []

        for idx, combo in enumerate(combos, start=1):
            run_id = f"run_{idx:03d}"
            run_dir = out_root / run_id
            run_dir.mkdir(parents=True, exist_ok=True)

            effective_cfg = copy.deepcopy(base_cfg)
            for k, v in combo.items():
                _set_nested(effective_cfg, k, v)

            cfg_path = run_dir / "config_effective.yaml"
            cfg_path.write_text(yaml.safe_dump(effective_cfg, sort_keys=False), encoding="utf-8")

            cmd = args.backtest_cmd.format(
                factors=str(Path(factors_path)),
                out_dir=str(run_dir),
                config=str(cfg_path),
                run_id=run_id,
            )
            proc = _run_cmd(cmd)
            status = "ok" if proc.returncode == 0 else "fail"

            metrics_path = run_dir / "metrics.json"
            nav_path = run_dir / "nav_clean.csv"
            metrics = {}
            costs = {}
            if status == "ok" and metrics_path.exists():
                metrics, costs = _read_metrics(metrics_path)
            elif status == "ok":
                status = "fail"
                proc = subprocess.CompletedProcess(proc.args, 2, proc.stdout, "missing metrics.json")

            report_status = "skipped"
            if status == "ok" and reports_enabled:
                report_dir = report_root / run_id
                report_dir.mkdir(parents=True, exist_ok=True)
                cost_tag = _build_cost_tag(effective_cfg)
                report_cmd = [
                    sys.executable,
                    str(Path(args.report_script)),
                    "--nav-csv",
                    str(nav_path),
                    "--out-dir",
                    str(report_dir),
                    "--title",
                    f"Batch Report {run_id}",
                    "--cost-tag",
                    cost_tag,
                ]
                bench = effective_cfg.get("data", {}).get("benchmarks_path")
                if bench:
                    report_cmd.extend(["--bench-csv", str(bench)])
                rep = subprocess.run(report_cmd, capture_output=True, text=True)
                report_status = "ok" if rep.returncode == 0 else "fail"
                if report_status == "fail":
                    failures.append(
                        {
                            "run_id": run_id,
                            "stage": "report",
                            "return_code": rep.returncode,
                            "stderr": rep.stderr[-800:] if rep.stderr else "",
                        }
                    )

            if status == "fail":
                failures.append(
                    {
                        "run_id": run_id,
                        "stage": "backtest",
                        "return_code": proc.returncode,
                        "stderr": proc.stderr[-800:] if proc.stderr else "",
                    }
                )
                if args.fail_fast == "yes":
                    break

            row = {"run_id": run_id, "status": status, "run_dir": str(run_dir), "report_status": report_status}
            row.update({f"param_{k}": v for k, v in combo.items()})
            row["cost_tag"] = _build_cost_tag(effective_cfg)
            for key in ("CAGR", "Sharpe", "MaxDD", "Turnover"):
                row[key] = metrics.get(key)
            for ck in ("fees_bps", "tax_bps", "slip_bps", "total_bps"):
                if ck in costs:
                    row[ck] = costs.get(ck)
            results_rows.append(row)

        results_df = pd.DataFrame(results_rows)
        results_df.to_csv(out_root / "grid_results.csv", index=False)

        summary = {
            "overall": _summarize(results_df, "Sharpe"),
            "by_cost_tag": {},
            "counts": {
                "total_runs": int(len(results_df)),
                "ok": int((results_df["status"] == "ok").sum()) if not results_df.empty else 0,
                "fail": int((results_df["status"] == "fail").sum()) if not results_df.empty else 0,
            },
        }
        if "cost_tag" in results_df.columns:
            for tag, g in results_df.groupby("cost_tag"):
                summary["by_cost_tag"][tag] = _summarize(g, "Sharpe")

        (out_root / "grid_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        (out_root / "failures.json").write_text(json.dumps(failures, indent=2), encoding="utf-8")

        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
