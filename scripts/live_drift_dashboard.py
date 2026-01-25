from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase4.drift import aggregate_monthly_drift, compute_daily_drift_metrics, evaluate_drift_gate  # noqa: E402
from alpha_core.phase4.errors import ExitCode, InputNotFoundError, Phase4Error  # noqa: E402
from alpha_core.phase4.ledger import atomic_write_text, write_parquet_atomic  # noqa: E402
from alpha_core.phase4.reporting import render_drift_dashboard_html  # noqa: E402


def _resolve_path(p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    return (_REPO_ROOT / path).resolve()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--exec-run-id", required=True)
    ap.add_argument("--p4-dir", default=None)
    ap.add_argument("--out-html", default=None)
    ap.add_argument("--drift-window-days", type=int, default=30)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    try:
        p4_dir = _resolve_path(args.p4_dir) if args.p4_dir else (_REPO_ROOT / "reports" / "p4" / args.as_of)
        out_html = _resolve_path(args.out_html) if args.out_html else (p4_dir / "live_drift_dashboard.html")
        replay_stats_path = p4_dir / "exec" / "replay_stats.parquet"
        if not replay_stats_path.exists():
            raise InputNotFoundError(f"replay stats not found: {replay_stats_path}")

        replay_stats = pd.read_parquet(replay_stats_path)
        daily = compute_daily_drift_metrics(replay_stats, None, None)
        monthly = aggregate_monthly_drift(daily)
        gate = evaluate_drift_gate(monthly, median_threshold_pct=0.3)

        if not monthly.empty:
            write_parquet_atomic(monthly, p4_dir / "drift_metrics.parquet")

        replay_row = replay_stats[replay_stats["symbol"] == "ALL"]
        if replay_row.empty:
            replay_row = replay_stats.iloc[[0]] if not replay_stats.empty else pd.DataFrame()
        replay_metrics = {}
        if not replay_row.empty:
            replay_metrics = {
                "p50_bps": replay_row["slippage_bps_p50"].iloc[0],
                "p95_bps": replay_row["slippage_bps_p95"].iloc[0],
            }

        impact_path = p4_dir / "exec" / "impact_calib.json"
        impact_metrics = {}
        if impact_path.exists():
            try:
                impact_metrics = json.loads(impact_path.read_text(encoding="utf-8"))
            except Exception:
                impact_metrics = {}

        summary = {
            "as_of": args.as_of,
            "run_id": args.exec_run_id,
            "status": gate.get("status", "unknown"),
            "reason_code": "DRIFT_GATE",
            "gates": {"drift": gate, "impact": impact_metrics.get("gate", {})},
            "metrics": {"replay": replay_metrics, "impact": {"mae_bps": impact_metrics.get("mae_bps")}},
        }
        html = render_drift_dashboard_html(summary, {"drift_monthly": monthly, "replay_stats": replay_stats})
        atomic_write_text(out_html, html)
        return int(ExitCode.OK)
    except Phase4Error as exc:
        sys.stderr.write(f"{exc}\n")
        return int(exc.exit_code)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"RUNTIME_ERROR: {type(exc).__name__}: {exc}\n")
        return int(ExitCode.SCHEMA_VALIDATION_FAILED)


if __name__ == "__main__":
    raise SystemExit(main())
