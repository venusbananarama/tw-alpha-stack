from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase4.align import align_exec_to_market  # noqa: E402
from alpha_core.phase4.bronze_loader import (  # noqa: E402
    canonicalize_bronze_trades,
    detect_incomplete_flag,
    load_bronze_trades,
)
from alpha_core.phase4.errors import (  # noqa: E402
    ExitCode,
    IncompleteDayError,
    Phase4Error,
    REASON_INCOMPLETE_INTRADAY_SKIPPED,
)
from alpha_core.phase4.exec_loader import load_exec_trades  # noqa: E402
from alpha_core.phase4.impact import compute_mae_bps, fit_impact_model, predict_impact, write_impact_calib  # noqa: E402
from alpha_core.phase4.ledger import ensure_out_dir  # noqa: E402
from alpha_core.phase4.replay import (  # noqa: E402
    aggregate_replay_stats,
    compute_slippage_bps,
    normalize_replay_stats_schema,
    write_replay_stats,
)
from alpha_core.phase4.schemas import REF_PRICE_MODE_LAST  # noqa: E402


def _resolve_path(p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    return (_REPO_ROOT / path).resolve()


def _gate_replay(stats_df: pd.DataFrame) -> Dict[str, object]:
    if stats_df.empty:
        return {"pass": False, "status": "insufficient_data", "p50_bps": None, "p95_bps": None}
    row = stats_df[stats_df["symbol"] == "ALL"]
    if row.empty:
        row = stats_df.iloc[[0]]
    p50 = row["slippage_bps_p50"].iloc[0]
    p95 = row["slippage_bps_p95"].iloc[0]
    if pd.isna(p50) or pd.isna(p95):
        return {"pass": False, "status": "insufficient_data", "p50_bps": p50, "p95_bps": p95}
    ok = (p50 <= 5.0) and (p95 <= 20.0)
    return {"pass": bool(ok), "status": "pass" if ok else "fail", "p50_bps": float(p50), "p95_bps": float(p95)}


def _gate_impact(mae_bps: float) -> Dict[str, object]:
    if mae_bps != mae_bps:  # NaN
        return {"pass": False, "status": "insufficient_data", "mae_bps": None}
    ok = mae_bps <= 2.0
    return {"pass": bool(ok), "status": "pass" if ok else "fail", "mae_bps": float(mae_bps)}


def run_exec_replay(args: argparse.Namespace) -> Tuple[pd.DataFrame, Dict[str, object], Dict[str, object]]:
    as_of = args.as_of
    run_id = args.run_id or f"p4_{as_of}"

    bronze_root = _resolve_path(args.bronze_root)
    exec_root = _resolve_path(args.exec_root)
    out_dir = _resolve_path(args.out_dir) if args.out_dir else (_REPO_ROOT / "reports" / "p4" / as_of)
    exec_out_dir = out_dir / "exec"

    bronze_day_dir = bronze_root / f"dt={as_of}"
    if detect_incomplete_flag(bronze_day_dir) and not args.ignore_incomplete:
        raise IncompleteDayError(f"incomplete bronze day: {bronze_day_dir}")

    if getattr(args, "skip_outdir_check", False):
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        ensure_out_dir(out_dir, force=args.force)
    exec_out_dir.mkdir(parents=True, exist_ok=True)

    exec_trades_path = getattr(args, "exec_trades_path", None)
    explicit_path = _resolve_path(exec_trades_path) if exec_trades_path else None
    exec_df = load_exec_trades(exec_root, exec_run_id=args.exec_run_id, explicit_path=explicit_path)
    if args.symbols:
        keep = set(args.symbols)
        exec_df = exec_df[exec_df["symbol"].isin(keep)]

    bronze_raw = load_bronze_trades(bronze_day_dir, ignore_incomplete=True)
    mkt_df = canonicalize_bronze_trades(bronze_raw)
    if args.symbols:
        mkt_df = mkt_df[mkt_df["symbol"].isin(set(args.symbols))]

    aligned = align_exec_to_market(
        exec_df,
        mkt_df,
        mode=args.ref_price_mode,
        window_sec=args.window_sec,
        tolerance_ms=args.tolerance_ms,
    )
    slippage_df = compute_slippage_bps(aligned)
    if "slippage_bps" not in slippage_df.columns:
        slippage_df = slippage_df.copy()
        slippage_df["slippage_bps"] = pd.Series(index=slippage_df.index, dtype="float64")
    slippage_df["as_of"] = as_of
    slippage_df["run_id"] = run_id

    stats_df = aggregate_replay_stats(
        slippage_df,
        as_of=as_of,
        run_id=run_id,
        ref_price_mode=args.ref_price_mode,
        window_sec=args.window_sec,
    )
    stats_df = normalize_replay_stats_schema(stats_df)
    write_replay_stats(stats_df, exec_out_dir / "replay_stats.parquet")

    params, fit_stats, used_features = fit_impact_model(slippage_df)
    pred = predict_impact(slippage_df, params)
    mae = compute_mae_bps(slippage_df["slippage_bps"], pred)
    impact_gate = _gate_impact(mae)
    impact_out = {
        "as_of": as_of,
        "run_id": run_id,
        "model_type": fit_stats.get("model_type", "linear_participation_v1"),
        "features": used_features,
        "params": params,
        "fit_stats": fit_stats,
        "mae_bps": mae,
        "gate": {
            "pass": impact_gate["pass"],
            "status": impact_gate["status"],
            "threshold_bps": 2.0,
        },
    }
    write_impact_calib(impact_out, exec_out_dir / "impact_calib.json")

    replay_gate = _gate_replay(stats_df)
    return stats_df, replay_gate, impact_gate


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--exec-run-id", required=True)
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--bronze-root", default="datahub/bronze/fubon/trades")
    ap.add_argument("--exec-root", default="reports/exec")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--ref-price-mode", default=REF_PRICE_MODE_LAST)
    ap.add_argument("--window-sec", type=int, default=5)
    ap.add_argument("--tolerance-ms", type=int, default=None)
    ap.add_argument("--ignore-incomplete", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    try:
        run_exec_replay(args)
        return int(ExitCode.OK)
    except IncompleteDayError as exc:
        sys.stderr.write(f"{REASON_INCOMPLETE_INTRADAY_SKIPPED}: {exc}\n")
        return int(ExitCode.OK)
    except Phase4Error as exc:
        sys.stderr.write(f"{exc}\n")
        return int(exc.exit_code)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"RUNTIME_ERROR: {type(exc).__name__}: {exc}\n")
        return int(ExitCode.SCHEMA_VALIDATION_FAILED)


if __name__ == "__main__":
    raise SystemExit(main())
