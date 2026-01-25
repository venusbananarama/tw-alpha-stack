from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple

import pandas as pd

from .bronze_loader import detect_incomplete_flag, list_bronze_symbols
from .calendar import is_trading_day, load_trading_days
from .coverage import compute_symbol_coverage, symbols_payload
from .drift import aggregate_monthly_drift, compute_daily_drift_metrics, evaluate_drift_gate
from .errors import (
    ExitCode,
    GateFailedError,
    IncompleteDayError,
    InputNotFoundError,
    Phase4Error,
    REASON_GATE_FAILED,
    REASON_INCOMPLETE_INTRADAY_SKIPPED,
    REASON_INPUT_NOT_FOUND,
    REASON_INSUFFICIENT_DATA,
    REASON_INSUFFICIENT_MARKET_COVERAGE,
    REASON_NOT_TRADING_DAY,
    REASON_OK,
    REASON_RUNTIME_ERROR,
    REASON_SCHEMA_VALIDATION_FAILED,
)
from .exec_loader import resolve_exec_trades_path as _resolve_exec_trades_path
from .ledger import (
    acquire_lock,
    append_ledger,
    atomic_write_text,
    ensure_out_dir,
    release_lock,
    write_ok_flag,
    write_parquet_atomic,
)
from .preflight_gate import build_preflight_gate
from .profile import apply_profile, should_write_ok
from .reporting import compose_p4_summary, render_drift_dashboard_html, write_summary_atomic


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / "alpha_core").exists():
            return parent
    return Path.cwd().resolve()


def _resolve_path(path: str, repo_root: Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return (repo_root / p).resolve()


def resolve_exec_trades_path(
    repo_root: Path,
    exec_run_id: str,
    exec_trades_path: Optional[Path] = None,
    exec_root: Optional[Path] = None,
) -> Path:
    base_root = exec_root if exec_root is not None else (repo_root / "reports" / "exec")
    return _resolve_exec_trades_path(base_root, exec_run_id, exec_trades_path)


def _log_line(log_path: Optional[Path], message: str) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    line = f"{ts} {message}"
    print(line)
    if log_path is None:
        return
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        return


def _infer_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            return cols[key]
    return None


def _collect_artifacts(out_dir: Path) -> Dict[str, str]:
    mapping = {}
    candidates = {
        "replay_stats": out_dir / "exec" / "replay_stats.parquet",
        "impact_calib": out_dir / "exec" / "impact_calib.json",
        "live_drift_dashboard": out_dir / "live_drift_dashboard.html",
        "wf_summary": out_dir / "wf_summary.parquet",
        "wf_gate": out_dir / "wf_gate.jsonl",
        "drift_metrics": out_dir / "drift_metrics.parquet",
    }
    for key, path in candidates.items():
        if path.exists():
            mapping[key] = str(path)
    mapping["p4_summary"] = str(out_dir / "p4_summary.json")
    return mapping


def _load_wf_gate(out_dir: Path) -> Dict[str, object]:
    gate_path = out_dir / "wf_gate.jsonl"
    if not gate_path.exists():
        return {"pass": False, "status": "insufficient_data", "pass_ratio": None}
    lines = gate_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {"pass": False, "status": "insufficient_data", "pass_ratio": None}
    last = lines[-1]
    try:
        obj = json.loads(last)
    except Exception:
        obj = {}
    ratio = obj.get("overall_pass_ratio")
    ok = bool(obj.get("pass")) if obj else False
    status = "pass" if ok else "fail"
    if ratio is None:
        status = "insufficient_data"
    return {"pass": ok, "status": status, "pass_ratio": ratio}


def _early_summary_path(repo_root: Path, as_of: str, run_id: str) -> Path:
    return repo_root / "metrics" / "p4_summaries" / f"p4_summary.{as_of}.{run_id}.json"


def _write_summary(
    *,
    out_dir: Path,
    as_of: str,
    run_id: str,
    status: str,
    reason_code: str,
    exit_code: int,
    inputs: Dict[str, object],
    thresholds: Dict[str, object],
    gates: Dict[str, Dict[str, object]],
    resolved_paths: Dict[str, object],
    coverage: Optional[Dict[str, object]] = None,
) -> None:
    summary = compose_p4_summary(
        as_of=as_of,
        run_id=run_id,
        status=status,
        reason_code=reason_code,
        gates=gates,
        artifacts=_collect_artifacts(out_dir),
    )
    summary["exit_code"] = int(exit_code)
    summary["inputs"] = inputs
    summary["thresholds"] = thresholds
    summary["resolved_paths"] = resolved_paths
    if coverage is not None:
        summary["coverage"] = coverage
    write_summary_atomic(summary, out_dir / "p4_summary.json")


def _write_early_summary(
    *,
    repo_root: Path,
    as_of: str,
    run_id: str,
    status: str,
    reason_code: str,
    exit_code: int,
    inputs: Dict[str, object],
    thresholds: Dict[str, object],
    gates: Dict[str, Dict[str, object]],
    resolved_paths: Dict[str, object],
    coverage: Optional[Dict[str, object]] = None,
) -> Path:
    summary_path = _early_summary_path(repo_root, as_of, run_id)
    summary = compose_p4_summary(
        as_of=as_of,
        run_id=run_id,
        status=status,
        reason_code=reason_code,
        gates=gates,
        artifacts={"p4_summary": str(summary_path)},
    )
    summary["exit_code"] = int(exit_code)
    summary["inputs"] = inputs
    summary["thresholds"] = thresholds
    summary["resolved_paths"] = resolved_paths
    if coverage is not None:
        summary["coverage"] = coverage
    write_summary_atomic(summary, summary_path)
    return summary_path


def _emit_summary_and_ledger(
    *,
    repo_root: Path,
    channel: str,
    out_dir: Optional[Path],
    as_of: str,
    run_id: str,
    status: str,
    reason_code: str,
    exit_code: int,
    inputs: Dict[str, object],
    thresholds: Dict[str, object],
    gates: Dict[str, Dict[str, object]],
    resolved_paths: Dict[str, object],
    coverage: Optional[Dict[str, object]] = None,
) -> None:
    if channel == "canonical":
        if out_dir is None:
            raise ValueError("canonical summary requires out_dir")
        _write_summary(
            out_dir=out_dir,
            as_of=as_of,
            run_id=run_id,
            status=status,
            reason_code=reason_code,
            exit_code=exit_code,
            inputs=inputs,
            thresholds=thresholds,
            gates=gates,
            resolved_paths=resolved_paths,
            coverage=coverage,
        )
        _write_early_summary(
            repo_root=repo_root,
            as_of=as_of,
            run_id=run_id,
            status=status,
            reason_code=reason_code,
            exit_code=exit_code,
            inputs=inputs,
            thresholds=thresholds,
            gates=gates,
            resolved_paths=resolved_paths,
            coverage=coverage,
        )
        artifacts = _collect_artifacts(out_dir)
    else:
        summary_path = _write_early_summary(
            repo_root=repo_root,
            as_of=as_of,
            run_id=run_id,
            status=status,
            reason_code=reason_code,
            exit_code=exit_code,
            inputs=inputs,
            thresholds=thresholds,
            gates=gates,
            resolved_paths=resolved_paths,
            coverage=coverage,
        )
        artifacts = {"p4_summary": str(summary_path)}

    append_ledger(
        repo_root / "metrics" / "p4_ledger.jsonl",
        {
            "as_of": as_of,
            "run_id": run_id,
            "status": status,
            "reason_code": reason_code,
            "artifacts": artifacts,
            "gates": gates,
        },
    )


def _classify_exec_error(exec_error: str) -> Tuple[str, int]:
    if exec_error in {"exec_trades_missing_symbol_column", "exec_trades_missing_ts_filled"}:
        return REASON_SCHEMA_VALIDATION_FAILED, int(ExitCode.SCHEMA_VALIDATION_FAILED)
    if exec_error.startswith("exec_trades_read_error"):
        return REASON_SCHEMA_VALIDATION_FAILED, int(ExitCode.SCHEMA_VALIDATION_FAILED)
    return REASON_INPUT_NOT_FOUND, int(ExitCode.INPUT_NOT_FOUND)


def _preflight(
    *,
    repo_root: Path,
    as_of: str,
    exec_root: Path,
    exec_run_id: str,
    bronze_root: Path,
    exec_trades_path: Optional[Path],
    min_trade_count: int,
    min_symbol_coverage: float,
) -> Tuple[Dict[str, object], Dict[str, object], Dict[str, object], Dict[str, object]]:
    resolved_exec_trades_path = None
    exec_trade_count = None
    exec_symbols: Set[str] = set()
    exec_error = None
    exec_error_detail = None

    try:
        resolved_exec_trades_path = resolve_exec_trades_path(
            repo_root,
            exec_run_id,
            exec_trades_path=exec_trades_path,
            exec_root=exec_root,
        )
        df = pd.read_csv(resolved_exec_trades_path)
        exec_trade_count = int(len(df))
        if "ts_filled" not in df.columns:
            exec_error = "exec_trades_missing_ts_filled"
        sym_col = _infer_col(df, ["symbol", "ticker", "stock_id", "sid"])
        if sym_col is None:
            exec_error = exec_error or "exec_trades_missing_symbol_column"
        else:
            exec_symbols = set(df[sym_col].astype(str).str.strip())
    except InputNotFoundError as exc:
        exec_error = "exec_trades_not_found"
        exec_error_detail = str(exc)
    except Exception as exc:  # noqa: BLE001
        exec_error = f"exec_trades_read_error:{type(exc).__name__}:{exc}"

    bronze_symbols: Set[str] = set()
    bronze_error = None
    bronze_day_dir = bronze_root / f"dt={as_of}"
    try:
        bronze_symbols = list_bronze_symbols(bronze_root, as_of)
    except Exception as exc:  # noqa: BLE001
        bronze_error = f"bronze_symbols_error:{type(exc).__name__}:{exc}"

    symbol_coverage, missing_symbols = compute_symbol_coverage(exec_symbols, bronze_symbols)

    exec_payload = symbols_payload(exec_symbols)
    bronze_payload = symbols_payload(bronze_symbols)
    missing_payload = symbols_payload(missing_symbols)

    inputs = {
        "as_of": as_of,
        "exec_run_id": exec_run_id,
        "resolved_exec_trades_path": str(resolved_exec_trades_path) if resolved_exec_trades_path else None,
        "bronze_dt_path": str(bronze_day_dir),
        "exec_trade_count": exec_trade_count,
        "exec_symbols": exec_payload["list"],
        "exec_symbols_count": exec_payload["count"],
        "exec_symbols_hash": exec_payload["hash"],
        "bronze_symbols": bronze_payload["list"],
        "bronze_symbols_count": bronze_payload["count"],
        "bronze_symbols_hash": bronze_payload["hash"],
        "missing_symbols": missing_payload["list"],
        "missing_symbols_count": missing_payload["count"],
        "missing_symbols_hash": missing_payload["hash"],
        "symbol_coverage": symbol_coverage,
    }
    if exec_error:
        inputs["exec_trades_error"] = exec_error
    if exec_error_detail:
        inputs["exec_trades_error_detail"] = exec_error_detail
    if bronze_error:
        inputs["bronze_symbols_error"] = bronze_error

    resolved_paths = {
        "exec_root": str(exec_root),
        "exec_run_id": exec_run_id,
        "exec_trades_path": str(resolved_exec_trades_path) if resolved_exec_trades_path else None,
        "bronze_dt_path": str(bronze_day_dir),
    }

    coverage = {
        "symbol_coverage": symbol_coverage,
        "missing_symbols": missing_payload,
        "exec_symbols": exec_payload,
        "bronze_symbols": bronze_payload,
    }

    preflight_gate = build_preflight_gate(
        exec_trade_count=exec_trade_count,
        symbol_coverage=symbol_coverage,
        missing_symbols_count=missing_payload["count"],
        min_trade_count=min_trade_count,
        min_symbol_coverage=min_symbol_coverage,
        exec_error=exec_error,
        bronze_error=bronze_error,
    )

    return inputs, resolved_paths, coverage, preflight_gate


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--exec-run-id", required=True)
    ap.add_argument("--exec-trades-path", default=None)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--bronze-root", default="datahub/bronze/fubon/trades")
    ap.add_argument("--exec-root", default="reports/exec")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--mode", default="all", choices=["all", "replay", "drift", "wf"])
    ap.add_argument("--ignore-incomplete", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--calendar-csv", default="datahub/ref/trading_days.csv")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--ref-price-mode", default="last_trade_before")
    ap.add_argument("--window-sec", type=int, default=5)
    ap.add_argument("--tolerance-ms", type=int, default=None)
    ap.add_argument("--on-insufficient-data", choices=["fail", "skip", "force"], default="fail")
    ap.add_argument("--min-symbol-coverage", type=float, default=0.6)
    ap.add_argument("--min-trade-count", type=int, default=10)
    ap.add_argument("--profile", choices=["prod", "dev"], default="prod")
    return ap


def run(args: argparse.Namespace, *, repo_root: Optional[Path] = None) -> int:
    repo_root = repo_root or _repo_root_from_here()
    apply_profile(args)
    profile = args.profile

    as_of = args.as_of
    run_id = args.run_id or f"p4_{as_of}"
    out_dir = _resolve_path(args.out_dir, repo_root) if args.out_dir else (repo_root / "reports" / "p4" / as_of)
    log_path = out_dir / "p4_run.log"
    exec_root = _resolve_path(args.exec_root, repo_root)
    bronze_root = _resolve_path(args.bronze_root, repo_root)
    exec_trades_path = _resolve_path(args.exec_trades_path, repo_root) if args.exec_trades_path else None

    lock_path = None
    ledger_written = False
    out_dir_ready = False
    status = "PASS"
    reason_code = REASON_OK
    exit_code = int(ExitCode.OK)
    gates: Dict[str, Dict[str, object]] = {}
    thresholds = {
        "min_symbol_coverage": float(args.min_symbol_coverage),
        "min_trade_count": int(args.min_trade_count),
        "on_insufficient_data": args.on_insufficient_data,
        "profile": profile,
    }
    gates["preflight"] = build_preflight_gate(
        exec_trade_count=None,
        symbol_coverage=None,
        missing_symbols_count=None,
        min_trade_count=int(args.min_trade_count),
        min_symbol_coverage=float(args.min_symbol_coverage),
    )
    inputs: Dict[str, object] = {}
    resolved_paths: Dict[str, object] = {}
    coverage: Dict[str, object] = {}

    try:
        trading_days = load_trading_days(_resolve_path(args.calendar_csv, repo_root))
        if not is_trading_day(as_of, trading_days):
            status = "FAIL"
            reason_code = REASON_NOT_TRADING_DAY
            exit_code = int(ExitCode.NOT_TRADING_DAY)
            early_inputs = {
                "as_of": as_of,
                "exec_run_id": args.exec_run_id,
                "resolved_exec_trades_path": None,
                "bronze_dt_path": str(bronze_root / f"dt={as_of}"),
                "exec_trade_count": None,
                "exec_symbols": [],
                "bronze_symbols": [],
                "missing_symbols": [],
                "symbol_coverage": 0.0,
            }
            early_paths = {
                "exec_root": str(exec_root),
                "exec_run_id": args.exec_run_id,
                "exec_trades_path": None,
                "bronze_dt_path": str(bronze_root / f"dt={as_of}"),
            }
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel="early",
                out_dir=None,
                as_of=as_of,
                run_id=run_id,
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                inputs=early_inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=early_paths,
            )
            ledger_written = True
            return int(exit_code)

        bronze_day_dir = bronze_root / f"dt={as_of}"
        if detect_incomplete_flag(bronze_day_dir) and not args.ignore_incomplete:
            status = "SKIP"
            reason_code = REASON_INCOMPLETE_INTRADAY_SKIPPED
            exit_code = int(ExitCode.OK)
            early_inputs = {
                "as_of": as_of,
                "exec_run_id": args.exec_run_id,
                "resolved_exec_trades_path": None,
                "bronze_dt_path": str(bronze_day_dir),
                "exec_trade_count": None,
                "exec_symbols": [],
                "bronze_symbols": [],
                "missing_symbols": [],
                "symbol_coverage": 0.0,
            }
            early_paths = {
                "exec_root": str(exec_root),
                "exec_run_id": args.exec_run_id,
                "exec_trades_path": None,
                "bronze_dt_path": str(bronze_day_dir),
            }
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel="early",
                out_dir=None,
                as_of=as_of,
                run_id=run_id,
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                inputs=early_inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=early_paths,
            )
            ledger_written = True
            return int(ExitCode.OK)

        inputs, resolved_paths, coverage, preflight_gate = _preflight(
            repo_root=repo_root,
            as_of=as_of,
            exec_root=exec_root,
            exec_run_id=args.exec_run_id,
            bronze_root=bronze_root,
            exec_trades_path=exec_trades_path,
            min_trade_count=int(args.min_trade_count),
            min_symbol_coverage=float(args.min_symbol_coverage),
        )
        gates["preflight"] = preflight_gate

        if inputs.get("exec_trades_error"):
            status = "FAIL"
            reason_code, exit_code = _classify_exec_error(str(inputs.get("exec_trades_error")))
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel="early",
                out_dir=None,
                as_of=as_of,
                run_id=run_id,
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                inputs=inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=resolved_paths,
                coverage=coverage,
            )
            ledger_written = True
            return int(exit_code)

        if inputs.get("bronze_symbols_error"):
            status = "FAIL"
            reason_code = REASON_INPUT_NOT_FOUND
            exit_code = int(ExitCode.INPUT_NOT_FOUND)
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel="early",
                out_dir=None,
                as_of=as_of,
                run_id=run_id,
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                inputs=inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=resolved_paths,
                coverage=coverage,
            )
            ledger_written = True
            return int(exit_code)

        exec_trade_count = int(inputs.get("exec_trade_count") or 0)
        symbol_coverage = float(inputs.get("symbol_coverage") or 0.0)
        insufficient = (exec_trade_count < int(args.min_trade_count)) or (
            symbol_coverage < float(args.min_symbol_coverage)
        )
        if insufficient:
            if symbol_coverage < float(args.min_symbol_coverage):
                reason_code = REASON_INSUFFICIENT_MARKET_COVERAGE
            else:
                reason_code = REASON_INSUFFICIENT_DATA

            if args.on_insufficient_data == "skip":
                status = "SKIP"
                exit_code = int(ExitCode.OK)
                _emit_summary_and_ledger(
                    repo_root=repo_root,
                    channel="early",
                    out_dir=None,
                    as_of=as_of,
                    run_id=run_id,
                    status=status,
                    reason_code=reason_code,
                    exit_code=exit_code,
                    inputs=inputs,
                    thresholds=thresholds,
                    gates=gates,
                    resolved_paths=resolved_paths,
                    coverage=coverage,
                )
                ledger_written = True
                return int(ExitCode.OK)

            if args.on_insufficient_data == "fail":
                status = "FAIL"
                exit_code = int(ExitCode.GATE_FAILED)
                _emit_summary_and_ledger(
                    repo_root=repo_root,
                    channel="early",
                    out_dir=None,
                    as_of=as_of,
                    run_id=run_id,
                    status=status,
                    reason_code=reason_code,
                    exit_code=exit_code,
                    inputs=inputs,
                    thresholds=thresholds,
                    gates=gates,
                    resolved_paths=resolved_paths,
                    coverage=coverage,
                )
                ledger_written = True
                return int(exit_code)

            status = "WARN"

        lock_dir = repo_root / "reports" / "p4" / "_locks"
        lock_path = acquire_lock(lock_dir, run_id)
        ensure_out_dir(out_dir, force=args.force)
        out_dir_ready = True
        _log_line(log_path, "run_start")

        replay_exec_trades_path = exec_trades_path
        if replay_exec_trades_path is None:
            resolved_exec_trades_path = inputs.get("resolved_exec_trades_path")
            if resolved_exec_trades_path:
                replay_exec_trades_path = Path(resolved_exec_trades_path)

        if args.mode in ("all", "replay"):
            _log_line(log_path, "stage_start:replay")
            from scripts.exec_replay import run_exec_replay

            replay_args = argparse.Namespace(
                as_of=as_of,
                run_id=run_id,
                exec_run_id=args.exec_run_id,
                exec_trades_path=str(replay_exec_trades_path) if replay_exec_trades_path else None,
                symbols=args.symbols or ([args.symbol] if args.symbol else None),
                bronze_root=str(bronze_root),
                exec_root=str(exec_root),
                out_dir=str(out_dir),
                ref_price_mode=args.ref_price_mode,
                window_sec=args.window_sec,
                tolerance_ms=args.tolerance_ms,
                ignore_incomplete=True,
                force=False,
                skip_outdir_check=True,
            )
            stats_df, replay_gate, impact_gate = run_exec_replay(replay_args)
            gates["replay"] = {
                "pass": replay_gate["pass"],
                "status": replay_gate["status"],
                "p50_bps": replay_gate.get("p50_bps"),
                "p95_bps": replay_gate.get("p95_bps"),
                "threshold_p50_bps": 5.0,
                "threshold_p95_bps": 20.0,
            }
            gates["impact"] = {
                "pass": impact_gate["pass"],
                "status": impact_gate["status"],
                "mae_bps": impact_gate.get("mae_bps"),
                "threshold_bps": 2.0,
            }
            _log_line(log_path, "stage_end:replay")

            insufficient = (
                replay_gate["status"] == "insufficient_data"
                or impact_gate["status"] == "insufficient_data"
            )
            if insufficient:
                if args.on_insufficient_data == "force":
                    status = "WARN"
                    reason_code = REASON_INSUFFICIENT_DATA
                else:
                    status = "FAIL"
                    reason_code = REASON_INSUFFICIENT_DATA
                    exit_code = int(ExitCode.GATE_FAILED)
                    raise GateFailedError("replay/impact insufficient data")
            if not insufficient and (not replay_gate["pass"] or not impact_gate["pass"]):
                status = "FAIL"
                reason_code = REASON_GATE_FAILED
                exit_code = int(ExitCode.GATE_FAILED)
                raise GateFailedError("replay/impact gate failed")

        if args.mode in ("all", "drift"):
            _log_line(log_path, "stage_start:drift")
            replay_stats_path = out_dir / "exec" / "replay_stats.parquet"
            if not replay_stats_path.exists():
                raise InputNotFoundError(f"replay stats missing: {replay_stats_path}")
            replay_stats = pd.read_parquet(replay_stats_path)
            daily = compute_daily_drift_metrics(replay_stats, None, None)
            monthly = aggregate_monthly_drift(daily)
            gate = evaluate_drift_gate(monthly, median_threshold_pct=0.3)
            if not monthly.empty:
                write_parquet_atomic(monthly, out_dir / "drift_metrics.parquet")
            gates["drift"] = {
                "pass": gate.get("pass"),
                "status": gate.get("status"),
                "median_pct": gate.get("median_pct"),
                "threshold_pct": 0.3,
            }
            replay_row = replay_stats[replay_stats["symbol"] == "ALL"]
            if replay_row.empty:
                replay_row = replay_stats.iloc[[0]] if not replay_stats.empty else pd.DataFrame()
            replay_metrics = {}
            if not replay_row.empty:
                replay_metrics = {
                    "p50_bps": replay_row["slippage_bps_p50"].iloc[0],
                    "p95_bps": replay_row["slippage_bps_p95"].iloc[0],
                }

            impact_path = out_dir / "exec" / "impact_calib.json"
            impact_metrics = {}
            if impact_path.exists():
                try:
                    impact_metrics = json.loads(impact_path.read_text(encoding="utf-8"))
                except Exception:
                    impact_metrics = {}

            summary = {
                "as_of": as_of,
                "run_id": args.exec_run_id,
                "status": gate.get("status", "unknown"),
                "reason_code": "DRIFT_GATE",
                "gates": {"drift": gate, "impact": impact_metrics.get("gate", {})},
                "metrics": {"replay": replay_metrics, "impact": {"mae_bps": impact_metrics.get("mae_bps")}},
            }
            html = render_drift_dashboard_html(summary, {"drift_monthly": monthly, "replay_stats": replay_stats})
            atomic_write_text(out_dir / "live_drift_dashboard.html", html)
            _log_line(log_path, "stage_end:drift")

            insufficient = gate.get("status") == "insufficient_data"
            if insufficient:
                if args.on_insufficient_data == "force":
                    status = "WARN"
                    reason_code = REASON_INSUFFICIENT_DATA
                else:
                    status = "FAIL"
                    reason_code = REASON_INSUFFICIENT_DATA
                    exit_code = int(ExitCode.GATE_FAILED)
                    raise GateFailedError("drift insufficient data")
            if not insufficient and not gate.get("pass"):
                status = "FAIL"
                reason_code = REASON_GATE_FAILED
                exit_code = int(ExitCode.GATE_FAILED)
                raise GateFailedError("drift gate failed")

        if args.mode in ("all", "wf"):
            _log_line(log_path, "stage_start:wf")
            from scripts.wf_runner import _run_p4 as wf_run_p4

            wf_args = argparse.Namespace(
                as_of=as_of,
                config=None,
                out_dir=str(out_dir),
                input=None,
                pass_threshold=0.70,
                force=False,
            )
            wf_run_p4(wf_args)
            wf_gate = _load_wf_gate(out_dir)
            gates["wf"] = {
                "pass": wf_gate.get("pass"),
                "status": wf_gate.get("status"),
                "pass_ratio": wf_gate.get("pass_ratio"),
                "threshold": 0.70,
            }
            _log_line(log_path, "stage_end:wf")
            insufficient = wf_gate.get("status") == "insufficient_data"
            if insufficient:
                if args.on_insufficient_data == "force":
                    status = "WARN"
                    reason_code = REASON_INSUFFICIENT_DATA
                else:
                    status = "FAIL"
                    reason_code = REASON_INSUFFICIENT_DATA
                    exit_code = int(ExitCode.GATE_FAILED)
                    raise GateFailedError("wf insufficient data")
            if not insufficient and not wf_gate.get("pass"):
                status = "FAIL"
                reason_code = REASON_GATE_FAILED
                exit_code = int(ExitCode.GATE_FAILED)
                raise GateFailedError("wf gate failed")

        _emit_summary_and_ledger(
            repo_root=repo_root,
            channel="canonical",
            out_dir=out_dir,
            as_of=as_of,
            run_id=run_id,
            status=status,
            reason_code=reason_code,
            exit_code=exit_code,
            inputs=inputs,
            thresholds=thresholds,
            gates=gates,
            resolved_paths=resolved_paths,
            coverage=coverage,
        )
        ledger_written = True

        if args.mode == "all" and should_write_ok(profile, status):
            write_ok_flag(repo_root / "_state" / "p4" / f"{as_of}.ok")

        _log_line(log_path, "run_end")
        return int(exit_code)
    except IncompleteDayError as exc:
        sys.stderr.write(f"{REASON_INCOMPLETE_INTRADAY_SKIPPED}: {exc}\n")
        if not ledger_written:
            early_inputs = inputs or {
                "as_of": as_of,
                "exec_run_id": args.exec_run_id,
                "resolved_exec_trades_path": None,
                "bronze_dt_path": str(bronze_root / f"dt={as_of}"),
                "exec_trade_count": None,
                "exec_symbols": [],
                "bronze_symbols": [],
                "missing_symbols": [],
                "symbol_coverage": 0.0,
            }
            early_paths = resolved_paths or {
                "exec_root": str(exec_root),
                "exec_run_id": args.exec_run_id,
                "exec_trades_path": None,
                "bronze_dt_path": str(bronze_root / f"dt={as_of}"),
            }
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel="early",
                out_dir=None,
                as_of=as_of,
                run_id=run_id,
                status="SKIP",
                reason_code=REASON_INCOMPLETE_INTRADAY_SKIPPED,
                exit_code=int(ExitCode.OK),
                inputs=early_inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=early_paths,
                coverage=coverage if coverage else None,
            )
            ledger_written = True
        return int(ExitCode.OK)
    except Phase4Error as exc:
        sys.stderr.write(f"{exc}\n")
        exit_code = int(exc.exit_code)
        if reason_code == REASON_OK:
            reason_code = exc.reason_code
        status = status if status != "PASS" else "FAIL"
        channel = "canonical" if out_dir_ready else "early"
        if not ledger_written:
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel=channel,
                out_dir=out_dir if out_dir_ready else None,
                as_of=as_of,
                run_id=run_id,
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                inputs=inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=resolved_paths,
                coverage=coverage,
            )
            ledger_written = True
        return int(exc.exit_code)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"{REASON_RUNTIME_ERROR}: {type(exc).__name__}: {exc}\n")
        if not ledger_written:
            channel = "canonical" if out_dir_ready else "early"
            _emit_summary_and_ledger(
                repo_root=repo_root,
                channel=channel,
                out_dir=out_dir if out_dir_ready else None,
                as_of=as_of,
                run_id=run_id,
                status="FAIL",
                reason_code=REASON_RUNTIME_ERROR,
                exit_code=int(ExitCode.SCHEMA_VALIDATION_FAILED),
                inputs=inputs,
                thresholds=thresholds,
                gates=gates,
                resolved_paths=resolved_paths,
                coverage=coverage,
            )
            ledger_written = True
        return int(ExitCode.SCHEMA_VALIDATION_FAILED)
    finally:
        release_lock(lock_path)
