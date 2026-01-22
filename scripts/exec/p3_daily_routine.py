from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

REPORT_SCHEMA_VERSION = "daily_exec_report.v1"
STDIO_TAIL_CHARS = 2000

EXIT_OK = 0
EXIT_STEP_FAILED = 71
EXIT_VALIDATION_FAILED = 72
EXIT_NOT_TRADING_DAY = 74
EXIT_NO_TARGET = 75


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _duration_sec(t0: float, t1: float) -> float:
    return round(t1 - t0, 6)


def _tail_text(text: Optional[str], limit: int = STDIO_TAIL_CHARS) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[-limit:]


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / "alpha_core").exists():
            return parent
    return Path.cwd().resolve()


def _resolve_path(path_str: str, *, base: Path) -> Path:
    p = Path(path_str)
    if not p.is_absolute():
        return (base / p).resolve()
    return p.resolve()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_trading_day(value: str) -> Optional[str]:
    s = value.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def load_trading_days(paths: List[Path]) -> Tuple[Optional[Set[str]], List[Path]]:
    tried: List[Path] = []
    for path in paths:
        tried.append(path)
        if not path.exists():
            continue
        days: Set[str] = set()
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    day = _parse_trading_day(str(cell))
                    if day:
                        days.add(day)
                        break
        return days, tried
    return None, tried


def check_trading_day(as_of: str, repo_root: Path) -> Tuple[int, str, str]:
    paths = [
        repo_root / "datahub" / "ref" / "trading_days.csv",
        repo_root / "cal" / "trading_days.csv",
    ]
    days, tried = load_trading_days(paths)
    if days is None:
        tried_s = "; ".join(str(p) for p in tried)
        return 40, f"CALENDAR_NOT_FOUND: {tried_s}", ""

    try:
        as_of_norm = datetime.strptime(as_of, "%Y-%m-%d").date().isoformat()
    except Exception:
        as_of_norm = as_of

    if as_of_norm not in days:
        return 41, f"NOT_TRADING_DAY: {as_of}", ""
    return 0, "OK", ""


def check_target(target_path: Path) -> Tuple[int, str, str]:
    if target_path.exists():
        return 0, "OK", ""
    return 2, f"TARGET_NOT_FOUND: {target_path}", ""


def check_base_out_dir(base_out_dir: Path, *, preexists: bool, force: bool) -> Tuple[int, str, str]:
    if preexists and not force:
        return 44, f"BASE_OUT_DIR_EXISTS: {base_out_dir}", ""
    return 0, "OK", ""


def _run_internal_step(name: str, cmd: str, fn) -> Dict[str, Any]:
    started_at = _now_iso()
    t0 = time.monotonic()
    try:
        exit_code, stdout, stderr = fn()
    except Exception as exc:
        exit_code = 1
        stdout = ""
        stderr = f"{type(exc).__name__}: {exc}"
    t1 = time.monotonic()
    finished_at = _now_iso()
    return {
        "name": name,
        "cmd": cmd,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": _duration_sec(t0, t1),
        "stdout_tail": _tail_text(stdout),
        "stderr_tail": _tail_text(stderr),
    }


def _run_subprocess_step(name: str, cmd: Sequence[str], cwd: Path) -> Dict[str, Any]:
    started_at = _now_iso()
    t0 = time.monotonic()
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    t1 = time.monotonic()
    finished_at = _now_iso()
    return {
        "name": name,
        "cmd": subprocess.list2cmdline(list(cmd)),
        "exit_code": proc.returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": _duration_sec(t0, t1),
        "stdout_tail": _tail_text(proc.stdout),
        "stderr_tail": _tail_text(proc.stderr),
    }


def _skipped_step(name: str, cmd: str, reason: str) -> Dict[str, Any]:
    now = _now_iso()
    return {
        "name": name,
        "cmd": cmd,
        "exit_code": None,
        "started_at": now,
        "finished_at": now,
        "duration_sec": 0.0,
        "stdout_tail": _tail_text(f"SKIPPED: {reason}"),
        "stderr_tail": "",
    }


def _count_csv_rows(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            _ = next(reader, None)
            return sum(1 for _ in reader)
    except Exception:
        return None


def _count_ndjson_lines(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return None


def _collect_manifest(paths: List[Path], *, report_path: Path) -> List[Dict[str, Any]]:
    manifest: List[Dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        if path.resolve() == report_path.resolve():
            continue
        if path.is_file():
            manifest.append(
                {
                    "path": str(path),
                    "bytes": int(path.stat().st_size),
                    "sha256": sha256_file(path),
                }
            )
    return manifest


def _build_counts(
    *,
    exec_run_dir: Path,
    snapshot_dir: Path,
    reconcile_dir: Path,
    bronze_root: Path,
    as_of: str,
    run_id: str,
) -> Dict[str, Any]:
    return {
        "exec_run": {
            "orders": _count_csv_rows(exec_run_dir / "orders.csv"),
            "trades": _count_csv_rows(exec_run_dir / "trades.csv"),
            "positions": _count_csv_rows(exec_run_dir / "positions.csv"),
        },
        "fubon_snapshot": {
            "orders": _count_csv_rows(snapshot_dir / "orders.csv"),
            "trades": _count_csv_rows(snapshot_dir / "trades.csv"),
            "positions": _count_csv_rows(snapshot_dir / "positions.csv"),
        },
        "record_orders": {
            "ndjson_lines": _count_ndjson_lines(
                bronze_root / "orders" / f"dt={as_of}" / f"orders_{run_id}.ndjson"
            ),
        },
        "record_trades": {
            "ndjson_lines": _count_ndjson_lines(
                bronze_root / "trades" / f"dt={as_of}" / f"trades_{run_id}.ndjson"
            ),
        },
        "reconcile": {
            "orders": _count_csv_rows(reconcile_dir / "orders.csv"),
            "trades": _count_csv_rows(reconcile_dir / "trades.csv"),
        },
    }


def _write_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, sort_keys=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-3 daily execution routine (one-click).")
    p.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    p.add_argument("--run-id", required=True, help="Run id")
    p.add_argument("--target", default=None, help="Target CSV (default: reports/target_portfolio_<as-of>.csv)")
    p.add_argument("--base-out-dir", default=None, help="Base output directory (default: reports/exec/<run-id>)")
    p.add_argument("--mode", default="PAPER", choices=["PAPER", "MOCK"], help="Execution mode (no LIVE)")
    p.add_argument("--force", action="store_true", help="Clear base-out-dir before running")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = _repo_root_from_here()

    as_of = args.as_of.strip()
    run_id = args.run_id.strip()
    mode = args.mode.strip().upper()

    target_default = repo_root / "reports" / f"target_portfolio_{as_of}.csv"
    target_path = _resolve_path(args.target, base=repo_root) if args.target else target_default

    base_out_dir = (
        _resolve_path(args.base_out_dir, base=repo_root)
        if args.base_out_dir
        else (repo_root / "reports" / "exec" / run_id)
    )
    report_path = base_out_dir / "daily_exec_report.json"

    base_out_dir_preexists = base_out_dir.exists()
    if not base_out_dir_preexists:
        base_out_dir.mkdir(parents=True, exist_ok=True)

    steps: List[Dict[str, Any]] = []
    status = "OK"
    reason_code = "EXECUTED"
    exit_code = EXIT_OK
    failure_reason = ""
    exec_run_dir = base_out_dir / "exec_run"
    snapshot_dir = base_out_dir / "fubon_snapshot"
    reconcile_dir = base_out_dir / "reconcile"
    bronze_root = base_out_dir / "bronze" / "fubon"

    step = _run_internal_step(
        "trading_day_gate",
        "internal:trading_day_gate",
        lambda: check_trading_day(as_of, repo_root),
    )
    steps.append(step)
    if step["exit_code"] != 0:
        if step["exit_code"] == 41:
            status = "NOOP"
            reason_code = "NOT_TRADING_DAY"
            exit_code = EXIT_NOT_TRADING_DAY
        else:
            status = "FAILED"
            reason_code = "STEP_FAILED"
            exit_code = EXIT_STEP_FAILED
        failure_reason = step["stdout_tail"] or step["stderr_tail"]
        remaining = [
            ("target_check", "internal:target_check"),
            ("base_out_dir_check", "internal:base_out_dir_check"),
            ("run_execution", ""),
            ("fubon_snapshot", ""),
            ("fubon_record_orders", ""),
            ("fubon_record_trades", ""),
            ("fubon_reconcile", ""),
            ("validate_exec_run", ""),
            ("validate_fubon_snapshot", ""),
            ("validate_reconcile", ""),
        ]
        for name, cmd in remaining:
            steps.append(_skipped_step(name, cmd, "prior step failed"))
        counts = _build_counts(
            exec_run_dir=exec_run_dir,
            snapshot_dir=snapshot_dir,
            reconcile_dir=reconcile_dir,
            bronze_root=bronze_root,
            as_of=as_of,
            run_id=run_id,
        )
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": run_id,
            "as_of": as_of,
            "status": status,
            "reason_code": reason_code,
            "steps": steps,
            "counts": counts,
            "artefacts_manifest": [],
            "ok": False,
            "failure_reason": failure_reason,
        }
        _write_report(report_path, report)
        return exit_code

    step = _run_internal_step(
        "target_check",
        "internal:target_check",
        lambda: check_target(target_path),
    )
    steps.append(step)
    if step["exit_code"] != 0:
        status = "NOOP"
        reason_code = "NO_TARGET"
        exit_code = EXIT_NO_TARGET
        failure_reason = step["stdout_tail"] or step["stderr_tail"]
        remaining = [
            ("base_out_dir_check", "internal:base_out_dir_check"),
            ("run_execution", ""),
            ("fubon_snapshot", ""),
            ("fubon_record_orders", ""),
            ("fubon_record_trades", ""),
            ("fubon_reconcile", ""),
            ("validate_exec_run", ""),
            ("validate_fubon_snapshot", ""),
            ("validate_reconcile", ""),
        ]
        for name, cmd in remaining:
            steps.append(_skipped_step(name, cmd, "no target"))
        counts = _build_counts(
            exec_run_dir=exec_run_dir,
            snapshot_dir=snapshot_dir,
            reconcile_dir=reconcile_dir,
            bronze_root=bronze_root,
            as_of=as_of,
            run_id=run_id,
        )
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": run_id,
            "as_of": as_of,
            "status": status,
            "reason_code": reason_code,
            "steps": steps,
            "counts": counts,
            "artefacts_manifest": [],
            "ok": False,
            "failure_reason": failure_reason,
        }
        _write_report(report_path, report)
        return exit_code

    step = _run_internal_step(
        "base_out_dir_check",
        "internal:base_out_dir_check",
        lambda: check_base_out_dir(base_out_dir, preexists=base_out_dir_preexists, force=bool(args.force)),
    )
    steps.append(step)
    if step["exit_code"] != 0:
        status = "FAILED"
        reason_code = "STEP_FAILED"
        exit_code = EXIT_STEP_FAILED
        failure_reason = step["stdout_tail"] or step["stderr_tail"]
        remaining = [
            ("run_execution", ""),
            ("fubon_snapshot", ""),
            ("fubon_record_orders", ""),
            ("fubon_record_trades", ""),
            ("fubon_reconcile", ""),
            ("validate_exec_run", ""),
            ("validate_fubon_snapshot", ""),
            ("validate_reconcile", ""),
        ]
        for name, cmd in remaining:
            steps.append(_skipped_step(name, cmd, "base_out_dir conflict"))
        counts = _build_counts(
            exec_run_dir=exec_run_dir,
            snapshot_dir=snapshot_dir,
            reconcile_dir=reconcile_dir,
            bronze_root=bronze_root,
            as_of=as_of,
            run_id=run_id,
        )
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": run_id,
            "as_of": as_of,
            "status": status,
            "reason_code": reason_code,
            "steps": steps,
            "counts": counts,
            "artefacts_manifest": [],
            "ok": False,
            "failure_reason": failure_reason,
        }
        _write_report(report_path, report)
        return exit_code

    if base_out_dir_preexists and args.force:
        shutil.rmtree(base_out_dir, ignore_errors=True)
        base_out_dir.mkdir(parents=True, exist_ok=True)

    py = sys.executable
    scripts_dir = repo_root / "scripts" / "exec"

    heavy_steps: List[Tuple[str, List[str]]] = [
        (
            "run_execution",
            [
                py,
                str(scripts_dir / "run_execution.py"),
                "--as-of",
                as_of,
                "--run-id",
                run_id,
                "--target",
                str(target_path),
                "--out-dir",
                str(exec_run_dir),
                "--mode",
                mode,
            ],
        ),
        (
            "fubon_snapshot",
            [
                py,
                str(scripts_dir / "fubon_snapshot.py"),
                "--as-of",
                as_of,
                "--run-id",
                run_id,
                "--out-dir",
                str(snapshot_dir),
            ],
        ),
        (
            "fubon_record_orders",
            [
                py,
                str(scripts_dir / "fubon_record_orders.py"),
                "--start",
                as_of,
                "--end",
                as_of,
                "--run-id",
                run_id,
                "--out-root",
                str(bronze_root),
            ],
        ),
        (
            "fubon_record_trades",
            [
                py,
                str(scripts_dir / "fubon_record_trades.py"),
                "--start",
                as_of,
                "--end",
                as_of,
                "--run-id",
                run_id,
                "--out-root",
                str(bronze_root),
            ],
        ),
        (
            "fubon_reconcile",
            [
                py,
                str(scripts_dir / "fubon_reconcile.py"),
                "--start",
                as_of,
                "--end",
                as_of,
                "--run-id",
                run_id,
                "--out-dir",
                str(reconcile_dir),
                "--src-root",
                str(bronze_root),
            ],
        ),
    ]

    heavy_failed = False
    for idx, (name, cmd) in enumerate(heavy_steps):
        step = _run_subprocess_step(name, cmd, cwd=repo_root)
        steps.append(step)
        if step["exit_code"] != 0 and not heavy_failed:
            heavy_failed = True
            status = "FAILED"
            reason_code = "STEP_FAILED"
            exit_code = EXIT_STEP_FAILED
            failure_reason = step["stderr_tail"] or step["stdout_tail"]
            remaining = heavy_steps[idx + 1 :]
            for r_name, r_cmd in remaining:
                steps.append(_skipped_step(r_name, subprocess.list2cmdline(r_cmd), "prior step failed"))
            break

    validate_steps: List[Tuple[str, List[str], Optional[str]]] = []
    exec_orders = exec_run_dir / "orders.csv"
    exec_trades = exec_run_dir / "trades.csv"
    exec_positions = exec_run_dir / "positions.csv"
    exec_account = exec_run_dir / "account_snapshot.json"
    exec_summary = exec_run_dir / "exec_summary.json"
    if exec_orders.exists():
        validate_steps.append(
            (
                "validate_exec_run",
                [
                    py,
                    str(scripts_dir / "validate_exec_logs.py"),
                    "--orders",
                    str(exec_orders),
                    "--trades",
                    str(exec_trades),
                    "--positions",
                    str(exec_positions),
                    "--account",
                    str(exec_account),
                    "--summary",
                    str(exec_summary),
                ],
                None,
            )
        )
    else:
        validate_steps.append(("validate_exec_run", [], "missing exec_run orders.csv"))

    snap_orders = snapshot_dir / "orders.csv"
    snap_trades = snapshot_dir / "trades.csv"
    snap_positions = snapshot_dir / "positions.csv"
    snap_account = snapshot_dir / "account_snapshot.json"
    snap_summary = snapshot_dir / "exec_summary.json"
    if snap_orders.exists():
        validate_steps.append(
            (
                "validate_fubon_snapshot",
                [
                    py,
                    str(scripts_dir / "validate_exec_logs.py"),
                    "--orders",
                    str(snap_orders),
                    "--trades",
                    str(snap_trades),
                    "--positions",
                    str(snap_positions),
                    "--account",
                    str(snap_account),
                    "--summary",
                    str(snap_summary),
                ],
                None,
            )
        )
    else:
        validate_steps.append(("validate_fubon_snapshot", [], "missing fubon_snapshot orders.csv"))

    rec_orders = reconcile_dir / "orders.csv"
    rec_trades = reconcile_dir / "trades.csv"
    if rec_orders.exists():
        validate_steps.append(
            (
                "validate_reconcile",
                [
                    py,
                    str(scripts_dir / "validate_exec_logs.py"),
                    "--orders",
                    str(rec_orders),
                    "--trades",
                    str(rec_trades),
                ],
                None,
            )
        )
    else:
        validate_steps.append(("validate_reconcile", [], "missing reconcile orders.csv"))

    validation_failed = False
    for name, cmd, skip_reason in validate_steps:
        if skip_reason:
            steps.append(_skipped_step(name, "", skip_reason))
            continue
        step = _run_subprocess_step(name, cmd, cwd=repo_root)
        steps.append(step)
        if step["exit_code"] != 0 and not validation_failed:
            validation_failed = True
            if reason_code == "EXECUTED":
                status = "FAILED"
                reason_code = "VALIDATION_FAILED"
                exit_code = EXIT_VALIDATION_FAILED
                failure_reason = step["stderr_tail"] or step["stdout_tail"]

    counts = _build_counts(
        exec_run_dir=exec_run_dir,
        snapshot_dir=snapshot_dir,
        reconcile_dir=reconcile_dir,
        bronze_root=bronze_root,
        as_of=as_of,
        run_id=run_id,
    )

    manifest_paths = [
        exec_run_dir / "orders.csv",
        exec_run_dir / "trades.csv",
        exec_run_dir / "positions.csv",
        exec_run_dir / "account_snapshot.json",
        exec_run_dir / "exec_summary.json",
        exec_run_dir / "ledger.json",
        snapshot_dir / "orders.csv",
        snapshot_dir / "trades.csv",
        snapshot_dir / "positions.csv",
        snapshot_dir / "account_snapshot.json",
        snapshot_dir / "exec_summary.json",
        snapshot_dir / "ledger.json",
        reconcile_dir / "orders.csv",
        reconcile_dir / "trades.csv",
        bronze_root / "orders" / f"dt={as_of}" / f"orders_{run_id}.ndjson",
        bronze_root / "trades" / f"dt={as_of}" / f"trades_{run_id}.ndjson",
    ]
    artefacts_manifest = _collect_manifest(manifest_paths, report_path=report_path)

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "as_of": as_of,
        "status": status,
        "reason_code": reason_code,
        "steps": steps,
        "counts": counts,
        "artefacts_manifest": artefacts_manifest,
        "ok": exit_code == EXIT_OK,
    }
    if failure_reason:
        report["failure_reason"] = failure_reason

    _write_report(report_path, report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
