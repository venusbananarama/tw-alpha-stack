from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase1 import checkpoints, dividend_scan, ledger, paths, rate_control, runner_hhd, runner_hhf, summary, trading_calendar  # noqa: E402


HHF_DATASETS = ["prices", "chip", "per", "dividend"]
HHD_DATASETS = ["shareholding", "inst_total", "gov_bank"]
ALLOW_EMPTY_ON_TRADING_DAY = {"dividend"}


def _safe_scope(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return "global"
    safe = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_", "."))
    return safe or "global"


def _partition_round_robin(items: List[str], n: int) -> List[List[str]]:
    n = max(1, int(n))
    buckets: List[List[str]] = [[] for _ in range(n)]
    for idx, item in enumerate(items):
        buckets[idx % n].append(item)
    return buckets


def _env_bool(name: str) -> Optional[bool]:
    raw = (os.environ.get(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return None


def _env_int(name: str) -> Optional[int]:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _env_float(name: str) -> Optional[float]:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _resolve_shared_bucket(
    args: argparse.Namespace,
    repo_root: Path,
) -> tuple[bool, Dict[str, str], Dict[str, object]]:
    env_flag = _env_bool("FINMIND_SHARED_BUCKET")
    if args.shared_bucket is not None:
        enabled = bool(args.shared_bucket)
        explicit = True
    elif env_flag is not None:
        enabled = bool(env_flag)
        explicit = False
    else:
        enabled = int(args.batch_workers) > 1 and not bool(args.batch_child)
        explicit = False

    env: Dict[str, str] = {}
    meta: Dict[str, object] = {"shared_bucket_enabled": bool(enabled)}

    if not enabled:
        if explicit and args.shared_bucket is False:
            env["FINMIND_SHARED_BUCKET"] = "0"
        return False, env, meta

    state_path_raw = (os.environ.get("FINMIND_BUCKET_STATE_PATH") or "").strip()
    state_path = Path(state_path_raw) if state_path_raw else Path(args.bucket_state_path or "")
    if not state_path_raw and not args.bucket_state_path:
        state_path = paths.finmind_bucket_state_path(repo_root)
    if not state_path.is_absolute():
        state_path = repo_root / state_path

    bucket_rpm = _env_int("FINMIND_BUCKET_RPM")
    bucket_burst = _env_int("FINMIND_BUCKET_BURST")
    bucket_lock_ttl = _env_int("FINMIND_BUCKET_LOCK_TTL_SEC")
    bucket_lease = _env_int("FINMIND_BUCKET_LEASE_SIZE")
    bucket_max_wait = _env_float("FINMIND_BUCKET_MAX_WAIT_SEC")

    total_budget_h = int(args.calls_per_hour) * max(1, int(args.batch_workers))
    if bool(args.allow_over_cap):
        cap_h = total_budget_h
    else:
        cap_h = int(args.api_hourly_cap) if int(args.api_hourly_cap) > 0 else total_budget_h
    safe_cap_h = max(1, cap_h - int(args.api_hourly_margin))
    global_h = min(total_budget_h, safe_cap_h)
    computed_rpm = max(1, int(global_h // 60))

    rpm = int(bucket_rpm) if bucket_rpm is not None else computed_rpm
    burst = int(bucket_burst) if bucket_burst is not None else int(args.bucket_burst)
    lock_ttl_sec = int(bucket_lock_ttl) if bucket_lock_ttl is not None else int(args.bucket_lock_ttl_sec)
    lease_size = int(bucket_lease) if bucket_lease is not None else int(args.bucket_lease_size)
    if bucket_max_wait is not None:
        max_wait_sec = float(bucket_max_wait)
    elif args.bucket_max_wait_sec is not None:
        max_wait_sec = float(args.bucket_max_wait_sec)
    else:
        max_wait_sec = None

    env.update(
        {
            "FINMIND_SHARED_BUCKET": "1",
            "FINMIND_BUCKET_STATE_PATH": str(state_path),
            "FINMIND_BUCKET_RPM": str(rpm),
            "FINMIND_BUCKET_BURST": str(burst),
            "FINMIND_BUCKET_LOCK_TTL_SEC": str(lock_ttl_sec),
            "FINMIND_BUCKET_LEASE_SIZE": str(lease_size),
        }
    )
    if max_wait_sec is not None:
        env["FINMIND_BUCKET_MAX_WAIT_SEC"] = str(max_wait_sec)
    meta.update(
        {
            "shared_bucket_global_rpm": rpm,
            "shared_bucket_burst": burst,
            "shared_bucket_margin": int(args.api_hourly_margin),
            "shared_bucket_state_path": str(state_path),
            "shared_bucket_lock_ttl_sec": lock_ttl_sec,
            "shared_bucket_lease_size": lease_size,
            "shared_bucket_max_wait_sec": max_wait_sec,
        }
    )
    return True, env, meta


def _resolve_dividend_settings(
    args: argparse.Namespace,
    repo_root: Path,
) -> tuple[str, Dict[str, str], Dict[str, object], int, int]:
    policy_effective = dividend_scan.resolve_policy(args.dividend_scan_policy, args.run_type)
    shard_count = max(1, int(args.dividend_shard_count))
    if args.dividend_max_staleness_trading_days is None:
        max_staleness = shard_count
    else:
        max_staleness = max(1, int(args.dividend_max_staleness_trading_days))
    state_path = Path(args.dividend_scan_state_path) if args.dividend_scan_state_path else paths.dividend_scan_state_path(repo_root)
    if not state_path.is_absolute():
        state_path = repo_root / state_path
    lock_ttl = max(1, int(args.dividend_scan_lock_ttl_sec))

    env = {
        "P1_RUN_TYPE": args.run_type,
        "P1_DIVIDEND_SCAN_POLICY": args.dividend_scan_policy,
        "P1_DIVIDEND_SHARD_COUNT": str(shard_count),
        "P1_DIVIDEND_MAX_STALENESS_TRADING_DAYS": str(max_staleness),
        "P1_DIVIDEND_SCAN_STATE_PATH": str(state_path),
        "P1_DIVIDEND_SCAN_LOCK_TTL_SEC": str(lock_ttl),
        "P1_DIVIDEND_FORCE_FULL_IF_EVIDENCE_MISMATCH": "1" if bool(args.dividend_force_full_if_evidence_mismatch) else "0",
    }
    meta = {
        "dividend_scan_policy": args.dividend_scan_policy,
        "dividend_scan_policy_effective": policy_effective,
        "dividend_shard_count": shard_count,
        "dividend_max_staleness_trading_days": max_staleness,
        "dividend_scan_state_path": str(state_path),
        "dividend_scan_lock_ttl_sec": lock_ttl,
        "dividend_force_full_if_evidence_mismatch": bool(args.dividend_force_full_if_evidence_mismatch),
    }
    return policy_effective, env, meta, shard_count, max_staleness


def _build_child_cmd(
    repo_root: Path,
    base_py: str,
    parent_args: argparse.Namespace,
    child_run_id: str,
    child_lock_scope: str,
    child_mode: str,
    child_hhf: List[str],
    child_hhd: List[str],
    batch_index: int,
) -> List[str]:
    script = repo_root / "scripts" / "p1_daily_routine.py"

    cmd: List[str] = [
        base_py,
        str(script),
        "--batch-child",
        "--batch-index",
        str(batch_index),
        "--run-id",
        child_run_id,
        "--lock-scope",
        child_lock_scope,
        "--mode",
        child_mode,
        "--run-type",
        parent_args.run_type,
        "--calls-per-hour",
        str(parent_args.calls_per_hour),
        "--batch-size",
        str(parent_args.batch_size),
        "--universe-path",
        str(parent_args.universe_path),
        "--live-lookback",
        str(parent_args.live_lookback),
        "--catch-up-max-days",
        str(parent_args.catch_up_max_days),
        "--lock-ttl-mins",
        str(parent_args.lock_ttl_mins),
        "--log-level",
        str(parent_args.log_level),
        "--dividend-scan-policy",
        str(parent_args.dividend_scan_policy),
        "--dividend-shard-count",
        str(parent_args.dividend_shard_count),
        "--dividend-scan-lock-ttl-sec",
        str(parent_args.dividend_scan_lock_ttl_sec),
        "--no-build-prices-daily",
        "--prices-daily-max-months",
        str(parent_args.prices_daily_max_months),
    ]

    if parent_args.prices_daily_allow_regression:
        cmd.append("--prices-daily-allow-regression")
    if parent_args.prices_daily_include_fromboss:
        cmd.append("--prices-daily-include-fromboss")
    else:
        cmd.append("--no-prices-daily-include-fromboss")

    if parent_args.cap_date:
        cmd.extend(["--cap-date", str(parent_args.cap_date)])

    if parent_args.dividend_max_staleness_trading_days is not None:
        cmd.extend(["--dividend-max-staleness-trading-days", str(parent_args.dividend_max_staleness_trading_days)])

    if parent_args.dividend_scan_state_path:
        cmd.extend(["--dividend-scan-state-path", str(parent_args.dividend_scan_state_path)])

    if parent_args.dividend_force_full_if_evidence_mismatch:
        cmd.append("--dividend-force-full-if-evidence-mismatch")
    else:
        cmd.append("--no-dividend-force-full-if-evidence-mismatch")

    if getattr(parent_args, "force_unlock", False):
        cmd.append("--force-unlock")

    if parent_args.skip_if_ok:
        cmd.append("--skip-if-ok")
    else:
        cmd.append("--no-skip-if-ok")

    if parent_args.catch_up:
        cmd.append("--catch-up")

    if parent_args.dry_run:
        cmd.append("--dry-run")
    if parent_args.apply:
        cmd.append("--apply")

    if parent_args.qps is not None:
        cmd.extend(["--qps", str(parent_args.qps)])
    if parent_args.rpm is not None:
        cmd.extend(["--rpm", str(parent_args.rpm)])

    if parent_args.date:
        cmd.extend(["--date", str(parent_args.date)])
    else:
        if parent_args.start:
            cmd.extend(["--start", str(parent_args.start)])
        if parent_args.end:
            cmd.extend(["--end", str(parent_args.end)])

    if child_hhf:
        cmd.extend(["--datasets-hhf", ",".join(child_hhf)])
    if child_hhd:
        cmd.extend(["--datasets-hhd", ",".join(child_hhd)])

    return cmd


def _run_batch_orchestrator(
    args: argparse.Namespace,
    run_id: str,
    run_dir: Path,
    log_path: Path,
    summary_path: Path,
    summary_obj: summary.RunSummary,
    shared_env: Dict[str, str],
) -> int:
    """Spawn N batch processes, each throttled by --calls-per-hour."""

    workers = int(args.batch_workers)
    if workers <= 1:
        raise SystemExit("batch orchestrator requires --batch-workers >= 2")

    api_cap = int(args.api_hourly_cap)
    total_budget = int(args.calls_per_hour) * workers
    if api_cap > 0 and total_budget > api_cap and not bool(args.allow_over_cap):
        raise SystemExit(
            f"batch budget exceeds api cap: calls_per_hour={args.calls_per_hour} workers={workers} => {total_budget}/h > cap={api_cap}/h"
        )

    hhf = _parse_list(args.datasets_hhf)
    hhd = _parse_list(args.datasets_hhd)
    if args.mode in ("hhf", "all"):
        hhf = [d for d in hhf if d in HHF_DATASETS]
    else:
        hhf = []
    if args.mode in ("hhd", "all"):
        hhd = [d for d in hhd if d in HHD_DATASETS]
    else:
        hhd = []

    active = hhf + hhd
    if not active:
        raise SystemExit("no datasets selected for batch mode")

    prefix = _safe_scope(args.batch_scope_prefix)
    buckets = _partition_round_robin(active, workers)

    base_py = _resolve_python(_REPO_ROOT)
    planned: List[Dict[str, object]] = []
    procs: List[subprocess.Popen[str]] = []

    summary_obj.meta["batch_workers"] = workers
    summary_obj.meta["batch_scope_prefix"] = prefix
    summary_obj.meta["api_hourly_cap"] = api_cap
    summary_obj.meta["batch_total_budget_per_hour"] = total_budget
    summary_obj.meta["batch_split_strategy"] = "round_robin_by_dataset"

    any_prices = False

    for i, group in enumerate(buckets, start=1):
        group_hhf = [d for d in group if d in HHF_DATASETS]
        group_hhd = [d for d in group if d in HHD_DATASETS]
        if not group_hhf and not group_hhd:
            continue

        if "prices" in group_hhf:
            any_prices = True

        if group_hhf and group_hhd:
            child_mode = "all"
        elif group_hhf:
            child_mode = "hhf"
        else:
            child_mode = "hhd"

        child_run_id = f"{run_id}.b{i:02d}"
        child_scope = f"{prefix}{i:02d}"
        cmd = _build_child_cmd(
            repo_root=_REPO_ROOT,
            base_py=base_py,
            parent_args=args,
            child_run_id=child_run_id,
            child_lock_scope=child_scope,
            child_mode=child_mode,
            child_hhf=group_hhf,
            child_hhd=group_hhd,
            batch_index=i,
        )

        planned.append(
            {
                "batch_index": i,
                "run_id": child_run_id,
                "lock_scope": child_scope,
                "mode": child_mode,
                "datasets_hhf": group_hhf,
                "datasets_hhd": group_hhd,
                "cmd": cmd,
            }
        )

    summary_obj.meta["batches"] = planned
    _write_summary(summary_path, summary_obj)

    if args.plan_batches:
        for item in planned:
            _append_event(
                log_path,
                f"BATCH_PLAN {item['run_id']} scope={item['lock_scope']} mode={item['mode']} hhf={item['datasets_hhf']} hhd={item['datasets_hhd']}",
            )
            print(" ".join(str(x) for x in item["cmd"]))
        summary_obj.finalize()
        _write_summary(summary_path, summary_obj)
        _append_event(log_path, f"FINISH status={summary_obj.status}")
        return 0

    for item in planned:
        cmd = [str(x) for x in item["cmd"]]
        _append_event(log_path, f"BATCH_START run_id={item['run_id']} scope={item['lock_scope']}")
        child_env = os.environ.copy()
        child_env.update(shared_env)
        proc = subprocess.Popen(cmd, cwd=str(_REPO_ROOT), env=child_env)
        procs.append(proc)

    exit_codes: List[int] = []
    for proc in procs:
        exit_codes.append(int(proc.wait()))

    if any_prices and bool(args.build_prices_daily) and not bool(args.dry_run):
        state_root = paths.state_root(_REPO_ROOT)
        store = checkpoints.CheckpointStore(state_root)
        cap = _parse_date(args.cap_date) if args.cap_date else trading_calendar.today_local()
        if args.date:
            cap = min(cap, _parse_date(args.date))
        _maybe_build_prices_daily(
            repo_root=_REPO_ROOT,
            cap_date=cap,
            build=True,
            dry_run=bool(args.dry_run),
            prices_daily_max_months=args.prices_daily_max_months,
            prices_daily_include_fromboss=bool(args.prices_daily_include_fromboss),
            prices_daily_allow_regression=bool(args.prices_daily_allow_regression),
            store=store,
            summary_obj=summary_obj,
            summary_path=summary_path,
            ledger_path=paths.ledger_path(_REPO_ROOT),
            run_id=run_id,
            run_type=args.run_type,
        )

    any_fail = any(code != 0 for code in exit_codes)
    summary_obj.meta["batch_exit_codes"] = exit_codes
    summary_obj.status = "OK" if not any_fail else "FAIL"
    summary_obj.finished_at = summary.now_iso()
    _write_summary(summary_path, summary_obj)
    _append_event(log_path, f"FINISH status={summary_obj.status}")
    return 0 if not any_fail else 2

def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:
        raise SystemExit(f"Invalid date: {value!r}") from exc


def _parse_list(value: str) -> List[str]:
    items = []
    for token in value.split(","):
        t = token.strip()
        if t:
            items.append(t)
    return items


def _resolve_repo_path(path_value: str | Path) -> Path:
    p = Path(path_value)
    if not p.is_absolute():
        p = _REPO_ROOT / p
    return p


def _resolve_python(repo_root: Path) -> str:
    if sys.executable:
        return sys.executable
    return "python"


def _tail_lines(text: str, max_lines: int = 20) -> List[str]:
    lines = [ln.rstrip() for ln in text.splitlines()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def _extract_prices_daily_stats(stdout: str) -> Dict[str, str]:
    stats: Dict[str, str] = {}
    if not stdout:
        return stats
    for line in stdout.splitlines():
        ln = line.strip()
        if ln.startswith("[RESULT]"):
            payload = ln[len("[RESULT]") :].strip()
            for token in payload.split():
                if "=" not in token:
                    continue
                key, value = token.split("=", 1)
                if key == "verdict":
                    stats["prices_daily_verdict"] = value
                elif key == "reason":
                    stats["prices_daily_reason"] = value
        elif ln.startswith("[INFO] coverage_new"):
            stats["prices_daily_coverage_new"] = ln
        elif ln.startswith("[INFO] coverage_old"):
            stats["prices_daily_coverage_old"] = ln
        elif ln.startswith("[INFO] inputs"):
            stats["prices_daily_inputs"] = ln
    return stats


def _append_event(log_path: Path, message: str) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {message}\n")


def _lock_is_stale(lock_path: Path, ttl_mins: int) -> bool:
    if ttl_mins <= 0:
        return True
    try:
        age_sec = time.time() - lock_path.stat().st_mtime
    except Exception:
        return False
    return age_sec > ttl_mins * 60


def _acquire_lock(lock_path: Path, ttl_mins: int, force: bool) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        if force or _lock_is_stale(lock_path, ttl_mins):
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            stale_path = lock_path.with_name(f"{lock_path.name}.stale.{ts}")
            lock_path.rename(stale_path)
        else:
            raise SystemExit(f"lock exists: {lock_path}")
    try:
        with lock_path.open("x", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()}\n")
            f.write(f"ts={datetime.utcnow().isoformat()}\n")
    except FileExistsError:
        raise SystemExit(f"lock exists: {lock_path}")


def _release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except Exception:
        return


def _setup_logging(log_path: Path, level: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _ensure_token(
    summary_obj: summary.RunSummary,
    ledger_path: Path,
    run_type: str,
    summary_path: Path,
    log_path: Path,
) -> None:
    token = (os.environ.get("FINMIND_TOKEN") or "").strip()
    if token:
        return
    msg = "FINMIND_TOKEN missing"
    raise SystemExit(msg)


def _collect_days_backfill(
    cal: trading_calendar.TradingCalendar,
    start: date,
    end: date,
    cap_date: date,
) -> List[date]:
    capped = trading_calendar.cap_dates(cal.dates, cap_date)
    return trading_calendar.trading_days_in_range(capped, start, end)


def _collect_days_live(
    cal: trading_calendar.TradingCalendar,
    cap_date: date,
    lookback: int,
) -> List[date]:
    return trading_calendar.recent_trading_days(cal.dates, cap_date, lookback)


def _compute_catchup_days(
    store: checkpoints.CheckpointStore,
    dataset: str,
    cal: trading_calendar.TradingCalendar,
    cap_date: date,
    max_days: int,
) -> List[date]:
    latest = store.latest_ok(dataset)
    if latest is None:
        days = [d for d in cal.dates if d <= cap_date]
    else:
        start = latest + timedelta(days=1)
        days = [d for d in cal.dates if start <= d <= cap_date]
    if max_days > 0 and len(days) > max_days:
        days = days[-max_days:]
    return sorted(days)


def _maybe_build_prices_daily(
    repo_root: Path,
    cap_date: date,
    build: bool,
    dry_run: bool,
    prices_daily_max_months: int,
    prices_daily_include_fromboss: bool,
    prices_daily_allow_regression: bool,
    store: checkpoints.CheckpointStore,
    summary_obj: summary.RunSummary,
    summary_path: Path,
    ledger_path: Path,
    run_id: str,
    run_type: str,
) -> None:
    if not build or dry_run:
        return
    script = repo_root / "scripts" / "p1_make_prices_daily.py"
    args = [
        _resolve_python(repo_root),
        str(script),
        "--as-of",
        cap_date.isoformat(),
    ]
    if prices_daily_max_months:
        args.extend(["--max-months", str(prices_daily_max_months)])
    if prices_daily_include_fromboss:
        args.append("--include-fromboss")
    else:
        args.append("--no-include-fromboss")
    if prices_daily_allow_regression:
        args.append("--allow-regression")
    t0 = time.time()
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    duration_ms = int((time.time() - t0) * 1000)
    ok = proc.returncode == 0
    stats = _extract_prices_daily_stats(proc.stdout or "")
    if ok:
        store.write_ok("prices_daily", cap_date)
    ledger.append_ledger(
        ledger_path,
        ledger.LedgerRecord(
            dataset="prices_daily",
            day=cap_date.isoformat(),
            exit=proc.returncode,
            retries=0,
            duration_ms=duration_ms,
            run_id=run_id,
            message="",
            run_type=run_type,
        ),
    )
    _record_task(
        summary_path,
        summary_obj,
        {
            "dataset": "prices_daily",
            "day": cap_date.isoformat(),
            "status": "ok" if ok else "fail",
            "exit_code": proc.returncode,
            "duration_ms": duration_ms,
            "stdout_tail": _tail_lines(proc.stdout or ""),
            "stderr_tail": _tail_lines(proc.stderr or ""),
            **stats,
        },
    )


def _write_summary(path: Path, summary_obj: summary.RunSummary) -> None:
    summary.write_summary(path, summary_obj)


def _record_task(summary_path: Path, summary_obj: summary.RunSummary, payload: Dict[str, object]) -> None:
    summary_obj.record_task(payload)
    _write_summary(summary_path, summary_obj)


def _append_fail_ledger(
    args: argparse.Namespace,
    summary_obj: summary.RunSummary,
    run_id: str,
    exc: BaseException,
    truncate_message: bool,
) -> None:
    day = args.date or summary_obj.started_at[:10]
    message = str(exc)
    if truncate_message and len(message) > 200:
        message = message[:200]
    ledger.append_ledger(
        paths.ledger_path(_REPO_ROOT),
        ledger.LedgerRecord(
            dataset="phase1",
            day=day,
            exit=2,
            retries=0,
            duration_ms=0,
            run_id=run_id,
            message=message,
            run_type=args.run_type,
        ),
    )


def _resolve_reconcile_day(args: argparse.Namespace, summary_obj: summary.RunSummary) -> date:
    if args.date:
        return _parse_date(args.date)
    if args.start:
        return _parse_date(args.start)
    return date.fromisoformat(summary_obj.started_at[:10])


def _run_reconcile_ok(
    args: argparse.Namespace,
    summary_obj: summary.RunSummary,
    summary_path: Path,
    run_id: str,
    log_path: Path,
) -> int:
    from scripts import p1_preflight_check as preflight_check

    datahub_root = paths.datahub_root(_REPO_ROOT)
    state_root = paths.state_root(_REPO_ROOT)
    ledger_path = paths.ledger_path(_REPO_ROOT)
    store = checkpoints.CheckpointStore(state_root)
    target_day = _resolve_reconcile_day(args, summary_obj)
    apply = bool(args.apply) and (not bool(args.dry_run))

    hhf = _parse_list(args.datasets_hhf)
    hhd = _parse_list(args.datasets_hhd)
    if args.mode in ("hhf", "all"):
        hhf = [d for d in hhf if d in HHF_DATASETS]
    else:
        hhf = []
    if args.mode in ("hhd", "all"):
        hhd = [d for d in hhd if d in HHD_DATASETS]
    else:
        hhd = []

    datasets = hhf + hhd
    if "prices_daily" not in datasets:
        datasets.append("prices_daily")
    summary_obj.meta["reconcile_ok"] = True
    summary_obj.meta["reconcile_apply"] = apply
    summary_obj.meta["reconcile_day"] = target_day.isoformat()
    summary_obj.meta["reconcile_datasets"] = list(datasets)
    summary_obj.meta["run_intent"] = "reconcile_state_only"

    for ds in datasets:
        if ds == "prices_daily":
            mx = preflight_check.max_date_prices_daily(datahub_root)
            rows_for_day = preflight_check.rows_for_day_prices_daily(datahub_root, target_day)
        elif ds == "dividend":
            mx = preflight_check.max_date_dividend_ssot(datahub_root)
            rows_for_day = preflight_check.rows_for_day_dividend(datahub_root, target_day)
        else:
            mx = preflight_check.max_date_generic(datahub_root, ds)
            rows_for_day = preflight_check.rows_for_day_generic(datahub_root, ds, target_day)

        allow_empty = ds in ALLOW_EMPTY_ON_TRADING_DAY
        has_rows = bool(mx is not None and mx >= target_day and (rows_for_day > 0 or allow_empty))
        if has_rows:
            if store.exists(ds, target_day):
                status = "skip"
                message = "SKIP_OK_EXISTS"
                exit_code = 0
                action = "SKIP_OK_EXISTS"
            elif apply:
                store.write_ok(ds, target_day)
                ledger.append_ledger(
                    ledger_path,
                    ledger.LedgerRecord(
                        dataset=ds,
                        day=target_day.isoformat(),
                        exit=0,
                        retries=0,
                        duration_ms=0,
                        run_id=run_id,
                        message="RECONCILE_OK",
                        run_type=args.run_type,
                    ),
                )
                status = "ok"
                message = "RECONCILE_OK"
                exit_code = 0
                action = "WRITE_OK"
            else:
                status = "skip"
                message = "PLAN_ONLY"
                exit_code = 0
                action = "PLAN_ONLY"
        else:
            status = "fail"
            message = "MISSING_SILVER"
            exit_code = 2
            action = "MISSING_SILVER"

        max_date_str = mx.isoformat() if mx is not None else "NONE"
        _append_event(
            log_path,
            "RECONCILE_OK dataset=%s day=%s max_date=%s rows_for_day=%s action=%s"
            % (ds, target_day.isoformat(), max_date_str, rows_for_day, action),
        )
        payload = {
            "dataset": ds,
            "day": target_day.isoformat(),
            "status": status,
            "exit_code": exit_code,
            "message": message,
            "action": action,
            "rows_for_day": rows_for_day,
        }
        if mx is not None:
            payload["max_date"] = mx.isoformat()
        _record_task(summary_path, summary_obj, payload)

    summary_obj.finalize()
    _write_summary(summary_path, summary_obj)
    return 0 if summary_obj.status == "OK" else 2


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Phase-1 daily routine (Python entrypoint).")
    ap.add_argument("--mode", required=True, choices=["hhf", "hhd", "all", "migrate-layout"])
    ap.add_argument("--run-type", default="backfill", choices=["backfill", "live"])
    ap.add_argument("--start", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", help="YYYY-MM-DD (exclusive)")
    ap.add_argument("--date", help="Single day YYYY-MM-DD")
    ap.add_argument("--datasets-hhf", default="prices,chip,per,dividend")
    ap.add_argument("--datasets-hhd", default="shareholding,inst_total,gov_bank")
    skip_group = ap.add_mutually_exclusive_group()
    skip_group.add_argument("--skip-if-ok", dest="skip_if_ok", action="store_true", default=True)
    skip_group.add_argument("--no-skip-if-ok", dest="skip_if_ok", action="store_false")
    ap.add_argument("--calls-per-hour", type=int, default=6000)
    ap.add_argument("--qps", type=float, default=None)
    ap.add_argument("--rpm", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--universe-path", default="configs/investable_universe.txt")
    ap.add_argument("--cap-date", default=None)
    ap.add_argument("--live-lookback", type=int, default=5)
    ap.add_argument("--catch-up", action="store_true", default=False)
    ap.add_argument("--catch-up-max-days", type=int, default=20)
    ap.add_argument("--lock-ttl-mins", type=int, default=180)
    ap.add_argument("--force-unlock", action="store_true", default=False)
    ap.add_argument(
        "--lock-scope",
        default="global",
        help="Lock scope. Use non-global to allow multiple non-overlapping batch runs concurrently.",
    )

    ap.add_argument("--batch-workers", type=int, default=1, help="Spawn N concurrent batch runs (split by dataset).")
    ap.add_argument("--batch-scope-prefix", default="b", help="Prefix for per-batch lock scopes (e.g., b01, b02).")
    ap.add_argument("--api-hourly-cap", type=int, default=6000, help="Safety cap for (calls_per_hour * batch_workers).")
    ap.add_argument("--api-hourly-margin", type=int, default=120, help="Margin reserved from api-hourly-cap.")
    ap.add_argument("--allow-over-cap", action="store_true", default=False, help="Allow exceeding --api-hourly-cap.")
    ap.add_argument("--plan-batches", action="store_true", default=False, help="Print batch commands and exit.")
    ap.add_argument("--batch-child", action="store_true", default=False, help=argparse.SUPPRESS)
    ap.add_argument("--batch-index", type=int, default=0, help=argparse.SUPPRESS)
    shared_group = ap.add_mutually_exclusive_group()
    shared_group.add_argument("--shared-bucket", dest="shared_bucket", action="store_true")
    shared_group.add_argument("--no-shared-bucket", dest="shared_bucket", action="store_false")
    ap.set_defaults(shared_bucket=None)
    ap.add_argument("--bucket-burst", type=int, default=5, help="Shared bucket burst capacity.")
    ap.add_argument("--bucket-lease-size", type=int, default=5, help="Shared bucket lease size per process.")
    ap.add_argument("--bucket-lock-ttl-sec", type=int, default=60, help="Shared bucket lock ttl in seconds.")
    ap.add_argument("--bucket-max-wait-sec", type=float, default=None, help="Shared bucket acquire max wait seconds.")
    ap.add_argument("--bucket-state-path", default=None, help="Shared bucket state path override.")
    ap.add_argument(
        "--dividend-scan-policy",
        default="auto",
        choices=["auto", "full", "sharded", "ttl"],
        help="Dividend scan policy (auto/full/sharded/ttl).",
    )
    ap.add_argument("--dividend-shard-count", type=int, default=5, help="Dividend shard count for sharded policy.")
    ap.add_argument("--dividend-max-staleness-trading-days", type=int, default=None)
    ap.add_argument("--dividend-scan-state-path", default=None)
    ap.add_argument("--dividend-scan-lock-ttl-sec", type=int, default=120)
    dividend_force_group = ap.add_mutually_exclusive_group()
    dividend_force_group.add_argument(
        "--dividend-force-full-if-evidence-mismatch",
        dest="dividend_force_full_if_evidence_mismatch",
        action="store_true",
    )
    dividend_force_group.add_argument(
        "--no-dividend-force-full-if-evidence-mismatch",
        dest="dividend_force_full_if_evidence_mismatch",
        action="store_false",
    )
    ap.set_defaults(dividend_force_full_if_evidence_mismatch=True)
    build_group = ap.add_mutually_exclusive_group()
    build_group.add_argument("--build-prices-daily", dest="build_prices_daily", action="store_true")
    build_group.add_argument("--no-build-prices-daily", dest="build_prices_daily", action="store_false")
    ap.set_defaults(build_prices_daily=True)
    ap.add_argument("--prices-daily-max-months", type=int, default=36)
    ap.add_argument("--prices-daily-allow-regression", action="store_true", default=False)
    prices_daily_include_group = ap.add_mutually_exclusive_group()
    prices_daily_include_group.add_argument(
        "--prices-daily-include-fromboss",
        dest="prices_daily_include_fromboss",
        action="store_true",
    )
    prices_daily_include_group.add_argument(
        "--no-prices-daily-include-fromboss",
        dest="prices_daily_include_fromboss",
        action="store_false",
    )
    ap.set_defaults(prices_daily_include_fromboss=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true", default=False)
    ap.add_argument("--reconcile-ok", action="store_true", default=False)
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--run-id", default=None)
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    run_dir = paths.run_dir(_REPO_ROOT, run_id)
    log_path = run_dir / "events.log"
    summary_path = run_dir / "summary.json"
    run_dir.mkdir(parents=True, exist_ok=True)
    _append_event(log_path, f"START run_id={run_id} mode={args.mode} run_type={args.run_type}")
    _setup_logging(log_path, args.log_level)

    lock_path = paths.lock_path(_REPO_ROOT, scope=args.lock_scope)
    logging.info("acquire lock path=%s", lock_path)

    summary_obj = summary.RunSummary(run_id=run_id, mode=args.mode, run_type=args.run_type)
    shared_bucket_enabled, shared_bucket_env, shared_bucket_meta = _resolve_shared_bucket(args, _REPO_ROOT)
    dividend_policy_effective, dividend_env, dividend_meta, dividend_shard_count, dividend_max_staleness = _resolve_dividend_settings(
        args,
        _REPO_ROOT,
    )
    child_env = {}
    child_env.update(shared_bucket_env)
    child_env.update(dividend_env)
    summary_obj.meta.update(
        {
            "repo_root": str(_REPO_ROOT),
            "dry_run": bool(args.dry_run),
            "calls_per_hour": args.calls_per_hour,
            "qps": args.qps,
            "rpm": args.rpm,
            "skip_if_ok": bool(args.skip_if_ok),
            "catch_up": bool(args.catch_up),
            "catch_up_max_days": args.catch_up_max_days,
            "lock_ttl_mins": args.lock_ttl_mins,
            "lock_scope": args.lock_scope,
            "build_prices_daily": bool(args.build_prices_daily),
            "prices_daily_max_months": args.prices_daily_max_months,
            "prices_daily_include_fromboss": bool(args.prices_daily_include_fromboss),
            "prices_daily_allow_regression": bool(args.prices_daily_allow_regression),
            "batch_workers": int(args.batch_workers),
            "batch_scope_prefix": args.batch_scope_prefix,
            "api_hourly_cap": int(args.api_hourly_cap),
            "plan_batches": bool(args.plan_batches),
            "api_hourly_margin": int(args.api_hourly_margin),
        }
    )
    summary_obj.meta.update(shared_bucket_meta)
    summary_obj.meta.update(dividend_meta)
    _write_summary(summary_path, summary_obj)
    logging.info("start run_id=%s mode=%s run_type=%s", run_id, args.mode, args.run_type)

    if int(args.batch_workers) > 1 and not bool(args.batch_child):
        if args.mode == "migrate-layout":
            raise SystemExit("batch mode is not compatible with --mode migrate-layout")
        if args.reconcile_ok:
            raise SystemExit("batch mode is not compatible with --reconcile-ok")
        exit_code = _run_batch_orchestrator(
            args=args,
            run_id=run_id,
            run_dir=run_dir,
            log_path=log_path,
            summary_path=summary_path,
            summary_obj=summary_obj,
            shared_env=child_env,
        )
        return exit_code

    lock_acquired = False
    try:
        _acquire_lock(lock_path, args.lock_ttl_mins, args.force_unlock)
        lock_acquired = True
        if args.mode == "migrate-layout":
            from scripts import p1_migrate_layout

            report = p1_migrate_layout.run_migration(
                repo_root=_REPO_ROOT,
                run_id=run_id,
                apply=bool(getattr(args, "apply", False)),
                dry_run=bool(args.dry_run),
            )
            summary_obj.meta["migrate_report"] = report.get("report_path")
            summary_obj.finalize()
            _write_summary(summary_path, summary_obj)
            _append_event(log_path, f"FINISH status={summary_obj.status}")
            return 0

        if args.reconcile_ok:
            exit_code = _run_reconcile_ok(args, summary_obj, summary_path, run_id, log_path)
            _append_event(log_path, f"FINISH status={summary_obj.status}")
            return exit_code

        _ensure_token(
            summary_obj,
            paths.ledger_path(_REPO_ROOT),
            args.run_type,
            summary_path,
            log_path,
        )

        datahub_root = paths.datahub_root(_REPO_ROOT)
        state_root = paths.state_root(_REPO_ROOT)
        ledger_path = paths.ledger_path(_REPO_ROOT)
        run_dir.mkdir(parents=True, exist_ok=True)

        rate_cfg = rate_control.resolve_rate(args.calls_per_hour, args.qps, args.rpm)
        if shared_bucket_enabled:
            qps_val = max(rate_cfg.qps or 0.0, 2.0)
            rpm_val = max(rate_cfg.rpm or 0, 120)
            rate_cfg = rate_control.RateConfig(
                qps=qps_val,
                rpm=rpm_val,
                calls_per_hour=rate_cfg.calls_per_hour,
            )
        controller = rate_control.RateController(rate_cfg.qps or 1.0)
        rpm_fixed = rate_cfg.rpm

        if args.date:
            start = _parse_date(args.date)
            end = start
        else:
            start = _parse_date(args.start) if args.start else None
            end = _parse_date(args.end) if args.end else None

        cap = _parse_date(args.cap_date) if args.cap_date else trading_calendar.today_local()
        if args.date:
            cap = min(cap, start)

        cal = trading_calendar.load_trading_calendar(_REPO_ROOT)
        summary_obj.meta["calendar_path"] = str(cal.path)
        summary_obj.meta["cap_date"] = cap.isoformat()

        catch_up = bool(args.catch_up or (args.run_type == "live" and not args.date))
        summary_obj.meta["catch_up_effective"] = catch_up

        if args.date:
            base_days = [start]
        elif catch_up:
            base_days = []
        elif args.run_type == "backfill":
            if start is None or end is None:
                raise SystemExit("--start and --end are required for backfill")
            base_days = _collect_days_backfill(cal, start, end, cap)
        else:
            base_days = _collect_days_live(cal, cap, args.live_lookback)

        hhf = _parse_list(args.datasets_hhf)
        hhd = _parse_list(args.datasets_hhd)
        if args.mode in ("hhf", "all"):
            hhf = [d for d in hhf if d in HHF_DATASETS]
        else:
            hhf = []
        if args.mode in ("hhd", "all"):
            hhd = [d for d in hhd if d in HHD_DATASETS]
        else:
            hhd = []

        store = checkpoints.CheckpointStore(state_root)
        config_path = _REPO_ROOT / "configs" / "dateid_datasets.yaml"

        universe_ids: List[str] = []
        universe_path_effective = _resolve_repo_path(args.universe_path)
        if hhf or hhd:
            universe_ids = runner_hhd.load_universe_ids(universe_path_effective)
        summary_obj.meta["universe_path_effective"] = str(universe_path_effective)
        summary_obj.meta["universe_size"] = len(universe_ids)

        active_datasets = hhf + hhd
        dataset_days: Dict[str, List[date]] = {}
        if args.date:
            for ds in active_datasets:
                dataset_days[ds] = [start]
        elif catch_up:
            for ds in active_datasets:
                dataset_days[ds] = _compute_catchup_days(
                    store, ds, cal, cap, args.catch_up_max_days
                )
        else:
            for ds in active_datasets:
                dataset_days[ds] = list(base_days)

        union_days = sorted({d for days in dataset_days.values() for d in days})

        if not union_days:
            logging.info("no trading days to process")
            summary_obj.finalize()
            _write_summary(summary_path, summary_obj)
            _append_event(log_path, f"FINISH status={summary_obj.status}")
            return 0

        total_duration_ms = 0
        rate_limit_hits = 0
        prices_updated = False

        for d in union_days:
            for ds in hhf:
                if d not in dataset_days.get(ds, []):
                    continue
                if args.skip_if_ok and store.exists(ds, d):
                    evidence_ok = True
                    evidence_mismatch = False
                    evidence = None
                    if ds == "dividend":
                        evidence = dividend_scan.read_evidence(paths.dividend_evidence_path(_REPO_ROOT, d))
                        if bool(args.dividend_force_full_if_evidence_mismatch):
                            evidence_ok = dividend_scan.evidence_satisfies(
                                evidence,
                                required_policy=dividend_policy_effective,
                                shard_count=dividend_shard_count,
                                max_staleness_trading_days=dividend_max_staleness,
                            )
                            if not evidence_ok:
                                evidence_mismatch = True
                                summary_obj.meta["dividend_skip_override"] = True
                    if not evidence_mismatch:
                        if not args.dry_run:
                            ledger.append_ledger(
                                ledger_path,
                                ledger.LedgerRecord(
                                    dataset=ds,
                                    day=d.isoformat(),
                                    exit=0,
                                    retries=0,
                                    duration_ms=0,
                                    run_id=run_id,
                                    message="SKIP_OK",
                                    qps=rate_cfg.qps,
                                    rpm=rate_cfg.rpm,
                                    run_type=args.run_type,
                                ),
                            )
                        payload = {
                            "dataset": ds,
                            "day": d.isoformat(),
                            "status": "skip",
                            "exit_code": 0,
                            "message": "SKIP_OK",
                        }
                        if ds == "dividend":
                            payload["evidence_policy_ok"] = evidence_ok
                            if evidence is not None:
                                payload["dividend_evidence"] = evidence
                                summary_obj.meta["dividend_evidence_last"] = evidence
                        _record_task(summary_path, summary_obj, payload)
                        continue

                if args.dry_run:
                    _record_task(
                        summary_path,
                        summary_obj,
                        {
                            "dataset": ds,
                            "day": d.isoformat(),
                            "status": "skip",
                            "exit_code": 0,
                            "message": "DRY_RUN",
                        },
                    )
                    continue

                env = {
                    "FINMIND_TOKEN": os.environ.get("FINMIND_TOKEN", "").strip(),
                    "FINMIND_QPS": controller.env_qps(),
                    "FINMIND_RPM": str(rpm_fixed or controller.derived_rpm()),
                }
                env.update(shared_bucket_env)
                if ds == "dividend":
                    env.update(dividend_env)
                res = runner_hhf.run_hhf_day(
                    repo_root=_REPO_ROOT,
                    datahub_root=datahub_root,
                    dataset=ds,
                    day=d,
                    env=env,
                    universe_ids=universe_ids,
                )
                total_duration_ms += res.duration_ms
                if res.rate_limited:
                    rate_limit_hits += 1
                    controller.record(429)
                else:
                    controller.record(200 if res.ok else None)

                if res.ok:
                    store.write_ok(ds, d)
                    if ds == "prices":
                        prices_updated = True

                ledger.append_ledger(
                    ledger_path,
                    ledger.LedgerRecord(
                        dataset=ds,
                        day=d.isoformat(),
                        exit=res.exit_code,
                        retries=0,
                        duration_ms=res.duration_ms,
                        run_id=run_id,
                        message="",
                        qps=rate_cfg.qps,
                        rpm=rate_cfg.rpm,
                        run_type=args.run_type,
                    ),
                )
                payload = {
                    "dataset": ds,
                    "day": d.isoformat(),
                    "status": "ok" if res.ok else "fail",
                    "exit_code": res.exit_code,
                    "duration_ms": res.duration_ms,
                    "stdout_tail": res.stdout_tail,
                    "stderr_tail": res.stderr_tail,
                }
                if ds == "dividend" and res.ok and not args.dry_run:
                    evidence = dividend_scan.read_evidence(paths.dividend_evidence_path(_REPO_ROOT, d))
                    if evidence is not None:
                        payload["dividend_evidence"] = evidence
                        summary_obj.meta["dividend_evidence_last"] = evidence
                _record_task(summary_path, summary_obj, payload)

            for ds in hhd:
                if d not in dataset_days.get(ds, []):
                    continue
                if args.skip_if_ok and store.exists(ds, d):
                    if not args.dry_run:
                        ledger.append_ledger(
                            ledger_path,
                            ledger.LedgerRecord(
                                dataset=ds,
                                day=d.isoformat(),
                                exit=0,
                                retries=0,
                                duration_ms=0,
                                run_id=run_id,
                                message="SKIP_OK",
                                qps=rate_cfg.qps,
                                rpm=rate_cfg.rpm,
                                run_type=args.run_type,
                            ),
                        )
                    _record_task(
                        summary_path,
                        summary_obj,
                        {
                            "dataset": ds,
                            "day": d.isoformat(),
                            "status": "skip",
                            "exit_code": 0,
                            "message": "SKIP_OK",
                        },
                    )
                    continue

                if args.dry_run:
                    _record_task(
                        summary_path,
                        summary_obj,
                        {
                            "dataset": ds,
                            "day": d.isoformat(),
                            "status": "skip",
                            "exit_code": 0,
                            "message": "DRY_RUN",
                        },
                    )
                    continue

                env = {
                    "FINMIND_TOKEN": os.environ.get("FINMIND_TOKEN", "").strip(),
                    "FINMIND_QPS": controller.env_qps(),
                    "FINMIND_RPM": str(rpm_fixed or controller.derived_rpm()),
                }
                env.update(shared_bucket_env)
                res = runner_hhd.run_hhd_day(
                    repo_root=_REPO_ROOT,
                    datahub_root=datahub_root,
                    dataset=ds,
                    day=d,
                    universe_ids=universe_ids,
                    batch_size=args.batch_size,
                    env=env,
                    config_path=config_path if config_path.exists() else None,
                )
                total_duration_ms += res.duration_ms
                if res.rate_limited:
                    rate_limit_hits += 1
                    controller.record(429)
                else:
                    controller.record(200 if res.ok else None)

                if res.ok:
                    store.write_ok(ds, d)

                ledger.append_ledger(
                    ledger_path,
                    ledger.LedgerRecord(
                        dataset=ds,
                        day=d.isoformat(),
                        exit=res.exit_code,
                        retries=0,
                        duration_ms=res.duration_ms,
                        run_id=run_id,
                        message="",
                        qps=rate_cfg.qps,
                        rpm=rate_cfg.rpm,
                        run_type=args.run_type,
                    ),
                )
                _record_task(
                    summary_path,
                    summary_obj,
                    {
                        "dataset": ds,
                        "day": d.isoformat(),
                        "status": "ok" if res.ok else "fail",
                        "exit_code": res.exit_code,
                        "duration_ms": res.duration_ms,
                        "stdout_tail": res.stdout_tail,
                        "stderr_tail": res.stderr_tail,
                        "batches": res.batches,
                    },
                )

        if prices_updated:
            _maybe_build_prices_daily(
                repo_root=_REPO_ROOT,
                cap_date=cap,
                build=bool(args.build_prices_daily),
                dry_run=bool(args.dry_run),
                prices_daily_max_months=args.prices_daily_max_months,
                prices_daily_include_fromboss=bool(args.prices_daily_include_fromboss),
                prices_daily_allow_regression=bool(args.prices_daily_allow_regression),
                store=store,
                summary_obj=summary_obj,
                summary_path=summary_path,
                ledger_path=ledger_path,
                run_id=run_id,
                run_type=args.run_type,
            )

        summary_obj.meta["rate_limit_hits"] = rate_limit_hits
        summary_obj.meta["total_duration_ms"] = total_duration_ms
        if total_duration_ms > 0:
            tasks = summary_obj.counts.get("ok", 0) + summary_obj.counts.get("fail", 0)
            summary_obj.meta["achieved_tasks_per_hour"] = round(
                tasks / (total_duration_ms / 1000.0 / 3600.0), 2
            )
        summary_obj.finalize()
        _write_summary(summary_path, summary_obj)
        _append_event(log_path, f"FINISH status={summary_obj.status}")
        return 0 if summary_obj.status == "OK" else 2
    except SystemExit as exc:
        summary_obj.status = "FAIL"
        summary_obj.meta["error"] = str(exc)
        summary_obj.finished_at = summary.now_iso()
        _write_summary(summary_path, summary_obj)
        _append_fail_ledger(args, summary_obj, run_id, exc, truncate_message=False)
        _append_event(log_path, f"ERROR {exc}")
        _append_event(log_path, f"FINISH status={summary_obj.status}")
        return 2
    except Exception as exc:
        summary_obj.status = "FAIL"
        summary_obj.meta["error"] = str(exc)
        tb = traceback.format_exc()
        summary_obj.meta["traceback_tail"] = tb.splitlines()[-50:]
        summary_obj.finished_at = summary.now_iso()
        _write_summary(summary_path, summary_obj)
        _append_fail_ledger(args, summary_obj, run_id, exc, truncate_message=True)
        _append_event(log_path, f"ERROR {exc}")
        _append_event(log_path, f"FINISH status={summary_obj.status}")
        return 2
    finally:
        if lock_acquired:
            _release_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())

# CLI added: --prices-daily-max-months, --prices-daily-allow-regression,
# --prices-daily-include-fromboss/--no-prices-daily-include-fromboss
# defaults: prices_daily_include_fromboss=True, prices_daily_allow_regression=False, prices_daily_max_months=36
