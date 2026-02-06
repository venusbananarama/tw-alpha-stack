from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.common.lockfile import FileLock, LockActiveError  # noqa: E402
from alpha_core.phase2.corelib.dates import parse_ymd  # noqa: E402
from alpha_core.phase4.calendar import is_trading_day, load_trading_days  # noqa: E402
from alpha_core.phase5 import schemas  # noqa: E402
from alpha_core.phase5.core import (  # noqa: E402
    allocate_strategies,
    build_seed_registry,
    compile_target_portfolio,
    decorrelate_strategies,
    evaluate_strategies,
    validate_target_portfolio,
    write_artifacts,
    write_summary,
)
from alpha_core.phase5.errors import (  # noqa: E402
    ExitCode,
    InputNotFoundError,
    LockedError,
    NotTradingDayError,
    OutDirNotEmptyError,
    Phase5Error,
    REASON_NOT_TRADING_DAY,
    REASON_OK,
    REASON_RUNTIME_ERROR,
    SchemaInvalidError,
)
from alpha_core.phase5.paths import (  # noqa: E402
    build_resolved_paths,
    default_run_id,
    known_artifacts,
    PHASE5_LOCK_NAME,
    resolve_out_dir,
    resolve_prices_path,
    resolve_universe_path,
)

LOCK_TTL_MINUTES = 1440


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


def _emit_resolved_paths(*, log_path: Optional[Path], print_stdout: bool, payload: Dict[str, object]) -> None:
    line = "resolved_paths=" + json.dumps(payload, ensure_ascii=True, sort_keys=True)
    _log_line(log_path, line)


def _parse_windows(raw: str) -> List[int]:
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in items]


def _prepare_out_dir(out_dir: Path, as_of: str, force: bool) -> None:
    if out_dir.exists():
        if not force:
            items = [p for p in out_dir.iterdir() if p.name != PHASE5_LOCK_NAME]
            if items:
                raise OutDirNotEmptyError("out_dir not empty")
        if force:
            for name in known_artifacts(as_of):
                target = out_dir / name
                if target.exists() and target.is_file():
                    try:
                        target.unlink()
                    except Exception:
                        continue
    out_dir.mkdir(parents=True, exist_ok=True)


def _write_csv_atomic(path: Path, df) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase-5 daily routine")
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--universe", default="investable_universe.txt")
    parser.add_argument("--windows", default="6,12,24")
    parser.add_argument("--corr-threshold", type=float, default=0.7)
    parser.add_argument("--min-pool-size", type=int, default=1)
    parser.add_argument("--alloc-method", default="equal_weight")
    parser.add_argument("--notional", type=int, default=1000000)
    parser.add_argument("--topn", type=int, default=50)
    parser.add_argument("--print-resolved-paths", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    as_of = str(args.as_of).strip()
    run_id = args.run_id or default_run_id(as_of, str(args.profile))
    out_dir = Path(resolve_out_dir(str(_REPO_ROOT), as_of, run_id, str(args.profile), args.out_dir))
    log_path = out_dir / schemas.ArtifactNames.P5_RUN_LOG
    summary_path = out_dir / schemas.ArtifactNames.P5_SUMMARY_JSON

    reports_target_path = (_REPO_ROOT / "reports" / f"target_portfolio_{as_of}.csv").resolve()
    out_target_path = out_dir / schemas.ArtifactNames.TARGET_PORTFOLIO_CSV_FMT.format(as_of=as_of)

    status = "failed"
    reason_code = REASON_RUNTIME_ERROR
    exit_code: int = int(ExitCode.RUNTIME_ERROR)
    gates: Dict[str, object] = {}
    artifacts: Dict[str, str] = {}
    notes: Dict[str, object] | None = None
    resolved_paths: Dict[str, object] = {
        "root": str(_REPO_ROOT),
        "as_of": as_of,
        "out_dir": str(out_dir),
        "universe_path": None,
        "prices_path": "",
        "reports_target_path": str(reports_target_path),
        "out_target_path": str(out_target_path),
    }
    lock_handle: Optional[FileLock] = None
    lock_path: Optional[Path] = None
    lock_acquired = False

    try:
        try:
            parse_ymd(as_of)
        except Exception as exc:
            raise SchemaInvalidError(f"invalid as_of: {as_of}") from exc

        universe_path, universe_fallback = resolve_universe_path(str(_REPO_ROOT), args.universe)
        prices_path = resolve_prices_path(str(_REPO_ROOT), as_of)
        resolved_paths = build_resolved_paths(
            root=str(_REPO_ROOT),
            as_of=as_of,
            out_dir=str(out_dir),
            universe_path=universe_path,
            prices_path=prices_path,
            reports_target_path=str(reports_target_path),
            out_target_path=str(out_target_path),
        )

        if args.print_resolved_paths:
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            _prepare_out_dir(out_dir, as_of, bool(args.force))

        _log_line(log_path, f"start as_of={as_of} run_id={run_id} profile={args.profile}")
        _emit_resolved_paths(
            log_path=log_path,
            print_stdout=bool(args.print_resolved_paths),
            payload=resolved_paths,
        )

        if args.print_resolved_paths:
            status = "skipped"
            reason_code = REASON_OK
            exit_code = int(ExitCode.OK)
            gates = {"universe_fallback": universe_fallback}
            summary = _build_summary(
                as_of=as_of,
                run_id=run_id,
                profile=str(args.profile),
                status=status,
                reason_code=reason_code,
                exit_code=exit_code,
                resolved_paths=resolved_paths,
                gates=gates,
                artifacts={"p5_summary": str(summary_path)},
                notes=None,
            )
            write_summary(str(summary_path), summary)
            return exit_code

        calendar_path = _REPO_ROOT / "datahub" / "ref" / "trading_days.csv"
        if not calendar_path.exists():
            raise InputNotFoundError(f"calendar not found: {calendar_path}")
        try:
            trading_days = load_trading_days(calendar_path)
        except Exception as exc:  # noqa: BLE001
            raise SchemaInvalidError("calendar invalid") from exc
        if not is_trading_day(as_of, trading_days):
            raise NotTradingDayError("not trading day")

        lock_path = out_dir / "p5.lock"
        command = " ".join(str(arg) for arg in sys.argv if arg is not None)
        lock_handle = FileLock(
            lock_path,
            ttl_minutes=LOCK_TTL_MINUTES,
            auto_break_stale=True,
            force_break=bool(args.force),
            command=command,
        )
        try:
            lock_handle.acquire()
        except LockActiveError as exc:
            raise LockedError(str(exc)) from exc
        lock_acquired = True

        windows = _parse_windows(str(args.windows))
        specs = build_seed_registry(str(args.profile), as_of, int(args.topn))
        gates["candidate_count"] = len(specs)

        _log_line(log_path, "stage=eval begin")
        eval_result = evaluate_strategies(specs, prices_path, universe_path, as_of, windows)
        _log_line(log_path, "stage=eval end")

        _log_line(log_path, "stage=decorrelate begin")
        decor = decorrelate_strategies(eval_result.returns, float(args.corr_threshold), int(args.min_pool_size))
        _log_line(log_path, "stage=decorrelate end")

        _log_line(log_path, "stage=allocate begin")
        alloc = allocate_strategies(decor.selected_strategy_ids, method=str(args.alloc_method))
        _log_line(log_path, "stage=allocate end")

        _log_line(log_path, "stage=compile begin")
        target = compile_target_portfolio(
            alloc=alloc,
            specs=specs,
            eval_result=eval_result,
            prices_path=prices_path,
            universe_path=universe_path,
            as_of=as_of,
            notional=int(args.notional),
        )
        _log_line(log_path, "stage=compile end")

        artifacts = write_artifacts(
            out_dir=str(out_dir),
            as_of=as_of,
            specs=specs,
            eval_result=eval_result,
            decor_result=decor,
            alloc_result=alloc,
            target_result=target,
        )

        artifacts["reports_target"] = str(reports_target_path)
        reports_target_exists = reports_target_path.exists()
        if reports_target_exists and not args.force:
            gates["reports_target_write"] = "skipped_existing"
            gates["reports_target_exists"] = True
            _log_line(
                log_path,
                f"reports_target_write=skipped_existing path={reports_target_path}",
            )
        else:
            _write_csv_atomic(reports_target_path, target.target_df)
            gates["reports_target_write"] = "written"
            gates["reports_target_exists"] = True
            _log_line(
                log_path,
                f"reports_target_write=written path={reports_target_path}",
            )
        for key, path in artifacts.items():
            _log_line(log_path, f"artifact {key}={path}")

        schema_check = validate_target_portfolio(str(out_target_path))
        if not schema_check.get("ok", False):
            raise SchemaInvalidError("target_portfolio schema invalid", details=schema_check)

        gates.update(
            {
                "selected_pool_size": len(decor.selected_strategy_ids),
                "dropped_count": len(specs) - len(decor.selected_strategy_ids),
                "skipped_symbols_count": len(target.skipped_symbols),
                "universe_fallback": universe_fallback,
            }
        )

        status = "ok"
        reason_code = REASON_OK
        exit_code = int(ExitCode.OK)
        _log_line(log_path, f"final status={status} reason={reason_code} exit={exit_code}")
    except Phase5Error as exc:
        reason_code = exc.reason_code
        exit_code = int(exc.exit_code)
        status = "skipped" if exc.reason_code == REASON_NOT_TRADING_DAY else "failed"
        notes = {"error": str(exc), "details": exc.details}
        _log_line(log_path, f"final status={status} reason={reason_code} exit={exit_code}")
    except Exception as exc:  # noqa: BLE001
        reason_code = REASON_RUNTIME_ERROR
        exit_code = int(ExitCode.RUNTIME_ERROR)
        status = "failed"
        notes = {"error": str(exc)}
        _log_line(log_path, f"final status={status} reason={reason_code} exit={exit_code}")
    finally:
        summary = _build_summary(
            as_of=as_of,
            run_id=run_id,
            profile=str(args.profile),
            status=status,
            reason_code=reason_code,
            exit_code=exit_code,
            resolved_paths=resolved_paths,
            gates=gates,
            artifacts=artifacts or {"p5_summary": str(summary_path)},
            notes=notes,
        )
        write_summary(str(summary_path), summary)
        if lock_acquired and lock_handle is not None:
            lock_handle.release()
    return int(exit_code)


def _build_summary(
    *,
    as_of: str,
    run_id: str,
    profile: str,
    status: str,
    reason_code: str,
    exit_code: int,
    resolved_paths: Dict[str, object],
    gates: Dict[str, object],
    artifacts: Dict[str, str],
    notes: Optional[Dict[str, object]],
) -> Dict[str, object]:
    return {
        "schema_version": schemas.P5_SCHEMA_VERSION,
        "as_of": as_of,
        "run_id": run_id,
        "profile": profile,
        "status": status,
        "reason_code": reason_code,
        "exit_code": int(exit_code),
        "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        "resolved_paths": resolved_paths,
        "gates": gates,
        "artifacts": artifacts,
        "notes": notes,
    }


if __name__ == "__main__":
    raise SystemExit(main())


