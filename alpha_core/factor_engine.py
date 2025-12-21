# -*- coding: utf-8 -*-
"""
alpha_core.factor_engine

Batch runner for Phase-2 factor computation.

This module is responsible for:
- Reading factor definitions from ``rules_factors.yaml``.
- Building a batch of (factor_id, window) tasks.
- Executing those tasks concurrently using an implementation module
  (by default ``alpha_core.factor_impl``).
- Writing a JSONL ledger per successful task.
- Writing a single JSON summary file describing the whole batch run.

Public entrypoints:
- :func:`run_factor_engine` – programmatic API used by scripts/factor_engine.py.
- :func:`main` – optional CLI entrypoint for direct execution.

The implementation is deterministic and idempotent with respect to inputs:
rerunning the same configuration will recompute factors but will not corrupt
existing outputs.

This file is intended to be a "C-segment" complete implementation that can be
dropped into ``C:\\AI\\tw-alpha-stack\\alpha_core\\factor_engine.py``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from uuid import uuid4
import inspect

import yaml  # type: ignore[import]
from alpha_core.io import load_factor_panel


LOG = logging.getLogger("factor_engine")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FactorEngineConfig:
    """
    Immutable configuration for a single factor batch run.
    """

    root: Path
    rules_path: Path
    impl_module: str
    factor_root: Path
    ledger_path: Path
    summary_path: Path

    end_date: date
    windows: List[int]
    factors: List[str]

    dry_run: bool = False
    run_id_prefix: str = ""
    max_workers: Optional[int] = None
    log_level: str = "INFO"

    def effective_max_workers(self) -> int:
        if self.max_workers and self.max_workers > 0:
            return self.max_workers
        # Defensive default: min(32, cpu_count) but never less than 4
        cpu = os.cpu_count() or 4
        return max(4, min(32, cpu))

    def init_logging(self) -> None:
        level = getattr(logging, self.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )


@dataclass
class FactorTaskConfig:
    """
    A single unit of work: compute one factor on one window up to ``end_date``.
    """

    factor_id: str
    window: int
    end_date: date
    requested_windows: List[int]
    supported_windows: Optional[List[int]] = None


@dataclass
class FactorTaskResult:
    """
    Result of a single factor task.
    """

    factor_id: str
    window: int
    requested_windows: List[int]
    supported_windows: Optional[List[int]]
    compute_window_used: Optional[int]
    status: str  # "ok" | "error" | "skipped"
    run_id: str
    started_at: datetime
    finished_at: datetime
    end_date: date
    deps: List[str]
    deps_loaded_ok: Mapping[str, bool]
    output_path: Optional[str] = None
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["started_at"] = self.started_at.isoformat()
        data["finished_at"] = self.finished_at.isoformat()
        data["end_date"] = self.end_date.isoformat()
        return data


@dataclass
class FactorBatchResult:
    """
    Aggregate result of running a batch of factor tasks.
    """

    tasks: List[FactorTaskResult]
    dry_run: bool
    started_at: datetime
    finished_at: datetime

    @property
    def stats(self) -> Mapping[str, int]:
        ok = sum(1 for t in self.tasks if t.status == "ok")
        err = sum(1 for t in self.tasks if t.status == "error")
        skipped = sum(1 for t in self.tasks if t.status == "skipped")
        return {"ok": ok, "error": err, "skipped": skipped, "total": len(self.tasks)}


@dataclass
class FactorEngineSummary:
    """
    Serializable summary of a full engine run.
    """

    root: str
    impl_module: str
    rules_path: str
    factor_root: str
    ledger_path: str
    windows: List[int]
    run_id_prefix: str
    dry_run: bool
    started_at: datetime
    finished_at: datetime
    stats: Mapping[str, int]
    tasks: List[Mapping[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "impl_module": self.impl_module,
            "rules_path": self.rules_path,
            "factor_root": self.factor_root,
            "ledger_path": self.ledger_path,
            "windows": list(self.windows),
            "run_id_prefix": self.run_id_prefix,
            "dry_run": self.dry_run,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "stats": dict(self.stats),
            "tasks": list(self.tasks),
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_factor_registry(rules_path: Path) -> Mapping[str, Mapping[str, Any]]:
    """
    Load factor definitions from rules_factors.yaml.

    支援兩種 schema：

    1) mapping 形式：
       factors:
         mom_6m:
           enabled: true
           wf_windows: [6, 12]
           engine: ta_mom_v1

    2) list 形式（你目前使用的寫法）：
       factors:
         - factor_id: mom_6m
           enabled: true
           wf_windows: [6, 12]
           engine: ta_mom_v1
         - factor_id: value_pe
           ...

    會統一整理成：
        { factor_id: { ...完整設定... } }
    """
    if not rules_path.is_file():
        raise FileNotFoundError(f"rules file not found: {rules_path}")

    raw = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
    raw_factors = raw.get("factors")

    if raw_factors is None:
        raise ValueError(f"rules file {rules_path} has no top-level 'factors' key")

    registry: Dict[str, Mapping[str, Any]] = {}

    # Case 1：mapping keyed by factor_id
    if isinstance(raw_factors, Mapping):
        for fid, cfg in raw_factors.items():
            if cfg is None:
                cfg = {}
            if not isinstance(cfg, Mapping):
                raise ValueError(
                    f"rules file {rules_path} has non-mapping config for factor_id={fid!r}"
                )
            cfg_dict = dict(cfg)
            # 若沒寫 factor_id，就用 key 補上
            cfg_dict.setdefault("factor_id", fid)
            registry[str(fid)] = cfg_dict
        return registry

    # Case 2：list of mappings, each with factor_id（rules_factors.yaml 現在的格式）
    if isinstance(raw_factors, Sequence):
        for idx, item in enumerate(raw_factors):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"rules file {rules_path} has non-mapping entry at factors[{idx}]"
                )

            factor_id = item.get("factor_id")
            if not factor_id:
                raise ValueError(
                    f"rules file {rules_path} has factor entry at index {idx} without 'factor_id'"
                )

            if factor_id in registry:
                raise ValueError(
                    f"rules file {rules_path} has duplicated factor_id={factor_id!r}"
                )

            registry[str(factor_id)] = dict(item)

        return registry

    # 其它型態一律視為錯誤
    raise ValueError(
        f"rules file {rules_path} has unsupported 'factors' type: {type(raw_factors)!r}"
    )


def select_compute_window(
    requested_windows: Sequence[int],
    supported_windows: Optional[Sequence[int]],
) -> Optional[int]:
    """
    Select a single compute window to avoid overwriting outputs across windows.

    Rules:
      - If requested_windows is empty -> None
      - If supported_windows is None/empty -> max(requested_windows)
      - Else take intersection; if empty -> None; else max(intersection)
    """
    req = [int(w) for w in requested_windows if w is not None]
    if not req:
        return None

    if not supported_windows:
        return max(req)

    sup = {int(w) for w in supported_windows}
    inter = [w for w in req if w in sup]
    if not inter:
        return None
    return max(inter)


def _build_tasks(
    cfg: FactorEngineConfig,
    registry: Mapping[str, Mapping[str, Any]],
) -> List[FactorTaskConfig]:
    """
    From the registry and requested factor/window lists, build concrete tasks.
    """
    tasks: List[FactorTaskConfig] = []

    requested = cfg.factors or list(registry.keys())
    windows = cfg.windows

    for factor_id in requested:
        meta = registry.get(factor_id)
        if meta is None:
            LOG.warning("Factor %s not found in registry; skip", factor_id)
            continue

        enabled = bool(meta.get("enabled", True))
        if not enabled:
            LOG.info("Factor %s is disabled in rules; skip", factor_id)
            continue

        supported_windows_raw = meta.get("wf_windows")
        supported_windows: Optional[List[int]]
        if isinstance(supported_windows_raw, Sequence) and supported_windows_raw:
            supported_windows = [int(w) for w in supported_windows_raw]
        else:
            supported_windows = None  # 全視為支援

        compute_window = select_compute_window(windows, supported_windows)
        if compute_window is None:
            LOG.info(
                "Factor %s has no overlapping windows with requested=%s supported=%s; skip",
                factor_id,
                windows,
                supported_windows,
            )
            continue

        tasks.append(
            FactorTaskConfig(
                factor_id=factor_id,
                window=int(compute_window),
                end_date=cfg.end_date,
                requested_windows=[int(w) for w in windows],
                supported_windows=list(supported_windows) if supported_windows is not None else None,
            )
        )

    return tasks


def _select_impl_function(impl_module: str):
    """
    Best-effort selection of the underlying implementation function from the
    implementation module.

    We try a few common names in order:
    - run_factor_task
    - run_factor
    - compute_factor

    The selected callable is returned.
    """
    mod = import_module(impl_module)
    for name in ("run_factor_task", "run_factor", "compute_factor"):
        fn = getattr(mod, name, None)
        if callable(fn):
            LOG.debug("Using implementation %s.%s", impl_module, name)
            return fn
    raise RuntimeError(
        f"Implementation module {impl_module!r} does not expose any of "
        "'run_factor_task', 'run_factor', or 'compute_factor'"
    )


def _run_single_task(
    engine_cfg: FactorEngineConfig,
    task_cfg: FactorTaskConfig,
    impl_fn,
    registry: Mapping[str, Mapping[str, Any]],
) -> FactorTaskResult:
    """
    Execute a single factor task using the provided implementation function.

    The implementation is called with keyword arguments filtered to match its
    signature, to avoid tight coupling.
    """
    started_at = datetime.now(timezone.utc)
    run_id = f"{engine_cfg.run_id_prefix}-{task_cfg.factor_id}-w{task_cfg.window}-{uuid4().hex[:8]}"

    # Fetch specific config for this factor
    factor_cfg: Mapping[str, Any] = registry.get(task_cfg.factor_id, {}) or {}
    rules_for_factor: Mapping[str, Any] = factor_cfg
    params_for_factor: Mapping[str, Any] = factor_cfg.get("params") or {}
    params_for_factor = dict(params_for_factor)

    # Build generic kwargs and then filter by the callable's signature.
    # parse dependencies from params.neutralize_with
    def _extract_deps(raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            return [raw]
        from collections.abc import Iterable as _Iterable

        if isinstance(raw, _Iterable) and not isinstance(raw, (bytes, bytearray)):
            out: List[str] = []
            for v in raw:
                try:
                    s = str(v).strip()
                except Exception:
                    continue
                if s:
                    out.append(s)
            return out
        return []

    deps = _extract_deps(params_for_factor.get("neutralize_with"))
    deps_loaded: Dict[str, bool] = {}
    aux_panels: Dict[str, Any] = {}

    # Load dependent factor panels (if any), fail-fast on missing
    for dep_id in deps:
        try:
            panel = load_factor_panel(
                factor_root=engine_cfg.factor_root,
                factor_id=dep_id,
                as_of=task_cfg.end_date,
                window_months=task_cfg.window,
            )
            aux_panels[dep_id] = panel
            deps_loaded[dep_id] = True
        except Exception as exc:  # noqa: BLE001
            deps_loaded[dep_id] = False
            raise RuntimeError(
                f"failed to load dependency factor={dep_id} for {task_cfg.factor_id} "
                f"as_of={task_cfg.end_date} window={task_cfg.window}: {exc}"
            ) from exc

    if aux_panels:
        params_for_factor["_aux_factor_panels"] = aux_panels

    call_kwargs: Dict[str, Any] = {
        "root": engine_cfg.root,
        "rules_path": engine_cfg.rules_path,
        "factor_root": engine_cfg.factor_root,
        "ledger_path": engine_cfg.ledger_path,
        "factor_id": task_cfg.factor_id,
        "window": task_cfg.window,
        "end": task_cfg.end_date,
        "end_date": task_cfg.end_date,  # some impls may prefer this name
        "dry_run": engine_cfg.dry_run,
        "run_id": run_id,
        "logger": LOG,
        # 新增參數以滿足 Step 1-5 需求
        "rules": rules_for_factor,
        "params": params_for_factor,
    }

    try:
        sig = inspect.signature(impl_fn)
        filtered = {k: v for k, v in call_kwargs.items() if k in sig.parameters}

        LOG.info(
            "Start factor task: factor=%s window=%s end=%s dry_run=%s",
            task_cfg.factor_id,
            task_cfg.window,
            task_cfg.end_date,
            engine_cfg.dry_run,
        )
        result = impl_fn(**filtered)
        output_path: Optional[str] = None
        if isinstance(result, Mapping):
            for key in ("parquet_path", "output_path", "path", "parquet_root"):
                val = result.get(key)
                if isinstance(val, (str, Path)):
                    output_path = str(val)
                    break
        finished_at = datetime.now(timezone.utc)
        
        status = result.get("status", "ok") if isinstance(result, Mapping) else "ok"
        msg = result.get("reason") or result.get("error") if isinstance(result, Mapping) else None

        if status == "error":
             LOG.error(
                "Done factor task (FAILED): factor=%s window=%s reason=%s",
                task_cfg.factor_id,
                task_cfg.window,
                msg,
            )
        else:
            LOG.info(
                "Done factor task: factor=%s window=%s status=%s output=%s",
                task_cfg.factor_id,
                task_cfg.window,
                status,
                output_path,
            )
            
        return FactorTaskResult(
            factor_id=task_cfg.factor_id,
            window=task_cfg.window,
            requested_windows=task_cfg.requested_windows,
            supported_windows=task_cfg.supported_windows,
            compute_window_used=task_cfg.window,
            status=status,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            end_date=task_cfg.end_date,
            deps=deps,
            deps_loaded_ok=deps_loaded,
            output_path=output_path,
            message=msg,
        )
    except Exception as exc:  # noqa: BLE001
        finished_at = datetime.now(timezone.utc)
        LOG.exception(
            "Factor task failed: factor=%s window=%s error=%s",
            task_cfg.factor_id,
            task_cfg.window,
            exc,
        )
        return FactorTaskResult(
            factor_id=task_cfg.factor_id,
            window=task_cfg.window,
            requested_windows=task_cfg.requested_windows,
            supported_windows=task_cfg.supported_windows,
            compute_window_used=task_cfg.window,
            status="error",
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            end_date=task_cfg.end_date,
            deps=deps,
            deps_loaded_ok=deps_loaded,
            output_path=None,
            message=str(exc),
        )


def _run_factor_batch(
    cfg: FactorEngineConfig,
    tasks: Sequence[FactorTaskConfig],
    impl_fn,
    registry: Mapping[str, Mapping[str, Any]],
) -> FactorBatchResult:
    """
    Execute all tasks concurrently and collect results.
    """
    started_at = datetime.now(timezone.utc)
    results: List[FactorTaskResult] = []

    if not tasks:
        finished_at = started_at
        return FactorBatchResult(tasks=[], dry_run=cfg.dry_run, started_at=started_at, finished_at=finished_at)

    max_workers = cfg.effective_max_workers()
    LOG.info("Starting factor batch: tasks=%d max_workers=%d dry_run=%s", len(tasks), max_workers, cfg.dry_run)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 傳遞 registry 給 _run_single_task
        future_to_task = {
            executor.submit(_run_single_task, cfg, task, impl_fn, registry): task for task in tasks
        }
        for future in as_completed(future_to_task):
            res = future.result()
            results.append(res)

    finished_at = datetime.now(timezone.utc)
    batch = FactorBatchResult(tasks=results, dry_run=cfg.dry_run, started_at=started_at, finished_at=finished_at)
    stats = batch.stats
    LOG.info(
        "Factor batch finished: ok=%d error=%d skipped=%d total=%d dry_run=%s",
        stats["ok"],
        stats["error"],
        stats["skipped"],
        stats["total"],
        cfg.dry_run,
    )
    return batch


def _append_ledger_entries(cfg: FactorEngineConfig, batch: FactorBatchResult) -> None:
    """
    Append per-task JSONL records to the factor ledger.

    Only tasks with status == "ok" are written. This function is a no-op if
    ``cfg.dry_run`` is True.
    """
    if cfg.dry_run:
        LOG.info("Dry-run enabled; skip writing ledger entries")
        return

    if not batch.tasks:
        return

    cfg.ledger_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    for t in batch.tasks:
        if t.status != "ok":
            continue
        rec = {
            "run_id": t.run_id,
            "factor_id": t.factor_id,
            "window": t.window,
            "status": t.status,
            "end_date": t.end_date.isoformat(),
            "output_path": t.output_path,
            "written_at": datetime.now(timezone.utc).isoformat(),
        }
        lines.append(json.dumps(rec, ensure_ascii=False))

    if not lines:
        return

    with cfg.ledger_path.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(line)
            f.write("\n")


def _write_summary(cfg: FactorEngineConfig, batch: FactorBatchResult) -> FactorEngineSummary:
    """
    Build and write the engine summary JSON file.
    """
    cfg.summary_path.parent.mkdir(parents=True, exist_ok=True)

    summary = FactorEngineSummary(
        root=str(cfg.root),
        impl_module=cfg.impl_module,
        rules_path=str(cfg.rules_path),
        factor_root=str(cfg.factor_root),
        ledger_path=str(cfg.ledger_path),
        windows=list(cfg.windows),
        run_id_prefix=cfg.run_id_prefix,
        dry_run=cfg.dry_run,
        started_at=batch.started_at,
        finished_at=batch.finished_at,
        stats=batch.stats,
        tasks=[t.to_dict() for t in batch.tasks],
    )

    payload = summary.to_dict()
    with cfg.summary_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    LOG.info("Wrote factor engine summary: %s", cfg.summary_path)
    return summary


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_factor_engine(cfg: FactorEngineConfig) -> FactorEngineSummary:
    """
    High-level orchestration entrypoint used by scripts/factor_engine.py.

    Steps:
    1. Initialise logging.
    2. Load registry from rules_factors.yaml.
    3. Build tasks (factor_id × window).
    4. Execute batch concurrently.
    5. Append ledger entries for successful tasks (unless dry_run).
    6. Write factor_engine_summary.json.

    Returns the :class:`FactorEngineSummary` instance.
    """
    cfg.init_logging()

    LOG.info("Factor engine started: root=%s", cfg.root)
    cfg.factor_root.mkdir(parents=True, exist_ok=True)

    registry = _load_factor_registry(cfg.rules_path)
    tasks = _build_tasks(cfg, registry)

    if not tasks:
        LOG.warning("No factor tasks to run (factors/windows/registry produced empty task list)")
        now = datetime.now(timezone.utc)
        empty_batch = FactorBatchResult(tasks=[], dry_run=cfg.dry_run, started_at=now, finished_at=now)
        return _write_summary(cfg, empty_batch)

    impl_fn = _select_impl_function(cfg.impl_module)

    # 傳入 registry 讓 worker 可以獲取詳細 rules
    batch = _run_factor_batch(cfg, tasks, impl_fn, registry)
    _append_ledger_entries(cfg, batch)
    summary = _write_summary(cfg, batch)

    return summary


# ---------------------------------------------------------------------------
# CLI glue (optional)
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase-2 factor engine batch runner")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root (default: current directory)")
    parser.add_argument("--rules", dest="rules_path", type=Path, default=None, help="Path to rules_factors.yaml")
    parser.add_argument(
        "--impl-module",
        dest="impl_module",
        default="alpha_core.factor_impl",
        help="Python module implementing factor computation (default: alpha_core.factor_impl)",
    )
    parser.add_argument(
        "--factor-root",
        dest="factor_root",
        type=Path,
        default=None,
        help="Root directory for factor parquet outputs (default: <root>/datahub/silver/alpha/factor)",
    )
    parser.add_argument(
        "--ledger",
        dest="ledger_path",
        type=Path,
        default=None,
        help="Path to factor_ledger.jsonl (default: <root>/metrics/factor_ledger.jsonl)",
    )
    parser.add_argument(
        "--summary",
        dest="summary_path",
        type=Path,
        default=None,
        help="Path to factor_engine_summary.json (default: <root>/reports/factor_engine_summary.json)",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default="",
        help="Comma-separated list of factor_ids to run (default: all enabled in rules_factors.yaml)",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="6",
        help="Comma-separated list of walk windows in months (e.g. '6,12,24')",
    )
    parser.add_argument(
        "--end",
        dest="end_date",
        type=str,
        required=True,
        help="As-of date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--run-id-prefix",
        dest="run_id_prefix",
        type=str,
        default="",
        help="Prefix for run_id; default is end_date",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Do not write parquet or ledger; only execute and write summary",
    )
    parser.add_argument(
        "--max-workers",
        dest="max_workers",
        type=int,
        default=None,
        help="Maximum concurrent workers (default: min(32, cpu_count), but ≥ 4)",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ...). Default: INFO",
    )
    return parser.parse_args(argv)


def _cfg_from_args(ns: argparse.Namespace) -> FactorEngineConfig:
    root = ns.root.resolve()
    rules_path = (ns.rules_path or (root / "rules_factors.yaml")).resolve()
    factor_root = (ns.factor_root or (root / "datahub" / "silver" / "alpha" / "factor")).resolve()
    ledger_path = (ns.ledger_path or (root / "metrics" / "factor_ledger.jsonl")).resolve()
    summary_path = (ns.summary_path or (root / "reports" / "factor_engine_summary.json")).resolve()

    end_date = date.fromisoformat(ns.end_date)

    factors: List[str] = []
    if ns.factors:
        # allow both comma-separated and accidental spaces
        for part in ns.factors.split(","):
            part = part.strip()
            if part:
                factors.append(part)

    windows: List[int] = []
    if ns.windows:
        for item in ns.windows.split(","):
            item = item.strip()
            if not item:
                continue
            windows.append(int(item))

    run_id_prefix = ns.run_id_prefix or end_date.isoformat()

    return FactorEngineConfig(
        root=root,
        rules_path=rules_path,
        impl_module=str(ns.impl_module),
        factor_root=factor_root,
        ledger_path=ledger_path,
        summary_path=summary_path,
        end_date=end_date,
        windows=windows,
        factors=factors,
        dry_run=bool(ns.dry_run),
        run_id_prefix=run_id_prefix,
        max_workers=ns.max_workers,
        log_level=str(ns.log_level),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint. Intended usage:

    .. code-block:: bash

        python -m alpha_core.factor_engine \\
          --root C:\\AI\\tw-alpha-stack \\
          --end 2025-11-28 \\
          --factors mom_6m,mom_12m,value_pe \\
          --windows 6,12 \\
          --dry-run

    ``scripts/factor_engine.py`` can also delegate here by importing
    :func:`run_factor_engine` or :func:`main`.
    """
    ns = _parse_args(argv)
    cfg = _cfg_from_args(ns)
    try:
        run_factor_engine(cfg)
        return 0
    except Exception:  # noqa: BLE001
        LOG.exception("Factor engine failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
