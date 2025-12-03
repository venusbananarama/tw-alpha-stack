# C:\AI\tw-alpha-stack\alpha_core\factor_engine.py
from __future__ import annotations

"""
alpha_core.factor_engine

Phase-2 因子引擎核心：

- FactorTask / FactorRunConfig / FactorRunResult / FactorBatchResult：
    統一管理「要跑哪些因子、怎麼跑、結果長什麼樣」。
- run_factor_batch：
    單一批次因子計算入口（compute → parquet → ledger）。
- run_factor_engine：
    舊版入口的兼容 wrapper，給 scripts/factor_engine.py 使用。

職責：
- 不碰 FinMind / API，只處理：
    - 讀取 rules_factors.yaml → FactorDefinition
    - 呼叫 alpha_core.factor_impl.compute_factor(...)
    - factor parquet + factor_ledger.jsonl + factor_engine_summary.json
"""

from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import importlib
import json
import logging

import pandas as pd

from .config import ConfigError, FactorDefinition, load_factor_definitions
from .io import append_jsonlines, ensure_dir, write_factor_parquet

LoggerLike = logging.Logger

# ---------------------------------------------------------------------------
# New core dataclasses（Task / Config / Result / Batch）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FactorTask:
    """
    單一因子的工作說明書。

    spec 一般會是 FactorDefinition（dataclass），
    但也可以是 dict；factor_impl 會用 _normalize_spec 處理。
    """

    factor_id: str
    spec: Any
    start_date: Optional[date]
    end_date: date
    tag: Optional[str] = None  # 例如 "compute+eval" / "eval_only"


@dataclass(frozen=True)
class FactorRunConfig:
    """
    整批因子的共用設定（底層核心）。

    - root        : repo 根目錄
    - factor_root : factor parquet 根目錄
    - ledger_path : factor_ledger.jsonl 路徑
    - impl_module : 實作模組（預設 alpha_core.factor_impl）
    - dry_run     : 只算不寫（不寫 parquet / ledger）
    - max_workers : 預留未來並行用，目前實作仍為單執行緒
    - run_id_prefix : run_id 前綴（寫 parquet / ledger 用）
    - windows     : WF 視窗列表（例如 (6, 12, 24)）
    """

    root: Path
    factor_root: Path
    ledger_path: Path
    impl_module: str = "alpha_core.factor_impl"
    dry_run: bool = False
    max_workers: int = 1
    run_id_prefix: str = "factor"
    windows: Tuple[int, ...] = ()
    logger_name: str = "alpha_core.factor_engine"


@dataclass
class FactorRunResult:
    """
    單顆因子單次 run 的結果摘要。
    """

    factor_id: str
    start_date: Optional[date]
    end_date: date
    run_id: Optional[str] = None

    num_rows: int = 0
    num_days: int = 0
    num_stocks: int = 0
    partitions_written: Sequence[Path] = ()

    status: str = "ok"  # "ok" | "dry_run" | "skipped" | "error"
    error_message: Optional[str] = None
    reason: Optional[str] = None
    tag: Optional[str] = None  # 同 FactorTask.tag（可選）


@dataclass
class FactorBatchResult:
    """
    一批因子跑完的彙總結果。
    """

    root: Path
    config: FactorRunConfig
    tasks_count: int
    results: Sequence[FactorRunResult]
    started_at: datetime
    finished_at: datetime

    @property
    def errors_count(self) -> int:
        return sum(1 for r in self.results if r.status == "error")

    @property
    def total_rows(self) -> int:
        return sum(int(r.num_rows) for r in self.results)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": str(self.root),
            "factor_root": str(self.config.factor_root),
            "ledger_path": str(self.config.ledger_path),
            "impl_module": self.config.impl_module,
            "windows": list(self.config.windows),
            "run_id_prefix": self.config.run_id_prefix,
            "dry_run": self.config.dry_run,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "tasks_count": self.tasks_count,
            "total_rows": self.total_rows,
            "counts": {
                "ok": sum(1 for r in self.results if r.status == "ok"),
                "dry_run": sum(1 for r in self.results if r.status == "dry_run"),
                "error": sum(1 for r in self.results if r.status == "error"),
                "skipped": sum(1 for r in self.results if r.status == "skipped"),
            },
            "results": [
                {
                    "factor_id": r.factor_id,
                    "run_id": r.run_id,
                    "status": r.status,
                    "reason": r.reason,
                    "error_message": r.error_message,
                    "start_date": r.start_date.isoformat() if r.start_date else None,
                    "end_date": r.end_date.isoformat(),
                    "num_rows": r.num_rows,
                    "num_days": r.num_days,
                    "num_stocks": r.num_stocks,
                    "files": [str(p) for p in r.partitions_written],
                    "tag": r.tag,
                }
                for r in self.results
            ],
        }


# ---------------------------------------------------------------------------
# 舊版 FactorEngineConfig（保留，給 scripts/factor_engine.py 用）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FactorEngineConfig:
    """
    Phase-2 因子引擎設定（由 scripts/factor_engine.py 組出來）。

    Attributes 與 CLI 對應：
        root          : --root
        impl_module   : --impl-module
        rules_path    : --rules
        factor_ids    : --factors（逗號分隔）拆解後
        start_date    : --start（可為 None）
        end_date      : --end（通常 = as-of 的隔日，半開區間）
        run_id_prefix : --run-id-prefix
        dry_run       : --dry-run
        max_factors   : --max-factors
        factor_root   : --factor-root（預設 datahub/silver/alpha/factor）
        ledger_path   : --ledger-path（預設 metrics/factor_ledger.jsonl）
        summary_path  : --summary-path（預設 reports/factor_engine_summary.json）
        windows       : --windows（逗號分隔 → Tuple[int,...]）
    """

    root: Path
    impl_module: str
    rules_path: Optional[Path]
    factor_ids: List[str]
    start_date: Optional[date]
    end_date: Optional[date]
    run_id_prefix: str
    dry_run: bool
    max_factors: Optional[int]
    factor_root: Optional[Path]
    ledger_path: Optional[Path]
    summary_path: Optional[Path]
    windows: Tuple[int, ...]


# ---------------------------------------------------------------------------
# 共用小工具
# ---------------------------------------------------------------------------


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)


def _get_logger(name: str) -> LoggerLike:
    return logging.getLogger(name)


def _load_compute_impl(module_name: str):
    """
    Import factor implementation module，並取得 compute_factor。
    """
    module = importlib.import_module(module_name)
    if not hasattr(module, "compute_factor"):
        raise RuntimeError(
            f"Implementation module {module_name!r} has no compute_factor(...) function."
        )
    return getattr(module, "compute_factor")


def _resolve_paths_from_engine_cfg(
    cfg: FactorEngineConfig,
) -> Tuple[Path, Path, Path]:
    """
    舊版 config 專用：根據 FactorEngineConfig 決定 factor_root / ledger_path / summary_path。
    """
    root = cfg.root.resolve()

    factor_root = cfg.factor_root or (root / "datahub" / "silver" / "alpha" / "factor")
    ledger_path = cfg.ledger_path or (root / "metrics" / "factor_ledger.jsonl")
    summary_path = cfg.summary_path or (root / "reports" / "factor_engine_summary.json")

    factor_root = factor_root.resolve()
    ledger_path = ledger_path.resolve()
    summary_path = summary_path.resolve()

    ensure_dir(factor_root)
    ensure_dir(ledger_path.parent)
    ensure_dir(summary_path.parent)

    return factor_root, ledger_path, summary_path


def _select_factor_ids(
    cfg: FactorEngineConfig,
    defs: Mapping[str, FactorDefinition],
) -> List[str]:
    """
    依 config.factor_ids / max_factors 決定本次要跑的因子清單。
    """
    if cfg.factor_ids:
        ids = [fid for fid in cfg.factor_ids if fid in defs]
    else:
        ids = sorted(defs.keys())

    if cfg.max_factors is not None and cfg.max_factors >= 0:
        ids = ids[: cfg.max_factors]

    return ids


def _resolve_factor_dates(
    cfg: FactorEngineConfig,
    spec: FactorDefinition,
) -> Tuple[Optional[date], date]:
    """
    決定單一因子的 (start_date, end_date)。

    規則：
      - end_date：必須來自 cfg.end_date（目前不自動推）。
      - start_date：優先 cfg.start_date，其次 spec.start_date。
      - 若 end_date 缺失 → raise ConfigError。
      - 若 start_date 存在且 >= end_date → raise ConfigError。
    """
    end_date = cfg.end_date
    if end_date is None:
        raise ConfigError("FactorEngineConfig.end_date is required (got None).")

    start_date = cfg.start_date or getattr(spec, "start_date", None)

    if start_date is not None and start_date >= end_date:
        raise ConfigError(
            f"Invalid date range for factor {spec.factor_id!r}: "
            f"start_date={start_date}, end_date={end_date}"
        )

    return start_date, end_date


def _run_compute_factor(
    compute_factor,
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: date,
    windows: Tuple[int, ...],
    logger: LoggerLike,
) -> pd.DataFrame:
    """
    呼叫實作層的 compute_factor。

    嘗試順序：
      1) compute_factor(root=..., factor_id=..., spec=..., start_date=..., end_date=..., windows=...)
      2) 若 TypeError 且包含 'windows' 字樣 → 重試不帶 windows。
    """
    try:
        return compute_factor(
            root=root,
            factor_id=factor_id,
            spec=spec,
            start_date=start_date,
            end_date=end_date,
            windows=windows,
        )
    except TypeError as exc:
        msg = str(exc)
        if "windows" in msg and "unexpected" in msg:
            logger.info(
                "compute_factor(%s) does not accept 'windows' kwarg, retrying without it.",
                factor_id,
            )
            return compute_factor(
                root=root,
                factor_id=factor_id,
                spec=spec,
                start_date=start_date,
                end_date=end_date,
            )
        raise


def _build_run_id(prefix: str, factor_id: str, end: date) -> str:
    return f"{prefix}-{factor_id}-{end.strftime('%Y%m%d')}"


def _append_factor_ledger(
    cfg: FactorRunConfig,
    result: FactorRunResult,
) -> None:
    """
    依 FactorRunResult 追加一筆 JSON line 到 factor_ledger.jsonl。
    在 dry_run 模式下不做任何事。
    """
    if cfg.dry_run:
        return

    record: Dict[str, Any] = {
        "ts": datetime.utcnow().isoformat(),
        "factor_id": result.factor_id,
        "run_id": result.run_id,
        "status": result.status,
        "reason": result.reason,
        "error_message": result.error_message,
        "start_date": result.start_date.isoformat() if result.start_date else None,
        "end_date": result.end_date.isoformat(),
        "rows": result.num_rows,
        "num_days": result.num_days,
        "num_stocks": result.num_stocks,
        "files": [str(p) for p in result.partitions_written],
        "tag": result.tag,
    }
    append_jsonlines(cfg.ledger_path, [record])


# ---------------------------------------------------------------------------
# 核心 compute API：compute_factor_to_dataframe / run_factor_batch
# ---------------------------------------------------------------------------


def compute_factor_to_dataframe(
    cfg: FactorRunConfig,
    task: FactorTask,
    compute_factor_fn=None,
    logger: Optional[LoggerLike] = None,
) -> tuple[pd.DataFrame, FactorRunResult]:
    """
    計算單一因子的 DataFrame（不寫 parquet / ledger）。

    回傳：
        (df, result)

    若計算失敗：
        - df 為空 DataFrame（含 date / stock_id / factor_value 欄位）
        - result.status = "error"
        - result.error_message 帶錯誤訊息
    """
    log = logger or _get_logger(cfg.logger_name)
    if compute_factor_fn is None:
        compute_factor_fn = _load_compute_impl(cfg.impl_module)

    result = FactorRunResult(
        factor_id=task.factor_id,
        start_date=task.start_date,
        end_date=task.end_date,
        tag=task.tag,
    )

    empty = pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    try:
        df = _run_compute_factor(
            compute_factor=compute_factor_fn,
            root=cfg.root,
            factor_id=task.factor_id,
            spec=task.spec,
            start_date=task.start_date,
            end_date=task.end_date,
            windows=cfg.windows,
            logger=log,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("compute_factor failed for %s: %s", task.factor_id, exc)
        result.status = "error"
        result.error_message = repr(exc)
        return empty, result

    if not isinstance(df, pd.DataFrame):
        msg = f"compute_factor must return a pandas.DataFrame, got {type(df).__name__}"
        log.error(msg)
        result.status = "error"
        result.error_message = msg
        return empty, result

    if df is None or df.empty:
        result.status = "ok"
        result.num_rows = 0
        result.num_days = 0
        result.num_stocks = 0
        return empty, result

    df = df.copy()
    if "date" not in df.columns or "stock_id" not in df.columns:
        msg = f"factor {task.factor_id!r} missing 'date' or 'stock_id' column"
        log.error(msg)
        result.status = "error"
        result.error_message = msg
        return empty, result

    # 標準化欄位
    df["date"] = pd.to_datetime(df["date"])
    df.dropna(subset=["date", "stock_id"], inplace=True)
    if "factor_value" not in df.columns:
        # 若實作層用其他命名，最後 fallback 成 factor_value
        for cand in ("value", task.factor_id):
            if cand in df.columns:
                df.rename(columns={cand: "factor_value"}, inplace=True)
                break
    if "factor_value" not in df.columns:
        msg = f"factor {task.factor_id!r} is missing 'factor_value' column after normalization"
        log.error(msg)
        result.status = "error"
        result.error_message = msg
        return empty, result

    df = df.dropna(subset=["factor_value"])
    if df.empty:
        result.status = "ok"
        result.num_rows = 0
        result.num_days = 0
        result.num_stocks = 0
        return empty, result

    result.status = "ok"
    result.num_rows = int(len(df))
    result.num_days = int(df["date"].nunique())
    result.num_stocks = int(df["stock_id"].nunique())

    return df[["date", "stock_id", "factor_value"]], result


def run_factor_batch(
    cfg: FactorRunConfig,
    tasks: Iterable[FactorTask],
    logger: Optional[LoggerLike] = None,
) -> FactorBatchResult:
    """
    Phase-2 因子計算核心入口。

    行為：
      - 對每一個 FactorTask：
          * 呼叫 compute_factor_to_dataframe(...)
          * 在非 dry_run 模式下寫 parquet + ledger
      - 回傳 FactorBatchResult（供 scripts 產生 summary / log 用）

    注意：
      - 錯誤不會丟給呼叫端（除非 root 等基本設定有問題），
        而是寫在 FactorRunResult.status / error_message。
    """
    log = logger or _get_logger(cfg.logger_name)

    root = cfg.root.resolve()
    cfg = FactorRunConfig(
        root=root,
        factor_root=cfg.factor_root.resolve(),
        ledger_path=cfg.ledger_path.resolve(),
        impl_module=cfg.impl_module,
        dry_run=cfg.dry_run,
        max_workers=cfg.max_workers,
        run_id_prefix=cfg.run_id_prefix,
        windows=cfg.windows,
        logger_name=cfg.logger_name,
    )

    ensure_dir(cfg.factor_root)
    ensure_dir(cfg.ledger_path.parent)

    compute_factor_fn = _load_compute_impl(cfg.impl_module)

    started_at = datetime.utcnow()
    results: List[FactorRunResult] = []

    for task in tasks:
        log.info(
            "Running factor=%s start=%s end=%s dry_run=%s",
            task.factor_id,
            task.start_date,
            task.end_date,
            cfg.dry_run,
        )

        df, res = compute_factor_to_dataframe(
            cfg=cfg,
            task=task,
            compute_factor_fn=compute_factor_fn,
            logger=log,
        )

        # run_id 在這裡統一產生
        res.run_id = _build_run_id(cfg.run_id_prefix, task.factor_id, task.end_date)

        if cfg.dry_run:
            res.status = "dry_run" if res.status == "ok" else res.status
            results.append(res)
            continue

        if res.status != "ok" or df.empty:
            # 計算失敗或沒有資料 → 仍然寫 ledger，但不寫檔
            _append_factor_ledger(cfg, res)
            results.append(res)
            continue

        # 寫 parquet
        try:
            rows, written_paths = write_factor_parquet(
                df=df,
                factor_root=cfg.factor_root,
                factor_id=task.factor_id,
                run_id=res.run_id,
                date_column="date",
            )
            res.num_rows = int(rows)
            res.partitions_written = list(written_paths)
        except Exception as exc:  # noqa: BLE001
            log.exception("write_factor_parquet failed for %s: %s", task.factor_id, exc)
            res.status = "error"
            res.error_message = repr(exc)
            res.partitions_written = ()
            res.num_rows = 0

        # ledger
        _append_factor_ledger(cfg, res)
        results.append(res)

    finished_at = datetime.utcnow()
    return FactorBatchResult(
        root=root,
        config=cfg,
        tasks_count=len(list(tasks)) if not isinstance(tasks, list) else len(tasks),
        results=results,
        started_at=started_at,
        finished_at=finished_at,
    )


# ---------------------------------------------------------------------------
# 舊版入口：run_factor_engine（兼容 wrapper）
# ---------------------------------------------------------------------------


def run_factor_engine(
    cfg: FactorEngineConfig,
    logger: Optional[LoggerLike] = None,
) -> None:
    """
    Phase-2 因子引擎主流程（舊版入口），
    現在包一層轉成 FactorRunConfig + FactorTask，再呼叫 run_factor_batch。

    步驟：
      1) 解析路徑：factor_root / ledger / summary。
      2) 讀取因子定義（alpha_core.config.load_factor_definitions）。
      3) 依 factor_ids 組成 FactorTask 清單（包含日期區間）。
      4) 呼叫 run_factor_batch(...)。
      5) 寫入整批 summary JSON（factor_engine_summary.json）。
    """
    log = logger or _get_logger("alpha_core.factor_engine")

    # 1) 路徑與目錄
    root = cfg.root.resolve()
    factor_root, ledger_path, summary_path = _resolve_paths_from_engine_cfg(cfg)

    log.info("Factor engine started: root=%s", root)
    log.info("factor_root=%s", factor_root)
    log.info("ledger_path=%s", ledger_path)
    log.info("summary_path=%s", summary_path)
    log.info("impl_module=%s windows=%s dry_run=%s", cfg.impl_module, cfg.windows, cfg.dry_run)

    # 2) 讀因子定義
    defs = load_factor_definitions(root=root, rules_path=cfg.rules_path)
    if not defs:
        raise ConfigError("No factor definitions loaded from rules_factors.yaml.")

    selected_ids = _select_factor_ids(cfg, defs)
    log.info("Total factors available=%d, selected=%d", len(defs), len(selected_ids))

    # 3) 組 FactorTask 清單（同時記錄因為錯誤被跳過的因子結果）
    tasks: List[FactorTask] = []
    pre_results: List[FactorRunResult] = []

    for fid in selected_ids:
        spec = defs.get(fid)
        if spec is None:
            log.warning("Factor %s not found in definitions, skipped.", fid)
            pre_results.append(
                FactorRunResult(
                    factor_id=fid,
                    start_date=None,
                    end_date=cfg.end_date or date.today(),
                    status="skipped",
                    reason="definition_missing",
                )
            )
            continue

        try:
            start_date, end_date = _resolve_factor_dates(cfg, spec)
        except ConfigError as exc:
            log.error("Date range error for factor=%s: %s", fid, exc)
            pre_results.append(
                FactorRunResult(
                    factor_id=fid,
                    start_date=None,
                    end_date=cfg.end_date or date.today(),
                    status="skipped",
                    reason=f"date_error: {exc}",
                )
            )
            continue

        tasks.append(
            FactorTask(
                factor_id=fid,
                spec=spec,
                start_date=start_date,
                end_date=end_date,
            )
        )

    # 若沒有任何有效任務，仍要寫 summary（全部視為 skipped）
    if not tasks and not pre_results:
        finished_at = datetime.utcnow().isoformat()
        summary = {
            "root": str(root),
            "impl_module": cfg.impl_module,
            "rules_path": str(cfg.rules_path) if cfg.rules_path else None,
            "factor_root": str(factor_root),
            "ledger_path": str(ledger_path),
            "windows": list(cfg.windows),
            "run_id_prefix": cfg.run_id_prefix,
            "dry_run": cfg.dry_run,
            "started_at": finished_at,
            "finished_at": finished_at,
            "total_factors": 0,
            "counts": {"ok": 0, "dry_run": 0, "error": 0, "skipped": 0},
            "factors": [],
        }
        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=_json_default)
        log.info("Factor engine finished: no tasks to run.")
        return

    # 4) 建立 FactorRunConfig + 呼叫 run_factor_batch
    core_cfg = FactorRunConfig(
        root=root,
        factor_root=factor_root,
        ledger_path=ledger_path,
        impl_module=cfg.impl_module,
        dry_run=cfg.dry_run,
        max_workers=1,
        run_id_prefix=cfg.run_id_prefix,
        windows=cfg.windows,
        logger_name="alpha_core.factor_engine",
    )

    batch = run_factor_batch(core_cfg, tasks, logger=log)

    # 合併 pre_results（definition_missing / date_error）+ batch.results
    all_results: List[FactorRunResult] = []
    all_results.extend(pre_results)
    all_results.extend(batch.results)

    # 5) summary JSON（維持舊版結構，以避免下游報表壞掉）
    counts = {
        "ok": sum(1 for r in all_results if r.status == "ok"),
        "dry_run": sum(1 for r in all_results if r.status == "dry_run"),
        "error": sum(1 for r in all_results if r.status == "error"),
        "skipped": sum(1 for r in all_results if r.status == "skipped"),
    }

    summary_factors: List[Dict[str, Any]] = []
    for r in all_results:
        summary_factors.append(
            {
                "factor_id": r.factor_id,
                "run_id": r.run_id,
                "status": r.status,
                "reason": r.reason or r.error_message,
                "start_date": r.start_date.isoformat() if r.start_date else None,
                "end_date": r.end_date.isoformat(),
                "rows": r.num_rows,
                "files": [str(p) for p in r.partitions_written],
            }
        )

    summary: Dict[str, Any] = {
        "root": str(root),
        "impl_module": cfg.impl_module,
        "rules_path": str(cfg.rules_path) if cfg.rules_path else None,
        "factor_root": str(factor_root),
        "ledger_path": str(ledger_path),
        "windows": list(cfg.windows),
        "run_id_prefix": cfg.run_id_prefix,
        "dry_run": cfg.dry_run,
        "started_at": batch.started_at.isoformat(),
        "finished_at": batch.finished_at.isoformat(),
        "total_factors": len(all_results),
        "counts": counts,
        "factors": summary_factors,
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=_json_default)

    logging.getLogger("alpha_core.factor_engine").info(
        "Factor engine finished: total=%d ok=%d error=%d skipped=%d",
        summary["total_factors"],
        summary["counts"]["ok"],
        summary["counts"]["error"],
        summary["counts"]["skipped"],
    )
