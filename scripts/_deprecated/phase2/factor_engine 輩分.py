from __future__ import annotations

"""Phase-2 factor engine: compute factor parquet from silver data.

設計重點：
- deterministic, idempotent：同一組 (factor_id, start, end, run_id_prefix) → 相同 run_id，可重跑覆寫。
- 不寫死路徑：root / rules / impl-module 都從 CLI 傳入。
- 專心做 orchestrate：呼叫 compute_factor() → 驗證 schema → 依 yyyymm 分區寫 parquet → 寫 factor_ledger。
- 進階強化：
    - factor_root / ledger_path / summary_path 皆可由外部覆寫，方便測試或多環境。
    - windows（例如 (6, 12, 24) 月）集中於 config 中傳入，需要時才轉交給 compute_factor。
    - 每次執行產出 summary JSON，提供 Gate / SLO 工具消費。
期待 contract：
- 實作模組（--impl-module）推薦提供：
    compute_factor(root: Path,
                   factor_id: str,
                   spec: dict | None,
                   start_date: date | None,
                   end_date: date | None,
                   windows: tuple[int, ...] | None = None,
                   logger: logging.Logger | None = None) -> pandas.DataFrame
 但為了向後相容，本 engine 會檢查函式簽章，如果沒有 windows/logger 參數會自動略過。
"""

import argparse
import importlib
import inspect
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Config / 型別定義
# ---------------------------------------------------------------------------


@dataclass
class FactorEngineConfig:
    """Top-level configuration for the factor engine run."""

    # 基本環境
    root: Path
    impl_module: str
    rules_path: Optional[Path]
    factor_ids: List[str]
    start_date: Optional[date]
    end_date: Optional[date]
    run_id_prefix: str = "factor"

    # 執行控制
    dry_run: bool = False
    max_factors: Optional[int] = None

    # 可覆寫路徑
    factor_root: Optional[Path] = None
    ledger_path: Optional[Path] = None
    summary_path: Optional[Path] = None

    # 因子計算相關
    windows: Tuple[int, ...] = field(default_factory=lambda: (6, 12, 24))
    date_column: str = "date"
    stock_id_column: str = "stock_id"
    value_column: str = "factor_value"

    def __post_init__(self) -> None:
        # 預設 factor_root / ledger_path / summary_path
        if self.factor_root is None:
            self.factor_root = self.root / "datahub" / "silver" / "alpha" / "factor"
        if self.ledger_path is None:
            self.ledger_path = self.root / "metrics" / "factor_ledger.jsonl"
        if self.summary_path is None:
            self.summary_path = self.root / "reports" / "factor_engine_summary.json"
        # 日期區間可為 None；若皆不為 None 時，要求 end_date > start_date（半開）
        if self.start_date and self.end_date and self.end_date <= self.start_date:
            raise ValueError(
                "end_date must be greater than start_date (half-open [start, end))"
            )


# ---------------------------------------------------------------------------
# 小工具：日期、registry、impl module
# ---------------------------------------------------------------------------


def _parse_date(s: Optional[str]) -> Optional[date]:
    """Parse YYYY-MM-DD into date; None stays None."""
    if not s:
        return None
    year, month, day = map(int, s.split("-"))
    return date(year, month, day)


def _load_registry_factor_defs(
    root: Path,
    rules_path: Optional[Path],
    logger: logging.Logger,
) -> Mapping[str, Mapping[str, Any]]:
    """從 factor_registry 載入因子定義，回傳 factor_id -> spec。

    預期 factor_registry.py 提供：
        load_factor_registry(root: Path, rules_path: Optional[Path]) -> registry
        registry.list_factor_ids() -> list[str]
        registry.get_factor_spec(factor_id: str) -> dict-like

    若載入失敗，回傳空 dict，engine 仍可運作（只是不帶 spec 給 compute_factor）。
    """
    registry_module = None

    # 1) 優先 scripts 底下的 factor_registry（新的 Phase-2 版本）
    try:
        import factor_registry as fr  # type: ignore

        registry_module = fr
        logger.debug("Loaded factor_registry from scripts package")
    except ImportError:
        # 2) 再退回 tools.factors.factor_registry（舊版仍可用）
        try:
            from tools.factors import factor_registry as fr  # type: ignore

            registry_module = fr
            logger.debug("Loaded factor_registry from tools.factors")
        except ImportError:
            registry_module = None

    defs: Dict[str, Mapping[str, Any]] = {}

    if registry_module is not None and hasattr(registry_module, "load_factor_registry"):
        try:
            registry = registry_module.load_factor_registry(
                root=root, rules_path=rules_path
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("load_factor_registry failed: %s", exc)
            return {}

        if hasattr(registry, "list_factor_ids") and hasattr(
            registry, "get_factor_spec"
        ):
            try:
                fids = list(registry.list_factor_ids())
            except Exception as exc:  # noqa: BLE001
                logger.warning("registry.list_factor_ids() failed: %s", exc)
                fids = []
            for fid in fids:
                try:
                    defs[str(fid)] = registry.get_factor_spec(fid)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "registry.get_factor_spec(%s) failed: %s", fid, exc
                    )
            logger.info("Loaded %d factor definitions from registry", len(defs))
        else:
            logger.warning(
                "Registry object missing list_factor_ids/get_factor_spec; "
                "registry specs will not be used."
            )
    else:
        if rules_path is not None:
            logger.info(
                "No factor_registry module found; rules_factors.yaml will only be used "
                "indirectly by other tools."
            )
        else:
            logger.info("Registry integration disabled (no module, no rules path).")

    return defs


def _resolve_factor_ids(
    cfg: FactorEngineConfig,
    registry_defs: Mapping[str, Mapping[str, Any]],
    logger: logging.Logger,
) -> List[str]:
    """決定這次要跑哪些 factor_id。

    優先順序：
    1) CLI 指定的 factor_ids（--factors）
    2) 若未指定，使用 registry 的全部（排序後）
    3) 若兩者皆空 → 視為配置錯誤
    """
    if cfg.factor_ids:
        requested = list(dict.fromkeys(cfg.factor_ids))  # 去重、保留順序
        if registry_defs:
            unknown = [f for f in requested if f not in registry_defs]
            if unknown:
                logger.warning(
                    "Requested factor_ids not present in registry: %s",
                    ", ".join(unknown),
                )
        factor_ids = requested
    else:
        if registry_defs:
            factor_ids = sorted(registry_defs.keys())
            logger.info("Using all %d factors from registry", len(factor_ids))
        else:
            raise RuntimeError(
                "No factor_ids specified and registry is empty; "
                "specify --factors or provide factor_registry/rules."
            )

    if cfg.max_factors is not None and cfg.max_factors >= 0:
        if cfg.max_factors == 0:
            logger.info("max_factors=0: nothing to do")
            return []
        if len(factor_ids) > cfg.max_factors:
            logger.info(
                "Limiting factors by max_factors=%d (original=%d)",
                cfg.max_factors,
                len(factor_ids),
            )
            factor_ids = factor_ids[: cfg.max_factors]

    return factor_ids


def _import_impl_module(name: str, logger: logging.Logger):
    """Import the implementation module which supplies compute_factor()."""
    try:
        module = importlib.import_module(name)
        logger.info("Using factor implementation module: %s", name)
        return module
    except ImportError as exc:  # noqa: BLE001
        raise RuntimeError(f"Cannot import impl_module {name!r}: {exc}") from exc


def _get_compute_func(module):
    """取得 compute_factor 函式，並做基本檢查。"""
    func = getattr(module, "compute_factor", None)
    if func is None or not callable(func):
        raise RuntimeError(
            "Implementation module must define callable "
            "compute_factor(root, factor_id, spec, start_date, end_date, ...)"
        )
    return func


def _build_run_id(cfg: FactorEngineConfig, factor_id: str) -> str:
    """建一個 deterministic 的 run_id。

    格式：
        <prefix>-<factor_id>-<start or none>-<end or none>
    """
    parts = [
        cfg.run_id_prefix,
        factor_id,
        cfg.start_date.isoformat() if cfg.start_date else "none",
        cfg.end_date.isoformat() if cfg.end_date else "none",
    ]
    return "-".join(parts)


# ---------------------------------------------------------------------------
# DataFrame 驗證與寫入
# ---------------------------------------------------------------------------


def _validate_factor_frame(
    df: pd.DataFrame,
    cfg: FactorEngineConfig,
    factor_id: str,
) -> pd.DataFrame:
    """驗證 compute_factor 回傳的 DataFrame。

    規則：
    - 必須包含 date / stock_id / factor_value 三個欄位。
    - date 轉成 Timestamp、stock_id 轉成字串。
    - 套用 [start, end) 半開區間（若 config 有設定）。
    - 去掉缺失值，依 (date, stock_id) 穩定排序。
    """
    if df is None:
        raise ValueError(f"factor {factor_id}: compute_factor returned None")
    if df.empty:
        return df

    required = {cfg.date_column, cfg.stock_id_column, cfg.value_column}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"factor {factor_id}: compute_factor missing required columns: {missing}"
        )

    df = df.copy()
    df[cfg.date_column] = pd.to_datetime(df[cfg.date_column])
    df[cfg.stock_id_column] = df[cfg.stock_id_column].astype(str)

    if cfg.start_date is not None:
        df = df[df[cfg.date_column] >= pd.Timestamp(cfg.start_date)]
    if cfg.end_date is not None:
        df = df[df[cfg.date_column] < pd.Timestamp(cfg.end_date)]

    df = df.dropna(subset=[cfg.date_column, cfg.stock_id_column])

    df = df.sort_values(
        by=[cfg.date_column, cfg.stock_id_column],
        ascending=[True, True],
        kind="mergesort",  # 穩定排序，方便重現
    ).reset_index(drop=True)

    return df


def _write_factor_parquet(
    cfg: FactorEngineConfig,
    factor_id: str,
    df: pd.DataFrame,
    run_id: str,
    logger: logging.Logger,
) -> int:
    """依 yyyymm 分區寫入 factor parquet，回傳寫入的分區數。"""
    if df.empty:
        logger.info("factor %s: no rows to write", factor_id)
        return 0

    # 依 config.factor_root 為根目錄
    factor_root = cfg.factor_root or (
        cfg.root / "datahub" / "silver" / "alpha" / "factor"
    )
    df = df.copy()
    df["yyyymm"] = df[cfg.date_column].dt.strftime("%Y%m")

    partitions_written = 0
    for yyyymm, part in df.groupby("yyyymm", sort=True):
        part_dir = factor_root / factor_id / f"yyyymm={yyyymm}"
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / f"{run_id}.parquet"
        logger.info(
            "factor %s: writing %d rows to %s",
            factor_id,
            len(part),
            file_path,
        )
        part.drop(columns=["yyyymm"]).to_parquet(file_path, index=False)
        partitions_written += 1

    return partitions_written


def _append_ledger(
    cfg: FactorEngineConfig,
    factor_id: str,
    run_id: str,
    row_count: int,
    partition_count: int,
    status: str,
    logger: logging.Logger,
    error_message: Optional[str] = None,
) -> None:
    """在 metrics/factor_ledger.jsonl 附加一筆 JSON line。"""
    ledger_path = cfg.ledger_path or (cfg.root / "metrics" / "factor_ledger.jsonl")
    ledger_path.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "run_id": run_id,
        "factor_id": factor_id,
        "start_date": cfg.start_date.isoformat() if cfg.start_date else None,
        "end_date": cfg.end_date.isoformat() if cfg.end_date else None,
        "row_count": row_count,
        "partition_count": partition_count,
        "status": status,  # ok / empty / error
        "dry_run": cfg.dry_run,
    }
    if error_message:
        entry["error"] = error_message

    line = json.dumps(entry, ensure_ascii=False, sort_keys=True)
    with ledger_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    logger.info(
        "factor %s: ledger appended (status=%s, rows=%d, partitions=%d)",
        factor_id,
        status,
        row_count,
        partition_count,
    )


def _write_summary(
    cfg: FactorEngineConfig,
    results: List[Dict[str, Any]],
    logger: logging.Logger,
) -> None:
    """將本次執行摘要寫入 JSON，供 Gate / SLO 工具食用。"""
    if not results:
        logger.info("No factor results to summarise; skip summary output.")
        return

    summary_path = cfg.summary_path or (
        cfg.root / "reports" / "factor_engine_summary.json"
    )
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {"ok": 0, "empty": 0, "error": 0}
    for r in results:
        status = r.get("status")
        if status in stats:
            stats[status] += 1

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "root": str(cfg.root),
        "start_date": cfg.start_date.isoformat() if cfg.start_date else None,
        "end_date": cfg.end_date.isoformat() if cfg.end_date else None,
        "windows": list(cfg.windows),
        "dry_run": cfg.dry_run,
        "factors": results,
        "stats": stats,
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)

    logger.info(
        "factor_engine summary written to %s (ok=%d, empty=%d, error=%d)",
        summary_path,
        stats["ok"],
        stats["empty"],
        stats["error"],
    )


# ---------------------------------------------------------------------------
# 核心入口：跑一輪因子引擎
# ---------------------------------------------------------------------------


def run_factor_engine(
    cfg: FactorEngineConfig,
    logger: Optional[logging.Logger] = None,
) -> None:
    """High-level entrypoint：給 Run-Phase2-OneClick 或其他 orchestrator 呼叫。"""
    log = logger or logging.getLogger(__name__)

    registry_defs = _load_registry_factor_defs(cfg.root, cfg.rules_path, log)
    factor_ids = _resolve_factor_ids(cfg, registry_defs, log)
    if not factor_ids:
        log.info("No factors to run (resolved factor_ids empty).")
        _write_summary(cfg, [], log)
        return

    impl_module = _import_impl_module(cfg.impl_module, log)
    compute_factor = _get_compute_func(impl_module)
    compute_sig = inspect.signature(compute_factor)

    if cfg.dry_run:
        log.info(
            "Dry-run mode enabled: parquet / ledger will still be written = %s",
            False,
        )

    results: List[Dict[str, Any]] = []
    ok_count = 0
    error_count = 0

    for fid in factor_ids:
        log.info("=== factor %s: start ===", fid)
        run_id = _build_run_id(cfg, fid)
        spec = registry_defs.get(fid) if registry_defs else None

        # 準備 compute_factor 參數，依照簽章決定是否傳 windows / logger
        kwargs: Dict[str, Any] = {
            "root": cfg.root,
            "factor_id": fid,
            "spec": spec,
            "start_date": cfg.start_date,
            "end_date": cfg.end_date,
        }
        if "windows" in compute_sig.parameters:
            kwargs["windows"] = cfg.windows
        if "logger" in compute_sig.parameters:
            kwargs["logger"] = log

        factor_result: Dict[str, Any] = {
            "factor_id": fid,
            "run_id": run_id,
            "status": "unknown",
            "rows": 0,
            "partitions": 0,
        }

        try:
            df = compute_factor(**kwargs)
            df = _validate_factor_frame(df, cfg, fid)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            log.exception("factor %s: compute/validate failed: %s", fid, msg)
            if not cfg.dry_run:
                _append_ledger(
                    cfg,
                    fid,
                    run_id,
                    row_count=0,
                    partition_count=0,
                    status="error",
                    logger=log,
                    error_message=msg,
                )
            factor_result["status"] = "error"
            factor_result["error"] = msg
            results.append(factor_result)
            error_count += 1
            log.info("=== factor %s: done (error) ===", fid)
            continue

        row_count = int(len(df))
        factor_result["rows"] = row_count

        if row_count == 0:
            log.warning("factor %s: no data produced after validation", fid)
            if not cfg.dry_run:
                _append_ledger(
                    cfg,
                    fid,
                    run_id,
                    row_count=0,
                    partition_count=0,
                    status="empty",
                    logger=log,
                )
            factor_result["status"] = "empty"
            results.append(factor_result)
            log.info("=== factor %s: done (empty) ===", fid)
            continue

        partitions = 0
        if not cfg.dry_run:
            partitions = _write_factor_parquet(cfg, fid, df, run_id, log)
            _append_ledger(
                cfg,
                fid,
                run_id,
                row_count=row_count,
                partition_count=partitions,
                status="ok",
                logger=log,
            )

        factor_result["status"] = "ok"
        factor_result["partitions"] = partitions
        results.append(factor_result)
        ok_count += 1
        log.info(
            "=== factor %s: done (rows=%d, partitions=%d) ===",
            fid,
            row_count,
            partitions,
        )

    _write_summary(cfg, results, log)

    log.info(
        "factor_engine finished: ok=%d, error=%d, total=%d",
        ok_count,
        error_count,
        len(factor_ids),
    )
    if error_count:
        raise RuntimeError(f"{error_count} factor(s) failed out of {len(factor_ids)}")


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor engine: compute factor parquet from silver data.",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root directory (default: current directory).",
    )
    parser.add_argument(
        "--impl-module",
        type=str,
        default="factor_impl",
        help=(
            "Python module that implements compute_factor(root, factor_id, spec, "
            "start_date, end_date, ...) -> pandas.DataFrame. "
            "Example: tools.factors.eval.factor_impl"
        ),
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        help="Path to rules_factors.yaml (optional, used by factor_registry).",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help=(
            "Comma-separated list of factor_ids to run. "
            "If omitted, engine will use all factors from registry."
        ),
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD, inclusive). If omitted, engine decides.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD, exclusive). If omitted, engine decides.",
    )
    parser.add_argument(
        "--run-id-prefix",
        type=str,
        default="factor",
        help="Prefix for run_id (default: factor).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and validate only; do not write parquet or ledger.",
    )
    parser.add_argument(
        "--max-factors",
        type=int,
        default=None,
        help="Maximum number of factors to run in this execution (default: no limit).",
    )
    parser.add_argument(
        "--factor-root",
        type=str,
        default=None,
        help=(
            "Optional override for factor parquet root directory. "
            "Default: <root>/datahub/silver/alpha/factor"
        ),
    )
    parser.add_argument(
        "--ledger-path",
        type=str,
        default=None,
        help=(
            "Optional override for factor ledger path. "
            "Default: <root>/metrics/factor_ledger.jsonl"
        ),
    )
    parser.add_argument(
        "--summary-path",
        type=str,
        default=None,
        help=(
            "Optional override for summary JSON path. "
            "Default: <root>/reports/factor_engine_summary.json"
        ),
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="6,12,24",
        help=(
            "Comma-separated list of integer windows (e.g. '6,12,24'). "
            "Passed to compute_factor if it accepts a 'windows' parameter."
        ),
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


def _configure_logging(level_name: str) -> logging.Logger:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger("factor_engine")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logger = _configure_logging(args.log_level)

    root = Path(args.root).resolve()
    rules_path = Path(args.rules).resolve() if args.rules else None
    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)

    factor_ids: List[str] = []
    if args.factors:
        factor_ids = [s.strip() for s in args.factors.split(",") if s.strip()]

    factor_root = Path(args.factor_root).resolve() if args.factor_root else None
    ledger_path = Path(args.ledger_path).resolve() if args.ledger_path else None
    summary_path = Path(args.summary_path).resolve() if args.summary_path else None

    # windows: 允許空字串；預設 "6,12,24"
    windows: Tuple[int, ...] = ()
    if args.windows:
        windows = tuple(
            int(x.strip()) for x in str(args.windows).split(",") if x.strip()
        )
    if not windows:
        windows = (6, 12, 24)

    cfg = FactorEngineConfig(
        root=root,
        impl_module=args.impl_module,
        rules_path=rules_path,
        factor_ids=factor_ids,
        start_date=start_date,
        end_date=end_date,
        run_id_prefix=args.run_id_prefix,
        dry_run=bool(args.dry_run),
        max_factors=args.max_factors,
        factor_root=factor_root,
        ledger_path=ledger_path,
        summary_path=summary_path,
        windows=windows,
    )

    try:
        run_factor_engine(cfg, logger=logger)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Factor engine failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
