#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/p2/factor_status.py

Phase-2 factor layer status checker (integrated version).

設計目標：
- deterministic、idempotent：只讀狀態，不改檔案。
- schema-first：明確定義輸出欄位與 required_action / desired_action 規則。
- 整合三個來源：
  1) factor parquet 目錄：實際資料 coverage（min/max YYYYMM, months_covered）。
  2) factor_registry / rules_factors.yaml：應存在的因子集合。
  3) factor_eval / wf_summary.json：WF 評估與 Gate 視角的因子集合。

輸出重點欄位：
- factor_id
- min_yyyymm / max_yyyymm / months_covered
- is_fresh / has_window
- in_registry / has_data / has_eval / in_wf_summary
- required_action: one of {ok, missing, rebuild, orphan_data, unknown}
- desired_action: one of {compute+eval, eval_only, skip} or null（保留給策略層覆寫用）
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Dict, Any

import pandas as pd


# ---------------------------------------------------------------------------
# 資料結構
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FactorStatus:
    """Integrated status for a single factor_id."""

    factor_id: str

    # Partition-level coverage（來自檔案系統）
    min_yyyymm: Optional[str]
    max_yyyymm: Optional[str]
    approx_min_date: Optional[date]
    approx_max_date: Optional[date]
    partition_count: int
    file_count: int
    months_covered: Optional[int]
    is_fresh: Optional[bool]
    has_window: Optional[bool]

    # Integration flags
    in_registry: Optional[bool]
    has_data: bool
    has_eval: Optional[bool]
    in_wf_summary: Optional[bool]

    # Orchestrator / Gate 可直接使用的動作建議
    #   - ok:           registry 有、資料與評估與 WF 都到位且新鮮
    #   - missing:      registry 有，但完全沒有資料
    #   - rebuild:      registry 有，但資料過舊 / 視窗不足 / 評估或 WF 狀態不足
    #   - orphan_data:  registry 無，但磁碟上有因子資料
    #   - unknown:      以上皆不屬（例如沒有 registry 也沒有資料）
    required_action: str

    # 策略意圖層，可由其他工具依 profile/engine_kind 填入
    #   - compute+eval / eval_only / skip
    #   - None 表示尚未指定，由 factor_plan 決定預設策略
    desired_action: Optional[str] = field(default=None)


# ---------------------------------------------------------------------------
# 小工具：月份與日期轉換
# ---------------------------------------------------------------------------


def _parse_yyyymm(s: str) -> date:
    """Parse YYYYMM into date (first day of that month)."""
    if len(s) != 6 or not s.isdigit():
        raise ValueError(f"invalid yyyymm string: {s!r}")
    year = int(s[:4])
    month = int(s[4:6])
    return date(year, month, 1)


def _yyyymm_from_date(d: date) -> str:
    """Format date as YYYYMM."""
    return f"{d.year:04d}{d.month:02d}"


def _add_months(d: date, months: int) -> date:
    """Add months to a date, forcing day=1 for simplicity."""
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def _months_diff(start_yyyymm: str, end_yyyymm: str) -> int:
    """Inclusive month diff between two YYYYMM strings."""
    s = _parse_yyyymm(start_yyyymm)
    e = _parse_yyyymm(end_yyyymm)
    return (e.year - s.year) * 12 + (e.month - s.month) + 1


# ---------------------------------------------------------------------------
# 來源 1：factor parquet 目錄
# ---------------------------------------------------------------------------


def _iter_factor_ids_from_fs(factor_root: Path) -> Iterable[str]:
    """Yield factor_id from factor root (direct child dirs only)."""
    if not factor_root.exists():
        return []
    for child in sorted(factor_root.iterdir()):
        if not child.is_dir():
            continue
        name = child.name
        if not name or name.startswith(".") or name.startswith("_"):
            continue
        yield name


def _scan_single_factor_dir(
    factor_dir: Path,
    expect_date: Optional[date],
    window_months: Optional[int],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Inspect a single factor directory and return partition-level info.

    Directory layout:
        factor_dir / yyyymm=YYYYMM / *.parquet

    不讀 parquet 內容，只用目錄與檔名推估 coverage。
    若 factor_dir 不存在，回傳零 coverage（has_data=False）。
    """
    if not factor_dir.exists():
        # 完全沒有資料
        return {
            "min_yyyymm": None,
            "max_yyyymm": None,
            "approx_min_date": None,
            "approx_max_date": None,
            "partition_count": 0,
            "file_count": 0,
            "months_covered": None,
            "is_fresh": None,
            "has_window": None,
        }

    yyyymm_dirs: List[Path] = []
    for p in sorted(factor_dir.glob("yyyymm=*")):
        if p.is_dir():
            yyyymm_dirs.append(p)

    partition_count = len(yyyymm_dirs)
    file_count = 0

    min_yyyymm: Optional[str] = None
    max_yyyymm: Optional[str] = None

    if partition_count > 0:
        yyyymms: List[str] = []
        for d in yyyymm_dirs:
            name = d.name
            _, _, val = name.partition("=")
            val = val.strip()
            if not val:
                logger.warning("invalid partition dir name (no value): %s", d)
                continue
            try:
                _ = _parse_yyyymm(val)
            except ValueError:
                logger.warning("invalid partition dir name (bad YYYYMM): %s", d)
                continue
            yyyymms.append(val)

            part_files = list(d.glob("*.parquet"))
            file_count += len(part_files)

        if yyyymms:
            min_yyyymm = min(yyyymms)
            max_yyyymm = max(yyyymms)
        else:
            partition_count = 0

    if partition_count == 0:
        # Fallback: flat parquet under factor_dir（不分月份子目錄）
        flat_parquet_files = sorted(factor_dir.glob("*.parquet"))
        file_count += len(flat_parquet_files)
        if flat_parquet_files:
            logger.warning(
                "factor dir %s has parquet files but no yyyymm partitions; "
                "date coverage cannot be inferred precisely",
                factor_dir,
            )

    approx_min_date: Optional[date] = None
    approx_max_date: Optional[date] = None
    months_covered: Optional[int] = None

    if min_yyyymm is not None and max_yyyymm is not None:
        approx_min_date = _parse_yyyymm(min_yyyymm)
        approx_max_date = _parse_yyyymm(max_yyyymm)
        months_covered = _months_diff(min_yyyymm, max_yyyymm)

    is_fresh: Optional[bool] = None
    has_window: Optional[bool] = None

    if expect_date is not None and max_yyyymm is not None:
        expect_yyyymm = _yyyymm_from_date(expect_date)
        is_fresh = max_yyyymm >= expect_yyyymm

    if (
        expect_date is not None
        and window_months is not None
        and min_yyyymm is not None
    ):
        window_start_date = _add_months(expect_date.replace(day=1), -window_months)
        window_start_yyyymm = _yyyymm_from_date(window_start_date)
        has_window = min_yyyymm <= window_start_yyyymm
    else:
        has_window = None

    return {
        "min_yyyymm": min_yyyymm,
        "max_yyyymm": max_yyyymm,
        "approx_min_date": approx_min_date,
        "approx_max_date": approx_max_date,
        "partition_count": partition_count,
        "file_count": file_count,
        "months_covered": months_covered,
        "is_fresh": is_fresh,
        "has_window": has_window,
    }


# ---------------------------------------------------------------------------
# 來源 2：factor_registry / rules_factors.yaml
# ---------------------------------------------------------------------------


def _load_registry_factor_ids(
    root: Path,
    rules_path: Optional[Path],
    logger: logging.Logger,
    strict: bool,
) -> Set[str]:
    """
    從 factor_registry 模組（優先）或 rules_factors.yaml（次要）載入 factor_id 集合。

    預期 contract（建議實作）：
      tools.factors.factor_registry.load_factor_registry(root: Path, rules_path: Optional[Path]) -> registry
      registry.list_factor_ids() -> list[str]

    若 strict=True 且無法解析任何 factor_id，會 raise RuntimeError。
    """
    factor_ids: Set[str] = set()

    # 1) 優先嘗試 factor_registry 模組
    registry_module = None
    try:
        from tools.factors import factor_registry as fr  # type: ignore
        registry_module = fr
        logger.debug("Loaded tools.factors.factor_registry")
    except ImportError:
        try:
            import factor_registry as fr  # type: ignore
            registry_module = fr
            logger.debug("Loaded factor_registry")
        except ImportError:
            registry_module = None

    if registry_module is not None:
        if hasattr(registry_module, "load_factor_registry"):
            try:
                registry = registry_module.load_factor_registry(
                    root=root, rules_path=rules_path
                )
                if hasattr(registry, "list_factor_ids"):
                    factor_ids = set(registry.list_factor_ids())
                elif hasattr(registry, "factor_ids"):
                    factor_ids = set(getattr(registry, "factor_ids"))
                else:
                    logger.warning(
                        "Registry object has no list_factor_ids() or .factor_ids; "
                        "registry integration will be skipped."
                    )
            except Exception as exc:  # noqa: BLE001
                msg = f"Failed to load factor registry via factor_registry module: {exc}"
                if strict:
                    raise RuntimeError(msg) from exc
                logger.warning("%s", msg)
        else:
            logger.warning(
                "factor_registry module has no load_factor_registry(); "
                "registry integration will be skipped."
            )

    # 2) 若 module 無法提供，且有 rules_path，可嘗試直接 parse YAML
    if not factor_ids and rules_path is not None:
        try:
            import yaml  # type: ignore

            if not rules_path.exists():
                msg = f"rules file not found: {rules_path}"
                if strict:
                    raise RuntimeError(msg)
                logger.warning("%s", msg)
            else:
                with rules_path.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)

                # 嘗試從常見結構抽 factor_id
                candidates: Set[str] = set()

                if isinstance(data, dict):
                    # 形式一：{"factors": [{"id": "...", ...}, ...]}
                    factors = data.get("factors")
                    if isinstance(factors, list):
                        for item in factors:
                            if isinstance(item, dict):
                                fid = (
                                    item.get("id")
                                    or item.get("factor_id")
                                    or item.get("name")
                                )
                                if isinstance(fid, str):
                                    candidates.add(fid)

                factor_ids = candidates
                if factor_ids:
                    logger.info(
                        "Loaded %d factor ids from YAML rules: %s",
                        len(factor_ids),
                        rules_path,
                    )
                else:
                    msg = (
                        "rules_factors.yaml parsed but no factor ids detected; "
                        "check schema or adjust parser."
                    )
                    if strict:
                        raise RuntimeError(msg)
                    logger.warning("%s", msg)
        except ImportError as exc:
            msg = (
                "PyYAML is not installed; cannot parse rules_factors.yaml. "
                "Install pyyaml or provide factor_registry module."
            )
            if strict:
                raise RuntimeError(msg) from exc
            logger.warning("%s", msg)
        except Exception as exc:  # noqa: BLE001
            msg = f"Failed to load factor ids from YAML rules: {exc}"
            if strict:
                raise RuntimeError(msg) from exc
            logger.warning("%s", msg)

    if strict and not factor_ids and rules_path is not None:
        raise RuntimeError(
            "Registry integration requested (--rules) but no factor ids could be loaded."
        )

    if factor_ids:
        logger.info("Registry factor count: %d", len(factor_ids))
    else:
        logger.info("Registry integration disabled or empty.")

    return factor_ids


# ---------------------------------------------------------------------------
# 來源 3：factor_eval / wf_summary.json
# ---------------------------------------------------------------------------


def _load_eval_factor_ids(eval_dir: Path, logger: logging.Logger) -> Set[str]:
    """
    讀取 reports/factor_eval/*.json，蒐集 factor_id 集合。

    預期 JSON 至少包含一個 top-level "factor_id" 欄位。
    若沒有，就退回用檔名 stem 當 factor_id。
    """
    factor_ids: Set[str] = set()
    if not eval_dir.exists():
        logger.info("Eval dir not found (skip eval integration): %s", eval_dir)
        return factor_ids

    json_files = sorted(eval_dir.glob("*.json"))
    if not json_files:
        logger.info("No eval JSON files found under %s", eval_dir)
        return factor_ids

    for p in json_files:
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            fid = data.get("factor_id")
            if not isinstance(fid, str) or not fid:
                fid = p.stem
            factor_ids.add(fid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse eval JSON %s: %s", p, exc)

    logger.info("Eval factor count: %d", len(factor_ids))
    return factor_ids


def _load_wf_summary_factor_ids(wf_path: Path, logger: logging.Logger) -> Set[str]:
    """
    讀取 reports/wf_summary.json 裡 factors 區段的 factor_id 集合。

    預期結構：
      {
        "factors": {
          "<factor_id>": { ... },
          ...
        },
        ...
      }
    """
    factor_ids: Set[str] = set()
    if not wf_path.exists():
        logger.info("WF summary not found (skip WF integration): %s", wf_path)
        return factor_ids

    try:
        with wf_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        factors = data.get("factors")
        if isinstance(factors, dict):
            factor_ids = set(map(str, factors.keys()))
            logger.info("WF summary factor count: %d", len(factor_ids))
        else:
            logger.warning(
                "wf_summary.json has no 'factors' dict; WF integration disabled."
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to parse wf_summary.json: %s", exc)

    return factor_ids


# ---------------------------------------------------------------------------
# required_action 決策邏輯
# ---------------------------------------------------------------------------


def _decide_required_action(
    in_registry: Optional[bool],
    has_data: bool,
    is_fresh: Optional[bool],
    has_window: Optional[bool],
    has_eval: Optional[bool],
    in_wf_summary: Optional[bool],
) -> str:
    """
    根據整合後旗標，產生 orchestrator 可直接使用的 required_action。

    規則：
      - 若 in_registry=True:
          - 沒有任何資料 → "missing"
          - 有資料但 freshness / window / eval / WF 任一不滿足 → "rebuild"
          - 其餘 → "ok"
      - 若 in_registry=False 或 None:
          - 有資料 → "orphan_data"
          - 無資料 → "unknown"
    """
    if in_registry:
        if not has_data:
            return "missing"
        # 任一條件不滿足 → rebuild
        bad_fresh = is_fresh is False
        bad_window = has_window is False
        bad_eval = has_eval is False
        bad_wf = in_wf_summary is False
        if bad_fresh or bad_window or bad_eval or bad_wf:
            return "rebuild"
        return "ok"

    # 沒在 registry
    if has_data:
        return "orphan_data"
    return "unknown"


# ---------------------------------------------------------------------------
# 核心 API：整合三個來源（低階）
# ---------------------------------------------------------------------------


def scan_factor_status(
    root: Path,
    expect_date: Optional[date] = None,
    window_months: Optional[int] = None,
    registry_factor_ids: Optional[Set[str]] = None,
    eval_factor_ids: Optional[Set[str]] = None,
    wf_factor_ids: Optional[Set[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[FactorStatus]:
    """
    Scan factor layer and return integrated status list.

    - root: repo root（例如 C:\\AI\\tw-alpha-stack）
    - expect_date: as-of date，用來判斷 freshness
    - window_months: 需要覆蓋的最小月數窗口（例如 24）
    - registry_factor_ids: 來自 registry 的因子集合（可為 None）
    - eval_factor_ids: 來自 factor_eval 的因子集合（可為 None）
    - wf_factor_ids: 來自 wf_summary 的因子集合（可為 None）
    """
    log = logger or logging.getLogger(__name__)
    factor_root = root / "datahub" / "silver" / "alpha" / "factor"

    # Fs, registry, eval, WF 四個來源取 union
    fs_factor_ids = set(_iter_factor_ids_from_fs(factor_root))
    reg_ids = registry_factor_ids or set()
    eval_ids = eval_factor_ids or set()
    wf_ids = wf_factor_ids or set()

    all_ids: Set[str] = fs_factor_ids | reg_ids | eval_ids | wf_ids

    if not all_ids:
        log.warning("No factor ids found from FS / registry / eval / WF.")
        return []

    statuses: List[FactorStatus] = []

    for fid in sorted(all_ids):
        factor_dir = factor_root / fid
        info = _scan_single_factor_dir(
            factor_dir=factor_dir,
            expect_date=expect_date,
            window_months=window_months,
            logger=log,
        )

        has_data = bool(info["partition_count"] > 0 or info["file_count"] > 0)
        in_registry = None if not registry_factor_ids else (fid in reg_ids)
        has_eval = None if not eval_factor_ids else (fid in eval_ids)
        in_wf = None if not wf_factor_ids else (fid in wf_ids)

        required_action = _decide_required_action(
            in_registry=in_registry,
            has_data=has_data,
            is_fresh=info["is_fresh"],
            has_window=info["has_window"],
            has_eval=has_eval,
            in_wf_summary=in_wf,
        )

        status = FactorStatus(
            factor_id=fid,
            min_yyyymm=info["min_yyyymm"],
            max_yyyymm=info["max_yyyymm"],
            approx_min_date=info["approx_min_date"],
            approx_max_date=info["approx_max_date"],
            partition_count=info["partition_count"],
            file_count=info["file_count"],
            months_covered=info["months_covered"],
            is_fresh=info["is_fresh"],
            has_window=info["has_window"],
            in_registry=in_registry,
            has_data=has_data,
            has_eval=has_eval,
            in_wf_summary=in_wf,
            required_action=required_action,
        )
        statuses.append(status)

    return statuses


# ---------------------------------------------------------------------------
# 高階 API：組合 payload（給 factor_plan / OneClick 用）
# ---------------------------------------------------------------------------


def build_factor_status_payload(
    root: Path,
    as_of_date: str,
    profile: str,
    engine_kind: str,
    window_months: Optional[int],
    rules_path: Optional[Path],
    eval_dir: Optional[Path],
    wf_summary_path: Optional[Path],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    產生一份完整的 factor_status payload（可直接寫成 JSON）。

    - as_of_date: W-FRI / Gate 評估日（YYYY-MM-DD）
    - profile: dev / test / live / prod
    - engine_kind: classic / ai（目前僅作為 metadata 記錄，不做 filter）
    - window_months: 最大 WF 視窗（例如 24），用來判斷 has_window
    """
    try:
        year, month, day = map(int, as_of_date.split("-"))
        expect_date = date(year, month, day)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid as_of_date {as_of_date!r}: {exc}") from exc

    rules_path = rules_path if rules_path is not None else (root / "rules_factors.yaml")
    eval_dir = eval_dir if eval_dir is not None else (root / "reports" / "factor_eval")
    wf_summary_path = (
        wf_summary_path if wf_summary_path is not None else (root / "reports" / "wf_summary.json")
    )

    registry_ids: Optional[Set[str]] = None
    if rules_path is not None:
        registry_ids = _load_registry_factor_ids(
            root=root,
            rules_path=rules_path,
            logger=logger,
            strict=True,
        )

    eval_ids = _load_eval_factor_ids(eval_dir, logger)
    wf_ids = _load_wf_summary_factor_ids(wf_summary_path, logger)

    statuses = scan_factor_status(
        root=root,
        expect_date=expect_date,
        window_months=window_months,
        registry_factor_ids=registry_ids,
        eval_factor_ids=eval_ids,
        wf_factor_ids=wf_ids,
        logger=logger,
    )

    # 轉成 JSON-safe 結構
    items: List[Dict[str, Any]] = []
    factors_map: Dict[str, Dict[str, Any]] = {}
    for s in statuses:
        d = asdict(s)
        if d["approx_min_date"] is not None:
            d["approx_min_date"] = d["approx_min_date"].isoformat()
        if d["approx_max_date"] is not None:
            d["approx_max_date"] = d["approx_max_date"].isoformat()
        # desired_action 目前預設為 None，由其他工具填入
        d.setdefault("desired_action", None)
        items.append(d)
        factors_map[d["factor_id"]] = d

    payload: Dict[str, Any] = {
        "as_of_date": as_of_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "engine_kind": engine_kind,
        "expect_date": as_of_date,
        "window_months": window_months,
        "items": items,
        "factors": factors_map,
        "schema_version": 1,
        "source": "factor_status",
    }
    return payload


# ---------------------------------------------------------------------------
# DataFrame helper（方便 CLI 印 summary / 寫 CSV）
# ---------------------------------------------------------------------------


def _status_list_to_dataframe(statuses: List[FactorStatus]) -> pd.DataFrame:
    """Convert status list to DataFrame（日期轉成 ISO 字串）。"""
    if not statuses:
        return pd.DataFrame(
            columns=[
                "factor_id",
                "min_yyyymm",
                "max_yyyymm",
                "approx_min_date",
                "approx_max_date",
                "partition_count",
                "file_count",
                "months_covered",
                "is_fresh",
                "has_window",
                "in_registry",
                "has_data",
                "has_eval",
                "in_wf_summary",
                "required_action",
                "desired_action",
            ]
        )
    rows = []
    for s in statuses:
        d = asdict(s)
        if d["approx_min_date"] is not None:
            d["approx_min_date"] = d["approx_min_date"].isoformat()
        if d["approx_max_date"] is not None:
            d["approx_max_date"] = d["approx_max_date"].isoformat()
        rows.append(d)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Self-tests：基本邏輯驗證（用 --self-test 啟動）
# ---------------------------------------------------------------------------


def _run_self_tests(logger: logging.Logger) -> None:
    import shutil
    import tempfile

    logger.info("Running self-tests for factor_status...")

    tmp_root = Path(tempfile.mkdtemp(prefix="factor_status_test_"))
    try:
        factor_root = tmp_root / "datahub" / "silver" / "alpha" / "factor"
        # 建一個因子 mom_12m，有兩個月份分區
        d1 = factor_root / "mom_12m" / "yyyymm=202401"
        d2 = factor_root / "mom_12m" / "yyyymm=202402"
        d1.mkdir(parents=True, exist_ok=True)
        d2.mkdir(parents=True, exist_ok=True)
        (d1 / "part1.parquet").touch()
        (d2 / "part2.parquet").touch()

        # 再建一個 orphan 因子 data_only（沒有在 registry 出現的）
        d3 = factor_root / "data_only" / "yyyymm=202401"
        d3.mkdir(parents=True, exist_ok=True)
        (d3 / "x.parquet").touch()

        # 模擬 registry / eval / WF
        registry_ids = {"mom_12m"}  # 只註冊 mom_12m
        eval_ids = {"mom_12m"}
        wf_ids = {"mom_12m"}

        statuses = scan_factor_status(
            root=tmp_root,
            expect_date=date(2024, 2, 15),
            window_months=2,
            registry_factor_ids=registry_ids,
            eval_factor_ids=eval_ids,
            wf_factor_ids=wf_ids,
            logger=logger,
        )

        assert len(statuses) == 2, "should see mom_12m and data_only"
        s_mom = next(s for s in statuses if s.factor_id == "mom_12m")
        s_data = next(s for s in statuses if s.factor_id == "data_only")

        assert s_mom.has_data is True
        assert s_mom.in_registry is True
        assert s_mom.required_action in {"ok", "rebuild"}
        assert s_data.has_data is True
        assert s_data.in_registry in (False, None)
        assert s_data.required_action == "orphan_data"

        logger.info("Self-tests passed.")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor layer integrated status checker."
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help=(
            "Repository root directory (default: current directory). "
            "This script will scan <root>/datahub/silver/alpha/factor."
        ),
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="As-of date (YYYY-MM-DD). Used as expect-date for freshness check.",
    )
    parser.add_argument(
        "--expect-date",
        type=str,
        default=None,
        help="Deprecated alias for --date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="dev",
        help="SLO profile (dev/test/live/prod). Default: dev.",
    )
    parser.add_argument(
        "--engine",
        dest="engine_kind",
        type=str,
        default="classic",
        help="Engine kind / factor category (classic/ai). Default: classic.",
    )
    parser.add_argument(
        "--window-months",
        type=int,
        default=None,
        help="Required lookback window in months (e.g. 24) for has_window check.",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        help=(
            "Path to rules_factors.yaml; if provided, registry integration is enabled. "
            "Will attempt factor_registry module first, then YAML fallback."
        ),
    )
    parser.add_argument(
        "--eval-dir",
        type=str,
        default=None,
        help="Directory for factor_eval JSONs (default: <root>/reports/factor_eval).",
    )
    parser.add_argument(
        "--wf-summary",
        type=str,
        default=None,
        help="Path to wf_summary.json (default: <root>/reports/wf_summary.json).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Path to write integrated status JSON. "
            "Default: <root>/reports/factor_status.<date>.json"
        ),
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Optional path to write integrated status as CSV (items only).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in self tests and exit.",
    )
    return parser.parse_args(argv)


def _configure_logging(level_name: str) -> logging.Logger:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger("factor_status")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logger = _configure_logging(args.log_level)

    if args.self_test:
        _run_self_tests(logger)
        return 0

    root = Path(args.root).resolve()
    logger.info("Using root directory: %s", root)

    as_of_date = args.date or args.expect_date
    if not as_of_date:
        logger.error("Either --date or --expect-date must be provided")
        return 1

    window_months: Optional[int] = args.window_months
    if window_months is not None and window_months <= 0:
        logger.error("--window-months must be positive if provided")
        return 1

    rules_path = Path(args.rules).resolve() if args.rules else None
    eval_dir = Path(args.eval_dir).resolve() if args.eval_dir else None
    wf_path = Path(args.wf_summary).resolve() if args.wf_summary else None

    try:
        # 先產出 payload（包含 items + factors map）
        payload = build_factor_status_payload(
            root=root,
            as_of_date=as_of_date,
            profile=args.profile,
            engine_kind=args.engine_kind,
            window_months=window_months,
            rules_path=rules_path,
            eval_dir=eval_dir,
            wf_summary_path=wf_path,
            logger=logger,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to build factor status payload: %s", exc)
        return 1

    # 從 payload 還原 FactorStatus list，用來印 summary / CSV
    items_raw = payload.get("items", [])
    statuses: List[FactorStatus] = []
    for d in items_raw:
        # approx_* 已是 ISO 字串，轉回 date 以符合 dataclass 型別；
        # 這裡只為了 DataFrame summary，用不到也無妨。
        approx_min_date = d.get("approx_min_date")
        approx_max_date = d.get("approx_max_date")
        min_date_obj = date.fromisoformat(approx_min_date) if approx_min_date else None
        max_date_obj = date.fromisoformat(approx_max_date) if approx_max_date else None
        statuses.append(
            FactorStatus(
                factor_id=d["factor_id"],
                min_yyyymm=d.get("min_yyyymm"),
                max_yyyymm=d.get("max_yyyymm"),
                approx_min_date=min_date_obj,
                approx_max_date=max_date_obj,
                partition_count=int(d.get("partition_count", 0)),
                file_count=int(d.get("file_count", 0)),
                months_covered=d.get("months_covered"),
                is_fresh=d.get("is_fresh"),
                has_window=d.get("has_window"),
                in_registry=d.get("in_registry"),
                has_data=d.get("has_data", False),
                has_eval=d.get("has_eval"),
                in_wf_summary=d.get("in_wf_summary"),
                required_action=d.get("required_action", "unknown"),
                desired_action=d.get("desired_action"),
            )
        )

    df = _status_list_to_dataframe(statuses)

    if df.empty:
        logger.warning("No factor status to report.")
    else:
        display_cols = [
            "factor_id",
            "min_yyyymm",
            "max_yyyymm",
            "months_covered",
            "is_fresh",
            "has_window",
            "in_registry",
            "has_data",
            "has_eval",
            "in_wf_summary",
            "required_action",
            "desired_action",
        ]
        for col in display_cols:
            if col not in df.columns:
                df[col] = None
        logger.info(
            "Factor status summary:\n%s",
            df[display_cols].to_string(index=False),
        )

    # 寫 JSON
    output_json = args.output
    if not output_json:
        # 預設路徑：reports/factor_status.<date>.json
        out_path = root / "reports" / f"factor_status.{as_of_date}.json"
    else:
        out_path = Path(output_json)
        if not out_path.is_absolute():
            out_path = (root / out_path).resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing JSON status to %s", out_path)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)

    # 寫 CSV（僅 items）
    if args.output_csv:
        csv_path = Path(args.output_csv)
        if not csv_path.is_absolute():
            csv_path = (root / csv_path).resolve()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing CSV status to %s", csv_path)
        df.to_csv(csv_path, index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

