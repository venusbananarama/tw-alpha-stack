#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/factor_diag.py

Phase-2 因子診斷工具箱（read-only）：

  1) deps 子指令：因子依賴檢查（原 factor_dep_check 功能）
     - 根據 rules_factors.yaml + 銀河 parquet 目錄，檢查每顆因子所需的
       dataset / 欄位是否存在。

  2) gate 子指令：gate check 檢視（原 show_factor_gate_checks 功能）
     - 根據 reports/wf_summary.json（以及可選 rules_factors.yaml），列出
       每顆因子的 gate_status 與 gate_checks 通過/失敗情況。

  3) eval 子指令：因子評估摘要（原 show_factor_eval_summary 構想）
     - 根據 reports/factor_eval/*_summary.json（以及可選 rules_factors.yaml），
       列出每顆因子在各個 WF 視窗下的 rank_ic / ic / coverage / sample_days 等摘要。

使用方式（在 repo root）：

  # 1) 因子依賴檢查
  python .\\scripts\\factor_diag.py deps --root . --rules .\\rules_factors.yaml

  # 2) gate 狀態查看
  python .\\scripts\\factor_diag.py gate --root . \\
      --wf-summary .\\reports\\wf_summary.json \\
      --rules .\\rules_factors.yaml

  # 3) eval 摘要查看
  python .\\scripts\\factor_diag.py eval --root . \\
      --factor-eval-dir .\\reports\\factor_eval \\
      --rules .\\rules_factors.yaml \\
      --windows 6,12,24
"""

from __future__ import annotations

import argparse
import json
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# 共用常數 / 型別
# ---------------------------------------------------------------------------

DATASETS_BASE = Path("datahub") / "silver" / "alpha"


@dataclass
class DatasetRequirement:
    name: str
    columns_any_of: Sequence[str] = field(default_factory=list)
    note: str = ""


@dataclass
class FactorDepResult:
    factor_id: str
    engine: str
    enabled: bool
    required_datasets: List[DatasetRequirement]
    dataset_status: Dict[str, str]  # dataset name -> short status
    overall_status: str  # "OK" / "WARN" / "MISSING"
    reason: str


@dataclass
class FactorGateCheckResult:
    factor_id: str
    engine: str = ""
    enabled: Optional[bool] = None
    gate_status: str = "UNKNOWN"  # PASS / FAIL / WARN / UNKNOWN / 其他
    passed_checks: List[str] = field(default_factory=list)
    failed_checks: List[str] = field(default_factory=list)
    unknown_checks: List[str] = field(default_factory=list)
    note: str = ""


@dataclass
class FactorEvalRow:
    factor_id: str
    window: str
    engine: str = ""
    enabled: Optional[bool] = None
    rank_ic_mean: Optional[float] = None
    rank_ic_std: Optional[float] = None
    ic_mean: Optional[float] = None
    ic_std: Optional[float] = None
    coverage_mean: Optional[float] = None
    sample_days: Optional[int] = None
    note: str = ""


# ---------------------------------------------------------------------------
# 共用：rules_factors.yaml loader / meta builder
# ---------------------------------------------------------------------------


def _load_rules_factors(path: Path) -> Mapping[str, object]:
    if not path.exists():
        raise SystemExit(f"rules_factors.yaml 不存在：{path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    from collections.abc import Mapping as _Mapping

    if not isinstance(data, _Mapping):
        raise SystemExit("rules_factors.yaml 格式錯誤（非 mapping）")
    return data


def _build_factor_meta_from_rules(
    rules: Mapping[str, object],
) -> Dict[str, Dict[str, object]]:
    """
    將 rules_factors.yaml 中的 factors 整理成：

        { factor_id: {"engine": str, "enabled": bool, "raw": dict}, ... }
    """
    meta: Dict[str, Dict[str, object]] = {}
    factors = rules.get("factors") or []
    if not isinstance(factors, list):
        return meta

    from collections.abc import Mapping as _Mapping

    for item in factors:
        if not isinstance(item, _Mapping):
            continue
        fid = str(item.get("factor_id", "")).strip()
        if not fid:
            continue
        engine = str(item.get("engine", "")).strip()
        enabled = bool(item.get("enabled", False))
        meta[fid] = {
            "engine": engine,
            "enabled": enabled,
            "raw": dict(item),
        }
    return meta


# ---------------------------------------------------------------------------
# deps 模式：因子依賴檢查（原 factor_dep_check）
# ---------------------------------------------------------------------------


def _iter_factor_specs(rules: Mapping[str, object]) -> Iterable[Mapping[str, object]]:
    factors = rules.get("factors") or []
    if not isinstance(factors, list):
        return []
    from collections.abc import Mapping as _Mapping

    for item in factors:
        if isinstance(item, _Mapping):
            yield item


def _infer_requirements_for_factor(spec: Mapping[str, object]) -> List[DatasetRequirement]:
    fid = str(spec.get("factor_id", "")).strip()
    engine = str(spec.get("engine", "")).strip()
    reqs: List[DatasetRequirement] = []

    # Minimal mapping based on current factor_impl engines
    if engine in ("ta_mom_v1", "ta_vol_v1", "ta_beta_v1"):
        cols = ["date", "stock_id", "adj_close", "close", "price"]
        if fid == "vol_20d":
            cols.append("volume")
        reqs.append(
            DatasetRequirement(
                name="prices",
                columns_any_of=cols,
                note="Phase-1 prices parquet for momentum/vol/beta.",
            )
        )
    elif engine == "microstructure_v1":
        cols = ["date", "stock_id", "adj_close", "close", "price"]
        cols.extend(["turnover_rate", "volume", "turnover_value", "market_cap"])
        reqs.append(
            DatasetRequirement(
                name="prices",
                columns_any_of=cols,
                note="OHLCV/microstructure columns (volume / turnover_rate / market_cap).",
            )
        )
        # size_log_mktcap 額外吃 bs
        if fid == "size_log_mktcap":
            reqs.append(
                DatasetRequirement(
                    name="bs",
                    columns_any_of=[
                        "CapitalStock",
                        "CommonStock",
                        "ShareCapital",
                        "PaidInCapital",
                        "StockholdersEquity",
                        "EquityAttributableToOwnersOfParent",
                    ],
                    note="Balance sheet 資本類欄位，給 size_log_mktcap 推市值基準。",
                )
            )
    elif engine == "fundamental_value_v1":
        cols = [
            "date",
            "stock_id",
            "pe",
            "PE",
            "pe_ratio",
            "PER",
            "per",
            "ttm_pe",
            "PER_ttm",
            "PE_ttm",
            "pe_raw",
        ]
        reqs.append(
            DatasetRequirement(
                name="per",
                columns_any_of=cols,
                note="PER / earnings-yield style columns for value_pe / value_pb.",
            )
        )
    elif engine == "fundamental_quality_v1":
        cols = [
            "date",
            "stock_id",
            # net income
            "net_income",
            "NetIncome",
            "NetIncomeLoss",
            "profit",
            "PAT",
            "NI",
            "IncomeAfterTaxes",
            "IncomeAfterTax",
            "IncomeFromContinuingOperations",
            "IncomeBeforeTaxFromContinuingOperations",
            "TotalConsolidatedProfitForThePeriod",
            # equity
            "equity",
            "Equity",
            "TotalEquity",
            "EquityTotal",
            "EquityAttributableToOwnersOfParent",
            "EquityAttributableToOwnersOfParentCompany",
            "shareholder_equity",
            "StockholdersEquity",
            "StockholdersEquityTotal",
            "book_value",
        ]
        reqs.append(
            DatasetRequirement(
                name="finstmt",
                columns_any_of=cols,
                note="Income + Equity-like columns for quality_roeq.",
            )
        )
    elif engine == "ai_xgb_v1":
        reqs.append(
            DatasetRequirement(
                name="factor_pool",
                columns_any_of=[],
                note="預留：需要 classic factor parquet 作為 feature pool。",
            )
        )
    else:
        # Unknown engine → just mark as meta
        reqs.append(
            DatasetRequirement(
                name="unknown",
                columns_any_of=[],
                note=f"未知 engine={engine}，只做記錄，不檢查 parquet。",
            )
        )

    return reqs


def _probe_dataset(root: Path, req: DatasetRequirement) -> str:
    """
    檢查資料夾 + 至少一個 parquet 檔存在與否，
    並（若 columns_any_of 非空）檢查是否有預期欄位。
    """
    if req.name in ("factor_pool", "unknown"):
        return "SKIP(meta)"

    base = root / DATASETS_BASE / req.name
    if not base.exists():
        return "MISSING(dir)"

    files = sorted(base.glob("yyyymm=*/*.parquet"))
    if not files:
        return "MISSING(parquet)"

    if not req.columns_any_of:
        return "OK"

    needed = set(req.columns_any_of)
    for p in files[:5]:
        try:
            df = pd.read_parquet(p, columns=None)
        except Exception:
            continue
        cols = set(df.columns)
        if cols & needed:
            return "OK"

    return "WARN(no-expected-columns)"


def _summarize_status(statuses: Mapping[str, str]) -> str:
    if not statuses:
        return "WARN(no-datasets)"
    values = list(statuses.values())
    if any(s.startswith("MISSING") for s in values):
        return "MISSING"
    if any(s.startswith("WARN") for s in values):
        return "WARN"
    return "OK"


def _build_reason(statuses: Mapping[str, str], enabled: bool) -> str:
    if not statuses:
        return "無 dataset 定義（只視為 meta）"
    parts = [f"{name}={st}" for name, st in statuses.items()]
    reason = "; ".join(parts)
    if not enabled:
        reason = f"[disabled] {reason}"
    return reason


def analyze_factors(root: Path, rules_path: Path) -> List[FactorDepResult]:
    rules = _load_rules_factors(rules_path)
    results: List[FactorDepResult] = []

    for spec in _iter_factor_specs(rules):
        fid = str(spec.get("factor_id", "")).strip() or "<missing-id>"
        engine = str(spec.get("engine", "")).strip() or "<missing-engine>"
        enabled = bool(spec.get("enabled", False))

        reqs = _infer_requirements_for_factor(spec)
        dataset_status: Dict[str, str] = {}
        for req in reqs:
            dataset_status[req.name] = _probe_dataset(root, req)

        overall = _summarize_status(dataset_status)
        reason = _build_reason(dataset_status, enabled)

        results.append(
            FactorDepResult(
                factor_id=fid,
                engine=engine,
                enabled=enabled,
                required_datasets=reqs,
                dataset_status=dataset_status,
                overall_status=overall,
                reason=reason,
            )
        )

    severity_rank = {"MISSING": 2, "WARN": 1, "OK": 0}

    results.sort(
        key=lambda r: (
            0 if r.enabled else 1,
            severity_rank.get(r.overall_status, 1),
            r.factor_id,
        )
    )
    return results


def _format_deps_table(rows: List[FactorDepResult]) -> str:
    headers = ["factor_id", "engine", "enabled", "status", "datasets", "reason"]

    def _row_to_cells(r: FactorDepResult):
        ds_names = ",".join(req.name for req in r.required_datasets) or "-"
        return [
            r.factor_id,
            r.engine or "-",
            "Y" if r.enabled else "N",
            r.overall_status,
            ds_names,
            r.reason,
        ]

    data_rows = [_row_to_cells(r) for r in rows]
    if not data_rows:
        return "（沒有任何 factor）"

    cols = list(zip(*([headers] + data_rows)))
    widths = [max(len(str(c)) for c in col) for col in cols]

    def _fmt_line(cells):
        return " | ".join(str(c).ljust(w) for c, w in zip(cells, widths))

    lines = [_fmt_line(headers), "-+-".join("-" * w for w in widths)]
    for cells in data_rows:
        lines.append(_fmt_line(cells))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# gate 模式：gate check 檢視（原 show_factor_gate_checks）
# ---------------------------------------------------------------------------


def _load_wf_summary(path: Path) -> Mapping[str, object]:
    if not path.exists():
        raise SystemExit(f"wf_summary.json 不存在：{path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    from collections.abc import Mapping as _Mapping

    if not isinstance(data, _Mapping):
        raise SystemExit("wf_summary.json 格式錯誤（非 mapping）")
    return data


def _iter_wf_factors(wf: Mapping[str, object]) -> Iterable[tuple[str, Mapping[str, object]]]:
    factors = wf.get("factors")
    from collections.abc import Mapping as _Mapping

    if isinstance(factors, _Mapping):
        for fid, info in factors.items():
            if isinstance(info, _Mapping):
                yield str(fid), info
        return

    if isinstance(factors, list):
        for item in factors:
            if not isinstance(item, _Mapping):
                continue
            fid = str(item.get("factor_id", "")).strip() or "<missing-id>"
            yield fid, item


def _normalize_checks(raw: object) -> Dict[str, Optional[bool]]:
    """
    將 gate_checks / checks 結構正規化成 {name: True/False/None}
    """
    from collections.abc import Mapping as _Mapping

    if not isinstance(raw, _Mapping):
        return {}

    checks: Dict[str, Optional[bool]] = {}
    for name, val in raw.items():
        ok: Optional[bool] = None

        if isinstance(val, bool):
            ok = val
        elif isinstance(val, _Mapping):
            if "ok" in val:
                v = val["ok"]
                if isinstance(v, bool):
                    ok = v
                else:
                    s = str(v).strip().lower()
                    if s in ("1", "true", "yes", "y", "pass", "passed", "ok"):
                        ok = True
                    elif s in ("0", "false", "no", "n", "fail", "failed", "ng"):
                        ok = False
            elif "status" in val:
                s = str(val["status"]).strip().lower()
                if s in ("pass", "ok", "passed", "true"):
                    ok = True
                elif s in ("fail", "failed", "ng", "false"):
                    ok = False

        checks[str(name)] = ok
    return checks


def _extract_gate_info(
    info: Mapping[str, object],
) -> tuple[str, List[str], List[str], List[str], str]:
    """
    從單一 factor 的 wf_summary 區塊中，抽出：
      gate_status, passed_checks, failed_checks, unknown_checks, note
    """
    from collections.abc import Mapping as _Mapping

    gate: Mapping[str, object] | None = None
    if "gate" in info and isinstance(info["gate"], _Mapping):
        gate = info["gate"]

    raw_checks = None
    if gate and isinstance(gate, _Mapping):
        raw_checks = gate.get("checks") or gate.get("gate_checks")
    if raw_checks is None and "gate_checks" in info and isinstance(info["gate_checks"], _Mapping):
        raw_checks = info["gate_checks"]

    checks = _normalize_checks(raw_checks)
    passed = [name for name, ok in checks.items() if ok is True]
    failed = [name for name, ok in checks.items() if ok is False]
    unknown = [name for name, ok in checks.items() if ok is None]

    status_raw = None
    if gate and isinstance(gate, _Mapping):
        for key in ("status", "gate_status", "overall_status"):
            if key in gate:
                status_raw = gate[key]
                break
    if status_raw is None:
        for key in ("gate_status", "status"):
            if key in info:
                status_raw = info[key]
                break

    if status_raw is None or str(status_raw).strip() == "":
        if failed:
            status = "FAIL"
        elif passed and not failed:
            status = "PASS"
        elif checks:
            status = "WARN"
        else:
            status = "UNKNOWN"
    else:
        status = str(status_raw).upper()

    note_parts: List[str] = []
    if not checks:
        note_parts.append("未找到 gate_checks/checks 欄位，只顯示 gate_status。")
    if status_raw is not None and status not in ("PASS", "FAIL", "WARN", "UNKNOWN"):
        note_parts.append(f"原始 status={status_raw!r}")
    note = " ".join(note_parts)

    return status, passed, failed, unknown, note


def analyze_gate_checks(
    root: Path,
    wf_summary_path: Path,
    rules_path: Optional[Path] = None,
) -> List[FactorGateCheckResult]:
    wf = _load_wf_summary(wf_summary_path)

    factor_meta: Dict[str, Dict[str, object]] = {}
    if rules_path is not None:
        rules = _load_rules_factors(rules_path)
        factor_meta = _build_factor_meta_from_rules(rules)

    results: List[FactorGateCheckResult] = []

    for fid, info in _iter_wf_factors(wf):
        from collections.abc import Mapping as _Mapping

        if not isinstance(info, _Mapping):
            continue

        meta = factor_meta.get(fid, {})
        engine = str(info.get("engine") or meta.get("engine") or "").strip()
        enabled = meta.get("enabled")
        status, passed, failed, unknown, note = _extract_gate_info(info)

        results.append(
            FactorGateCheckResult(
                factor_id=fid,
                engine=engine,
                enabled=enabled if isinstance(enabled, bool) else None,
                gate_status=status,
                passed_checks=passed,
                failed_checks=failed,
                unknown_checks=unknown,
                note=note,
            )
        )

    severity_rank = {"FAIL": 2, "WARN": 1, "UNKNOWN": 1, "PASS": 0}

    def _enabled_rank(x: Optional[bool]) -> int:
        return 1 if x is False else 0

    results.sort(
        key=lambda r: (
            _enabled_rank(r.enabled),
            severity_rank.get(r.gate_status, 1),
            r.factor_id,
        )
    )
    return results


def _format_gate_table(rows: List[FactorGateCheckResult]) -> str:
    headers = ["factor_id", "engine", "enabled", "gate_status", "checks", "note"]

    def _enabled_str(x: Optional[bool]) -> str:
        if x is True:
            return "Y"
        if x is False:
            return "N"
        return "?"

    def _checks_summary(r: FactorGateCheckResult) -> str:
        parts: List[str] = []
        if r.failed_checks:
            parts.append("FAIL[" + ",".join(r.failed_checks) + "]")
        if r.unknown_checks:
            parts.append("UNK[" + ",".join(r.unknown_checks) + "]")
        if r.passed_checks and not r.failed_checks:
            parts.append("PASS[" + ",".join(r.passed_checks) + "]")
        return "; ".join(parts) if parts else "-"

    def _short_note(text: str, width: int = 60) -> str:
        if not text:
            return ""
        return textwrap.shorten(text, width=width, placeholder="...")

    def _row_to_cells(r: FactorGateCheckResult):
        return [
            r.factor_id,
            r.engine or "-",
            _enabled_str(r.enabled),
            r.gate_status,
            _checks_summary(r),
            _short_note(r.note),
        ]

    data_rows = [_row_to_cells(r) for r in rows]
    if not data_rows:
        return "（沒有任何 factor 或 wf_summary.factors 為空）"

    cols = list(zip(*([headers] + data_rows)))
    widths = [max(len(str(c)) for c in col) for col in cols]

    def _fmt_line(cells):
        return " | ".join(str(c).ljust(w) for c, w in zip(cells, widths))

    lines = [_fmt_line(headers), "-+-".join("-" * w for w in widths)]
    for cells in data_rows:
        lines.append(_fmt_line(cells))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# eval 模式：因子評估摘要（show_factor_eval_summary）
# ---------------------------------------------------------------------------


def _iter_factor_eval_files(factor_eval_dir: Path) -> Iterable[Path]:
    if not factor_eval_dir.exists():
        return []
    # 以 *_summary.json 優先；若沒有就吃全部 json
    files = sorted(factor_eval_dir.glob("*_summary.json"))
    if not files:
        files = sorted(factor_eval_dir.glob("*.json"))
    return files


def _safe_get(d: Mapping[str, object], key: str) -> Optional[float]:
    val = d.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val))
        except Exception:
            return None


def analyze_factor_eval(
    root: Path,
    factor_eval_dir: Path,
    rules_path: Optional[Path] = None,
    windows_filter: Optional[Sequence[str]] = None,
    factor_ids_filter: Optional[Sequence[str]] = None,
    min_sample_days: int = 0,
) -> List[FactorEvalRow]:
    """
    掃描 factor_eval_dir 底下的 *_summary.json，將每顆因子在各個 window 的
    rank_ic/ic/coverage/sample_days 摘要出來。
    """
    factor_meta: Dict[str, Dict[str, object]] = {}
    if rules_path is not None:
        rules = _load_rules_factors(rules_path)
        factor_meta = _build_factor_meta_from_rules(rules)

    if windows_filter:
        win_filter_set = {str(w).strip() for w in windows_filter if str(w).strip()}
    else:
        win_filter_set = None

    fid_filter_set = None
    if factor_ids_filter:
        fid_filter_set = {fid.strip() for fid in factor_ids_filter if fid.strip()}

    results: List[FactorEvalRow] = []

    for path in _iter_factor_eval_files(factor_eval_dir):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        from collections.abc import Mapping as _Mapping

        if not isinstance(data, _Mapping):
            continue

        fid = str(data.get("factor_id") or path.stem.replace("_summary", "")).strip()
        if not fid:
            continue

        if fid_filter_set is not None and fid not in fid_filter_set:
            continue

        windows = data.get("windows") or {}
        if not isinstance(windows, _Mapping):
            continue

        meta = factor_meta.get(fid, {})
        engine = str(meta.get("engine") or "").strip()
        enabled_val = meta.get("enabled")
        enabled = enabled_val if isinstance(enabled_val, bool) else None

        for w_key, metrics in windows.items():
            if win_filter_set is not None and w_key not in win_filter_set:
                continue
            if not isinstance(metrics, Mapping):
                continue

            rank_ic_mean = _safe_get(metrics, "rank_ic_mean")
            if rank_ic_mean is None:
                rank_ic_mean = _safe_get(metrics, "rank_ic")

            rank_ic_std = _safe_get(metrics, "rank_ic_std")

            ic_mean = _safe_get(metrics, "ic_mean")
            if ic_mean is None:
                ic_mean = _safe_get(metrics, "ic")

            ic_std = _safe_get(metrics, "ic_std")

            coverage_mean = _safe_get(metrics, "coverage_mean")
            if coverage_mean is None:
                coverage_mean = _safe_get(metrics, "coverage")

            sample_days_raw = metrics.get("sample_days")
            try:
                sample_days = int(sample_days_raw) if sample_days_raw is not None else None
            except Exception:
                sample_days = None

            if min_sample_days and (sample_days is None or sample_days < min_sample_days):
                # 樣本天數太少 → 先略過
                continue

            note_parts: List[str] = []
            if sample_days is not None and sample_days < 60:
                note_parts.append("樣本天數偏少")
            if rank_ic_mean is None and ic_mean is None:
                note_parts.append("無 IC 指標")
            note = "；".join(note_parts)

            results.append(
                FactorEvalRow(
                    factor_id=fid,
                    window=str(w_key),
                    engine=engine,
                    enabled=enabled,
                    rank_ic_mean=rank_ic_mean,
                    rank_ic_std=rank_ic_std,
                    ic_mean=ic_mean,
                    ic_std=ic_std,
                    coverage_mean=coverage_mean,
                    sample_days=sample_days,
                    note=note,
                )
            )

    # 排序：先依 window（數字），再依 rank_ic_mean desc，再 factor_id
    def _window_sort_key(w: str) -> int:
        try:
            return int(w)
        except Exception:
            return 999999

    results.sort(
        key=lambda r: (
            _window_sort_key(r.window),
            -(r.rank_ic_mean if r.rank_ic_mean is not None else float("-inf")),
            r.factor_id,
        )
    )
    return results


def _format_eval_table(rows: List[FactorEvalRow]) -> str:
    headers = [
        "factor_id",
        "window",
        "engine",
        "enabled",
        "rank_ic",
        "ic",
        "cov_mean",
        "days",
        "note",
    ]

    def _enabled_str(x: Optional[bool]) -> str:
        if x is True:
            return "Y"
        if x is False:
            return "N"
        return "?"

    def _fmt_f(v: Optional[float], ndigits: int = 4) -> str:
        if v is None:
            return "-"
        try:
            return f"{v:.{ndigits}f}"
        except Exception:
            return str(v)

    def _row_to_cells(r: FactorEvalRow):
        return [
            r.factor_id,
            str(r.window),
            r.engine or "-",
            _enabled_str(r.enabled),
            _fmt_f(r.rank_ic_mean),
            _fmt_f(r.ic_mean),
            _fmt_f(r.coverage_mean, ndigits=3),
            str(r.sample_days) if r.sample_days is not None else "-",
            (r.note or ""),
        ]

    data_rows = [_row_to_cells(r) for r in rows]
    if not data_rows:
        return "（沒有任何 factor_eval 摘要或符合條件的視窗）"

    cols = list(zip(*([headers] + data_rows)))
    widths = [max(len(str(c)) for c in col) for col in cols]

    def _fmt_line(cells):
        return " | ".join(str(c).ljust(w) for c, w in zip(cells, widths))

    lines = [_fmt_line(headers), "-+-".join("-" * w for w in widths)]
    for cells in data_rows:
        lines.append(_fmt_line(cells))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor diagnostics (deps / gate / eval).",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=False)

    # deps 子指令
    p_deps = subparsers.add_parser(
        "deps",
        help="檢查因子依賴的 dataset / 欄位（rules_factors.yaml + parquet）。",
    )
    p_deps.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repo root（預設為 .）",
    )
    p_deps.add_argument(
        "--rules",
        type=Path,
        default=Path("rules_factors.yaml"),
        help="rules_factors.yaml 路徑（預設為 ./rules_factors.yaml）",
    )

    # gate 子指令
    p_gate = subparsers.add_parser(
        "gate",
        help="檢視因子在 wf_summary.json 中的 gate_status / gate_checks。",
    )
    p_gate.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repo root（預設為 .）",
    )
    p_gate.add_argument(
        "--wf-summary",
        type=Path,
        default=Path("reports") / "wf_summary.json",
        help="wf_summary.json 路徑（預設為 ./reports/wf_summary.json）",
    )
    p_gate.add_argument(
        "--rules",
        type=Path,
        default=None,
        help="rules_factors.yaml 路徑（選填，提供時可顯示 engine/enabled 狀態）",
    )
    p_gate.add_argument(
        "--factor-id",
        dest="factor_ids",
        action="append",
        help="只顯示指定因子，可重複給多次。",
    )
    p_gate.add_argument(
        "--status",
        choices=["PASS", "FAIL", "WARN", "UNKNOWN"],
        help="只顯示特定 gate_status。",
    )

    # eval 子指令
    p_eval = subparsers.add_parser(
        "eval",
        help="檢視因子在 factor_eval JSON 中的 rank_ic / ic / coverage / sample_days 摘要。",
    )
    p_eval.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repo root（預設為 .）",
    )
    p_eval.add_argument(
        "--factor-eval-dir",
        type=Path,
        default=None,
        help="factor_eval 摘要所在資料夾（預設為 ./reports/factor_eval）",
    )
    p_eval.add_argument(
        "--rules",
        type=Path,
        default=None,
        help="rules_factors.yaml 路徑（選填，提供時可顯示 engine/enabled 狀態）",
    )
    p_eval.add_argument(
        "--windows",
        type=str,
        default=None,
        help="只顯示指定視窗（逗號分隔，例如 '6,12,24'；預設為全部）",
    )
    p_eval.add_argument(
        "--factor-id",
        dest="factor_ids",
        action="append",
        help="只顯示指定因子，可重複給多次。",
    )
    p_eval.add_argument(
        "--min-days",
        type=int,
        default=0,
        help="最少 sample_days，低於此值的視窗不顯示（預設 0 = 不過濾）。",
    )

    args = parser.parse_args(argv)

    # 若沒指定子指令，預設跑 deps
    cmd = args.command or "deps"

    if cmd == "deps":
        root: Path = args.root.resolve()
        rules_path: Path = (
            args.rules if args.rules.is_absolute() else (root / args.rules)
        )
        rows = analyze_factors(root, rules_path)
        print(_format_deps_table(rows))

        print()
        print(
            textwrap.dedent(
                """
                說明：
                  - status=OK      ：需要的資料夾/parquet/欄位大致齊全。
                  - status=WARN    ：資料夾與 parquet 存在，但沒偵測到預期欄位（可能是欄名不同）。
                  - status=MISSING ：缺資料夾或 parquet，該因子目前一定跑不出結果。
                  - enabled=N      ：即使 dataset OK，目前因子仍不會被 Phase-2 pipeline 使用（rules_factors.yaml.enabled=false）。
                """.rstrip()
            )
        )
    elif cmd == "gate":
        root: Path = args.root.resolve()
        wf_path: Path = (
            args.wf_summary
            if args.wf_summary.is_absolute()
            else (root / args.wf_summary)
        )
        if args.rules is None:
            rules_path = None
        else:
            rules_path = (
                args.rules if args.rules.is_absolute() else (root / args.rules)
            )

        rows = analyze_gate_checks(root, wf_path, rules_path)

        if args.factor_ids:
            wanted = {fid.strip() for fid in args.factor_ids if fid.strip()}
            rows = [r for r in rows if r.factor_id in wanted]

        if args.status:
            rows = [r for r in rows if r.gate_status == args.status]

        print(_format_gate_table(rows))
        print()
        print(
            textwrap.dedent(
                """
                說明：
                  - gate_status=PASS   ：所有 gate_checks 都通過（或明確標示 PASS）。
                  - gate_status=FAIL   ：至少一個 gate_check 失敗。
                  - gate_status=WARN   ：有 gate_checks 但部分狀態不明，或沒有任何明確 PASS/FAIL。
                  - gate_status=UNKNOWN：找不到 gate_checks / status，只能表示「看不出來」。
                """.rstrip()
            )
        )
    elif cmd == "eval":
        root: Path = args.root.resolve()
        if args.factor_eval_dir is None:
            factor_eval_dir = root / "reports" / "factor_eval"
        else:
            factor_eval_dir = (
                args.factor_eval_dir
                if args.factor_eval_dir.is_absolute()
                else (root / args.factor_eval_dir)
            )

        if args.rules is None:
            rules_path = None
        else:
            rules_path = (
                args.rules if args.rules.is_absolute() else (root / args.rules)
            )

        if args.windows:
            windows = [w.strip() for w in str(args.windows).split(",") if w.strip()]
        else:
            windows = None

        factor_ids_filter = args.factor_ids if args.factor_ids else None

        rows = analyze_factor_eval(
            root=root,
            factor_eval_dir=factor_eval_dir,
            rules_path=rules_path,
            windows_filter=windows,
            factor_ids_filter=factor_ids_filter,
            min_sample_days=args.min_days,
        )

        print(_format_eval_table(rows))
        print()
        print(
            textwrap.dedent(
                """
                說明：
                  - rank_ic / ic      ：為方便閱讀，使用 mean 值（若有 *_mean 欄位則優先）。
                  - cov_mean          ：橫斷面 coverage 平均值（0~1；越接近 1 越好）。
                  - days              ：sample_days，代表此視窗內實際統計到的交易日數。
                  - note              ：例如「樣本天數偏少」「無 IC 指標」等補充說明。
                """.rstrip()
            )
        )
    else:
        parser.error(f"未知子指令：{cmd!r}")


if __name__ == "__main__":
    main()
