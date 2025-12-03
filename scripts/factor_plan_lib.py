#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/factor_plan_lib.py

Phase-2 因子層：因子計畫 (factor plan) 產生器。

角色：
- 讀取 rules_factors.yaml（因子 registry + gate_ready SLO）。
- 讀取可選的 factor_status.json。
- 針對指定的 as-of date / profile / engine_kind（classic / ai），
  產生一份 deterministic 的因子執行計畫：
  - 每顆 factor 的 decided_action：compute+eval / eval_only / skip。
  - 對應的 WF 視窗、理由(reasons)、SLO 覆蓋統計等。

這個模組可以被：
- tools/factors/Run-Phase2-OneClick.ps1 以 Python 函式方式呼叫，或
- 直接以 CLI 方式呼叫（for debug / ad-hoc）。
"""

from __future__ import annotations

import dataclasses
import json
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Literal

try:
    import yaml  # type: ignore[import]
except Exception as e:  # pragma: no cover - import guard
    raise RuntimeError("PyYAML is required to use factor_plan_lib") from e


# ---------------------------------------------------------------------------
# Dataclasses：因子計畫的核心結構
# ---------------------------------------------------------------------------


@dataclass
class FactorPlanItem:
    """
    單一因子在某次計畫中的決策結果。
    """

    factor_id: str
    category: str
    engine: Optional[str]
    enabled: bool
    decided_action: Literal["compute+eval", "eval_only", "skip"]
    reasons: List[str] = field(default_factory=list)
    wf_windows: List[int] = field(default_factory=list)

    # 附屬資訊（多半直接從 rules_factors.yaml 帶出，方便後續使用）
    data_freq: Optional[str] = None
    universe: Optional[str] = None
    gate_rules: Optional[Mapping[str, Any]] = None
    status: Optional[Mapping[str, Any]] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FactorPlanSummary:
    """
    整份因子計畫的摘要與 SLO 覆蓋狀態。
    """

    total_factors: int
    enabled_factors: int
    compute_and_eval: int
    eval_only: int
    skip: int

    profile: str
    engine_kind: str
    wf_windows: List[int]

    # SLO 覆蓋
    meets_profile_slo: bool
    slo_expected_min_factors: int
    slo_expected_min_per_window: Dict[str, int]

    # active = compute+eval + eval_only
    active_factors: int
    active_per_window: Dict[str, int]


@dataclass
class FactorPlan:
    """
    某個 as-of date / profile / engine 的完整因子計畫。
    """

    as_of_date: str
    generated_at: str
    profile: str
    engine_kind: str
    wf_windows: List[int]
    items: List[FactorPlanItem]
    summary: FactorPlanSummary
    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 基本 IO：讀 rules_factors / factor_status
# ---------------------------------------------------------------------------


def _parse_date(date_str: str) -> dt.date:
    """
    確認輸入是合法的 ISO 日期（YYYY-MM-DD），並回傳 datetime.date。
    """
    try:
        return dt.date.fromisoformat(date_str)
    except ValueError as e:
        raise ValueError(f"Invalid ISO date string: {date_str!r}") from e


def load_rules_factors(root: Path, rules_path: Optional[Path] = None) -> Mapping[str, Any]:
    """
    讀取 rules_factors.yaml，回傳原始 Mapping。

    Parameters
    ----------
    root:
        repo root（例：C:\\AI\\tw-alpha-stack）。
    rules_path:
        明確指定 rules_factors.yaml 位置；若為 None，預設 root / "rules_factors.yaml"。
    """
    if rules_path is None:
        rules_path = root / "rules_factors.yaml"
    if not rules_path.exists():
        raise FileNotFoundError(f"rules_factors.yaml not found at {rules_path}")
    with rules_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, Mapping):
        raise ValueError(f"rules_factors.yaml must be a mapping at top-level, got {type(data)!r}")
    return data


def load_factor_status(root: Path, status_path: Optional[Path] = None) -> Mapping[str, Any]:
    """
    讀取 factor_status.json（若存在）。

    - 若檔案不存在 → 回傳 {}。
    - 若是 JSON object → 原樣回傳。
    - 若是 JSON array → 包成 {"items": [...]}。
    """
    if status_path is None:
        status_path = root / "reports" / "factor_status.json"
    if not status_path.exists():
        return {}
    with status_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, Mapping):
        return data
    if isinstance(data, list):
        return {"items": data}
    raise ValueError(f"factor_status.json must be an object or array, got {type(data)!r}")


def _index_factor_status(raw_status: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    """
    從鬆散結構的 status 資料，建立 factor_id → status_entry 的索引。

    優先假設格式：
      {"factors": {"mom_12m": {...}, ...}}

    若無 "factors" 欄位，就嘗試把頂層 key 當作 factor_id。
    """
    if not raw_status:
        return {}

    factors = raw_status.get("factors")
    if isinstance(factors, Mapping):
        out: Dict[str, Mapping[str, Any]] = {}
        for fid, entry in factors.items():
            if isinstance(entry, Mapping):
                out[str(fid)] = entry
        if out:
            return out

    out: Dict[str, Mapping[str, Any]] = {}
    for k, v in raw_status.items():
        if isinstance(v, Mapping):
            out[str(k)] = v
    return out


def _get_profile_slo(rules: Mapping[str, Any], profile: str) -> Tuple[int, Dict[str, int]]:
    """
    從 rules_factors.gate_ready.* 中，取出指定 profile 的 SLO 門檻：
    - min_factors
    - per_window["6"/"12"/"24"].min_factors
    """
    gate_ready = rules.get("gate_ready") or {}
    profiles = gate_ready.get("profiles") or {}
    profile_cfg = profiles.get(profile)

    if not isinstance(profile_cfg, Mapping):
        # 沒有對應 profile，就退回全域 gate_ready
        min_factors = int(gate_ready.get("min_factors", 0))
        per_window_cfg = gate_ready.get("per_window") or {}
    else:
        min_factors = int(profile_cfg.get("min_factors", gate_ready.get("min_factors", 0)))
        per_window_cfg = profile_cfg.get("per_window") or gate_ready.get("per_window") or {}

    per_window_min: Dict[str, int] = {}
    if isinstance(per_window_cfg, Mapping):
        for w, cfg in per_window_cfg.items():
            if isinstance(cfg, Mapping):
                try:
                    per_window_min[str(w)] = int(cfg.get("min_factors", 0))
                except (TypeError, ValueError):
                    per_window_min[str(w)] = 0
    return min_factors, per_window_min


def _infer_default_wf_windows(rules: Mapping[str, Any], engine_kind: str) -> List[int]:
    """
    若呼叫端沒有指定 wf_windows，就從 rules_factors.engines[engine_kind].wf_windows 推出預設值。
    """
    engines = rules.get("engines") or {}
    if isinstance(engines, Mapping):
        eng = engines.get(engine_kind)
        if isinstance(eng, Mapping):
            wf_windows = eng.get("wf_windows")
            from collections.abc import Sequence as SeqABC
            if isinstance(wf_windows, SeqABC) and not isinstance(wf_windows, (str, bytes)):
                out: List[int] = []
                for w in wf_windows:
                    try:
                        out.append(int(w))
                    except (TypeError, ValueError):
                        continue
                if out:
                    return sorted(set(out))
    # fallback
    return [6, 12, 24]


# ---------------------------------------------------------------------------
# 核心邏輯：build_factor_plan
# ---------------------------------------------------------------------------


def build_factor_plan(
    as_of_date: str,
    profile: str,
    engine_kind: str,
    wf_windows: Sequence[int] | None,
    rules: Mapping[str, Any],
    factor_status: Optional[Mapping[str, Any]] = None,
) -> FactorPlan:
    """
    建立某日 / profile / engine 的因子執行計畫。

    Parameters
    ----------
    as_of_date:
        W-FRI / 評估日（YYYY-MM-DD）。
    profile:
        SLO profile 名稱（例：dev / test / live / prod）。
    engine_kind:
        要規劃的 engine 類型（通常對應 factors[*].category，例如 "classic" / "ai"）。
    wf_windows:
        本次要考慮的 WF 視窗（單位：月）。若 None 或空集合，則從
        rules_factors.engines[engine_kind].wf_windows 推出預設值。
    rules:
        已解析的 rules_factors.yaml。
    factor_status:
        可選的 status payload（結構鬆散）。若含有 desired_action / state 等欄位，
        會影響 decided_action 與 reasons。

    Returns
    -------
    FactorPlan
        含 per-factor 決策與整體 SLO 統計。
    """
    _parse_date(as_of_date)  # 先做輸入檢查

    engine_kind = str(engine_kind)
    if wf_windows:
        requested_windows = sorted({int(w) for w in wf_windows})
    else:
        requested_windows = _infer_default_wf_windows(rules, engine_kind)

    raw_factors = rules.get("factors") or []
    if not isinstance(raw_factors, Sequence):
        raise ValueError("rules_factors.yaml factors must be a sequence")

    status_index = _index_factor_status(factor_status or {})
    items: List[FactorPlanItem] = []

    for entry in raw_factors:
        if not isinstance(entry, Mapping):
            continue

        factor_id = str(entry.get("factor_id") or "").strip()
        if not factor_id:
            continue

        category = str(entry.get("category") or "").strip() or "classic"
        if category != engine_kind:
            # 只規劃指定 engine_kind 下的因子
            continue

        enabled_flag = bool(entry.get("enabled", False))
        engine_name = entry.get("engine")
        data_freq = entry.get("data_freq")
        universe = entry.get("universe")

        # WF 視窗：以 factor 自己宣告的 wf_windows 與 requested_windows 交集為準
        wf = entry.get("wf_windows") or []
        factor_windows: List[int] = []
        if isinstance(wf, Sequence) and not isinstance(wf, (str, bytes)):
            for w in wf:
                try:
                    factor_windows.append(int(w))
                except (TypeError, ValueError):
                    continue
        if not factor_windows:
            factor_windows = requested_windows
        effective_windows = sorted({w for w in factor_windows if w in requested_windows})

        status_entry = status_index.get(factor_id)
        gate_rules = entry.get("gate_rules") if isinstance(entry.get("gate_rules"), Mapping) else None

        reasons: List[str] = []
        reasons.append(f"category={category}")
        reasons.append(f"engine={engine_name}")
        if enabled_flag:
            reasons.append("enabled=true in rules_factors")
        else:
            reasons.append("enabled=false in rules_factors")

        # 預設決策：enabled → compute+eval；disabled → skip
        decided_action: Literal["compute+eval", "eval_only", "skip"]
        if not enabled_flag:
            decided_action = "skip"
        else:
            decided_action = "compute+eval"

        # 若 factor_status 中有 desired_action，就可以覆寫預設
        if status_entry:
            desired = status_entry.get("desired_action")
            if isinstance(desired, str):
                desired_norm = desired.strip().lower()
                mapping = {
                    "compute+eval": "compute+eval",
                    "compute_eval": "compute+eval",
                    "compute-and-eval": "compute+eval",
                    "eval_only": "eval_only",
                    "eval-only": "eval_only",
                    "eval": "eval_only",
                    "skip": "skip",
                    "disabled": "skip",
                }
                mapped = mapping.get(desired_norm)
                if mapped is not None:
                    decided_action = mapped  # type: ignore[assignment]
                    reasons.append(f"override from factor_status.desired_action={desired}")

            state = status_entry.get("state")
            if isinstance(state, str):
                reasons.append(f"status.state={state}")
            # 把 required_action 也記錄進 reasons（方便 debug）
            req = status_entry.get("required_action")
            if isinstance(req, str):
                reasons.append(f"status.required_action={req}")

        # 多餘欄位直接丟到 extras，保留彈性
        extras: Dict[str, Any] = {}
        for k, v in entry.items():
            if k in {
                "factor_id",
                "category",
                "engine",
                "enabled",
                "wf_windows",
                "data_freq",
                "universe",
                "gate_rules",
                "description",
                "impl_notes",
            }:
                continue
            extras[k] = v

        item = FactorPlanItem(
            factor_id=factor_id,
            category=category,
            engine=str(engine_name) if engine_name is not None else None,
            enabled=enabled_flag,
            decided_action=decided_action,
            reasons=reasons,
            wf_windows=effective_windows,
            data_freq=str(data_freq) if data_freq is not None else None,
            universe=str(universe) if universe is not None else None,
            gate_rules=gate_rules,
            status=status_entry,
            extras=extras,
        )
        items.append(item)

    # -----------------------------------------------------------------------
    # Summary + SLO 覆蓋
    # -----------------------------------------------------------------------
    total = len(items)
    enabled_count = sum(1 for it in items if it.enabled)
    compute_and_eval = sum(1 for it in items if it.decided_action == "compute+eval")
    eval_only_count = sum(1 for it in items if it.decided_action == "eval_only")
    skip_count = sum(1 for it in items if it.decided_action == "skip")

    min_factors_slo, per_window_min = _get_profile_slo(rules, profile)

    # active = compute+eval + eval_only
    active_items = [it for it in items if it.decided_action in ("compute+eval", "eval_only")]
    active_count = len(active_items)

    active_per_window: Dict[str, int] = {}
    for w in requested_windows:
        key = str(w)
        active_per_window[key] = sum(1 for it in active_items if w in it.wf_windows)

    meets_total = active_count >= min_factors_slo if min_factors_slo > 0 else True
    meets_each_window = True
    for w_key, min_cnt in per_window_min.items():
        if min_cnt <= 0:
            continue
        actual = active_per_window.get(w_key, 0)
        if actual < min_cnt:
            meets_each_window = False
            break
    meets_profile_slo = meets_total and meets_each_window

    summary = FactorPlanSummary(
        total_factors=total,
        enabled_factors=enabled_count,
        compute_and_eval=compute_and_eval,
        eval_only=eval_only_count,
        skip=skip_count,
        profile=profile,
        engine_kind=engine_kind,
        wf_windows=requested_windows,
        meets_profile_slo=meets_profile_slo,
        slo_expected_min_factors=min_factors_slo,
        slo_expected_min_per_window=per_window_min,
        active_factors=active_count,
        active_per_window=active_per_window,
    )

    metadata: Dict[str, Any] = {
        "schema_version": 1,
        "source": "factor_plan_lib",
    }

    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()

    return FactorPlan(
        as_of_date=as_of_date,
        generated_at=generated_at,
        profile=profile,
        engine_kind=engine_kind,
        wf_windows=requested_windows,
        items=items,
        summary=summary,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# JSON 輸出工具
# ---------------------------------------------------------------------------


def factor_plan_to_json_dict(plan: FactorPlan) -> Dict[str, Any]:
    """
    將 FactorPlan dataclass 轉成可 JSON 序列化的 dict。
    """
    return {
        "as_of_date": plan.as_of_date,
        "generated_at": plan.generated_at,
        "profile": plan.profile,
        "engine_kind": plan.engine_kind,
        "wf_windows": plan.wf_windows,
        "summary": dataclasses.asdict(plan.summary),
        "items": [dataclasses.asdict(item) for item in plan.items],
        "metadata": plan.metadata,
    }


def save_factor_plan(plan: FactorPlan, path: Path) -> None:
    """
    將因子計畫寫入檔案（pretty-printed JSON）。
    """
    payload = factor_plan_to_json_dict(plan)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# 簡易 CLI：方便 debug / ad-hoc 使用
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    允許直接執行本檔：

        python scripts/factor_plan_lib.py \
          --root . \
          --date 2025-11-10 \
          --profile dev \
          --engine classic

    Run-Phase2-OneClick.ps1 可以：
    - 直接匯入 build_factor_plan() / save_factor_plan()，或
    - 以 CLI 模式呼叫本程式（視你當初 B 段規劃而定）。
    """
    import argparse

    parser = argparse.ArgumentParser(description="Build Phase-2 factor execution plan.")
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root (default: current directory).",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="As-of date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="dev",
        help="SLO profile (dev/test/live/prod).",
    )
    parser.add_argument(
        "--engine",
        dest="engine_kind",
        type=str,
        default="classic",
        help="Engine kind / factor category (e.g. classic, ai).",
    )
    parser.add_argument(
        "--wf-window",
        dest="wf_windows",
        action="append",
        type=int,
        help=(
            "WF 視窗（月）。可重複指定多次；"
            "若完全不指定，則使用 rules_factors.engines[engine].wf_windows。"
        ),
    )
    parser.add_argument(
        "--rules-path",
        type=str,
        default=None,
        help="Optional explicit path to rules_factors.yaml.",
    )
    parser.add_argument(
        "--status-path",
        type=str,
        default=None,
        help="Optional explicit path to factor_status.json.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for factor_plan JSON. "
             "Default: <root>/reports/factor_plan.<date>.<engine>.json",
    )

    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    rules_path = Path(args.rules_path) if args.rules_path else None
    status_path = Path(args.status_path) if args.status_path else None

    rules = load_rules_factors(root, rules_path)
    status = load_factor_status(root, status_path)

    plan = build_factor_plan(
        as_of_date=args.date,
        profile=args.profile,
        engine_kind=args.engine_kind,
        wf_windows=args.wf_windows,
        rules=rules,
        factor_status=status,
    )

    if args.output:
        out_path = Path(args.output)
    else:
        # 例：reports/factor_plan.2025-11-10.classic.json
        out_path = root / "reports" / f"factor_plan.{args.date}.{args.engine_kind}.json"

    save_factor_plan(plan, out_path)

    s = plan.summary
    print(
        f"[factor_plan] date={plan.as_of_date} profile={s.profile} engine={s.engine_kind} "
        f"wf_windows={s.wf_windows} active={s.active_factors} "
        f"compute+eval={s.compute_and_eval} eval_only={s.eval_only} skip={s.skip} "
        f"SLO_ok={s.meets_profile_slo}"
    )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
