# alpha_core/phase2/corelib/capacity_lib.py
from __future__ import annotations

import logging
import math
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

LOG = logging.getLogger("capacity_lib")


@dataclass
class CapacityConfig:
    """Capacity SLO configuration for factor combos."""
    topn: int = 20
    max_single_name_weight: float = 0.05
    max_adv_participation_pct: float = 0.05
    max_turnover: float = 0.5
    min_coverage: float = 1000.0


@dataclass
class FactorCapacityInput:
    factor_id: str
    window: int
    rank_ic: Optional[float]
    ic: Optional[float]
    turnover: Optional[float]
    coverage: Optional[float]


@dataclass
class FactorCapacityResult:
    factor_id: str
    window: int
    turnover: Optional[float]
    coverage: Optional[float]
    ok_turnover: Optional[bool]
    ok_coverage: Optional[bool]
    ok_adv: Optional[bool]
    capacity_score: float
    missing_eval: bool = False


@dataclass
class CapacitySummary:
    as_of: str
    windows: List[int]
    config: CapacityConfig
    per_factor: List[FactorCapacityResult]
    all_pass: bool
    failed_factors: List[str]


def _norm_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def load_capacity_config(
    root: Path,
    rules_path: Optional[Path] = None,
    overrides: Optional[Mapping[str, Any]] = None,
) -> CapacityConfig:
    """
    Load capacity SLO config from rules_factors.yaml if available.

    Tolerant structure:
      capacity:
        topn: 20
        max_turnover: 0.5
        min_coverage: 1000
        ...
    """
    root = root.resolve()
    path = (rules_path or (root / "rules_factors.yaml")).resolve()

    cfg = CapacityConfig()

    data: Mapping[str, Any] = {}
    if path.is_file() and yaml is not None:
        try:
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            if isinstance(loaded, Mapping):
                data = loaded
        except Exception as exc:  # pragma: no cover
            LOG.warning("Failed to load rules_factors.yaml: %s", exc)

    cap = data.get("capacity") if isinstance(data, Mapping) else None
    cap_cfg: Mapping[str, Any] = cap if isinstance(cap, Mapping) else {}

    def _get_float(name: str, default: float) -> float:
        try:
            return float(cap_cfg.get(name, default))
        except Exception:
            return float(default)

    try:
        cfg.topn = int(cap_cfg.get("topn", cfg.topn))
    except Exception:
        pass
    cfg.max_single_name_weight = _get_float("max_single_name_weight", cfg.max_single_name_weight)
    cfg.max_adv_participation_pct = _get_float("max_adv_participation_pct", cfg.max_adv_participation_pct)
    cfg.max_turnover = _get_float("max_turnover", cfg.max_turnover)
    cfg.min_coverage = _get_float("min_coverage", cfg.min_coverage)

    if overrides:
        if "topn" in overrides:
            cfg.topn = int(overrides["topn"])  # type: ignore[arg-type]
        if "max_single_name_weight" in overrides:
            cfg.max_single_name_weight = float(overrides["max_single_name_weight"])  # type: ignore[arg-type]
        if "max_adv_participation_pct" in overrides:
            cfg.max_adv_participation_pct = float(overrides["max_adv_participation_pct"])  # type: ignore[arg-type]
        if "max_turnover" in overrides:
            cfg.max_turnover = float(overrides["max_turnover"])  # type: ignore[arg-type]
        if "min_coverage" in overrides:
            cfg.min_coverage = float(overrides["min_coverage"])  # type: ignore[arg-type]

    return cfg


def load_combo_plan(path: Path) -> Mapping[str, Any]:
    """
    Expected structure (produced by scripts/p2/factor_combo.py):
      - as_of: "YYYY-MM-DD"
      - windows_selected: {"6": [...], ...}
      - score_table: [{factor_id, window, rank_ic, ic, turnover, coverage, ...}, ...]
      - meta: {...}
    """
    path = path.resolve()
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, Mapping):
        raise ValueError(f"Combo plan JSON must be an object, got {type(obj)!r}")
    return obj


def build_inputs_from_combo(
    plan: Mapping[str, Any],
    windows_filter: Optional[Sequence[int]] = None,
) -> Tuple[str, List[int], List[FactorCapacityInput]]:
    """
    Extract FactorCapacityInput rows from a combo plan.
    Returns: (plan_as_of, windows, inputs)
    """
    as_of = plan.get("as_of")
    meta = plan.get("meta") or {}
    if not isinstance(as_of, str):
        as_of = meta.get("as_of")
    if not isinstance(as_of, str):
        raise ValueError("combo plan missing 'as_of'")

    windows_selected_raw = plan.get("windows_selected") or {}
    if not isinstance(windows_selected_raw, Mapping):
        raise ValueError("combo plan 'windows_selected' must be a mapping")

    windows_selected: Dict[int, List[str]] = {}
    for k, v in windows_selected_raw.items():
        try:
            win = int(k)
        except Exception:
            continue
        if not isinstance(v, list):
            continue
        windows_selected[win] = [str(x) for x in v]

    windows_all = sorted(windows_selected.keys())
    if windows_filter:
        wf = set(int(x) for x in windows_filter)
        windows = [w for w in windows_all if w in wf]
    else:
        windows = windows_all

    score_table = plan.get("score_table") or []
    score_lookup: Dict[Tuple[str, int], Mapping[str, Any]] = {}
    if isinstance(score_table, list):
        for row in score_table:
            if not isinstance(row, Mapping):
                continue
            fid = row.get("factor_id")
            win = row.get("window")
            if not isinstance(fid, str):
                continue
            try:
                win_i = int(win)
            except Exception:
                continue
            score_lookup[(fid, win_i)] = row

    inputs: List[FactorCapacityInput] = []
    for w in windows:
        for fid in windows_selected.get(w, []):
            row = score_lookup.get((fid, w), {})
            inputs.append(
                FactorCapacityInput(
                    factor_id=fid,
                    window=w,
                    rank_ic=_norm_float(row.get("rank_ic")),
                    ic=_norm_float(row.get("ic")),
                    turnover=_norm_float(row.get("turnover")),
                    # Prefer effective coverage size when present.
                    coverage=_norm_float(
                        row.get("coverage_count")
                        if row.get("coverage_count") is not None
                        else row.get("coverage")
                    ),
                )
            )

    return as_of, windows, inputs


def evaluate_capacity(
    as_of: str,
    windows: Sequence[int],
    config: CapacityConfig,
    inputs: Sequence[FactorCapacityInput],
) -> CapacitySummary:
    """
    Conservative v1 rules:
      - ok_turnover: turnover <= max_turnover
      - ok_coverage: coverage >= min_coverage
      - ok_adv: placeholder (None) (until ADV/participation data is wired)
      - capacity_score:
          * start 1.0
          * explicit violation -> min(score, 0.0)
          * unknown -> min(score, 0.5)
          * missing_eval (turnover and coverage both None) -> 0.0
      - all_pass: true iff no failed entries (including missing_eval)
    """
    per_factor: List[FactorCapacityResult] = []
    failed: List[str] = []

    for inp in inputs:
        turnover = _norm_float(inp.turnover)
        coverage = _norm_float(inp.coverage)

        missing_eval = (turnover is None) and (coverage is None)

        ok_turnover: Optional[bool] = None
        ok_coverage: Optional[bool] = None
        ok_adv: Optional[bool] = None  # not implemented in v1

        if turnover is not None:
            ok_turnover = turnover <= float(config.max_turnover)
        if coverage is not None:
            ok_coverage = coverage >= float(config.min_coverage)

        score = 1.0

        def penalize(flag: Optional[bool]) -> None:
            nonlocal score
            if flag is False:
                score = min(score, 0.0)
            elif flag is None:
                score = min(score, 0.5)

        penalize(ok_turnover)
        penalize(ok_coverage)
        penalize(ok_adv)

        if missing_eval:
            score = 0.0

        res = FactorCapacityResult(
            factor_id=inp.factor_id,
            window=int(inp.window),
            turnover=turnover,
            coverage=coverage,
            ok_turnover=ok_turnover,
            ok_coverage=ok_coverage,
            ok_adv=ok_adv,
            capacity_score=float(score),
            missing_eval=missing_eval,
        )
        per_factor.append(res)

        if missing_eval or ok_turnover is False or ok_coverage is False or ok_adv is False:
            failed.append(f"{inp.factor_id}@{int(inp.window)}")

    all_pass = len(failed) == 0
    return CapacitySummary(
        as_of=str(as_of),
        windows=sorted(set(int(w) for w in windows)),
        config=config,
        per_factor=per_factor,
        all_pass=all_pass,
        failed_factors=failed,
    )


def capacity_summary_to_json(
    summary: CapacitySummary,
    meta: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    params = {
        "topn": summary.config.topn,
        "max_single_name_weight": summary.config.max_single_name_weight,
        "max_adv_participation_pct": summary.config.max_adv_participation_pct,
        "max_turnover": summary.config.max_turnover,
        "min_coverage": summary.config.min_coverage,
    }

    per_factor_payload: List[Dict[str, Any]] = []
    min_score = 1.0 if summary.per_factor else 0.0
    for r in summary.per_factor:
        per_factor_payload.append(
            {
                "factor_id": r.factor_id,
                "window": r.window,
                "turnover": r.turnover,
                "coverage": r.coverage,
                "ok_turnover": r.ok_turnover,
                "ok_coverage": r.ok_coverage,
                "ok_adv": r.ok_adv,
                "capacity_score": r.capacity_score,
                "missing_eval": r.missing_eval,
            }
        )
        min_score = min(min_score, float(r.capacity_score))

    summary_node = {
        "all_pass": summary.all_pass,
        "num_factors": len(summary.per_factor),
        "min_capacity_score": float(min_score),
        "failed_factors": list(summary.failed_factors),
    }

    payload: Dict[str, Any] = {
        "spec_version": "factor_capacity.v1",
        "as_of": summary.as_of,
        "windows": list(summary.windows),
        "params": params,
        "per_factor": per_factor_payload,
        "summary": summary_node,
        "meta": dict(meta or {}),
    }
    return payload
