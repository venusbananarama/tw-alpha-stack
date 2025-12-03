# C:\AI\tw-alpha-stack\alpha_core\capacity_lib.py
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore


logger = logging.getLogger(__name__)


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
    """Normalize numeric values from JSON/NumPy to Python floats.

    - Returns None for None / NaN / +/-inf / non-numerics.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    return None


def load_capacity_config(
    root: Path,
    rules_path: Optional[Path] = None,
    overrides: Optional[Mapping[str, Any]] = None,
) -> CapacityConfig:
    """Load capacity SLO config from rules_factors.yaml if available.

    The structure is intentionally forgiving. It looks for a top-level
    "capacity" mapping and reads a few well-known keys. Missing values are
    filled with safe defaults.
    """
    cfg = CapacityConfig()
    path = rules_path or (root / "rules_factors.yaml")

    data: Mapping[str, Any] = {}
    if path.is_file() and yaml is not None:
        try:
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            if isinstance(loaded, Mapping):
                data = loaded
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to load rules_factors.yaml for capacity: %s", exc)

    cap_cfg: Mapping[str, Any] = {}
    if isinstance(data, Mapping) and isinstance(data.get("capacity"), Mapping):
        cap_cfg = data["capacity"]  # type: ignore[assignment]

    def _get(name: str, default: float) -> float:
        raw = cap_cfg.get(name, default)
        try:
            return float(raw)
        except Exception:
            return default

    cfg.topn = int(cap_cfg.get("topn", cfg.topn))
    cfg.max_single_name_weight = _get("max_single_name_weight", cfg.max_single_name_weight)
    cfg.max_adv_participation_pct = _get("max_adv_participation_pct", cfg.max_adv_participation_pct)
    cfg.max_turnover = _get("max_turnover", cfg.max_turnover)
    cfg.min_coverage = _get("min_coverage", cfg.min_coverage)

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

    logger.debug("CapacityConfig loaded: %s", cfg)
    return cfg


def load_combo_plan(path: Path) -> Mapping[str, Any]:
    """Load a factor combo plan JSON file.

    The expected structure is the one produced by scripts/factor_combo.py,
    containing at least:

    - as_of: str
    - windows_selected: {"6": [factor_id, ...], ...}
    - score_table: list of {factor_id, window, rank_ic, ic, turnover, coverage, ...}
    """
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, Mapping):
        raise ValueError(f"Combo plan JSON must be an object, got {type(data)!r}")
    return data


def build_inputs_from_combo(
    plan: Mapping[str, Any],
    windows_filter: Optional[Sequence[int]] = None,
) -> Tuple[str, List[int], List[FactorCapacityInput]]:
    """Extract FactorCapacityInput rows from a combo plan.

    Returns (as_of, windows, inputs).
    """
    meta = plan.get("meta") or {}
    as_of = plan.get("as_of") or meta.get("as_of")
    if not isinstance(as_of, str):
        raise ValueError("combo plan missing 'as_of' field")

    windows_selected_raw = plan.get("windows_selected") or {}
    if not isinstance(windows_selected_raw, Mapping):
        raise ValueError("combo plan 'windows_selected' must be a mapping")

    # Normalize window keys to int -> List[str]
    windows_selected: Dict[int, List[str]] = {}
    for key, value in windows_selected_raw.items():
        try:
            win = int(key)
        except Exception:
            continue
        if not isinstance(value, list):
            continue
        factors = [str(fid) for fid in value]
        windows_selected[win] = factors

    # Determine the universe of windows
    windows_from_plan = sorted(windows_selected.keys())
    if windows_filter:
        windows = [w for w in windows_from_plan if w in set(windows_filter)]
    else:
        windows = windows_from_plan

    # Build a lookup from score_table[(factor_id, window)] -> dict
    score_table_raw = plan.get("score_table") or []
    score_lookup: Dict[Tuple[str, int], Mapping[str, Any]] = {}
    if isinstance(score_table_raw, list):
        for row in score_table_raw:
            if not isinstance(row, Mapping):
                continue
            fid = row.get("factor_id")
            win = row.get("window")
            if not isinstance(fid, str):
                continue
            try:
                win_int = int(win)
            except Exception:
                continue
            score_lookup[(fid, win_int)] = row

    inputs: List[FactorCapacityInput] = []
    for win in windows:
        factor_ids = windows_selected.get(win, [])
        for fid in factor_ids:
            row = score_lookup.get((fid, win), {})
            rank_ic = _norm_float(row.get("rank_ic"))
            ic = _norm_float(row.get("ic"))
            turnover = _norm_float(row.get("turnover"))
            coverage = _norm_float(row.get("coverage"))
            inputs.append(
                FactorCapacityInput(
                    factor_id=fid,
                    window=win,
                    rank_ic=rank_ic,
                    ic=ic,
                    turnover=turnover,
                    coverage=coverage,
                )
            )

    return as_of, windows, inputs


def evaluate_capacity(
    as_of: str,
    windows: Sequence[int],
    config: CapacityConfig,
    inputs: Sequence[FactorCapacityInput],
) -> CapacitySummary:
    """Evaluate capacity SLO for a set of factors.

    The logic is intentionally simple and conservative for v1:

    - ok_turnover: True if turnover <= max_turnover, False otherwise, None if unknown.
    - ok_coverage: True if coverage >= min_coverage, False otherwise, None if unknown.
    - ok_adv: currently always None (no ADV estimates yet).
    - capacity_score:
        * start at 1.0
        * each explicit violation (flag is False) -> score = 0.0
        * each unknown (flag is None)          -> score = min(score, 0.5)
        * missing_eval (no turnover & coverage) -> score = 0.0
    - all_pass: True iff there is no explicit violation and no missing_eval.
    """
    per_factor: List[FactorCapacityResult] = []
    failed: List[str] = []

    for inp in inputs:
        turnover = _norm_float(inp.turnover)
        coverage = _norm_float(inp.coverage)

        missing_eval = turnover is None and coverage is None

        ok_turnover: Optional[bool] = None
        ok_coverage: Optional[bool] = None
        ok_adv: Optional[bool] = None  # reserved for future use

        if turnover is not None and config.max_turnover is not None:
            ok_turnover = turnover <= config.max_turnover

        if coverage is not None and config.min_coverage is not None:
            ok_coverage = coverage >= config.min_coverage

        score = 1.0

        def penalize(flag: Optional[bool]) -> None:
            nonlocal score
            if flag is False:
                score = 0.0
            elif flag is None:
                score = min(score, 0.5)

        penalize(ok_turnover)
        penalize(ok_coverage)
        penalize(ok_adv)

        if missing_eval:
            score = 0.0

        result = FactorCapacityResult(
            factor_id=inp.factor_id,
            window=inp.window,
            turnover=turnover,
            coverage=coverage,
            ok_turnover=ok_turnover,
            ok_coverage=ok_coverage,
            ok_adv=ok_adv,
            capacity_score=score,
            missing_eval=missing_eval,
        )
        per_factor.append(result)

        if missing_eval or ok_turnover is False or ok_coverage is False or ok_adv is False:
            failed.append(f"{inp.factor_id}@{inp.window}")

    all_pass = len(failed) == 0
    summary = CapacitySummary(
        as_of=as_of,
        windows=sorted(set(int(w) for w in windows)),
        config=config,
        per_factor=per_factor,
        all_pass=all_pass,
        failed_factors=failed,
    )
    return summary


def capacity_summary_to_json(
    summary: CapacitySummary,
    meta: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    """Convert CapacitySummary into a JSON-serializable dict."""
    params = {
        "topn": summary.config.topn,
        "max_single_name_weight": summary.config.max_single_name_weight,
        "max_adv_participation_pct": summary.config.max_adv_participation_pct,
        "max_turnover": summary.config.max_turnover,
        "min_coverage": summary.config.min_coverage,
    }

    per_factor_payload: List[Dict[str, Any]] = []
    min_score: float = 1.0 if summary.per_factor else 0.0
    for res in summary.per_factor:
        per_factor_payload.append(
            {
                "factor_id": res.factor_id,
                "window": res.window,
                "turnover": res.turnover,
                "coverage": res.coverage,
                "ok_turnover": res.ok_turnover,
                "ok_coverage": res.ok_coverage,
                "ok_adv": res.ok_adv,
                "capacity_score": res.capacity_score,
                "missing_eval": res.missing_eval,
            }
        )
        min_score = min(min_score, float(res.capacity_score))

    summary_node = {
        "all_pass": summary.all_pass,
        "num_factors": len(summary.per_factor),
        "min_capacity_score": min_score,
        "failed_factors": list(summary.failed_factors),
    }

    payload: Dict[str, Any] = {
        "as_of": summary.as_of,
        "windows": list(summary.windows),
        "params": params,
        "per_factor": per_factor_payload,
        "summary": summary_node,
        "meta": dict(meta or {}),
    }
    return payload
