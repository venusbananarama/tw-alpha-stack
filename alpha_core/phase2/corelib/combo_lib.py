# alpha_core/phase2/corelib/combo_lib.py
# -*- coding: utf-8 -*-
"""
Phase-2 Step-3: Factor Combo Plan (corelib)

- Build per-window factor score table from reports/factor_eval/*_summary.json
- (Optional) apply de-correlation selection using a corr matrix (csv/parquet)
- Emit a combo plan JSON consumed by CLI entry: scripts/p2/factor_combo.py

Design:
- deterministic / idempotent: same inputs -> same outputs
- schema-tolerant: try best-effort extraction from common factor_eval summary keys
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd


@dataclass
class FactorWindowMetrics:
    """Metrics for one factor in one window."""
    factor_id: str
    window: int
    score: float
    rank_ic: Optional[float] = None
    ic: Optional[float] = None
    sharpe: Optional[float] = None
    psr: Optional[float] = None
    t_stat: Optional[float] = None
    turnover: Optional[float] = None
    coverage: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FactorComboPlan:
    """
    windows_selected: per window selected factor_id list
    score_table: list of per (factor_id, window) metric rows (records)
    meta: spec/version/params
    """
    as_of: str
    windows_selected: Dict[int, List[str]]
    score_table: List[Dict[str, Any]]
    meta: Dict[str, Any]

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "as_of": self.as_of,
            "windows_selected": {str(k): v for k, v in self.windows_selected.items()},
            "score_table": self.score_table,
            "meta": self.meta,
        }


_SCORE_METRIC_CANDIDATES: Sequence[tuple[str, float]] = (
    ("rank_ic", 1.0),
    ("rank_ic_mean", 1.0),
    ("ic", 0.8),
    ("ic_mean", 0.8),
    ("sharpe_after_costs", 0.7),
    ("sharpe", 0.7),
    ("psr", 0.6),
    ("t_stat", 0.6),
    ("t_value", 0.6),
)


def _load_json(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, Mapping):
        raise ValueError(f"JSON root must be an object: {path}")
    return obj


def _extract_numeric(m: Mapping[str, Any], key: str) -> Optional[float]:
    if key not in m:
        return None
    v = m[key]
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _discover_factor_eval_files(root: Path, factor_ids: Optional[Iterable[str]] = None) -> Dict[str, Path]:
    """
    Discover reports/factor_eval/*_summary.json
    Filename convention: <factor_id>_summary.json
    """
    root = root.resolve()
    eval_dir = root / "reports" / "factor_eval"
    if not eval_dir.is_dir():
        return {}

    wanted = set(factor_ids) if factor_ids is not None else None

    result: Dict[str, Path] = {}
    for p in eval_dir.glob("*_summary.json"):
        stem = p.stem  # "<factor_id>_summary"
        if not stem.endswith("_summary"):
            continue
        factor_id = stem[: -len("_summary")]
        if wanted is not None and factor_id not in wanted:
            continue
        result[factor_id] = p

    return result


def _choose_window_block(summary: Mapping[str, Any], window: int) -> Mapping[str, Any]:
    """
    Expected (tolerant) structure:
    {
      "windows": {"6": {...}, "12": {...}},
      ...
    }
    If missing, return entire summary.
    """
    windows = summary.get("windows")
    if isinstance(windows, Mapping):
        block = windows.get(str(window))
        if isinstance(block, Mapping):
            return block
        block = windows.get(window)
        if isinstance(block, Mapping):
            return block
    return summary


def _compute_metrics(block: Mapping[str, Any]) -> Dict[str, Optional[float]]:
    rank_ic = (
        _extract_numeric(block, "rank_ic")
        or _extract_numeric(block, "weekly_rank_ic")
        or _extract_numeric(block, "rank_ic_mean")
    )
    ic = _extract_numeric(block, "ic") or _extract_numeric(block, "ic_mean")
    sharpe = _extract_numeric(block, "sharpe_after_costs") or _extract_numeric(block, "sharpe")
    psr = _extract_numeric(block, "psr")
    t_stat = _extract_numeric(block, "t_stat") or _extract_numeric(block, "t_value")
    turnover = _extract_numeric(block, "turnover")
    # In current factor_eval summaries, coverage/coverage_ratio are often ratio-like values.
    # The effective coverage size is usually in coverage_count.
    coverage = _extract_numeric(block, "coverage") or _extract_numeric(block, "coverage_ratio")
    coverage_count = (
        _extract_numeric(block, "coverage_count")
        or _extract_numeric(block, "coverage_n")
        or _extract_numeric(block, "coverage_num")
    )
    return {
        "rank_ic": rank_ic,
        "ic": ic,
        "sharpe": sharpe,
        "psr": psr,
        "t_stat": t_stat,
        "turnover": turnover,
        "coverage": coverage,
        "coverage_count": coverage_count,
    }


def _compute_score(block: Mapping[str, Any]) -> float:
    """
    Weighted average over available candidate metrics.
    If no metric available -> 0.0
    """
    score = 0.0
    wsum = 0.0
    for key, w in _SCORE_METRIC_CANDIDATES:
        val = _extract_numeric(block, key)
        if val is None:
            continue
        score += w * float(val)
        wsum += abs(float(w))
    return (score / wsum) if wsum > 0 else 0.0


def build_score_table(
    root: Path,
    as_of: str,
    windows: Sequence[int],
    factor_ids: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
      factor_id, window, score, rank_ic, ic, sharpe, psr, t_stat, turnover, coverage
    """
    root = root.resolve()
    windows = [int(w) for w in windows]

    files = _discover_factor_eval_files(root=root, factor_ids=factor_ids)
    records: List[Dict[str, Any]] = []

    for fid in sorted(files.keys()):  # deterministic
        path = files[fid]
        try:
            summary = _load_json(path)
        except Exception as exc:
            print(f"[combo_lib] WARNING: failed to read {path}: {exc}")
            continue

        for w in windows:
            block = _choose_window_block(summary, w)
            metrics = _compute_metrics(block)
            score = _compute_score(block)

            row: Dict[str, Any] = {
                "factor_id": fid,
                "window": int(w),
                "score": float(score),
                **metrics,
            }
            records.append(row)

    if not records:
        raise RuntimeError(
            f"[combo_lib] no valid factor_eval summaries found. root={root} as_of={as_of} windows={windows}"
        )

    df = pd.DataFrame.from_records(records)
    df.sort_values(["window", "score", "factor_id"], ascending=[True, False, True], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _load_corr_matrix(corr_path: Optional[Path]) -> Optional[pd.DataFrame]:
    if corr_path is None:
        return None
    corr_path = corr_path.resolve()
    if not corr_path.exists():
        print(f"[combo_lib] WARNING: corr file not found: {corr_path}; ignore corr")
        return None

    if corr_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(corr_path)
    else:
        df = pd.read_csv(corr_path, index_col=0)

    # best-effort normalization
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    return df


def select_factors_for_window(
    df_scores: pd.DataFrame,
    window: int,
    max_factors: int,
    corr_matrix: Optional[pd.DataFrame] = None,
    max_corr: float = 0.7,
) -> List[str]:
    """
    Select factors for one window by score, optionally applying abs(corr) <= max_corr.
    """
    sub = df_scores[df_scores["window"] == int(window)].copy()
    if sub.empty:
        return []

    selected: List[str] = []
    for _, row in sub.iterrows():
        fid = str(row["factor_id"])
        if fid in selected:
            continue

        if corr_matrix is not None and fid in corr_matrix.index:
            ok = True
            for s in selected:
                if s not in corr_matrix.columns:
                    continue
                try:
                    v = float(corr_matrix.loc[fid, s])
                except Exception:
                    continue
                if abs(v) > float(max_corr):
                    ok = False
                    break
            if not ok:
                continue

        selected.append(fid)
        if len(selected) >= int(max_factors):
            break

    return selected


def build_combo_plan(
    root: Path,
    as_of: str,
    windows: Sequence[int],
    max_factors_per_window: int,
    factor_ids: Optional[Iterable[str]] = None,
    corr_path: Optional[Path] = None,
    max_corr: float = 0.7,
    spec_version: str = "factor_combo.v1",
) -> FactorComboPlan:
    root = root.resolve()
    windows = [int(w) for w in windows]

    df_scores = build_score_table(root=root, as_of=as_of, windows=windows, factor_ids=factor_ids)
    corr = _load_corr_matrix(corr_path)

    windows_selected: Dict[int, List[str]] = {}
    for w in windows:
        windows_selected[w] = select_factors_for_window(
            df_scores=df_scores,
            window=w,
            max_factors=int(max_factors_per_window),
            corr_matrix=corr,
            max_corr=float(max_corr),
        )

    meta = {
        "spec_version": spec_version,
        "root": str(root),
        "as_of": as_of,
        "windows": list(windows),
        "max_factors_per_window": int(max_factors_per_window),
        "factor_ids": list(factor_ids) if factor_ids is not None else None,
        "corr_path": str(corr_path.resolve()) if corr_path is not None else None,
        "max_corr": float(max_corr),
        "entrypoint": "scripts/p2/factor_combo.py",
    }

    return FactorComboPlan(
        as_of=as_of,
        windows_selected=windows_selected,
        score_table=df_scores.to_dict(orient="records"),
        meta=meta,
    )


def save_combo_plan(plan: FactorComboPlan, output_path: Path) -> None:
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(plan.to_json_dict(), f, ensure_ascii=False, indent=2)
    print(f"[combo_lib] Combo plan written to {output_path}")
