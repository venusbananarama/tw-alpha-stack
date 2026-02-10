from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from alpha_core.phase2.corelib import factor_eval as factor_eval_mod

from .xforms import XFormPipeline


@dataclass(frozen=True)
class EvalResult:
    passed: bool
    early_stopped: bool
    windows: Dict[str, Dict[str, Optional[float]]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "early_stopped": self.early_stopped,
            "windows": {k: dict(v) for k, v in self.windows.items()},
        }


class EvalAdapter:
    def __init__(self, *, root: Path, as_of: str, windows: Sequence[int]) -> None:
        self.root = root.resolve()
        self.as_of = as_of
        self.windows = sorted(set(int(w) for w in windows if int(w) > 0))
        self.as_of_ts = pd.to_datetime(as_of)

        self._factor_cache: Dict[str, pd.DataFrame] = {}
        self._prices = factor_eval_mod._load_prices(self.root)
        self._target_returns = factor_eval_mod._compute_target_returns(
            prices=self._prices,
            horizon_days=factor_eval_mod._HORIZON_DAYS_DEFAULT,
            as_of=self.as_of_ts,
        )

    def _load_factor(self, factor_id: str) -> pd.DataFrame:
        if factor_id in self._factor_cache:
            return self._factor_cache[factor_id].copy()

        df = factor_eval_mod._load_factor_frame(self.root, factor_id)
        df = df.loc[pd.to_datetime(df["date"]) <= self.as_of_ts].copy()
        if df.empty:
            raise ValueError(f"no samples before as_of for factor {factor_id!r}")
        self._factor_cache[factor_id] = df
        return df.copy()

    def _prepare_daily(
        self,
        *,
        factor_id: str,
        transforms: Sequence[Mapping[str, Any]],
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
    ) -> pd.DataFrame:
        panel = self._load_factor(factor_id)
        pipeline = XFormPipeline.from_specs(transforms)
        transformed = pipeline.apply(panel)
        if not isinstance(transformed, pd.DataFrame):
            raise TypeError("xforms pipeline must return DataFrame for panel input")

        merged = transformed.merge(self._target_returns, on=["date", "stock_id"], how="inner")
        if merged.empty:
            return pd.DataFrame()

        merged = merged.copy()
        merged["_date_ts"] = pd.to_datetime(merged["date"], errors="coerce")
        merged = merged.loc[merged["_date_ts"].notna()].copy()
        if merged.empty:
            return pd.DataFrame()

        if date_start is not None:
            start_ts = pd.to_datetime(date_start)
            merged = merged.loc[merged["_date_ts"] >= start_ts].copy()
        if date_end is not None:
            end_ts = pd.to_datetime(date_end)
            merged = merged.loc[merged["_date_ts"] <= end_ts].copy()
        if merged.empty:
            return pd.DataFrame()

        merged = merged.drop(columns=["_date_ts"])
        daily = factor_eval_mod._compute_daily_stats(merged)
        if daily.empty:
            return pd.DataFrame()
        return daily

    @staticmethod
    def _sorted_daily_dates(daily: pd.DataFrame) -> List[pd.Timestamp]:
        if daily.empty or "date" not in daily.columns:
            return []
        values = pd.to_datetime(daily["date"], errors="coerce").dropna()
        if values.empty:
            return []
        unique = pd.to_datetime(values.unique())
        return sorted(pd.Timestamp(ts).normalize() for ts in unique)

    @staticmethod
    def _window_metrics(daily: pd.DataFrame, window: int, as_of_ts: pd.Timestamp) -> Dict[str, Optional[float]]:
        agg = factor_eval_mod._aggregate_window(daily, window_months=int(window), as_of=as_of_ts)
        return {
            "rank_ic": agg.get("rank_ic_mean"),
            "coverage": agg.get("coverage"),
            "sample_days": float(agg.get("sample_days") or 0),
        }

    @staticmethod
    def _passes_threshold(node: Mapping[str, Any], thresholds: Mapping[str, Any]) -> bool:
        rank_ic_th = thresholds.get("min_rank_ic")
        cov_th = thresholds.get("min_coverage")

        rank_ic = node.get("rank_ic")
        coverage = node.get("coverage")

        if rank_ic_th is not None:
            if rank_ic is None or float(rank_ic) < float(rank_ic_th):
                return False
        if cov_th is not None:
            if coverage is None or float(coverage) < float(cov_th):
                return False
        return True

    def evaluate_variant(
        self,
        *,
        factor_id: str,
        transforms: Sequence[Mapping[str, Any]],
        thresholds: Mapping[str, Any],
        bottleneck_window: Optional[int],
        stop_fast: bool,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
    ) -> EvalResult:
        daily = self._prepare_daily(
            factor_id=factor_id,
            transforms=transforms,
            date_start=date_start,
            date_end=date_end,
        )
        if daily.empty:
            return EvalResult(passed=False, early_stopped=True, windows={})

        resolved: Dict[str, Dict[str, Optional[float]]] = {}
        early_stopped = False

        if bottleneck_window is not None and int(bottleneck_window) > 0:
            b = int(bottleneck_window)
            b_metrics = self._window_metrics(daily, b, self.as_of_ts)
            resolved[str(b)] = b_metrics
            if stop_fast and not self._passes_threshold(b_metrics, thresholds):
                early_stopped = True
                return EvalResult(passed=False, early_stopped=True, windows=resolved)

        for window in self.windows:
            key = str(window)
            if key in resolved:
                continue
            resolved[key] = self._window_metrics(daily, window, self.as_of_ts)

        passed = True
        for node in resolved.values():
            if not self._passes_threshold(node, thresholds):
                passed = False
                break

        return EvalResult(passed=passed, early_stopped=early_stopped, windows=resolved)

    def resolve_holdout_split(
        self,
        *,
        factor_id: str,
        transforms: Sequence[Mapping[str, Any]],
        ratio: float = 0.2,
    ) -> Optional[Dict[str, Any]]:
        try:
            ratio_value = float(ratio)
        except Exception:
            ratio_value = 0.2
        if ratio_value <= 0:
            ratio_value = 0.2
        if ratio_value > 1:
            ratio_value = 1.0

        daily = self._prepare_daily(factor_id=factor_id, transforms=transforms)
        dates = self._sorted_daily_dates(daily)
        if not dates:
            return None

        holdout_days = max(int(len(dates) * ratio_value), 1)
        holdout_days = min(holdout_days, len(dates))
        start_idx = max(len(dates) - holdout_days, 0)
        holdout_start = dates[start_idx]
        train_end = dates[start_idx - 1] if start_idx > 0 else None

        return {
            "ratio": ratio_value,
            "train_end": train_end.strftime("%Y-%m-%d") if train_end is not None else None,
            "holdout_start": holdout_start.strftime("%Y-%m-%d"),
        }

    @staticmethod
    def summarize_metrics(result: EvalResult) -> Dict[str, Any]:
        rank_vals: List[float] = []
        cov_vals: List[float] = []
        for node in result.windows.values():
            r = node.get("rank_ic")
            c = node.get("coverage")
            if isinstance(r, (int, float)):
                rank_vals.append(float(r))
            if isinstance(c, (int, float)):
                cov_vals.append(float(c))

        return {
            "rank_ic_min": min(rank_vals) if rank_vals else None,
            "coverage_min": min(cov_vals) if cov_vals else None,
            "windows": result.windows,
            "passed": result.passed,
            "early_stopped": result.early_stopped,
        }
