from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Union

import numpy as np
import pandas as pd
import yaml


SeriesLike = Union[pd.Series, pd.DataFrame]


@dataclass(frozen=True)
class XFormStep:
    name: str
    params: Dict[str, Any]


_SERIES_TRANSFORM_NAMES = frozenset(
    {
        "winsorize",
        "zscore",
        "rank",
        "clip",
        "fillna",
        "sign_flip",
    }
)
_PANEL_ONLY_TRANSFORM_NAMES = frozenset({"lag", "smooth"})


def supported_transform_names() -> List[str]:
    """Return registered transform names in deterministic order."""
    return sorted(_SERIES_TRANSFORM_NAMES | _PANEL_ONLY_TRANSFORM_NAMES)


class XFormPipeline:
    def __init__(self, steps: Sequence[XFormStep]) -> None:
        self.steps = list(steps)

    @classmethod
    def from_yaml(cls, path: Path | str) -> "XFormPipeline":
        p = Path(path)
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        steps_raw = data.get("transforms", []) if isinstance(data, Mapping) else []
        steps: List[XFormStep] = []
        for raw in steps_raw:
            if not isinstance(raw, Mapping):
                continue
            name = str(raw.get("name") or "").strip().lower()
            if not name:
                continue
            params = dict(raw.get("params") or {})
            steps.append(XFormStep(name=name, params=params))
        return cls(steps)

    @classmethod
    def from_specs(cls, specs: Iterable[Mapping[str, Any]]) -> "XFormPipeline":
        steps: List[XFormStep] = []
        for raw in specs:
            name = str(raw.get("name") or "").strip().lower()
            if not name:
                continue
            params = dict(raw.get("params") or {})
            steps.append(XFormStep(name=name, params=params))
        return cls(steps)

    def apply(self, value: SeriesLike) -> SeriesLike:
        out: SeriesLike = value.copy()
        for step in self.steps:
            out = self._apply_step(out, step)
        return out

    def _apply_step(self, value: SeriesLike, step: XFormStep) -> SeriesLike:
        if isinstance(value, pd.Series):
            return _apply_to_series(value, step)
        if isinstance(value, pd.DataFrame):
            if {"date", "stock_id", "factor_value"}.issubset(set(value.columns)):
                return _apply_to_panel(value, step)
            if "factor_value" in value.columns:
                out = value.copy()
                out["factor_value"] = _apply_to_series(out["factor_value"], step)
                return out
            return value
        return value


def _clean_series(series: pd.Series) -> pd.Series:
    return series.astype(float).replace([np.inf, -np.inf], np.nan)


def _winsorize(series: pd.Series, lower_q: float, upper_q: float) -> pd.Series:
    s = _clean_series(series)
    lo = float(s.quantile(lower_q))
    hi = float(s.quantile(upper_q))
    return s.clip(lower=lo, upper=hi)


def _zscore(series: pd.Series, ddof: int = 0, clip_std: float | None = None) -> pd.Series:
    s = _clean_series(series)
    mean = s.mean()
    std = s.std(ddof=ddof)
    if std == 0 or np.isnan(std):
        out = s - mean
    else:
        out = (s - mean) / std
    if clip_std is not None and clip_std > 0:
        out = out.clip(lower=-float(clip_std), upper=float(clip_std))
    return out


def _rank(series: pd.Series, pct: bool = True, center: bool = True) -> pd.Series:
    s = _clean_series(series)
    ranked = s.rank(method="average", pct=pct)
    if pct and center:
        ranked = ranked - 0.5
    return ranked


def _fillna(series: pd.Series, *, value: float | None = None, method: str | None = None) -> pd.Series:
    if method:
        return series.fillna(method=str(method))
    if value is not None:
        return series.fillna(float(value))
    return series


def _apply_to_series(series: pd.Series, step: XFormStep) -> pd.Series:
    name = step.name
    p = step.params

    if name == "winsorize":
        return _winsorize(series, float(p.get("lower_q", 0.01)), float(p.get("upper_q", 0.99)))
    if name == "zscore":
        return _zscore(series, ddof=int(p.get("ddof", 0)), clip_std=p.get("clip_std"))
    if name == "rank":
        return _rank(series, pct=bool(p.get("pct", True)), center=bool(p.get("center", True)))
    if name == "clip":
        lo = p.get("lower", None)
        hi = p.get("upper", None)
        return _clean_series(series).clip(lower=lo, upper=hi)
    if name == "fillna":
        return _fillna(series, value=p.get("value"), method=p.get("method"))
    if name == "sign_flip":
        return _clean_series(series) * -1.0

    return series


def _apply_cross_section(df: pd.DataFrame, fn) -> pd.DataFrame:
    out = df.copy()
    out["factor_value"] = out.groupby("date", sort=False)["factor_value"].transform(fn)
    return out.reset_index(drop=True)


def _apply_time_series(df: pd.DataFrame, fn) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["_row_id"] = np.arange(len(out), dtype=int)
    out = out.sort_values(["stock_id", "date", "_row_id"], kind="stable")
    out["factor_value"] = out.groupby("stock_id", sort=False)["factor_value"].transform(fn)
    out = out.sort_values(["_row_id"], kind="stable").drop(columns=["_row_id"])
    return out.reset_index(drop=True)


def _apply_to_panel(panel: pd.DataFrame, step: XFormStep) -> pd.DataFrame:
    name = step.name
    p = step.params

    if name in _SERIES_TRANSFORM_NAMES:
        return _apply_cross_section(panel, lambda s: _apply_to_series(s, step))

    if name == "lag":
        periods = int(p.get("periods", 1))
        return _apply_time_series(panel, lambda s: s.shift(periods))

    if name == "smooth":
        window = int(p.get("window", 3))
        min_periods = int(p.get("min_periods", 1))

        def _smooth(s: pd.Series) -> pd.Series:
            return _clean_series(s).rolling(window=window, min_periods=min_periods).mean()

        return _apply_time_series(panel, _smooth)

    return panel
