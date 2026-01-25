from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .ledger import write_json_atomic


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "participation_rate" not in out.columns and "mkt_window_qty" in out.columns:
        denom = pd.to_numeric(out["mkt_window_qty"], errors="coerce").replace(0, np.nan)
        out["participation_rate"] = pd.to_numeric(out["qty"], errors="coerce") / denom
    if "log_qty" not in out.columns:
        out["log_qty"] = np.log1p(pd.to_numeric(out["qty"], errors="coerce").fillna(0.0))
    return out


def fit_impact_model(
    slippage_df: pd.DataFrame,
    *,
    model_type: str = "linear_participation_v1",
    features: List[str] | None = None,
) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
    if slippage_df.empty:
        return {"intercept": 0.0}, {"r2": 0.0, "n": 0.0, "robust": 0.0}, []

    df = _prepare_features(slippage_df)
    y = pd.to_numeric(df["slippage_bps"], errors="coerce").to_numpy()
    mask = np.isfinite(y)
    df = df.loc[mask].copy()
    y = y[mask]
    if features is None:
        candidates = ["participation_rate", "log_qty"]
        features = [c for c in candidates if c in df.columns]

    X_cols: List[str] = []
    X_parts = [np.ones(len(df))]
    for f in features:
        col = pd.to_numeric(df[f], errors="coerce").fillna(0.0).to_numpy()
        X_parts.append(col)
        X_cols.append(f)

    X = np.vstack(X_parts).T
    if len(df) == 0:
        return {"intercept": 0.0}, {"r2": 0.0, "n": 0.0, "robust": 0.0}, X_cols

    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    params: Dict[str, float] = {"intercept": float(beta[0])}
    for idx, col in enumerate(X_cols, start=1):
        params[f"coef_{col}"] = float(beta[idx])

    y_hat = X @ beta
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2)) if len(y) > 1 else 0.0
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    stats = {"r2": float(r2), "n": float(len(y)), "robust": 0.0, "model_type": model_type}
    return params, stats, X_cols


def predict_impact(slippage_df: pd.DataFrame, params: Dict[str, float]) -> pd.Series:
    if slippage_df.empty:
        return pd.Series([], dtype=float)
    df = _prepare_features(slippage_df)
    y_hat = np.full(len(df), params.get("intercept", 0.0), dtype=float)
    for key, value in params.items():
        if not key.startswith("coef_"):
            continue
        feature = key.replace("coef_", "")
        if feature in df.columns:
            y_hat = y_hat + float(value) * pd.to_numeric(df[feature], errors="coerce").fillna(0.0).to_numpy()
    return pd.Series(y_hat, index=df.index, dtype=float)


def compute_mae_bps(y_true: pd.Series, y_pred: pd.Series) -> float:
    y_true = pd.to_numeric(y_true, errors="coerce")
    y_pred = pd.to_numeric(y_pred, errors="coerce")
    mask = y_true.notna() & y_pred.notna()
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs(y_true[mask] - y_pred[mask])))


def write_impact_calib(params_dict: Dict[str, object], out_path) -> None:
    write_json_atomic(params_dict, out_path)
