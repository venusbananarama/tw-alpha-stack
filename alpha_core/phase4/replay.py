from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .ledger import write_parquet_atomic


def compute_slippage_bps(aligned_df: pd.DataFrame) -> pd.DataFrame:
    if aligned_df.empty:
        cols = list(aligned_df.columns)
        for col in ("slippage_bps", "missing_ref"):
            if col not in cols:
                cols.append(col)
        return pd.DataFrame(columns=cols)

    df = aligned_df.copy()
    df["ref_price"] = pd.to_numeric(df["ref_price"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    side = df["side"].astype(str).str.upper()
    ref = df["ref_price"]
    exec_px = df["price"]

    slippage = np.where(
        side == "BUY",
        (exec_px - ref) / ref,
        (ref - exec_px) / ref,
    )
    slippage = slippage * 1e4
    slippage = np.where(np.isfinite(slippage), slippage, np.nan)
    df["slippage_bps"] = slippage
    df["missing_ref"] = df["ref_price"].isna()
    return df


def _quantile(series: pd.Series, q: float) -> Optional[float]:
    if series.empty:
        return None
    return float(series.quantile(q, interpolation="linear"))


def aggregate_replay_stats(
    slippage_df: pd.DataFrame,
    *,
    as_of: Optional[str] = None,
    run_id: Optional[str] = None,
    ref_price_mode: Optional[str] = None,
    window_sec: Optional[int] = None,
    min_trades: int = 10,
) -> pd.DataFrame:
    if slippage_df.empty:
        return pd.DataFrame()

    df = slippage_df.copy()
    if as_of is None and "as_of" in df.columns:
        as_of = str(df["as_of"].iloc[0])
    if run_id is None and "run_id" in df.columns:
        run_id = str(df["run_id"].iloc[0])
    if ref_price_mode is None and "ref_mode" in df.columns:
        ref_price_mode = str(df["ref_mode"].iloc[0])
    if window_sec is None and "window_sec" in df.columns:
        try:
            window_sec = int(df["window_sec"].iloc[0])
        except Exception:
            window_sec = None

    rows = []
    for symbol, grp in df.groupby("symbol", sort=False):
        valid = grp[grp["slippage_bps"].notna()]
        n_total = len(grp)
        n_valid = len(valid)
        coverage = float(n_valid / n_total) if n_total else 0.0
        p50 = _quantile(valid["slippage_bps"], 0.5)
        p95 = _quantile(valid["slippage_bps"], 0.95)
        status = "pass"
        if n_valid < min_trades:
            status = "insufficient_data"
        row = {
            "as_of": as_of or "",
            "symbol": str(symbol),
            "n_exec_trades": int(n_total),
            "coverage_rate": coverage,
            "missing_ref_trades": int(n_total - n_valid),
            "slippage_bps_p50": p50,
            "slippage_bps_p95": p95,
            "ref_price_mode": ref_price_mode or "",
            "window_sec": int(window_sec or 0),
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            "run_id": run_id or "",
            "status": status,
        }
        rows.append(row)

    stats_df = pd.DataFrame(rows)
    if "symbol" in stats_df.columns and "ALL" not in stats_df["symbol"].tolist():
        all_row = stats_df.iloc[0].copy()
        valid_all = df[df["slippage_bps"].notna()]
        n_total = len(df)
        n_valid = len(valid_all)
        all_row["symbol"] = "ALL"
        all_row["n_exec_trades"] = int(n_total)
        all_row["coverage_rate"] = float(n_valid / n_total) if n_total else 0.0
        all_row["missing_ref_trades"] = int(n_total - n_valid)
        all_row["slippage_bps_p50"] = _quantile(valid_all["slippage_bps"], 0.5)
        all_row["slippage_bps_p95"] = _quantile(valid_all["slippage_bps"], 0.95)
        all_row["status"] = "insufficient_data" if n_valid < min_trades else "pass"
        stats_df = pd.concat([stats_df, pd.DataFrame([all_row])], ignore_index=True)

    return stats_df


def normalize_replay_stats_schema(stats_df: pd.DataFrame) -> pd.DataFrame:
    if stats_df is None:
        return stats_df

    df = stats_df.copy()
    has_slippage = "slippage_bps" in df.columns
    has_p50 = "slippage_bps_p50" in df.columns
    has_p95 = "slippage_bps_p95" in df.columns

    if not has_slippage and has_p50:
        df["slippage_bps"] = df["slippage_bps_p50"]
        has_slippage = True
    if not has_p50 and has_slippage:
        df["slippage_bps_p50"] = df["slippage_bps"]
        has_p50 = True
    if not has_p95 and has_slippage:
        df["slippage_bps_p95"] = df["slippage_bps"]
        has_p95 = True

    if "slippage_bps" not in df.columns:
        df["slippage_bps"] = np.nan
    if "slippage_bps_p50" not in df.columns:
        df["slippage_bps_p50"] = np.nan
    if "slippage_bps_p95" not in df.columns:
        df["slippage_bps_p95"] = np.nan

    return df


def write_replay_stats(stats_df: pd.DataFrame, out_path: Path) -> None:
    normalized = normalize_replay_stats_schema(stats_df)
    write_parquet_atomic(normalized, out_path)
