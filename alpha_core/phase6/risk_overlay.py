from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .portfolio_construction import summarize_targets


def build_adjustment_trace(
    *,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    prices: Dict[str, float],
    nav: float,
    policy: str,
    scale_factor: float,
    triggered_by: List[str],
    notes: Dict[str, object],
) -> Dict[str, object]:
    dropped = []
    if "symbol" in before_df.columns and "target_qty" in before_df.columns:
        before_qty = before_df.set_index("symbol")["target_qty"]
        after_qty = after_df.set_index("symbol")["target_qty"] if "symbol" in after_df.columns else before_qty
        for sym, qty in before_qty.items():
            if sym in after_qty.index and qty != 0 and after_qty.loc[sym] == 0:
                dropped.append(sym)
    return {
        "policy": policy,
        "scale_factor": float(scale_factor),
        "triggered_by": triggered_by,
        "before": summarize_targets(before_df, prices, nav),
        "after": summarize_targets(after_df, prices, nav),
        "dropped_symbols_sample": dropped[:20],
        "notes": notes,
    }
