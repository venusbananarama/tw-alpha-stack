\
import pandas as pd
from utils import ensure_sorted

def make_labels(df: pd.DataFrame, fwd_days: int = 5, cls_q: float = 0.2) -> pd.DataFrame:
    df = ensure_sorted(df.copy())
    # Forward N-day return as regression target
    df["target_fwd_ret"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: s.pct_change(periods=fwd_days).shift(-fwd_days))

    # Cross-sectional classification labels by date
    # Rank today's momentum (or next returns if available) for label; here we use target_fwd_ret (not available in live)
    def label_by_day(g):
        q_hi = g["target_fwd_ret"].quantile(1.0 - cls_q)
        q_lo = g["target_fwd_ret"].quantile(cls_q)
        g["label_long"] = (g["target_fwd_ret"] >= q_hi).astype(int)
        g["label_short"] = (g["target_fwd_ret"] <= q_lo).astype(int)
        return g

    df = df.groupby("date", sort=False, group_keys=False).apply(label_by_day)
    return df
