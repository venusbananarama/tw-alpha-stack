from __future__ import annotations
import pandas as pd
import numpy as np

def twin_range_filter(close: pd.Series, fast: int = 7, slow: int = 21) -> pd.DataFrame:
    # 簡化：以 rolling range 近似（可替換為正式 TRF）
    r_fast = close.rolling(fast).max() - close.rolling(fast).min()
    r_slow = close.rolling(slow).max() - close.rolling(slow).min()
    line = (r_fast / (r_slow + 1e-9)).clip(0, 5.0)
    sig = (line > 0.6).astype(float) - (line < 0.4).astype(float)
    return pd.DataFrame({"trf_line": line, "trf_sig": sig})

def custom_block(df: pd.DataFrame, trf_fast=7, trf_slow=21, lrc_enabled=True) -> pd.DataFrame:
    out = df.copy()
    trf = twin_range_filter(out["close"], trf_fast, trf_slow)
    out = pd.concat([out, trf], axis=1)
    if lrc_enabled:
        # 佔位：以簡單 momentum 當作 LRC 分數替身
        out["lrc_score"] = (out["close"] / out["close"].shift(5) - 1.0).clip(-0.2, 0.2)
    else:
        out["lrc_score"] = 0.0
    return out
