from __future__ import annotations
import pandas as pd
import numpy as np

def choch(df: pd.DataFrame, window: int = 20) -> pd.Series:
    # 簡化示意：新高破/新低破（實務可擴充為 BOS/OB/LIQ SWEEP）
    hh = df["high"].rolling(window).max()
    ll = df["low"].rolling(window).min()
    up_break = (df["close"] > hh.shift(1)).astype(float)
    down_break = (df["close"] < ll.shift(1)).astype(float) * -1.0
    sig = up_break + down_break
    sig = sig.replace(0.0, np.nan).ffill().fillna(0.0)
    return sig

def smc_block(df: pd.DataFrame, choch_window: int = 20, enabled: bool = True) -> pd.DataFrame:
    if not enabled:
        return df.assign(choch_sig=0.0, smc=0.0)
    out = df.copy()
    out["choch_sig"] = choch(out, window=choch_window)
    out["smc"] = out["choch_sig"].clip(-1.0, 1.0)
    return out
