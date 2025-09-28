from __future__ import annotations
import pandas as pd

def detect_regime(df: pd.DataFrame, price_col: str = "close", ema_fast: int = 26, ema_slow: int = 121) -> pd.Series:
    px = df[price_col].astype(float)
    ema_f = px.ewm(span=ema_fast, adjust=False).mean()
    ema_s = px.ewm(span=ema_slow, adjust=False).mean()
    slope = ema_f.diff()
    regime = pd.Series("range", index=df.index)
    regime[(ema_f > ema_s) & (slope > 0)] = "trend"
    return regime
