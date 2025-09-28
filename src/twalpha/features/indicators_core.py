from __future__ import annotations
import pandas as pd
import numpy as np

def ema(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(span=window, adjust=False).mean()

def rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.rolling(window).mean()
    roll_down = down.rolling(window).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100.0 - (100.0 / (1.0 + rs))

def atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window).mean()

def core_feature_block(df: pd.DataFrame, ema_windows=(26,121), rsi_window=14, atr_window=14) -> pd.DataFrame:
    out = df.copy()
    out["ema_fast"] = ema(out["close"], ema_windows[0])
    out["ema_slow"] = ema(out["close"], ema_windows[1])
    out["rsi"] = rsi(out["close"], rsi_window)
    out["atr"] = atr(out, atr_window)
    out["core_trend"] = np.where(out["ema_fast"] > out["ema_slow"], 1.0, -1.0)
    out["momentum"] = (out["close"] / out["close"].shift(10) - 1.0).clip(-0.5, 0.5)
    out["volume_z"] = (out["volume"] - out["volume"].rolling(60).mean()) / (out["volume"].rolling(60).std() + 1e-12)
    return out
