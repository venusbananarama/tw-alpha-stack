\
import pandas as pd
import numpy as np
from utils import ensure_sorted, add_dollar_volume

def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1.0/window, adjust=False).mean()
    roll_down = down.ewm(alpha=1.0/window, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist

def build_features(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = ensure_sorted(df.copy())
    df["date"] = pd.to_datetime(df["date"])
    add_dollar_volume(df)

    # Returns
    for w in cfg["returns_windows"]:
        df[f"ret_{w}"] = df.groupby("symbol", sort=False)["close"].pct_change(w)

    # Rolling mean/std (close)
    for w in cfg["rolling_mean_windows"]:
        df[f"sma_{w}"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(w, min_periods=2).mean())
    for w in cfg["rolling_std_windows"]:
        df[f"vol_{w}"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: s.pct_change().rolling(w, min_periods=2).std())

    # Momentum proxies
    df["mom_20"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: s.pct_change(20))
    df["mom_60"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: s.pct_change(60))

    # RSI
    df["rsi"] = df.groupby("symbol", sort=False)["close"].transform(lambda s: _rsi(s, cfg["rsi_window"]))

    # MACD
    tmp = df.groupby("symbol", sort=False)["close"].apply(lambda s: pd.DataFrame({
        "macd": _macd(s, cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"])[0],
        "macd_signal": _macd(s, cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"])[1],
        "macd_hist": _macd(s, cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"])[2],
    }))
    tmp = tmp.reset_index(level=0, drop=True).reset_index(drop=True)
    df[["macd","macd_signal","macd_hist"]] = tmp[["macd","macd_signal","macd_hist"]]

    # Liquidity proxy
    df["adv_20"] = df.groupby("symbol", sort=False)["dollar_volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())

    return df
