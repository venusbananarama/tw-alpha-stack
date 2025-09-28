from __future__ import annotations
import numpy as np
import pandas as pd

def ema(s: pd.Series, span: int) -> pd.Series:
    return pd.Series(s, index=s.index, dtype='float64').ewm(span=span, adjust=False).mean()

def roc(s: pd.Series, n: int) -> pd.Series:
    s = pd.Series(s, dtype='float64')
    return s.pct_change(n)

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([(h-l).abs(), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def rsi(s: pd.Series, n: int = 14) -> pd.Series:
    s = pd.Series(s, dtype='float64')
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/n, adjust=False).mean()
    roll_down = down.ewm(alpha=1/n, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - (100/(1+rs))

def mfi(df: pd.DataFrame, n: int = 14) -> pd.Series:
    tp = (df['high'] + df['low'] + df['close']) / 3.0
    mf = tp * df['volume']
    pos = mf.where(tp > tp.shift(), 0.0)
    neg = mf.where(tp < tp.shift(), 0.0)
    pos_roll = pos.rolling(n).sum()
    neg_roll = neg.rolling(n).sum()
    mr = pos_roll / (neg_roll + 1e-12)
    return 100 - (100 / (1 + mr))

def _dm_pos(df: pd.DataFrame):
    up = df['high'].diff()
    down = -df['low'].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    return pd.Series(plus_dm, index=df.index), pd.Series(minus_dm, index=df.index)

def adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([(h-l).abs(), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    tr_n = tr.rolling(n).sum()
    plus_dm, minus_dm = _dm_pos(df)
    plus_di = 100 * (plus_dm.rolling(n).sum() / (tr_n + 1e-12))
    minus_di = 100 * (minus_dm.rolling(n).sum() / (tr_n + 1e-12))
    dx = ( (plus_di - minus_di).abs() / ((plus_di + minus_di) + 1e-12) ) * 100
    return dx.rolling(n).mean()

def vol_z(vol: pd.Series, n: int = 60) -> pd.Series:
    vma = vol.rolling(n).mean()
    vstd = vol.rolling(n).std(ddof=0)
    return (vol - vma) / (vstd + 1e-12)
