from __future__ import annotations
import pandas as pd
from twalpha.features.indicators_core import core_feature_block

def test_core_block_shapes():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=50, freq="D"),
        "open": range(50),
        "high": [x+1 for x in range(50)],
        "low": [max(0, x-1) for x in range(50)],
        "close": [x*1.0 for x in range(50)],
        "volume": [1000]*(50),
    })
    out = core_feature_block(df)
    assert set(["ema_fast","ema_slow","rsi","atr","core_trend","momentum","volume_z"]).issubset(out.columns)
    assert len(out) == 50
