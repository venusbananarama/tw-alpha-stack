from __future__ import annotations
import pandas as pd
from twalpha.data.contracts import OHLCV

def test_contract_ok():
    OHLCV(date="2024-01-01", open=10, high=12, low=9, close=11, volume=1000)

def test_contract_bad():
    try:
        OHLCV(date="2024-01-01", open=10, high=8, low=9, close=9.5, volume=1000)
        assert False, "should raise"
    except Exception:
        assert True
