import pandas as pd

from alpha_core.phase4.replay import normalize_replay_stats_schema


def test_normalize_adds_slippage_bps_from_p50() -> None:
    df = pd.DataFrame({"slippage_bps_p50": [1.25], "slippage_bps_p95": [3.5]})
    out = normalize_replay_stats_schema(df)
    assert "slippage_bps" in out.columns
    assert out["slippage_bps"].iloc[0] == 1.25


def test_normalize_adds_p50_p95_from_slippage() -> None:
    df = pd.DataFrame({"slippage_bps": [2.75]})
    out = normalize_replay_stats_schema(df)
    assert "slippage_bps_p50" in out.columns
    assert "slippage_bps_p95" in out.columns
    assert out["slippage_bps_p50"].iloc[0] == 2.75
    assert out["slippage_bps_p95"].iloc[0] == 2.75
