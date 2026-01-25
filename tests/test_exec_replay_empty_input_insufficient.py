import argparse

import pandas as pd

import scripts.exec_replay as exec_replay
from alpha_core.phase4.schemas import REF_PRICE_MODE_LAST


def test_run_exec_replay_empty_aligned_insufficient(monkeypatch, tmp_path) -> None:
    def _empty_df(*_args, **_kwargs):
        return pd.DataFrame()

    monkeypatch.setattr(exec_replay, "load_exec_trades", _empty_df)
    monkeypatch.setattr(exec_replay, "load_bronze_trades", _empty_df)
    monkeypatch.setattr(exec_replay, "canonicalize_bronze_trades", lambda df, *a, **k: df)
    monkeypatch.setattr(exec_replay, "detect_incomplete_flag", lambda *_: False)
    monkeypatch.setattr(exec_replay, "align_exec_to_market", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(exec_replay, "write_replay_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(exec_replay, "write_impact_calib", lambda *args, **kwargs: None)

    args = argparse.Namespace(
        as_of="2026-01-19",
        run_id=None,
        bronze_root=str(tmp_path / "bronze"),
        exec_root=str(tmp_path / "exec"),
        out_dir=str(tmp_path / "out"),
        exec_run_id="R20260119_fix",
        symbols=None,
        ref_price_mode=REF_PRICE_MODE_LAST,
        window_sec=5,
        tolerance_ms=None,
        ignore_incomplete=False,
        force=False,
    )

    _, replay_gate, impact_gate = exec_replay.run_exec_replay(args)
    assert replay_gate["status"] == "insufficient_data"
    assert replay_gate["pass"] is False
    assert impact_gate["status"] == "insufficient_data"
    assert impact_gate["pass"] is False
