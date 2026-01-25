import pandas as pd

import scripts.exec_replay as exec_replay
from alpha_core.phase4.runner import build_parser, run


def test_replay_uses_preflight_resolved_exec_path(monkeypatch, tmp_path) -> None:
    as_of = "2026-01-19"
    exec_run_id = "R20260119_fix"

    calendar_dir = tmp_path / "datahub" / "ref"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    (calendar_dir / "trading_days.csv").write_text("date\n2026-01-19\n", encoding="utf-8")

    bronze_day_dir = tmp_path / "datahub" / "bronze" / "fubon" / "trades" / f"dt={as_of}"
    bronze_day_dir.mkdir(parents=True, exist_ok=True)

    exec_run_dir = tmp_path / "reports" / "exec" / exec_run_id / "exec_run"
    exec_run_dir.mkdir(parents=True, exist_ok=True)
    resolved_path = exec_run_dir / "trades.csv"
    resolved_path.write_text(
        "ts_filled,symbol\n2026-01-19 09:00:00,TEST\n",
        encoding="utf-8",
    )

    def _load_exec_trades(exec_root, exec_run_id=None, explicit_path=None):
        assert explicit_path == resolved_path
        return pd.DataFrame({"symbol": []})

    monkeypatch.setattr(exec_replay, "detect_incomplete_flag", lambda *_: False)
    monkeypatch.setattr(exec_replay, "load_exec_trades", _load_exec_trades)
    monkeypatch.setattr(exec_replay, "load_bronze_trades", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(exec_replay, "canonicalize_bronze_trades", lambda df, *args, **kwargs: df)
    monkeypatch.setattr(exec_replay, "align_exec_to_market", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(exec_replay, "write_replay_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(exec_replay, "fit_impact_model", lambda *_: ({}, {"model_type": "stub"}, []))
    monkeypatch.setattr(exec_replay, "predict_impact", lambda *_: pd.Series(dtype="float64"))
    monkeypatch.setattr(exec_replay, "compute_mae_bps", lambda *_: float("nan"))
    monkeypatch.setattr(exec_replay, "write_impact_calib", lambda *args, **kwargs: None)

    parser = build_parser()
    args = parser.parse_args(
        [
            "--as-of",
            as_of,
            "--exec-run-id",
            exec_run_id,
            "--mode",
            "replay",
            "--profile",
            "dev",
        ]
    )
    exit_code = run(args, repo_root=tmp_path)
    assert exit_code == 0
