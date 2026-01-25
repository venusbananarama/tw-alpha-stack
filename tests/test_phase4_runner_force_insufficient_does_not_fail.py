import json

import pandas as pd

import scripts.exec_replay as exec_replay
from alpha_core.phase4.runner import build_parser, run


def test_runner_force_insufficient_does_not_fail(monkeypatch, tmp_path) -> None:
    as_of = "2026-01-19"
    exec_run_id = "R20260119_fix"

    calendar_dir = tmp_path / "datahub" / "ref"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    (calendar_dir / "trading_days.csv").write_text("date\n2026-01-19\n", encoding="utf-8")

    bronze_day_dir = tmp_path / "datahub" / "bronze" / "fubon" / "trades" / f"dt={as_of}"
    bronze_day_dir.mkdir(parents=True, exist_ok=True)

    trades_dir = tmp_path / "reports" / "exec" / exec_run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "trades.csv").write_text(
        "ts_filled,symbol\n2026-01-19 09:00:00,TEST\n",
        encoding="utf-8",
    )

    def _stub_run_exec_replay(_args):
        stats_df = pd.DataFrame()
        replay_gate = {"status": "insufficient_data", "pass": False, "p50_bps": None, "p95_bps": None}
        impact_gate = {"status": "insufficient_data", "pass": False, "mae_bps": None}
        return stats_df, replay_gate, impact_gate

    monkeypatch.setattr(exec_replay, "run_exec_replay", _stub_run_exec_replay)

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

    summary_path = tmp_path / "reports" / "p4" / as_of / "p4_summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "WARN"
    assert summary["reason_code"] == "INSUFFICIENT_DATA"
