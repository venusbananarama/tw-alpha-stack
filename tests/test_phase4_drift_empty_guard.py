import json
from pathlib import Path

import pandas as pd

import scripts.exec_replay as exec_replay
import scripts.wf_runner as wf_runner
from alpha_core.phase4 import runner


def test_runner_drift_empty_all_continues(monkeypatch, tmp_path) -> None:
    as_of = "2026-01-19"
    exec_run_id = "R20260119_fix"

    calendar_dir = tmp_path / "datahub" / "ref"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    (calendar_dir / "trading_days.csv").write_text("date\n2026-01-19\n", encoding="utf-8")

    bronze_day_dir = tmp_path / "datahub" / "bronze" / "fubon" / "trades" / f"dt={as_of}"
    bronze_day_dir.mkdir(parents=True, exist_ok=True)
    (bronze_day_dir / "trades.jsonl").write_text('{"symbol":"TEST"}\n', encoding="utf-8")

    exec_dir = tmp_path / "reports" / "exec" / exec_run_id
    exec_dir.mkdir(parents=True, exist_ok=True)
    rows = "\n".join([f"2026-01-19 09:00:0{i % 10},TEST" for i in range(10)])
    (exec_dir / "trades.csv").write_text(f"ts_filled,symbol\n{rows}\n", encoding="utf-8")

    def _stub_run_exec_replay(_args):
        exec_out_dir = Path(_args.out_dir) / "exec"
        exec_out_dir.mkdir(parents=True, exist_ok=True)
        (exec_out_dir / "replay_stats.parquet").write_text("stub", encoding="utf-8")
        replay_gate = {"status": "pass", "pass": True, "p50_bps": 0.0, "p95_bps": 0.0}
        impact_gate = {"status": "pass", "pass": True, "mae_bps": 0.0}
        return pd.DataFrame(), replay_gate, impact_gate

    def _stub_read_parquet(_path):
        return pd.DataFrame(columns=["slippage_bps", "slippage_bps_p50", "slippage_bps_p95"])

    def _stub_wf_run(args):
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "wf_gate.jsonl").write_text(
            json.dumps({"pass": True, "overall_pass_ratio": 0.8}) + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(exec_replay, "run_exec_replay", _stub_run_exec_replay)
    monkeypatch.setattr(runner.pd, "read_parquet", _stub_read_parquet)
    monkeypatch.setattr(wf_runner, "_run_p4", _stub_wf_run)

    parser = runner.build_parser()
    args = parser.parse_args(
        [
            "--as-of",
            as_of,
            "--exec-run-id",
            exec_run_id,
            "--mode",
            "all",
            "--profile",
            "prod",
        ]
    )
    exit_code = runner.run(args, repo_root=tmp_path)
    assert exit_code == 0

    summary_path = tmp_path / "reports" / "p4" / as_of / "p4_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "WARN"
    assert summary["reason_code"] == "INSUFFICIENT_DATA"
    assert summary["gates"]["drift"]["status"] == "insufficient_data"
    assert summary["gates"]["drift"]["detail"] == "empty_replay_stats"
    assert (tmp_path / "reports" / "p4" / as_of / "wf_gate.jsonl").exists()
