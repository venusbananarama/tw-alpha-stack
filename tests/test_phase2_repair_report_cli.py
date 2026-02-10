from __future__ import annotations

import json
from pathlib import Path

from alpha_core.phase2.repair.report_cli import main


def test_repair_report_cli_outputs_attempts_and_final(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "reports" / "p2_runs" / "2026-02-07" / "p2.test.repair"
    attempt_dir_2 = run_dir / "attempt_logs" / "a_second"
    attempt_dir_1 = run_dir / "attempt_logs" / "b_first"
    attempt_dir_2.mkdir(parents=True, exist_ok=True)
    attempt_dir_1.mkdir(parents=True, exist_ok=True)

    attempt_payload_2 = {
        "seq": 2,
        "attempt_id": "attempt_002",
        "variant": {
            "factor_id": "mom_6m",
            "variant_id": "mom_6m__lag_2",
        },
        "bottleneck_window": 6,
        "passed": False,
        "early_stopped": False,
        "metrics": {
            "rank_ic_min": 0.02,
            "coverage_min": 0.90,
        },
        "elapsed_sec": 0.22,
        "error": None,
    }
    attempt_payload_1 = {
        "seq": 1,
        "attempt_id": "attempt_001",
        "variant": {
            "factor_id": "mom_6m",
            "variant_id": "mom_6m__lag_1",
        },
        "bottleneck_window": 6,
        "passed": True,
        "early_stopped": False,
        "metrics": {
            "rank_ic_min": 0.05,
            "coverage_min": 0.91,
        },
        "elapsed_sec": 0.12,
        "error": None,
    }
    (attempt_dir_2 / "attempt_summary.json").write_text(
        json.dumps(attempt_payload_2, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (attempt_dir_1 / "attempt_summary.json").write_text(
        json.dumps(attempt_payload_1, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    final_payload = {
        "status": "ok",
        "summary": {
            "repair_result": {
                "attempted": 2,
                "passed": True,
                "selected_variant_id": "mom_6m__lag_1",
                "decision_reason": "best_margin_with_minimal_changes",
            }
        },
    }
    (run_dir / "final_result.json").write_text(
        json.dumps(final_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "holdout_check.json").write_text(
        json.dumps(
            {
                "schema": "p2_repair_holdout.v1",
                "status": "ok",
                "reason": None,
                "passed": True,
                "selected_variant_id": "mom_6m__lag_1",
                "selected": {
                    "variant_id": "mom_6m__lag_1",
                    "factor_id": "mom_6m",
                    "passed": True,
                    "metrics": {
                        "rank_ic_min": 0.07,
                        "coverage_min": 0.93,
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    rc = main(["--run-dir", str(run_dir)])
    captured = capsys.readouterr().out

    assert rc == 0
    assert "seq" in captured
    assert "attempt_id" in captured
    assert captured.find("attempt_001") < captured.find("attempt_002")
    assert "decision_reason=best_margin_with_minimal_changes" in captured
    assert "holdout_status=ok" in captured
    assert "holdout_rank_ic_min=0.070000" in captured
    assert "holdout_coverage_min=0.930000" in captured


def test_repair_report_cli_missing_run_dir_returns_2(tmp_path: Path, capsys) -> None:
    rc = main(["--run-dir", str(tmp_path / "missing")])
    captured = capsys.readouterr().out

    assert rc == 2
    assert "run_dir not found" in captured
