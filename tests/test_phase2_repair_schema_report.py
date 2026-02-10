from __future__ import annotations

import json
from pathlib import Path

from alpha_core.phase2.repair.schema_report import build_schema_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_schema_report_flags_missing_files_and_attempt_logs(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "p2_runs" / "2026-02-09" / "p2.test.repair"
    _write_json(
        run_dir / "final_result.json",
        {
            "status": "ok",
            "summary": {
                "repair_result": {
                    "attempted": 1,
                    "passed": False,
                    "selected_variant_id": None,
                }
            },
        },
    )

    report = build_schema_report(run_dir=run_dir)
    assert report["ok"] is False
    assert "manifest.json" in report["missing_files"]
    assert "gate_before.json" in report["missing_files"]
    assert "gate_after.json" in report["missing_files"]
    assert "attempt_logs:missing_attempt_summary" in report["schema_errors"]
    assert report["data_stats"]["attempted"] == 1
    assert report["data_stats"]["gate_after_present"] is False
    assert report["data_stats"]["selected_variant_id"] is None
    assert report["data_stats"]["metrics_summary_has_windows"] is False
    assert report["data_stats"]["rank_ic_min_is_number"] is False
    assert report["data_stats"]["coverage_min_is_number"] is False


def test_schema_report_ok_when_required_files_exist(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "p2_runs" / "2026-02-09" / "p2.test.repair"
    _write_json(
        run_dir / "manifest.json",
        {
            "schema": "p2_repair_manifest.v1",
            "versions": {"repair_schema": "p2_repair.v1"},
            "resolved_paths": {"gate_summary": "/tmp/gate_summary.json"},
            "hashes": {"gate_summary": "abc"},
        },
    )
    _write_json(run_dir / "params.json", {"enabled": True})
    _write_json(run_dir / "metrics.json", {"attempted": 1})
    _write_json(
        run_dir / "repair_plan.json",
        {
            "variant_sort_policy": "priority_desc, transforms_len_asc, variant_id_asc",
            "variants": [{"seq": 1, "variant_id": "mom_6m__lag_1"}],
        },
    )
    _write_json(
        run_dir / "final_result.json",
        {
            "status": "ok",
            "summary": {
                "repair_result": {
                    "attempted": 1,
                    "passed": True,
                    "selected_variant_id": "mom_6m__lag_1",
                }
            },
        },
    )
    _write_json(run_dir / "tags.json", {"profile": "test"})
    _write_json(
        run_dir / "gate_before.json",
        {
            "schema": "p2_repair_gate_before.v1",
            "as_of": "2026-02-09",
            "run_id": "p2.test.repair",
            "checks_count": 1,
            "checks": [{"factor_id": "mom_6m", "reasons": ["rank_ic_min_threshold"]}],
        },
    )
    _write_json(
        run_dir / "gate_after.json",
        {
            "schema": "p2_repair_gate_after.v1",
            "as_of": "2026-02-09",
            "run_id": "p2.test.repair",
            "status": "ok",
            "selected_variant_id": "mom_6m__lag_1",
            "factor_id": "mom_6m",
            "passed": True,
            "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
            "metrics_summary": {"rank_ic_min": 0.05, "coverage_min": 0.95},
        },
    )
    _write_json(
        run_dir / "holdout_check.json",
        {
            "schema": "p2_repair_holdout.v1",
            "status": "ok",
            "reason": None,
        },
    )
    _write_json(
        run_dir / "attempt_logs" / "attempt_001" / "attempt_summary.json",
        {
            "attempt_id": "attempt_001",
            "variant": {"variant_id": "mom_6m__lag_1", "factor_id": "mom_6m"},
        },
    )

    report = build_schema_report(run_dir=run_dir, attempted=1)
    assert report["ok"] is True
    assert report["missing_files"] == []
    assert report["schema_errors"] == []
    assert report["attempt_logs"]["count"] == 1
    assert report["data_stats"]["attempted"] == 1
    assert report["data_stats"]["gate_after_present"] is True
    assert report["data_stats"]["selected_variant_id"] == "mom_6m__lag_1"
    assert report["data_stats"]["metrics_summary_has_windows"] is False
    assert report["data_stats"]["rank_ic_min_is_number"] is True
    assert report["data_stats"]["coverage_min_is_number"] is True
