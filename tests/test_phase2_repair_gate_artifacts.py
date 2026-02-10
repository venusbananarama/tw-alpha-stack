from __future__ import annotations

from alpha_core.phase2.repair.gate_artifacts import build_gate_after, build_gate_before
from alpha_core.phase2.repair.models import RepairAttempt, VariantSpec


def test_build_gate_before_extracts_fail_checks() -> None:
    gate_summary = {
        "checks": [
            {
                "factor_id": "mom_6m",
                "reasons": ["rank_ic_min_threshold"],
                "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
            },
            {
                "factor_id": "quality_roe",
                "reasons": [],
                "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
            },
        ]
    }

    payload = build_gate_before(gate_summary, as_of="2026-02-09", run_id="p2.test.repair")
    assert payload["schema"] == "p2_repair_gate_before.v1"
    assert payload["run_id"] == "p2.test.repair"
    assert payload["checks_count"] == 1
    assert payload["checks"][0]["factor_id"] == "mom_6m"


def test_build_gate_after_skipped_when_no_selected_variant() -> None:
    payload = build_gate_after(None, as_of="2026-02-09", run_id="p2.test.repair")
    assert payload["schema"] == "p2_repair_gate_after.v1"
    assert payload["status"] == "skipped"
    assert payload["reason"] == "no_selected_variant"
    assert payload["run_id"] == "p2.test.repair"
    assert payload["attempt_id"] is None
    assert payload["metrics_summary"] == {}
    assert payload["passed"] is False


def test_build_gate_after_from_selected_attempt() -> None:
    variant = VariantSpec(
        variant_id="mom_6m__lag_1",
        factor_id="mom_6m",
        reason="rank_ic_min_threshold",
        transforms=[{"name": "lag", "params": {"periods": 1}}],
    )
    attempt = RepairAttempt(
        attempt_id="attempt_001",
        variant=variant,
        bottleneck_window=6,
        early_stopped=False,
        passed=True,
        metrics={
            "rank_ic_min": 0.05,
            "coverage_min": 0.95,
            "windows": {
                "6": {"rank_ic": 0.05, "coverage": 0.95},
                "12": {"rank_ic": 0.04, "coverage": 0.94},
            },
        },
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )
    payload = build_gate_after(attempt, as_of="2026-02-09", run_id="p2.test.repair")
    assert payload["status"] == "ok"
    assert payload["attempt_id"] == "attempt_001"
    assert payload["selected_variant_id"] == "mom_6m__lag_1"
    assert payload["factor_id"] == "mom_6m"
    assert payload["passed"] is True


def test_build_gate_after_has_attempt_id_and_failed_pass_flag() -> None:
    variant = VariantSpec(
        variant_id="mom_6m__lag_1",
        factor_id="mom_6m",
        reason="rank_ic_min_threshold",
        transforms=[{"name": "lag", "params": {"periods": 1}}],
    )
    attempt = RepairAttempt(
        attempt_id="attempt_009",
        variant=variant,
        bottleneck_window=6,
        early_stopped=False,
        passed=False,
        metrics={
            "rank_ic_min": 0.01,
            "coverage_min": 0.95,
            "windows": {
                "6": {"rank_ic": 0.01, "coverage": 0.95},
            },
        },
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )

    payload = build_gate_after(attempt, as_of="2026-02-09", run_id="p2.test.repair")
    assert payload["status"] == "ok"
    assert payload["attempt_id"] == "attempt_009"
    assert payload["passed"] is False
