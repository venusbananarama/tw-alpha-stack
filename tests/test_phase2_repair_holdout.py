from __future__ import annotations

from alpha_core.phase2.repair.holdout import run_holdout_check
from alpha_core.phase2.repair.models import RepairAttempt, VariantSpec


def test_holdout_skips_when_no_selected_variant() -> None:
    payload = run_holdout_check(selected_attempt=None, adapter=None)
    assert payload["status"] == "skipped"
    assert payload["reason"] == "no_selected_variant"
    assert payload["selected_variant_id"] is None
    assert payload["split"] is None
    assert payload["selected"] is None


def test_holdout_runs_with_selected_variant() -> None:
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
        metrics={"windows": {"6": {"rank_ic": 0.05, "coverage": 0.95}}},
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )

    class DummyEvalResult:
        def __init__(self) -> None:
            self.passed = True
            self.early_stopped = False
            self.windows = {"6": {"rank_ic": 0.06, "coverage": 0.95}}

    class DummyAdapter:
        @staticmethod
        def resolve_holdout_split(**kwargs):  # type: ignore[no-untyped-def]
            assert kwargs["ratio"] == 0.2
            return {
                "ratio": 0.2,
                "train_end": "2026-01-31",
                "holdout_start": "2026-02-01",
            }

        def evaluate_variant(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs["factor_id"] == "mom_6m"
            assert kwargs["stop_fast"] is False
            assert kwargs["date_start"] == "2026-02-01"
            return DummyEvalResult()

        @staticmethod
        def summarize_metrics(result):  # type: ignore[no-untyped-def]
            return {
                "rank_ic_min": 0.06,
                "coverage_min": 0.95,
                "windows": result.windows,
                "passed": result.passed,
                "early_stopped": result.early_stopped,
            }

    payload = run_holdout_check(selected_attempt=attempt, adapter=DummyAdapter())  # type: ignore[arg-type]
    assert payload["status"] == "ok"
    assert payload["selected_variant_id"] == "mom_6m__lag_1"
    assert payload["factor_id"] == "mom_6m"
    assert payload["passed"] is True
    assert payload["metrics"]["rank_ic_min"] == 0.06
    assert payload["split"]["ratio"] == 0.2
    assert payload["selected"]["variant_id"] == "mom_6m__lag_1"
    assert payload["selected"]["metrics"]["rank_ic_min"] == 0.06


def test_holdout_skips_when_split_resolver_unavailable_but_keeps_selected() -> None:
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
        passed=False,
        metrics={},
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )

    class NoSplitAdapter:
        pass

    payload = run_holdout_check(selected_attempt=attempt, adapter=NoSplitAdapter())  # type: ignore[arg-type]
    assert payload["status"] == "skipped"
    assert payload["reason"] == "holdout_split_unavailable"
    assert payload["split"] is None
    assert payload["selected"]["variant_id"] == "mom_6m__lag_1"
    assert payload["selected"]["factor_id"] == "mom_6m"
    assert payload["selected"]["passed"] is False
