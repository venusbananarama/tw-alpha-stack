from __future__ import annotations

from typing import Any, Dict, Optional

from .eval_adapter import EvalAdapter
from .models import RepairAttempt


def run_holdout_check(
    *,
    selected_attempt: Optional[RepairAttempt],
    adapter: Optional[EvalAdapter],
    holdout_ratio: float = 0.2,
) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "schema": "p2_repair_holdout.v1",
        "selected_variant_id": None,
        "factor_id": None,
        "split": None,
        "selected": None,
    }

    if selected_attempt is None:
        return {
            **base,
            "status": "skipped",
            "reason": "no_selected_variant",
            "passed": False,
            "metrics": {},
        }

    if adapter is None:
        return {
            **base,
            "status": "skipped",
            "reason": "no_eval_adapter",
            "selected_variant_id": selected_attempt.variant.variant_id,
            "factor_id": selected_attempt.variant.factor_id,
            "passed": False,
            "metrics": {},
            "selected": {
                "variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": False,
                "metrics": {},
            },
        }

    try:
        selected = {
            "variant_id": selected_attempt.variant.variant_id,
            "factor_id": selected_attempt.variant.factor_id,
            "passed": False,
            "metrics": {},
        }

        resolve_split = getattr(adapter, "resolve_holdout_split", None)
        if not callable(resolve_split):
            return {
                **base,
                "status": "skipped",
                "reason": "holdout_split_unavailable",
                "selected_variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": False,
                "metrics": {},
                "selected": selected,
            }

        split = resolve_split(
            factor_id=selected_attempt.variant.factor_id,
            transforms=selected_attempt.variant.transforms,
            ratio=holdout_ratio,
        )
        if split is None:
            return {
                **base,
                "status": "skipped",
                "reason": "holdout_split_unavailable",
                "selected_variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": False,
                "metrics": {},
                "selected": selected,
            }
        if not isinstance(split, dict) or not split.get("holdout_start"):
            return {
                **base,
                "status": "skipped",
                "reason": "holdout_split_unavailable",
                "selected_variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": False,
                "metrics": {},
                "selected": selected,
            }

        result = adapter.evaluate_variant(
            factor_id=selected_attempt.variant.factor_id,
            transforms=selected_attempt.variant.transforms,
            thresholds=selected_attempt.thresholds,
            bottleneck_window=None,
            stop_fast=False,
            date_start=str(split["holdout_start"]),
        )
        metrics = adapter.summarize_metrics(result)
        return {
            **base,
            "status": "ok",
            "reason": None,
            "selected_variant_id": selected_attempt.variant.variant_id,
            "factor_id": selected_attempt.variant.factor_id,
            "passed": bool(result.passed),
            "metrics": metrics,
            "split": {
                "ratio": split.get("ratio"),
                "train_end": split.get("train_end"),
                "holdout_start": split.get("holdout_start"),
            },
            "selected": {
                "variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": bool(result.passed),
                "metrics": metrics,
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **base,
            "status": "error",
            "reason": "holdout_eval_failed",
            "selected_variant_id": selected_attempt.variant.variant_id,
            "factor_id": selected_attempt.variant.factor_id,
            "passed": False,
            "metrics": {},
            "selected": {
                "variant_id": selected_attempt.variant.variant_id,
                "factor_id": selected_attempt.variant.factor_id,
                "passed": False,
                "metrics": {},
            },
            "error": str(exc),
        }
