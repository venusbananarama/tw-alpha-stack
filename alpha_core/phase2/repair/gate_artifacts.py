from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from .models import RepairAttempt


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def build_gate_before(
    gate_summary: Mapping[str, Any],
    *,
    as_of: str,
    run_id: str,
) -> Dict[str, Any]:
    checks_out: List[Dict[str, Any]] = []
    checks = gate_summary.get("checks", [])
    if isinstance(checks, list):
        for item in checks:
            if not isinstance(item, Mapping):
                continue
            factor_id = str(item.get("factor_id") or "").strip()
            reasons = item.get("reasons")
            if not factor_id or not isinstance(reasons, list) or not reasons:
                continue
            checks_out.append(
                {
                    "factor_id": factor_id,
                    "reasons": [str(x) for x in reasons if str(x).strip()],
                    "thresholds": dict(item.get("thresholds") or {}),
                }
            )

    return {
        "schema": "p2_repair_gate_before.v1",
        "as_of": as_of,
        "run_id": run_id,
        "checks_count": len(checks_out),
        "checks": checks_out,
    }


def _passes_thresholds(metrics: Mapping[str, Any], thresholds: Mapping[str, Any]) -> bool:
    windows = metrics.get("windows")
    if not isinstance(windows, Mapping) or not windows:
        return False
    min_rank_ic = _to_float(thresholds.get("min_rank_ic"))
    min_coverage = _to_float(thresholds.get("min_coverage"))

    for node in windows.values():
        if not isinstance(node, Mapping):
            return False
        rank_ic = _to_float(node.get("rank_ic"))
        coverage = _to_float(node.get("coverage"))
        if min_rank_ic is not None and (rank_ic is None or rank_ic < min_rank_ic):
            return False
        if min_coverage is not None and (coverage is None or coverage < min_coverage):
            return False
    return True


def build_gate_after(
    selected_attempt: Optional[RepairAttempt],
    *,
    as_of: str,
    run_id: str,
) -> Dict[str, Any]:
    if selected_attempt is None:
        return {
            "schema": "p2_repair_gate_after.v1",
            "as_of": as_of,
            "run_id": run_id,
            "status": "skipped",
            "reason": "no_selected_variant",
            "attempt_id": None,
            "selected_variant_id": None,
            "factor_id": None,
            "bottleneck_window": None,
            "early_stopped": False,
            "passed": False,
            "thresholds": {},
            "metrics_summary": {},
        }

    metrics_summary = {
        "rank_ic_min": selected_attempt.metrics.get("rank_ic_min"),
        "coverage_min": selected_attempt.metrics.get("coverage_min"),
        "windows": selected_attempt.metrics.get("windows", {}),
        "early_stopped": bool(selected_attempt.early_stopped),
    }
    passed = _passes_thresholds(selected_attempt.metrics, selected_attempt.thresholds)
    return {
        "schema": "p2_repair_gate_after.v1",
        "as_of": as_of,
        "run_id": run_id,
        "status": "ok",
        "reason": None,
        "attempt_id": selected_attempt.attempt_id,
        "selected_variant_id": selected_attempt.variant.variant_id,
        "factor_id": selected_attempt.variant.factor_id,
        "bottleneck_window": selected_attempt.bottleneck_window,
        "early_stopped": bool(selected_attempt.early_stopped),
        "passed": bool(passed),
        "thresholds": dict(selected_attempt.thresholds),
        "metrics_summary": metrics_summary,
    }
