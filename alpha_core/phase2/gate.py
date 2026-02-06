from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from alpha_core.phase2.corelib.config import FactorDefinition, GateRule
from alpha_core.phase2.corelib import factor_eval_lib
from alpha_core.phase2.corelib.io import atomic_write_json

from .contracts import now_iso
from . import paths


@dataclass(frozen=True)
class GateFactorResult:
    factor_id: str
    passed: bool
    reasons: List[str]
    metrics: Dict[str, Optional[float]]
    thresholds: Dict[str, Optional[float]]
    windows: List[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "factor_id": self.factor_id,
            "pass": self.passed,
            "reasons": list(self.reasons),
            "metrics": dict(self.metrics),
            "thresholds": dict(self.thresholds),
            "windows": list(self.windows),
        }


@dataclass(frozen=True)
class GateEvaluation:
    passed: List[str]
    failed: List[str]
    checks: List[GateFactorResult]


def _metric_spec() -> Dict[str, str]:
    return {
        "min_rank_ic": "rank_ic",
        "min_psr": "psr",
        "max_turnover": "turnover",
        "max_corr": "max_corr",
        "min_coverage": "coverage",
        "max_maxdd": "max_dd",
        "min_t_value": "t_value",
        "min_dsr": "dsr",
        "max_replay_mae_bps": "replay_mae_bps",
        "min_replay_match": "replay_match",
    }


def _check_threshold(value: Optional[float], threshold: Optional[float], mode: str) -> bool:
    if threshold is None:
        return True
    if value is None:
        return False
    if mode == "min":
        return value >= threshold
    if mode == "max":
        return value <= threshold
    return False


def _metric_mode(key: str) -> str:
    return "max" if key.startswith("max_") else "min"


def evaluate_gate(
    *,
    factor_defs: Mapping[str, FactorDefinition],
    eval_files: Mapping[str, factor_eval_lib.FactorEvalFile],
    windows: Sequence[int],
) -> GateEvaluation:
    passed: List[str] = []
    failed: List[str] = []
    checks: List[GateFactorResult] = []
    metric_spec = _metric_spec()

    for fid in sorted(factor_defs.keys()):
        fd = factor_defs[fid]
        gate_rules: GateRule = fd.gate_rules
        eval_file = eval_files.get(fid)
        reasons: List[str] = []
        metrics: Dict[str, Optional[float]] = {}
        thresholds: Dict[str, Optional[float]] = {}

        if eval_file is None:
            reasons.append("missing_eval")
            checks.append(
                GateFactorResult(
                    factor_id=fid,
                    passed=False,
                    reasons=reasons,
                    metrics=metrics,
                    thresholds=thresholds,
                    windows=list(windows),
                )
            )
            failed.append(fid)
            continue

        eval_obj = eval_file.data

        ok = True
        for rule_key, metric in metric_spec.items():
            threshold = getattr(gate_rules, rule_key, None)
            thresholds[rule_key] = threshold
            if threshold is None:
                continue
            mode = _metric_mode(rule_key)
            value = factor_eval_lib.get_aggregated_metric(eval_obj, metric, list(windows), mode=mode)
            metrics[metric] = value
            if not _check_threshold(value, threshold, mode):
                ok = False
                reasons.append(f"{metric}_{mode}_threshold")

        checks.append(
            GateFactorResult(
                factor_id=fid,
                passed=ok,
                reasons=reasons,
                metrics=metrics,
                thresholds=thresholds,
                windows=list(windows),
            )
        )
        if ok:
            passed.append(fid)
        else:
            failed.append(fid)

    return GateEvaluation(passed=passed, failed=failed, checks=checks)


def write_gate_outputs(
    *,
    root: Path,
    as_of: str,
    engine: str,
    profile: str,
    mode: str,
    preset: str,
    gate_eval: GateEvaluation,
    wf_summary_path: Path,
    factor_slo: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Path]:
    root = root.resolve()
    pass_path = paths.pass_results_path(root)
    fail_path = paths.fail_results_path(root)
    gate_path = paths.gate_summary_path(root)

    pass_path.parent.mkdir(parents=True, exist_ok=True)

    headers = ["factor_id", "window", "pass", "reason"]

    with pass_path.open("w", encoding="utf-8", newline="") as f_pass:
        w_pass = csv.DictWriter(f_pass, fieldnames=headers)
        w_pass.writeheader()
        for check in gate_eval.checks:
            if not check.passed:
                continue
            for w in check.windows:
                w_pass.writerow(
                    {
                        "factor_id": check.factor_id,
                        "window": w,
                        "pass": True,
                        "reason": ";".join(check.reasons),
                    }
                )

    with fail_path.open("w", encoding="utf-8", newline="") as f_fail:
        w_fail = csv.DictWriter(f_fail, fieldnames=headers)
        w_fail.writeheader()
        for check in gate_eval.checks:
            if check.passed:
                continue
            for w in check.windows:
                w_fail.writerow(
                    {
                        "factor_id": check.factor_id,
                        "window": w,
                        "pass": False,
                        "reason": ";".join(check.reasons),
                    }
                )

    total = len(gate_eval.passed) + len(gate_eval.failed)
    pass_rate = (len(gate_eval.passed) / total) if total > 0 else 0.0
    gate_pass = len(gate_eval.failed) == 0
    if total == 0:
        gate_pass = False

    if factor_slo is not None and isinstance(factor_slo, Mapping):
        gate_pass = gate_pass and bool(factor_slo.get("satisfied", True))

    reason = "ok" if gate_pass else "fail"
    if total == 0 and not gate_pass:
        reason = "empty_factor_set_after_materialize"

    payload = {
        "schema": "gate_summary.v1",
        "spec": "gate_rules.v2.0",
        "overall": {
            "stage": "p2",
            "mode": mode,
            "gate": "PASS" if gate_pass else "FAIL",
            "pass": gate_pass,
            "pass_rate": pass_rate,
            "reason": reason,
        },
        "counts": {
            "passed": len(gate_eval.passed),
            "failed": len(gate_eval.failed),
            "total": total,
        },
        "factors": {
            "passed": list(gate_eval.passed),
            "failed": list(gate_eval.failed),
        },
        "checks": [check.to_dict() for check in gate_eval.checks],
        "run": {
            "as_of": as_of,
            "engine": engine,
            "profile": profile,
            "preset": preset,
            "generated": now_iso(),
            "wf_summary": str(wf_summary_path),
        },
    }

    if factor_slo is not None:
        payload["factor_slo"] = dict(factor_slo)

    atomic_write_json(gate_path, payload, ensure_ascii=False, indent=2)

    return {
        "pass_results": pass_path,
        "fail_results": fail_path,
        "gate_summary": gate_path,
    }
