from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml

from .eval_adapter import EvalAdapter
from .gate_artifacts import build_gate_after, build_gate_before
from .holdout import run_holdout_check
from .models import (
    VARIANT_SORT_POLICY,
    RepairAttempt,
    RepairPlan,
    RepairResult,
    SelectionDecision,
    VariantSpec,
)
from .promote import promote_selected_variant
from .recorder import RunDirRecorder
from .schema_report import build_schema_report
from .search import build_candidates, collect_fail_reasons, load_repair_profile, resolve_bottleneck_window, select_best

logger = logging.getLogger(__name__)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_default_transforms(path: Path) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    node = data.get("transforms", []) if isinstance(data, Mapping) else []
    out: List[Dict[str, Any]] = []
    for item in node:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        out.append({"name": name, "params": dict(item.get("params") or {})})
    return out


def _extract_thresholds(gate_summary: Mapping[str, Any], factor_id: str) -> Dict[str, Any]:
    checks = gate_summary.get("checks", []) if isinstance(gate_summary, Mapping) else []
    if not isinstance(checks, list):
        return {}
    for item in checks:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("factor_id") or "") != factor_id:
            continue
        raw = item.get("thresholds")
        if isinstance(raw, Mapping):
            return dict(raw)
    return {}


def _expand_variants(
    *,
    fail_reason_map: Mapping[str, List[str]],
    allowlist: Sequence[str],
    allow_all_reasons: bool,
    reason_config_root: Path,
    default_transforms: Sequence[Mapping[str, Any]],
    max_attempts_per_factor: int,
) -> List[VariantSpec]:
    resolved_allow = {str(x).strip() for x in allowlist if str(x).strip()}
    all_variants: List[VariantSpec] = []

    for factor_id in sorted(fail_reason_map.keys()):
        if allow_all_reasons:
            reasons = list(fail_reason_map[factor_id])
        else:
            reasons = [r for r in fail_reason_map[factor_id] if r in resolved_allow]
        if not reasons:
            continue
        candidates = build_candidates(
            factor_id=factor_id,
            fail_reasons=reasons,
            reason_config_root=reason_config_root,
            max_attempts=max(max_attempts_per_factor, 0),
        )
        for c in candidates:
            all_variants.append(
                replace(
                    c,
                    transforms=[*list(default_transforms), *list(c.transforms)],
                )
            )

    all_variants = sorted(
        all_variants,
        key=lambda v: (
            -int(v.metadata.get("priority", 100) or 100),
            len(v.transforms),
            v.variant_id,
        ),
    )
    return all_variants


def _empty_result(
    recorder: RunDirRecorder,
    run_id: str,
    reason: str,
    *,
    gate_before: Optional[Mapping[str, Any]] = None,
) -> RepairResult:
    if not (recorder.run_dir / "repair_plan.json").is_file():
        recorder.write_artifact_json(
            "repair_plan.json",
            {
                "as_of": recorder.as_of,
                "run_id": run_id,
                "fail_reasons": {},
                "variant_sort_policy": VARIANT_SORT_POLICY,
                "variants": [],
            },
        )

    gate_before_payload = dict(gate_before or {})
    if not gate_before_payload:
        gate_before_payload = build_gate_before({}, as_of=recorder.as_of, run_id=run_id)
    gate_after_payload = build_gate_after(None, as_of=recorder.as_of, run_id=run_id)
    recorder.write_artifact_json("gate_before.json", gate_before_payload)
    recorder.write_artifact_json("gate_after.json", gate_after_payload)

    recorder.log_metrics({"attempted": 0, "repaired_pass": False, "selected_variant": ""})
    holdout = run_holdout_check(selected_attempt=None, adapter=None)
    recorder.write_artifact_json("holdout_check.json", holdout)
    recorder.finalize(
        status="skip",
        summary={
            "run_id": run_id,
            "attempted": 0,
            "passed": False,
            "decision": reason,
            "gate_after_present": True,
            "gate_after_status": gate_after_payload.get("status"),
            "holdout_present": True,
            "holdout_status": holdout.get("status"),
            "holdout_passed": bool(holdout.get("passed")),
        },
    )
    schema_report = build_schema_report(run_dir=recorder.run_dir, attempted=0)
    recorder.write_artifact_json("schema_report.json", schema_report)
    return RepairResult(
        run_id=run_id,
        run_dir=str(recorder.run_dir),
        attempted=0,
        passed=False,
        selected_variant_id=None,
        decision_reason=reason,
    )


def run_auto_repair(
    *,
    root: Path,
    as_of: str,
    profile: str,
    run_id: str,
    gate_summary_path: Path,
    wf_summary_path: Path,
    fail_results_path: Path,
    windows: Sequence[int],
) -> Optional[RepairResult]:
    root = root.resolve()
    repair_cfg_path = root / "configs" / "p2" / "repair" / "repair_profile.yaml"
    profile_cfg = load_repair_profile(repair_cfg_path, profile=profile)
    if not bool(profile_cfg.get("enabled", False)):
        return None

    repair_run_id = f"{run_id}.repair"
    recorder = RunDirRecorder(root=root, as_of=as_of, run_id=repair_run_id)
    recorder.start_run(tags={"profile": profile, "stage": "p2_repair"})
    recorder.log_params(profile_cfg)

    default_xform_path = root / "configs" / "p2" / "xforms" / "default.yaml"
    reason_xform_root = root / "configs" / "p2" / "xforms" / "by_reason"

    recorder.write_manifest(
        resolved_paths={
            "gate_summary": gate_summary_path,
            "wf_summary": wf_summary_path,
            "fail_results": fail_results_path,
            "repair_profile": repair_cfg_path,
            "default_xforms": default_xform_path,
            "reason_xforms": reason_xform_root,
        },
        versions={"repair_schema": "p2_repair.v1"},
    )

    recorder.copy_artifact(gate_summary_path, "before_gate_summary.json")
    recorder.copy_artifact(wf_summary_path, "before_wf_summary.json")

    gate_summary = _load_json(gate_summary_path)
    gate_before_payload = build_gate_before(gate_summary, as_of=as_of, run_id=repair_run_id)
    recorder.write_artifact_json("gate_before.json", gate_before_payload)
    fail_reason_map = collect_fail_reasons(gate_summary)
    if not fail_reason_map:
        return _empty_result(recorder, repair_run_id, "no_fail_reasons", gate_before=gate_before_payload)

    max_attempts_per_factor = int(profile_cfg.get("max_attempts_per_factor", profile_cfg.get("max_attempts", 3)))
    variants = _expand_variants(
        fail_reason_map=fail_reason_map,
        allowlist=profile_cfg.get("resolved_reason_allowlist", []),
        allow_all_reasons=bool(profile_cfg.get("allow_all_reasons", False)),
        reason_config_root=reason_xform_root,
        default_transforms=_load_default_transforms(default_xform_path),
        max_attempts_per_factor=max_attempts_per_factor,
    )

    plan = RepairPlan(
        as_of=as_of,
        run_id=repair_run_id,
        fail_reasons={k: list(v) for k, v in fail_reason_map.items()},
        variants=variants,
        variant_sort_policy=VARIANT_SORT_POLICY,
    )
    recorder.write_artifact_json("repair_plan.json", plan.to_dict())

    if not variants:
        return _empty_result(recorder, repair_run_id, "no_variants", gate_before=gate_before_payload)

    adapter = EvalAdapter(root=root, as_of=as_of, windows=windows)

    attempts: List[RepairAttempt] = []
    for idx, variant in enumerate(variants, start=1):
        t0 = time.perf_counter()
        thresholds = _extract_thresholds(gate_summary, variant.factor_id)
        bottleneck = resolve_bottleneck_window(
            root=root,
            fail_results_path=fail_results_path,
            factor_id=variant.factor_id,
            reason_key=variant.reason,
            fallback_windows=windows,
        )
        attempt_id = f"attempt_{idx:03d}"

        try:
            eval_result = adapter.evaluate_variant(
                factor_id=variant.factor_id,
                transforms=variant.transforms,
                thresholds=thresholds,
                bottleneck_window=bottleneck,
                stop_fast=bool(profile_cfg.get("stop_fast", True)),
            )
            metrics = adapter.summarize_metrics(eval_result)
            attempt = RepairAttempt(
                attempt_id=attempt_id,
                variant=variant,
                bottleneck_window=bottleneck,
                early_stopped=eval_result.early_stopped,
                passed=eval_result.passed,
                metrics=metrics,
                thresholds=thresholds,
                elapsed_sec=time.perf_counter() - t0,
                error=None,
                seq=int(idx),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("auto-repair attempt failed: %s", attempt_id)
            attempt = RepairAttempt(
                attempt_id=attempt_id,
                variant=variant,
                bottleneck_window=bottleneck,
                early_stopped=False,
                passed=False,
                metrics={},
                thresholds=thresholds,
                elapsed_sec=time.perf_counter() - t0,
                error=str(exc),
                seq=int(idx),
            )

        attempts.append(attempt)
        recorder.log_attempt(attempt_id, attempt.to_dict())

    decision: SelectionDecision = select_best(attempts)
    selected_attempt = next(
        (a for a in attempts if a.variant.variant_id == decision.selected_variant_id),
        None,
    )
    gate_after_payload = build_gate_after(selected_attempt, as_of=as_of, run_id=repair_run_id)
    recorder.write_artifact_json("gate_after.json", gate_after_payload)
    promote_result = promote_selected_variant(
        mode=profile_cfg.get("promotion_mode", "off"),
        variant_id=decision.selected_variant_id,
    )

    recorder.copy_artifact(gate_summary_path, "after_gate_summary.json")
    recorder.copy_artifact(wf_summary_path, "after_wf_summary.json")

    recorder.log_metrics(
        {
            "attempted": len(attempts),
            "repaired_pass": bool(decision.passed),
            "selected_variant": decision.selected_variant_id or "",
        }
    )

    holdout = run_holdout_check(selected_attempt=selected_attempt, adapter=adapter)
    recorder.write_artifact_json("holdout_check.json", holdout)
    final_result: Dict[str, Any] = {
        "repair_result": {
            "attempted": len(attempts),
            "passed": decision.passed,
            "selected_variant_id": decision.selected_variant_id,
            "decision_reason": decision.reason,
            "decision_details": decision.details,
        },
        "promotion": promote_result.to_dict(),
        "gate_after_present": True,
        "gate_after_status": gate_after_payload.get("status"),
        "holdout_present": True,
        "holdout_status": holdout.get("status"),
        "holdout_passed": bool(holdout.get("passed")),
    }
    recorder.finalize(status="ok", summary=final_result)
    schema_report = build_schema_report(run_dir=recorder.run_dir, attempted=len(attempts))
    recorder.write_artifact_json("schema_report.json", schema_report)

    return RepairResult(
        run_id=repair_run_id,
        run_dir=str(recorder.run_dir),
        attempted=len(attempts),
        passed=decision.passed,
        selected_variant_id=decision.selected_variant_id,
        decision_reason=decision.reason,
    )


__all__ = ["run_auto_repair", "RepairResult"]
