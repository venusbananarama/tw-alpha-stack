from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from alpha_core.phase2.corelib import factor_slo_lib
from alpha_core.phase2.corelib.io import atomic_write_json

from .contracts import now_iso
from .gate import GateEvaluation
from . import paths


def build_wf_summary(
    *,
    root: Path,
    as_of: str,
    engine: str,
    profile: str,
    preset: str,
    gate_eval: GateEvaluation,
    rules_path: Path,
    windows: Optional[Sequence[int]] = None,
    materialize_source: Optional[str] = None,
) -> Dict[str, Any]:
    total = len(gate_eval.passed) + len(gate_eval.failed)
    pass_rate = (len(gate_eval.passed) / total) if total > 0 else 0.0

    resolved_windows: List[int] = []
    if windows:
        for w in windows:
            try:
                w_int = int(w)
            except Exception:
                continue
            if w_int > 0:
                resolved_windows.append(w_int)
    if not resolved_windows:
        for check in gate_eval.checks:
            for w in check.windows:
                try:
                    w_int = int(w)
                except Exception:
                    continue
                if w_int > 0:
                    resolved_windows.append(w_int)
    if not resolved_windows:
        resolved_windows = [6, 12, 24]
    resolved_windows = sorted(set(resolved_windows))

    wf_summary: Dict[str, Any] = {
        "schema": "wf_summary.v1",
        "overall": {
            "as_of": as_of,
            "engine": engine,
            "profile": profile,
            "preset": preset,
            "generated": now_iso(),
            "windows": resolved_windows,
            "wf": {
                "pass_rate": pass_rate,
                "passed": len(gate_eval.passed),
                "failed": len(gate_eval.failed),
                "source": "phase2_compose",
            },
        },
        "factors_by_status": {
            "passed": list(gate_eval.passed),
            "failed": list(gate_eval.failed),
        },
    }

    factors_node: Dict[str, Any] = {}
    for check in gate_eval.checks:
        factors_node[check.factor_id] = {
            "pass": check.passed,
            "windows": list(check.windows),
            "reasons": list(check.reasons),
            "metrics": dict(check.metrics),
        }
    wf_summary["factors"] = factors_node
    if materialize_source:
        wf_summary["overall"]["materialize_source"] = str(materialize_source)

    slo_cfg = factor_slo_lib.load_factor_slo_config(
        rules_path=rules_path,
        profile=profile,
        engine=engine,
    )
    slo_result = factor_slo_lib.evaluate_factor_slo(
        wf_summary=wf_summary,
        slo=slo_cfg,
        windows=resolved_windows,
        wf_summary_path=str(paths.wf_summary_path(root)),
    )
    wf_summary["factor_slo"] = asdict(slo_result)

    return wf_summary


def write_wf_summary(path: Path, wf_summary: Mapping[str, Any]) -> None:
    atomic_write_json(path, wf_summary, ensure_ascii=False, indent=2)
