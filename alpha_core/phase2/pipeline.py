from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from alpha_core.phase2.corelib import factor_eval_lib
from alpha_core.phase2.corelib.factor_engine import FactorEngineConfig, run_factor_engine
from alpha_core.phase2.corelib.factor_eval import evaluate_factors as _evaluate_factors

from . import compose as compose_mod
from . import evidence as evidence_mod
from . import gate as gate_mod
from . import paths
from . import plan as plan_mod
from . import registry
from . import status as status_mod
from .contracts import (
    GateFailError,
    MissingInputsError,
    Phase2Plan,
    Phase2RunConfig,
    Phase2RunResult,
    StageResult,
    now_iso,
)


PRESET_STAGES: Dict[str, List[str]] = {
    "full": ["status", "plan", "engine", "eval", "corr", "compose", "gate", "evidence"],
    "debug_eval_only": ["status", "plan", "eval", "compose", "gate", "evidence"],
    "debug_status_only": ["status", "plan"],
}

PRESET_FLAGS: Dict[str, Dict[str, bool]] = {
    "full": {"corr": True},
    "debug_eval_only": {"corr": False},
    "debug_status_only": {"corr": False},
}
DEFAULT_EVAL_WINDOWS = [6, 12, 24]


def _stage_ok(name: str, outputs: Optional[Mapping[str, str]] = None, message: Optional[str] = None) -> StageResult:
    ts = now_iso()
    return StageResult(
        name=name,
        status="ok",
        started_at=ts,
        finished_at=ts,
        outputs=dict(outputs or {}),
        message=message,
    )


def _stage_skip(name: str, message: str) -> StageResult:
    ts = now_iso()
    return StageResult(
        name=name,
        status="skip",
        started_at=ts,
        finished_at=ts,
        outputs={},
        message=message,
    )


def _ensure_preset(preset: str) -> List[str]:
    if preset in PRESET_STAGES:
        return PRESET_STAGES[preset]
    raise MissingInputsError(f"Unknown preset: {preset!r}")


def _load_defs_and_statuses(
    *,
    root: Path,
    rules_path: Path,
    engine: str,
) -> Tuple[Dict[str, object], List[object]]:
    defs = registry.load_factor_definitions(
        root=root,
        rules_path=rules_path,
        engine=engine,
        only_enabled=True,
    )
    statuses = status_mod.build_factor_status(root=root, factor_defs=defs, engine=engine)
    return defs, statuses


def run_status(
    *,
    root: Path,
    rules_path: Path,
    as_of: str,
    engine: str,
    profile: str,
) -> Path:
    defs, statuses = _load_defs_and_statuses(root=root, rules_path=rules_path, engine=engine)
    status_path = paths.status_path(root, as_of, engine, profile)
    status_mod.write_status_file(
        status_path,
        as_of=as_of,
        engine=engine,
        profile=profile,
        statuses=statuses,
    )
    return status_path


def run_plan(
    *,
    root: Path,
    rules_path: Path,
    as_of: str,
    engine: str,
    profile: str,
    preset: str,
    force: bool,
) -> Tuple[Path, Phase2Plan]:
    defs, statuses = _load_defs_and_statuses(root=root, rules_path=rules_path, engine=engine)
    status_map = {s.factor_id: s for s in statuses}
    plan = plan_mod.build_plan(
        as_of=as_of,
        engine=engine,
        profile=profile,
        preset=preset,
        factor_defs=defs,
        statuses=status_map,
        force=force,
    )
    plan_path = paths.plan_path(root, as_of, engine, profile)
    plan_mod.write_plan_file(plan_path, plan)
    return plan_path, plan


def _load_gate_summary(path: Path) -> Optional[Mapping[str, object]]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _is_gate_stage(summary: Mapping[str, object], stage: str) -> bool:
    overall = summary.get("overall")
    if isinstance(overall, Mapping):
        return str(overall.get("stage") or "").lower() == stage.lower()
    return False


def _load_p1_gate_summary(root: Path) -> Optional[Mapping[str, object]]:
    p1_path = paths.p1_gate_summary_path(root)
    summary = _load_gate_summary(p1_path)
    if summary is not None and _is_gate_stage(summary, "p1"):
        return summary

    fallback = _load_gate_summary(paths.gate_summary_path(root))
    if fallback is not None and _is_gate_stage(fallback, "p1"):
        return fallback
    return None


def _is_gate_pass(summary: Mapping[str, object]) -> bool:
    overall = summary.get("overall")
    if isinstance(overall, Mapping):
        if overall.get("pass") is True:
            return True
        gate = overall.get("gate")
        if isinstance(gate, str) and gate.upper() == "PASS":
            return True
    return False


def _ensure_p1_gate(root: Path, policy: str) -> None:
    if policy == "ignore":
        return
    summary = _load_p1_gate_summary(root)
    if summary and _is_gate_pass(summary):
        return
    if policy == "require_pass":
        raise GateFailError("Phase-1 WFGate is not PASS")
    if policy == "auto_run_core":
        raise MissingInputsError("Phase-1 core auto-run is not implemented")
    raise MissingInputsError(f"Unknown p1 policy: {policy!r}")


def _write_corr_stub(path: Path, *, as_of: str, engine: str, profile: str) -> None:
    payload = {
        "schema": "phase2_corr.v1",
        "status": "skipped",
        "as_of": as_of,
        "engine": engine,
        "profile": profile,
        "generated": now_iso(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _dedupe_keep_order(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _resolve_plan_windows(plan: Phase2Plan) -> List[int]:
    wins: List[int] = []
    for w in plan.windows:
        try:
            w_int = int(w)
        except Exception:
            continue
        if w_int > 0:
            wins.append(w_int)

    if not wins:
        for item in plan.items:
            for w in item.wf_windows:
                try:
                    w_int = int(w)
                except Exception:
                    continue
                if w_int > 0:
                    wins.append(w_int)

    if not wins:
        wins = list(DEFAULT_EVAL_WINDOWS)
    return sorted(set(wins))


def _is_eval_usable(
    eval_file: factor_eval_lib.FactorEvalFile,
    *,
    as_of: Optional[str],
    windows: Sequence[int],
) -> bool:
    data = eval_file.data if isinstance(eval_file.data, Mapping) else {}
    eval_as_of = str(data.get("as_of") or "").strip()
    if as_of:
        if not eval_as_of or eval_as_of != as_of:
            return False

    if not windows:
        return True

    windows_node = data.get("windows")
    if not isinstance(windows_node, Mapping):
        return False
    for w in windows:
        if str(int(w)) not in windows_node:
            return False
    return True


def _load_eval_files(
    root: Path,
    factor_ids: Sequence[str],
    *,
    as_of: Optional[str] = None,
    windows: Sequence[int] = (),
    require_usable: bool = False,
) -> Dict[str, factor_eval_lib.FactorEvalFile]:
    result: Dict[str, factor_eval_lib.FactorEvalFile] = {}
    for fid in _dedupe_keep_order(factor_ids):
        try:
            ef = factor_eval_lib.load_factor_eval(root, fid)
        except Exception:
            continue
        if require_usable and not _is_eval_usable(ef, as_of=as_of, windows=windows):
            continue
        result[fid] = ef
    return result


def _resolve_final_status(gate_pass: bool, gate_policy: str) -> str:
    if gate_pass:
        return "OK"
    if str(gate_policy).strip().lower() == "allow_fail":
        return "OK"
    return "FAIL"


def run_phase2(cfg: Phase2RunConfig) -> Phase2RunResult:
    root = cfg.root.resolve()
    stages: List[StageResult] = []
    artefacts: Dict[str, str] = {}
    preset_stages = _ensure_preset(cfg.preset)

    _ensure_p1_gate(root, cfg.p1_policy)

    defs, statuses = _load_defs_and_statuses(root=root, rules_path=cfg.rules_path, engine=cfg.engine)
    status_map = {s.factor_id: s for s in statuses}

    if "status" in preset_stages:
        status_path = paths.status_path(root, cfg.as_of.isoformat(), cfg.engine, cfg.profile)
        status_mod.write_status_file(
            status_path,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            statuses=statuses,
        )
        stages.append(_stage_ok("status", outputs={"status": str(status_path)}))
        artefacts["status"] = str(status_path)
    else:
        stages.append(_stage_skip("status", "preset_skip"))

    plan_path = paths.plan_path(root, cfg.as_of.isoformat(), cfg.engine, cfg.profile)
    plan = plan_mod.build_plan(
        as_of=cfg.as_of.isoformat(),
        engine=cfg.engine,
        profile=cfg.profile,
        preset=cfg.preset,
        factor_defs=defs,
        statuses=status_map,
        force=cfg.force,
    )
    plan_mod.write_plan_file(plan_path, plan)
    stages.append(_stage_ok("plan", outputs={"plan": str(plan_path)}))
    artefacts["plan"] = str(plan_path)

    if cfg.mode == "dry-run":
        return Phase2RunResult(
            run_id=cfg.run_id,
            status="DRY_RUN",
            gate_pass=None,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            preset=cfg.preset,
            mode=cfg.mode,
            stages=stages,
            artefacts=artefacts,
        )

    if "engine" not in preset_stages and "eval" not in preset_stages and "compose" not in preset_stages:
        return Phase2RunResult(
            run_id=cfg.run_id,
            status="PARTIAL",
            gate_pass=None,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            preset=cfg.preset,
            mode=cfg.mode,
            stages=stages,
            artefacts=artefacts,
        )

    resolved_windows = _resolve_plan_windows(plan)
    all_plan_factors = _dedupe_keep_order([item.factor_id for item in plan.items])
    compute_factors = [item.factor_id for item in plan.items if item.action == "compute"]
    eval_only_factors = [item.factor_id for item in plan.items if item.action == "eval_only"]
    skip_factors = [item.factor_id for item in plan.items if item.action == "skip"]

    if "engine" in preset_stages and compute_factors:
        engine_cfg = FactorEngineConfig(
            root=root,
            rules_path=cfg.rules_path,
            impl_module="alpha_core.phase2.factor_impl",
            factor_root=paths.factor_root(root),
            ledger_path=root / "metrics" / "factor_ledger.jsonl",
            summary_path=root / "reports" / "factor_engine_summary.json",
            end_date=cfg.as_of,
            windows=list(resolved_windows),
            factors=compute_factors,
            dry_run=False,
            run_id_prefix=cfg.run_id,
        )
        summary = run_factor_engine(engine_cfg)
        stages.append(_stage_ok("engine", outputs={"summary": str(engine_cfg.summary_path)}))
        artefacts["factor_engine_summary"] = str(engine_cfg.summary_path)
    elif "engine" in preset_stages:
        stages.append(_stage_skip("engine", "no_compute_tasks"))
    else:
        stages.append(_stage_skip("engine", "preset_skip"))

    eval_files: Dict[str, factor_eval_lib.FactorEvalFile] = {}
    materialize_source = "none"
    as_of_iso = cfg.as_of.isoformat()
    if "eval" in preset_stages:
        if not all_plan_factors:
            stages.append(_stage_skip("eval", "no_plan_items"))
        else:
            loaded_skip = _load_eval_files(
                root,
                skip_factors,
                as_of=as_of_iso,
                windows=resolved_windows,
                require_usable=True,
            )
            fallback_eval_factors = [fid for fid in skip_factors if fid not in loaded_skip]
            eval_targets = _dedupe_keep_order(compute_factors + eval_only_factors + fallback_eval_factors)

            if eval_targets:
                _evaluate_factors(
                    root=root,
                    factor_ids=eval_targets,
                    wf_windows=list(resolved_windows),
                    as_of=as_of_iso,
                )

            eval_files = _load_eval_files(
                root,
                all_plan_factors,
                as_of=as_of_iso,
                windows=resolved_windows,
                require_usable=True,
            )
            loaded_count = len(loaded_skip)
            evaluated_count = len(eval_targets)
            if loaded_count > 0 and evaluated_count > 0:
                materialize_source = "mixed"
            elif loaded_count > 0:
                materialize_source = "loaded"
            elif evaluated_count > 0:
                materialize_source = "eval_only"

            stages.append(
                _stage_ok(
                    "eval",
                    outputs={"factor_eval_dir": str(paths.factor_eval_dir(root))},
                    message=(
                        f"materialized={materialize_source} "
                        f"resolved={len(eval_files)}/{len(all_plan_factors)}"
                    ),
                )
            )
            artefacts["factor_eval_dir"] = str(paths.factor_eval_dir(root))
    else:
        eval_files = _load_eval_files(
            root,
            all_plan_factors,
            as_of=as_of_iso,
            windows=resolved_windows,
            require_usable=True,
        )
        stages.append(_stage_skip("eval", "preset_skip"))

    corr_enabled = PRESET_FLAGS.get(cfg.preset, {}).get("corr", False)
    if corr_enabled and "corr" in preset_stages:
        corr_path = paths.corr_summary_path(root, cfg.as_of.isoformat(), cfg.engine, cfg.profile)
        _write_corr_stub(corr_path, as_of=cfg.as_of.isoformat(), engine=cfg.engine, profile=cfg.profile)
        stages.append(_stage_ok("corr", outputs={"corr": str(corr_path)}))
        artefacts["corr_summary"] = str(corr_path)
    elif "corr" in preset_stages:
        stages.append(_stage_skip("corr", "preset_skip"))

    if "compose" not in preset_stages:
        return Phase2RunResult(
            run_id=cfg.run_id,
            status="PARTIAL",
            gate_pass=None,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            preset=cfg.preset,
            mode=cfg.mode,
            stages=stages,
            artefacts=artefacts,
        )

    gate_eval = gate_mod.evaluate_gate(
        factor_defs=defs,
        eval_files=eval_files,
        windows=resolved_windows,
    )

    wf_summary = compose_mod.build_wf_summary(
        root=root,
        as_of=cfg.as_of.isoformat(),
        engine=cfg.engine,
        profile=cfg.profile,
        preset=cfg.preset,
        gate_eval=gate_eval,
        windows=resolved_windows,
        materialize_source=materialize_source,
        rules_path=cfg.rules_path,
    )
    wf_path = paths.wf_summary_path(root)
    compose_mod.write_wf_summary(wf_path, wf_summary)
    stages.append(_stage_ok("compose", outputs={"wf_summary": str(wf_path)}))
    artefacts["wf_summary"] = str(wf_path)

    if "gate" not in preset_stages:
        return Phase2RunResult(
            run_id=cfg.run_id,
            status="PARTIAL",
            gate_pass=None,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            preset=cfg.preset,
            mode=cfg.mode,
            stages=stages,
            artefacts=artefacts,
        )

    factor_slo = wf_summary.get("factor_slo")
    gate_outputs = gate_mod.write_gate_outputs(
        root=root,
        as_of=cfg.as_of.isoformat(),
        engine=cfg.engine,
        profile=cfg.profile,
        mode=cfg.mode,
        preset=cfg.preset,
        gate_eval=gate_eval,
        wf_summary_path=wf_path,
        factor_slo=factor_slo if isinstance(factor_slo, Mapping) else None,
    )
    stages.append(
        _stage_ok(
            "gate",
            outputs={
                "gate_summary": str(gate_outputs["gate_summary"]),
                "pass_results": str(gate_outputs["pass_results"]),
                "fail_results": str(gate_outputs["fail_results"]),
            },
        )
    )
    artefacts.update({k: str(v) for k, v in gate_outputs.items()})

    if "evidence" not in preset_stages:
        gate_pass = len(gate_eval.failed) == 0 and bool(
            factor_slo.get("satisfied", True) if isinstance(factor_slo, Mapping) else True
        )
        status = _resolve_final_status(gate_pass, cfg.gate_policy)
        return Phase2RunResult(
            run_id=cfg.run_id,
            status=status,
            gate_pass=gate_pass,
            as_of=cfg.as_of.isoformat(),
            engine=cfg.engine,
            profile=cfg.profile,
            preset=cfg.preset,
            mode=cfg.mode,
            stages=stages,
            artefacts=artefacts,
        )

    evidence_dir = evidence_mod.build_evidence_pack(
        root=root,
        run_id=cfg.run_id,
        artefacts={k: Path(v) for k, v in artefacts.items()},
    )
    stages.append(_stage_ok("evidence", outputs={"evidence_dir": str(evidence_dir)}))
    artefacts["evidence_dir"] = str(evidence_dir)

    gate_pass = len(gate_eval.failed) == 0 and bool(
        factor_slo.get("satisfied", True) if isinstance(factor_slo, Mapping) else True
    )

    status = _resolve_final_status(gate_pass, cfg.gate_policy)
    return Phase2RunResult(
        run_id=cfg.run_id,
        status=status,
        gate_pass=gate_pass,
        as_of=cfg.as_of.isoformat(),
        engine=cfg.engine,
        profile=cfg.profile,
        preset=cfg.preset,
        mode=cfg.mode,
        stages=stages,
        artefacts=artefacts,
    )
