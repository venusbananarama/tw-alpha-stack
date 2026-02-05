from __future__ import annotations

import json
from pathlib import Path
from typing import List, Mapping

from .contracts import Phase2Plan, Phase2PlanItem, Phase2PlanSummary, now_iso


DEFAULT_WINDOWS = [6, 12, 24]


def _collect_windows(factor_defs: Mapping[str, object]) -> List[int]:
    windows: List[int] = []
    for fd in factor_defs.values():
        wf_windows = getattr(fd, "wf_windows", None)
        if not wf_windows:
            continue
        for w in wf_windows:
            try:
                w_int = int(w)
            except Exception:
                continue
            if w_int > 0:
                windows.append(w_int)
    if not windows:
        return list(DEFAULT_WINDOWS)
    return sorted(set(windows))


def build_plan(
    *,
    as_of: str,
    engine: str,
    profile: str,
    preset: str,
    factor_defs: Mapping[str, object],
    statuses: Mapping[str, object],
    force: bool,
) -> Phase2Plan:
    windows = _collect_windows(factor_defs)
    items: List[Phase2PlanItem] = []
    compute = 0
    eval_only = 0
    skip = 0

    for fid in sorted(factor_defs.keys()):
        status = statuses.get(fid)
        has_data = bool(getattr(status, "has_data", False))
        has_eval = bool(getattr(status, "has_eval", False))

        reasons: List[str] = []
        if force:
            action = "compute"
            reasons.append("forced")
        elif not has_data:
            action = "compute"
            reasons.append("missing_data")
        elif not has_eval:
            action = "eval_only"
            reasons.append("missing_eval")
        else:
            action = "skip"
            reasons.append("up_to_date")

        if action == "compute":
            compute += 1
        elif action == "eval_only":
            eval_only += 1
        else:
            skip += 1

        items.append(
            Phase2PlanItem(
                factor_id=fid,
                action=action,
                reasons=reasons,
                wf_windows=list(windows),
            )
        )

    summary = Phase2PlanSummary(
        total=len(items),
        compute=compute,
        eval_only=eval_only,
        skip=skip,
    )
    return Phase2Plan(
        as_of=as_of,
        engine=engine,
        profile=profile,
        preset=preset,
        windows=list(windows),
        items=items,
        summary=summary,
        generated=now_iso(),
    )


def write_plan_file(path: Path, plan: Phase2Plan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
