from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_gate_policy(
    profile: str,
    gate_policy_arg: Optional[str],
) -> Literal["require_pass", "allow_fail"]:
    val = (gate_policy_arg or "").strip().lower()
    if val in ("require_pass", "allow_fail"):
        return val
    p = (profile or "").strip().lower()
    if p in ("live", "prod", "production"):
        return "require_pass"
    return "allow_fail"


class Phase2Error(RuntimeError):
    """Base error for Phase-2 core."""


class RulesSchemaError(Phase2Error):
    """Raised when registry or rules parsing fails."""


class MissingInputsError(Phase2Error):
    """Raised when required inputs are missing."""


class GateFailError(Phase2Error):
    """Raised when WFGate fails."""


@dataclass(frozen=True)
class Phase2RunConfig:
    root: Path
    rules_path: Path
    as_of: date
    engine: str
    profile: str
    mode: Literal["dry-run", "apply"]
    preset: str
    force: bool
    run_id: str
    p1_policy: Literal["ignore", "require_pass", "auto_run_core"]
    gate_policy: Literal["require_pass", "allow_fail"] = "allow_fail"


@dataclass(frozen=True)
class FactorStatus:
    factor_id: str
    engine: str
    has_data: bool
    has_eval: bool
    latest_partition: Optional[str] = None
    eval_path: Optional[Path] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "factor_id": self.factor_id,
            "engine": self.engine,
            "has_data": self.has_data,
            "has_eval": self.has_eval,
            "latest_partition": self.latest_partition,
            "eval_path": str(self.eval_path) if self.eval_path else None,
        }


@dataclass(frozen=True)
class Phase2PlanItem:
    factor_id: str
    action: Literal["compute", "eval_only", "skip"]
    reasons: List[str] = field(default_factory=list)
    wf_windows: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "factor_id": self.factor_id,
            "action": self.action,
            "reasons": list(self.reasons),
            "wf_windows": list(self.wf_windows),
        }


@dataclass(frozen=True)
class Phase2PlanSummary:
    total: int
    compute: int
    eval_only: int
    skip: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "compute": self.compute,
            "eval_only": self.eval_only,
            "skip": self.skip,
        }


@dataclass(frozen=True)
class Phase2Plan:
    as_of: str
    engine: str
    profile: str
    preset: str
    windows: List[int]
    items: List[Phase2PlanItem]
    summary: Phase2PlanSummary
    generated: str = field(default_factory=now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": "phase2_plan.v1",
            "as_of": self.as_of,
            "engine": self.engine,
            "profile": self.profile,
            "preset": self.preset,
            "windows": list(self.windows),
            "generated": self.generated,
            "summary": self.summary.to_dict(),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class StageResult:
    name: str
    status: str
    started_at: str
    finished_at: str
    outputs: Dict[str, str] = field(default_factory=dict)
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "outputs": dict(self.outputs),
            "message": self.message,
        }


@dataclass(frozen=True)
class Phase2RunResult:
    run_id: str
    status: str
    gate_pass: Optional[bool]
    as_of: str
    engine: str
    profile: str
    preset: str
    mode: str
    stages: List[StageResult]
    artefacts: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "gate_pass": self.gate_pass,
            "as_of": self.as_of,
            "engine": self.engine,
            "profile": self.profile,
            "preset": self.preset,
            "mode": self.mode,
            "stages": [stage.to_dict() for stage in self.stages],
            "artefacts": dict(self.artefacts),
        }
