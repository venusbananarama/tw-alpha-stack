from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


VARIANT_SORT_POLICY = "priority_desc, transforms_len_asc, variant_id_asc"


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    factor_id: str
    reason: str
    transforms: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        pr: Optional[int] = None
        try:
            pr = int(self.metadata.get("priority")) if "priority" in self.metadata else None
        except Exception:
            pr = None
        return {
            "variant_id": self.variant_id,
            "factor_id": self.factor_id,
            "reason": self.reason,
            "variant_priority": pr,
            "transforms_len": len(self.transforms),
            "transforms": list(self.transforms),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class RepairPlan:
    as_of: str
    run_id: str
    fail_reasons: Dict[str, List[str]] = field(default_factory=dict)
    variants: List[VariantSpec] = field(default_factory=list)
    variant_sort_policy: str = VARIANT_SORT_POLICY

    def to_dict(self) -> Dict[str, Any]:
        variant_rows: List[Dict[str, Any]] = []
        for idx, variant in enumerate(self.variants, start=1):
            row = variant.to_dict()
            row["seq"] = idx
            row["sort_key_debug"] = {
                "priority": row.get("variant_priority"),
                "transforms_len": len(variant.transforms),
                "variant_id": variant.variant_id,
            }
            variant_rows.append(row)
        return {
            "as_of": self.as_of,
            "run_id": self.run_id,
            "fail_reasons": {k: list(v) for k, v in self.fail_reasons.items()},
            "variant_sort_policy": self.variant_sort_policy,
            "variants": variant_rows,
        }


@dataclass(frozen=True)
class RepairAttempt:
    attempt_id: str
    variant: VariantSpec
    bottleneck_window: Optional[int]
    early_stopped: bool
    passed: bool
    metrics: Dict[str, Any] = field(default_factory=dict)
    thresholds: Dict[str, Any] = field(default_factory=dict)
    elapsed_sec: float = 0.0
    error: Optional[str] = None
    seq: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        seq_value: Optional[int]
        if self.seq is None:
            seq_value = None
        else:
            seq_value = int(self.seq)
        return {
            "attempt_id": self.attempt_id,
            "seq": seq_value,
            "variant": self.variant.to_dict(),
            "bottleneck_window": self.bottleneck_window,
            "early_stopped": self.early_stopped,
            "passed": self.passed,
            "metrics": dict(self.metrics),
            "thresholds": dict(self.thresholds),
            "elapsed_sec": float(self.elapsed_sec),
            "error": self.error,
        }


@dataclass(frozen=True)
class SelectionDecision:
    selected_variant_id: Optional[str]
    passed: bool
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selected_variant_id": self.selected_variant_id,
            "passed": self.passed,
            "reason": self.reason,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class RepairResult:
    run_id: str
    run_dir: str
    attempted: int
    passed: bool
    selected_variant_id: Optional[str]
    decision_reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "attempted": int(self.attempted),
            "passed": bool(self.passed),
            "selected_variant_id": self.selected_variant_id,
            "decision_reason": self.decision_reason,
        }
