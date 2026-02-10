from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PromoteResult:
    promoted: bool
    mode: str
    message: str
    target: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "promoted": self.promoted,
            "mode": self.mode,
            "message": self.message,
            "target": self.target,
        }


def normalize_promotion_mode(mode: Any) -> str:
    if isinstance(mode, bool):
        return "on" if mode else "off"
    if isinstance(mode, (int, float)):
        return "off" if float(mode) == 0.0 else "on"
    value = str(mode or "").strip().lower()
    if value in {"", "0", "false", "f", "no", "n", "off", "disabled", "none"}:
        return "off"
    if value in {"1", "true", "t", "yes", "y", "on", "enabled", "enable"}:
        return "on"
    return "on"


def promote_selected_variant(*, mode: Any, variant_id: Optional[str]) -> PromoteResult:
    resolved_mode = normalize_promotion_mode(mode)
    if resolved_mode == "off":
        return PromoteResult(
            promoted=False,
            mode="off",
            message="promotion disabled by config",
            target=variant_id,
        )

    # Promotion hook intentionally no-op in MVP to avoid shared path mutation.
    return PromoteResult(
        promoted=False,
        mode="on",
        message="promotion mode is not enabled in MVP",
        target=variant_id,
    )
