from __future__ import annotations

from pathlib import Path
from typing import Dict

from alpha_core.config import ConfigError, FactorDefinition, load_factor_definitions as _load_defs

from .contracts import RulesSchemaError


CATEGORY_KEYS = {"classic", "ai", "other"}


def _is_disabled(fd: FactorDefinition) -> bool:
    if not bool(getattr(fd, "enabled", True)):
        return True
    meta = fd.meta or {}
    if isinstance(meta, dict):
        if meta.get("disabled") is True:
            return True
        enabled = meta.get("enabled")
        if enabled is False:
            return True
    return False


def load_factor_definitions(
    *,
    root: Path,
    rules_path: Path,
    engine: str,
    only_enabled: bool = True,
) -> Dict[str, FactorDefinition]:
    try:
        defs = _load_defs(root=root, rules_path=rules_path)
    except ConfigError as exc:
        raise RulesSchemaError(str(exc)) from exc

    key = (engine or "").strip().lower()

    # Main path: treat CLI --engine as category selector (classic/ai/other).
    if key in CATEGORY_KEYS:
        defs = {
            fid: fd
            for fid, fd in defs.items()
            if str(getattr(fd, "category", "")).strip().lower() == key
        }
    elif key:
        # Fallback path: allow impl-engine routing key filters (ta_mom_v1, ...).
        defs = {
            fid: fd
            for fid, fd in defs.items()
            if str(getattr(fd, "engine", "")).strip().lower() == key
        }

    if only_enabled:
        defs = {fid: fd for fid, fd in defs.items() if not _is_disabled(fd)}

    return defs
