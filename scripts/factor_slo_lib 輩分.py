# C:\AI\tw-alpha-stack\scripts\factor_slo_lib.py
#!/usr/bin/env python
"""
factor_slo_lib.py

Shared library for factor gate-ready SLO handling.

設計重點：
- 以 rules_factors.yaml 的 gate_ready 區塊為 SSOT。
- precedence = root.gate_ready → gate_ready.engines[engine] → gate_ready.profiles[profile]。
- 本模組是純函式庫，不讀寫檔案以外的任何 side effect：
  - 不自己讀 wf_summary.json
  - 不決定要不要讓 Gate fail，只回傳結果（satisfied=True/False）
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Any

import json

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # Checked at runtime


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FactorSloConfig:
    """
    Parsed gate_ready SLO config derived from rules_factors.yaml.

    Precedence:
      1) root.gate_ready
      2) gate_ready.engines[engine]
      3) gate_ready.profiles[profile]  (highest priority)

    NOTE:
    - per_window_min 來自「合併後」的 per_window（已含 engine/profile override）。
    """

    source: str
    profile: Optional[str]
    engine: str
    min_factors: int
    min_per_window: int
    required_factors: List[str]
    per_window_min: Dict[str, int]
    raw_gate_ready: Optional[Mapping[str, Any]]


@dataclass(frozen=True)
class FactorSloResult:
    """
    SLO evaluation result against a given wf_summary object.

    Consumers can convert it to dict via dataclasses.asdict() and then
    serialize to JSON or embed into wf_summary["factor_slo"] / gate_summary.json.
    """

    name: str
    profile: Optional[str]
    engine: str
    source: str
    wf_summary_path: str
    min_factors: int
    min_factors_per_window: int
    per_window_min: Dict[str, int]
    required_factors: List[str]
    total_factors: int
    windows: List[int]
    per_window_counts: Dict[int, int]
    missing_required_factors: List[str]
    satisfied: bool


# ---------------------------------------------------------------------------
# YAML loading helpers
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> Mapping[str, Any]:
    """
    Load YAML mapping from file, with basic sanity checks.

    This function is kept small and reusable so both CLI and PS1 wrappers
    can rely on the same semantics when interpreting rules_factors.yaml.
    """
    if not path.exists():
        raise FileNotFoundError(f"rules file not found: {path}")
    if yaml is None:
        raise RuntimeError("PyYAML is not installed; cannot parse YAML.")

    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("rules file is empty")

    doc = yaml.safe_load(text)
    if not isinstance(doc, Mapping):
        raise ValueError("rules file must contain a mapping at top level")

    return doc


# ---------------------------------------------------------------------------
# Public API: load SLO config
# ---------------------------------------------------------------------------


def load_factor_slo_config(
    rules_path: Path,
    profile: Optional[str],
    engine: str,
) -> FactorSloConfig:
    """
    Load gate_ready SLO from rules_factors.yaml.

    Precedence:
      1) root.gate_ready
      2) gate_ready.engines[engine]
      3) gate_ready.profiles[profile]  (highest priority)

    On failure or when no gate_ready section is present, returns a config
    with zero thresholds, meaning "no SLO is configured".
    """
    profile_key = profile.strip().lower() if profile else None
    engine_key = engine.strip().lower()

    try:
        doc = _load_yaml(rules_path)
    except FileNotFoundError:
        return FactorSloConfig(
            source="missing_rules_file",
            profile=profile,
            engine=engine,
            min_factors=0,
            min_per_window=0,
            required_factors=[],
            per_window_min={},
            raw_gate_ready=None,
        )
    except Exception as exc:
        return FactorSloConfig(
            source=f"yaml_error: {exc}",
            profile=profile,
            engine=engine,
            min_factors=0,
            min_per_window=0,
            required_factors=[],
            per_window_min={},
            raw_gate_ready=None,
        )

    gate_ready = doc.get("gate_ready")
    if not isinstance(gate_ready, Mapping):
        # rules file exists but gate_ready is absent or invalid → no SLO
        return FactorSloConfig(
            source="no_gate_ready",
            profile=profile,
            engine=engine,
            min_factors=0,
            min_per_window=0,
            required_factors=[],
            per_window_min={},
            raw_gate_ready=None,
        )

    merged: Dict[str, Any] = {}

    # 1) root gate_ready
    merged.update(gate_ready)

    # 2) engines.<engine>
    engines_node = gate_ready.get("engines")
    if isinstance(engines_node, Mapping) and engine_key in engines_node:
        engine_cfg = engines_node.get(engine_key)
        if isinstance(engine_cfg, Mapping):
            merged.update(engine_cfg)

    # 3) profiles.<profile> (highest priority)
    profiles_node = gate_ready.get("profiles")
    if profile_key and isinstance(profiles_node, Mapping) and profile_key in profiles_node:
        profile_cfg = profiles_node.get(profile_key)
        if isinstance(profile_cfg, Mapping):
            merged.update(profile_cfg)

    # Extract SLO fields from merged view
    min_factors = int(merged.get("min_factors") or 0)
    min_per_window = int(merged.get("min_factors_per_window") or 0)

    required_raw = merged.get("required_factors") or []
    required: List[str] = []
    if isinstance(required_raw, str):
        required = [required_raw]
    elif isinstance(required_raw, Iterable):
        for v in required_raw:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                required.append(s)

    per_window_min: Dict[str, int] = {}
    # ⚠️ 這裡故意用 merged，而不是 gate_ready：
    #    這樣 engine/profile 可以覆蓋 per_window 設定。
    per_window_node = merged.get("per_window")
    if isinstance(per_window_node, Mapping):
        for key, node in per_window_node.items():
            if not isinstance(node, Mapping):
                continue
            v = int(node.get("min_factors") or 0)
            k_str = str(key).strip()
            if v > 0 and k_str:
                per_window_min[k_str] = v

    return FactorSloConfig(
        source="rules_factors.yaml",
        profile=profile,
        engine=engine,
        min_factors=min_factors,
        min_per_window=min_per_window,
        required_factors=required,
        per_window_min=per_window_min,
        raw_gate_ready=gate_ready,
    )


# ---------------------------------------------------------------------------
# Public API: evaluate SLO against wf_summary (already loaded)
# ---------------------------------------------------------------------------


def _normalize_factor_map(factors_node: Any) -> Dict[str, Mapping[str, Any]]:
    """
    Normalize wf_summary.factors to a mapping factor_id -> dict-like object.

    Supported shapes:
      1) {factor_id: {...}}
      2) [{factor_id: "xxx", ...}, ...]
    """
    result: Dict[str, Mapping[str, Any]] = {}

    if isinstance(factors_node, Mapping):
        for key, val in factors_node.items():
            fid = str(key).strip()
            if not fid:
                continue
            if isinstance(val, Mapping):
                result[fid] = val
            else:
                result[fid] = {"value": val}
        return result

    if isinstance(factors_node, Iterable) and not isinstance(factors_node, (str, bytes)):
        for item in factors_node:
            if not isinstance(item, Mapping):
                continue
            fid: Optional[str] = None
            if "factor_id" in item:
                fid = str(item.get("factor_id") or "").strip()
            elif "id" in item:
                fid = str(item.get("id") or "").strip()
            if not fid:
                continue
            result[fid] = item
        return result

    return result


def _infer_windows_from_slo_and_wf(
    slo: FactorSloConfig,
    wf: Mapping[str, Any],
    explicit_windows: Optional[List[int]],
) -> List[int]:
    """
    Determine which windows in months to evaluate.

    Priority:
      1) explicit_windows (caller-specified)
      2) SLO per_window_min keys
      3) wf_summary.overall.windows
      4) default [6, 12, 24]
    """
    if explicit_windows:
        return sorted(set(explicit_windows))

    # 2) SLO per_window_min keys
    if slo.per_window_min:
        wins: List[int] = []
        for key in slo.per_window_min.keys():
            try:
                wins.append(int(str(key)))
            except ValueError:
                continue
        if wins:
            return sorted(set(wins))

    # 3) wf_summary.overall.windows
    overall = wf.get("overall")
    if isinstance(overall, Mapping):
        wins_node = overall.get("windows")
        if isinstance(wins_node, Iterable) and not isinstance(wins_node, (str, bytes)):
            wins: List[int] = []
            for w in wins_node:
                try:
                    wins.append(int(str(w)))
                except ValueError:
                    continue
            if wins:
                return sorted(set(wins))

    # 4) fallback
    return [6, 12, 24]


def evaluate_factor_slo(
    wf_summary: Mapping[str, Any],
    slo: FactorSloConfig,
    windows: Optional[List[int]] = None,
    wf_summary_path: str = "",
) -> FactorSloResult:
    """
    Evaluate factor gate-ready SLO against an in-memory wf_summary object.

    This function does NOT throw on SLO violations; it encodes the outcome
    in the returned FactorSloResult.satisfied flag. Callers can decide how
    to react (e.g., log-only, or fail Gate).
    """
    factors_node = wf_summary.get("factors")
    factor_map = _normalize_factor_map(factors_node)
    factor_ids = sorted(factor_map.keys())
    total_factors = len(factor_ids)

    has_any_constraint = (
        slo.min_factors > 0
        or slo.min_per_window > 0
        or bool(slo.required_factors)
        or bool(slo.per_window_min)
    )

    # Decide windows to inspect
    win_list = _infer_windows_from_slo_and_wf(slo, wf_summary, windows)
    per_window_counts: Dict[int, int] = {w: 0 for w in win_list}

    # If no constraints at all, just return informational result
    if not has_any_constraint:
        return FactorSloResult(
            name="factor_gate_ready",
            profile=slo.profile,
            engine=slo.engine,
            source=slo.source,
            wf_summary_path=wf_summary_path,
            min_factors=slo.min_factors,
            min_factors_per_window=slo.min_per_window,
            per_window_min=dict(slo.per_window_min),
            required_factors=list(slo.required_factors),
            total_factors=total_factors,
            windows=list(win_list),
            per_window_counts=per_window_counts,
            missing_required_factors=[],
            satisfied=True,
        )

    # Count per-window coverage
    for fid in factor_ids:
        fobj = factor_map.get(fid, {})
        for w in win_list:
            present = False
            win_node = fobj.get("windows") if isinstance(fobj, Mapping) else None

            if isinstance(win_node, Mapping):
                key = str(w)
                if key in win_node:
                    present = True
            elif isinstance(win_node, Iterable) and not isinstance(win_node, (str, bytes)):
                for wn in win_node:
                    if str(wn) == str(w):
                        present = True
                        break

            # If schema is unknown (no "windows" field), assume factor exists for all windows
            if win_node is None:
                present = True

            if present:
                per_window_counts[w] = per_window_counts.get(w, 0) + 1

    # Apply constraints
    satisfied = True
    missing_required: List[str] = []

    # Global min factors
    if slo.min_factors > 0 and total_factors < slo.min_factors:
        satisfied = False

    # Per-window minima (combined SLO)
    for w in win_list:
        count = per_window_counts.get(w, 0)
        w_key = str(w)
        w_min_specific = int(slo.per_window_min.get(w_key) or 0)
        effective_min = max(slo.min_per_window, w_min_specific)
        if effective_min > 0 and count < effective_min:
            satisfied = False

    # Required factors must exist
    if slo.required_factors:
        for rf in slo.required_factors:
            if rf not in factor_ids:
                missing_required.append(rf)
        if missing_required:
            satisfied = False

    return FactorSloResult(
        name="factor_gate_ready",
        profile=slo.profile,
        engine=slo.engine,
        source=slo.source,
        wf_summary_path=wf_summary_path,
        min_factors=slo.min_factors,
        min_factors_per_window=slo.min_per_window,
        per_window_min=dict(slo.per_window_min),
        required_factors=list(slo.required_factors),
        total_factors=total_factors,
        windows=list(win_list),
        per_window_counts=per_window_counts,
        missing_required_factors=missing_required,
        satisfied=satisfied,
    )


# ---------------------------------------------------------------------------
# Optional helper: round-trip JSON for debugging / logging
# ---------------------------------------------------------------------------


def slo_result_to_json(result: FactorSloResult, indent: int = 2) -> str:
    """
    Convenience function to convert FactorSloResult to JSON text.

    This is purely for logging or debugging; production callers can either
    embed asdict(result) into their own JSON output or ignore this helper.
    """
    return json.dumps(asdict(result), ensure_ascii=False, indent=indent)
