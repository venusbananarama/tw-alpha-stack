from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml

from .models import RepairAttempt, SelectionDecision, VariantSpec


_DEFAULT_REASON_ALLOWLIST = ["rank_ic_min_threshold"]
_DEFAULT_VARIANT_PRIORITY = 100


def _dedupe_keep_order(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = str(value).strip()
        if not s or s in seen:
            continue
        out.append(s)
        seen.add(s)
    return out


def _normalize_reason_allowlist(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return _dedupe_keep_order([str(v).strip() for v in value if str(v).strip()])


def resolve_reason_allowlist(raw_allowlist: Any, default_allowlist: Sequence[str]) -> Dict[str, Any]:
    default_norm = _dedupe_keep_order([str(v).strip() for v in default_allowlist if str(v).strip()])
    if not default_norm:
        default_norm = list(_DEFAULT_REASON_ALLOWLIST)

    # Missing / null => use default allowlist.
    if raw_allowlist is None:
        return {
            "allowlist_mode": "default",
            "allow_all_reasons": False,
            "resolved_reason_allowlist": list(default_norm),
        }

    if not isinstance(raw_allowlist, list):
        return {
            "allowlist_mode": "default",
            "allow_all_reasons": False,
            "resolved_reason_allowlist": list(default_norm),
        }

    normalized = _normalize_reason_allowlist(raw_allowlist)
    if not normalized:
        # Explicit empty list => deny all.
        return {
            "allowlist_mode": "deny_all",
            "allow_all_reasons": False,
            "resolved_reason_allowlist": [],
        }

    if "*" in normalized:
        return {
            "allowlist_mode": "allow_all",
            "allow_all_reasons": True,
            "resolved_reason_allowlist": ["*"],
        }

    return {
        "allowlist_mode": "explicit",
        "allow_all_reasons": False,
        "resolved_reason_allowlist": list(normalized),
    }


def load_repair_profile(path: Path | str, profile: str) -> Dict[str, Any]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    node = data.get("auto_repair", {}) if isinstance(data, Mapping) else {}
    default_cfg = dict(node.get("default", {})) if isinstance(node, Mapping) else {}
    profiles = node.get("profiles", {}) if isinstance(node, Mapping) else {}
    profile_cfg = dict(profiles.get(profile, {})) if isinstance(profiles, Mapping) else {}

    merged = dict(default_cfg)
    merged.update(profile_cfg)
    merged.setdefault("enabled", False)
    merged.setdefault("max_attempts", 3)
    merged.setdefault("stop_fast", True)
    merged.setdefault("promotion_mode", "off")

    raw_attempts = merged.get("max_attempts_per_factor", merged.get("max_attempts", 3))
    try:
        max_attempts_per_factor = int(raw_attempts)
    except Exception:
        max_attempts_per_factor = 3
    if max_attempts_per_factor < 0:
        max_attempts_per_factor = 0
    merged["max_attempts_per_factor"] = max_attempts_per_factor
    merged["max_attempts"] = max_attempts_per_factor

    default_allowlist = _normalize_reason_allowlist(default_cfg.get("reason_allowlist"))
    if not default_allowlist:
        default_allowlist = list(_DEFAULT_REASON_ALLOWLIST)

    profile_has_reason_allowlist = "reason_allowlist" in profile_cfg
    raw_allowlist = profile_cfg.get("reason_allowlist") if profile_has_reason_allowlist else None
    resolved = resolve_reason_allowlist(raw_allowlist, default_allowlist=default_allowlist)

    merged["reason_allowlist_default"] = list(default_allowlist)
    merged["reason_allowlist_raw"] = raw_allowlist if profile_has_reason_allowlist else None
    merged["allowlist_mode"] = str(resolved["allowlist_mode"])
    merged["allow_all_reasons"] = bool(resolved["allow_all_reasons"])
    merged["resolved_reason_allowlist"] = list(resolved["resolved_reason_allowlist"])
    merged["reason_allowlist"] = list(resolved["resolved_reason_allowlist"])

    return merged


def collect_fail_reasons(gate_summary: Mapping[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    checks = gate_summary.get("checks", []) if isinstance(gate_summary, Mapping) else []
    if not isinstance(checks, list):
        return out
    for item in checks:
        if not isinstance(item, Mapping):
            continue
        factor_id = str(item.get("factor_id") or "").strip()
        reasons = item.get("reasons")
        if not factor_id or not isinstance(reasons, list):
            continue
        out[factor_id] = [str(r) for r in reasons if str(r).strip()]
    return out


def find_bottleneck_window(
    fail_results_path: Path,
    *,
    factor_id: str,
    reason_key: str,
    fallback_windows: Sequence[int],
) -> Optional[int]:
    if fail_results_path.is_file():
        counts: Dict[int, int] = {}
        with fail_results_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row.get("factor_id") or "") != factor_id:
                    continue
                reason = str(row.get("reason") or "")
                if reason_key not in reason:
                    continue
                try:
                    w = int(row.get("window") or 0)
                except Exception:
                    continue
                if w <= 0:
                    continue
                counts[w] = counts.get(w, 0) + 1
        if counts:
            return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    if fallback_windows:
        return int(sorted(set(int(w) for w in fallback_windows))[0])
    return None


def _rank_ic_from_window_node(node: Mapping[str, Any]) -> Optional[float]:
    for key in ("rank_ic", "rank_ic_mean"):
        value = node.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    metrics = node.get("metrics")
    if isinstance(metrics, Mapping):
        for key in ("rank_ic", "rank_ic_mean"):
            value = metrics.get(key)
            if isinstance(value, (int, float)):
                return float(value)
    return None


def find_bottleneck_window_from_factor_summary(root: Path, factor_id: str) -> Optional[int]:
    summary_path = root / "reports" / "factor_eval" / f"{factor_id}_summary.json"
    if not summary_path.is_file():
        return None
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None

    candidates: List[tuple[int, float]] = []
    windows = payload.get("windows")
    if isinstance(windows, Mapping):
        for window_key, node in windows.items():
            if not isinstance(node, Mapping):
                continue
            try:
                w = int(window_key)
            except Exception:
                continue
            if w <= 0:
                continue
            rank_ic = _rank_ic_from_window_node(node)
            if rank_ic is None:
                continue
            candidates.append((w, rank_ic))
    elif isinstance(windows, list):
        for item in windows:
            if not isinstance(item, Mapping):
                continue
            try:
                w = int(item.get("window"))
            except Exception:
                continue
            if w <= 0:
                continue
            rank_ic = _rank_ic_from_window_node(item)
            if rank_ic is None:
                continue
            candidates.append((w, rank_ic))

    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda kv: (kv[1], kv[0]))
    return int(candidates[0][0])


def resolve_bottleneck_window(
    *,
    root: Path,
    fail_results_path: Path,
    factor_id: str,
    reason_key: str,
    fallback_windows: Sequence[int],
) -> Optional[int]:
    from_summary = find_bottleneck_window_from_factor_summary(root, factor_id)
    if from_summary is not None:
        return from_summary
    return find_bottleneck_window(
        fail_results_path,
        factor_id=factor_id,
        reason_key=reason_key,
        fallback_windows=fallback_windows,
    )


def build_candidates(
    *,
    factor_id: str,
    fail_reasons: Sequence[str],
    reason_config_root: Path,
    max_attempts: int,
) -> List[VariantSpec]:
    reasons = [str(r).strip() for r in fail_reasons if str(r).strip()]
    variants: List[VariantSpec] = []

    for reason in reasons:
        cfg_path = reason_config_root / f"{reason}.yaml"
        if not cfg_path.is_file():
            continue
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        for item in cfg.get("variants", []):
            if not isinstance(item, Mapping):
                continue
            variant_id = str(item.get("id") or "").strip()
            if not variant_id:
                continue

            pr = item.get("priority", _DEFAULT_VARIANT_PRIORITY)
            try:
                priority = int(pr)
            except Exception:
                priority = _DEFAULT_VARIANT_PRIORITY

            transforms = list(item.get("transforms") or [])
            metadata = {
                "description": str(item.get("description") or "").strip(),
                "reason": reason,
                "priority": priority,
            }
            variants.append(
                VariantSpec(
                    variant_id=f"{factor_id}__{variant_id}",
                    factor_id=factor_id,
                    reason=reason,
                    transforms=transforms,
                    metadata=metadata,
                )
            )

    # deterministic order for reproducibility
    variants = sorted(
        variants,
        key=lambda v: (
            -int(v.metadata.get("priority", _DEFAULT_VARIANT_PRIORITY) or _DEFAULT_VARIANT_PRIORITY),
            len(v.transforms),
            v.variant_id,
        ),
    )
    if max_attempts > 0:
        variants = variants[: max_attempts]
    return variants


def early_stop_check(
    metrics: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    *,
    window: Optional[int],
) -> bool:
    if window is None:
        return False
    win = str(int(window))
    window_metrics = metrics.get("windows", {})
    if not isinstance(window_metrics, Mapping):
        return False
    node = window_metrics.get(win)
    if not isinstance(node, Mapping):
        return True

    rank_ic = node.get("rank_ic")
    cov = node.get("coverage")

    rank_ic_th = thresholds.get("min_rank_ic")
    cov_th = thresholds.get("min_coverage")

    if rank_ic_th is not None:
        if rank_ic is None or float(rank_ic) < float(rank_ic_th):
            return True
    if cov_th is not None:
        if cov is None or float(cov) < float(cov_th):
            return True
    return False


def _margin(attempt: RepairAttempt) -> float:
    windows = attempt.metrics.get("windows", {})
    if not isinstance(windows, Mapping) or not windows:
        return -1e9

    rank_ic_th = attempt.thresholds.get("min_rank_ic")
    cov_th = attempt.thresholds.get("min_coverage")

    rank_margin = 1e9
    cov_margin = 1e9
    for node in windows.values():
        if not isinstance(node, Mapping):
            return -1e9
        rank_ic = node.get("rank_ic")
        cov = node.get("coverage")
        if rank_ic_th is not None:
            if rank_ic is None:
                return -1e9
            rank_margin = min(rank_margin, float(rank_ic) - float(rank_ic_th))
        if cov_th is not None:
            if cov is None:
                return -1e9
            cov_margin = min(cov_margin, float(cov) - float(cov_th))

    margin = min(rank_margin, cov_margin)
    if margin == 1e9:
        return -1e9
    return margin


def select_best(attempts: Sequence[RepairAttempt]) -> SelectionDecision:
    if not attempts:
        return SelectionDecision(selected_variant_id=None, passed=False, reason="no_attempts", details={})

    passed = [a for a in attempts if a.passed]
    if not passed:
        return SelectionDecision(selected_variant_id=None, passed=False, reason="all_failed", details={})

    ranked = sorted(
        passed,
        key=lambda a: (
            -_margin(a),
            len(a.variant.transforms),
            a.variant.variant_id,
        ),
    )
    chosen = ranked[0]
    return SelectionDecision(
        selected_variant_id=chosen.variant.variant_id,
        passed=True,
        reason="best_margin_with_minimal_changes",
        details={
            "attempt_id": chosen.attempt_id,
            "margin": _margin(chosen),
            "transform_count": len(chosen.variant.transforms),
        },
    )
