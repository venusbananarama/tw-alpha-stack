from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


_DEFAULT_REQUIRED_FILES = [
    "manifest.json",
    "params.json",
    "metrics.json",
    "repair_plan.json",
    "final_result.json",
    "tags.json",
    "gate_before.json",
    "gate_after.json",
    "holdout_check.json",
]

_DEFAULT_REQUIRED_KEYS = {
    "manifest.json": ["schema", "versions", "resolved_paths", "hashes"],
    "repair_plan.json": ["variant_sort_policy", "variants"],
    "final_result.json": ["status", "summary"],
    "gate_before.json": ["schema", "run_id", "checks_count", "checks"],
    "gate_after.json": ["schema", "status", "passed", "metrics_summary"],
}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    return dict(payload)


def _has_nested_key(payload: Mapping[str, Any], dotted_key: str) -> bool:
    node: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return False
        node = node[part]
    return True


def _resolve_attempted(run_dir: Path, explicit_attempted: Optional[int]) -> int:
    if explicit_attempted is not None:
        try:
            return max(int(explicit_attempted), 0)
        except Exception:
            return 0

    final_payload = _load_json(run_dir / "final_result.json")
    summary = final_payload.get("summary")
    if not isinstance(summary, Mapping):
        return 0
    repair_result = summary.get("repair_result")
    if not isinstance(repair_result, Mapping):
        return 0
    try:
        return max(int(repair_result.get("attempted", 0)), 0)
    except Exception:
        return 0


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def build_schema_report(
    *,
    run_dir: Path,
    required_files: Optional[Sequence[str]] = None,
    required_keys_map: Optional[Mapping[str, Sequence[str]]] = None,
    attempted: Optional[int] = None,
) -> Dict[str, Any]:
    run_dir = run_dir.resolve()
    files = list(required_files or _DEFAULT_REQUIRED_FILES)
    keys_map = dict(required_keys_map or _DEFAULT_REQUIRED_KEYS)

    missing_files: List[str] = []
    schema_errors: List[str] = []

    for rel in files:
        if not (run_dir / rel).is_file():
            missing_files.append(rel)

    for rel, keys in keys_map.items():
        payload = _load_json(run_dir / rel)
        if not payload:
            if rel not in missing_files:
                schema_errors.append(f"{rel}:invalid_or_empty_json")
            continue
        for key in keys:
            if not _has_nested_key(payload, str(key)):
                schema_errors.append(f"{rel}:missing_key:{key}")

    manifest = _load_json(run_dir / "manifest.json")
    if manifest and manifest.get("schema") != "p2_repair_manifest.v1":
        schema_errors.append("manifest.json:invalid_schema")

    attempted_count = _resolve_attempted(run_dir, attempted)
    attempt_paths = sorted((run_dir / "attempt_logs").glob("*/attempt_summary.json"))
    attempt_required = attempted_count > 0
    if attempt_required and not attempt_paths:
        schema_errors.append("attempt_logs:missing_attempt_summary")

    gate_after = _load_json(run_dir / "gate_after.json")
    metrics_summary = gate_after.get("metrics_summary") if isinstance(gate_after.get("metrics_summary"), Mapping) else {}
    windows = metrics_summary.get("windows") if isinstance(metrics_summary.get("windows"), Mapping) else None
    data_stats = {
        "attempted": attempted_count,
        "gate_after_present": bool((run_dir / "gate_after.json").is_file()),
        "selected_variant_id": gate_after.get("selected_variant_id"),
        "metrics_summary_has_windows": isinstance(windows, Mapping),
        "rank_ic_min_is_number": _is_number(metrics_summary.get("rank_ic_min")),
        "coverage_min_is_number": _is_number(metrics_summary.get("coverage_min")),
    }

    ok = not missing_files and not schema_errors
    return {
        "schema": "p2_repair_schema_report.v1",
        "ok": ok,
        "missing_files": missing_files,
        "schema_errors": schema_errors,
        "attempt_logs": {
            "required": attempt_required,
            "count": len(attempt_paths),
        },
        "data_stats": data_stats,
    }
