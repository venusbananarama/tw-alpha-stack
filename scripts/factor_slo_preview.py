#!/usr/bin/env python
"""
factor_slo_preview.py

Optional helper CLI to preview factor gate-ready SLO coverage based on
rules_factors.yaml and reports/wf_summary.json.

It does not modify any files and is safe to run multiple times.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # will be checked at runtime


@dataclass
class FactorSloConfig:
    source: str
    profile: Optional[str]
    engine: str
    min_factors: int
    min_per_window: int
    required_factors: List[str]
    per_window_min: Dict[str, int]
    raw_gate_ready: Mapping[str, object] | None


@dataclass
class FactorSloResult:
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


def _load_yaml(path: Path) -> Mapping[str, object]:
    """Load YAML mapping from file, with basic sanity checks."""
    if not path.exists():
        raise FileNotFoundError(f"rules file not found: {path}")
    if yaml is None:
        raise RuntimeError("PyYAML is not installed, cannot load YAML.")
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("rules file is empty")
    doc = yaml.safe_load(text)
    if not isinstance(doc, Mapping):
        raise ValueError("rules file must contain a mapping at top level")
    return doc


def load_factor_slo_config(
    rules_path: Path,
    profile: Optional[str],
    engine: str,
) -> FactorSloConfig:
    """
    Load gate_ready SLO config from rules_factors.yaml with precedence:

      root.gate_ready -> gate_ready.engines[engine] -> gate_ready.profiles[profile]
    """
    profile_key = profile.strip().lower() if profile else None
    engine_key = engine.strip().lower()

    try:
        doc = _load_yaml(rules_path)
    except FileNotFoundError:
        # No rules_factors.yaml → no SLO, treated as "nothing to check".
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
        # YAML error → surface as special source, but still allow caller to see it。
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

    gr = doc.get("gate_ready")
    if not isinstance(gr, Mapping):
        # rules_factors.yaml 存在，但沒有 gate_ready 區塊 → 視為沒設定 SLO
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

    merged: Dict[str, object] = {}
    # 1) root gate_ready
    merged.update(gr)

    # 2) engines.<engine>
    engines_node = gr.get("engines")
    if isinstance(engines_node, Mapping) and engine_key in engines_node:
        engine_cfg = engines_node.get(engine_key)
        if isinstance(engine_cfg, Mapping):
            merged.update(engine_cfg)

    # 3) profiles.<profile> (highest priority)
    profiles_node = gr.get("profiles")
    if profile_key and isinstance(profiles_node, Mapping) and profile_key in profiles_node:
        prof_cfg = profiles_node.get(profile_key)
        if isinstance(prof_cfg, Mapping):
            merged.update(prof_cfg)

    # Extract SLO fields
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

    # Per-window minimums: gate_ready.per_window.<window>.min_factors
    per_window_min: Dict[str, int] = {}
    per_window_node = gr.get("per_window")
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
        raw_gate_ready=gr,
    )


def _load_wf_summary(path: Path) -> Mapping[str, object]:
    """Load wf_summary.json as mapping with minimal validation."""
    if not path.exists():
        raise FileNotFoundError(f"wf_summary.json not found: {path}")
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("wf_summary.json is empty")
    data = json.loads(text)
    if not isinstance(data, Mapping):
        raise ValueError("wf_summary.json must contain an object at top level")
    return data


def _normalize_factor_map(factors_node: object) -> Dict[str, Mapping[str, object]]:
    """
    Normalize wf_summary.factors to a mapping factor_id -> dict-like object.

    支援兩種常見格式：
      1) {factor_id: {...}}
      2) [{factor_id: "xxx", ...}, ...]
    """
    result: Dict[str, Mapping[str, object]] = {}

    if isinstance(factors_node, Mapping):
        # Already dict-like mapping factor_id -> metrics
        for key, val in factors_node.items():
            fid = str(key).strip()
            if not fid:
                continue
            if isinstance(val, Mapping):
                result[fid] = val
            else:
                # Wrap non-mapping as simple dict with 'value'
                result[fid] = {"value": val}
        return result

    if isinstance(factors_node, Iterable) and not isinstance(factors_node, (str, bytes)):
        for item in factors_node:
            if not isinstance(item, Mapping):
                continue
            fid = None
            if "factor_id" in item:
                fid = str(item.get("factor_id") or "").strip()
            elif "id" in item:
                fid = str(item.get("id") or "").strip()
            if not fid:
                continue
            result[fid] = item
        return result

    # Unknown schema → no factors
    return result


def _infer_windows(
    slo: FactorSloConfig,
    wf: Mapping[str, object],
    cli_windows: Optional[List[int]],
) -> List[int]:
    """
    Determine which windows (in months) to evaluate.

    Priority:
      1) CLI --windows
      2) per-window SLO keys
      3) wf_summary.overall.windows
      4) default [6, 12, 24]
    """
    if cli_windows:
        return cli_windows

    # 2) SLO per-window keys
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
    wf: Mapping[str, object],
    slo: FactorSloConfig,
    windows: List[int],
) -> FactorSloResult:
    """
    Evaluate factor gate-ready SLO against wf_summary.

    注意：這裡只做「是否滿足 SLO」的計算，不改變 Gate 主流程。
    """
    factors_node = wf.get("factors")
    factor_map = _normalize_factor_map(factors_node)
    factor_ids = sorted(factor_map.keys())
    total_factors = len(factor_ids)

    # Determine if any constraint is actually configured
    has_any_constraint = (
        slo.min_factors > 0
        or slo.min_per_window > 0
        or bool(slo.required_factors)
        or bool(slo.per_window_min)
    )

    # If no constraints at all, always satisfied, counts are informational
    per_window_counts: Dict[int, int] = {w: 0 for w in windows}
    if not has_any_constraint:
        return FactorSloResult(
            name="factor_gate_ready",
            profile=slo.profile,
            engine=slo.engine,
            source=slo.source,
            wf_summary_path="",  # filled by caller
            min_factors=slo.min_factors,
            min_factors_per_window=slo.min_per_window,
            per_window_min={k: int(v) for k, v in slo.per_window_min.items()},
            required_factors=list(slo.required_factors),
            total_factors=total_factors,
            windows=list(windows),
            per_window_counts=per_window_counts,
            missing_required_factors=[],
            satisfied=True,
        )

    # Count how many factors have data per window
    for fid in factor_ids:
        fobj = factor_map.get(fid, {})
        for w in windows:
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

            # If schema is unknown (no "windows" field), assume factor is present in all windows
            if win_node is None:
                present = True

            if present:
                per_window_counts[w] = per_window_counts.get(w, 0) + 1

    # Apply SLO conditions
    satisfied = True
    missing_required: List[str] = []

    # Global min factors
    if slo.min_factors > 0 and total_factors < slo.min_factors:
        satisfied = False

    # Per-window minima
    for w in windows:
        count = per_window_counts.get(w, 0)
        # Combine min_per_window and per_window_min[w]
        w_key = str(w)
        w_min_specific = int(slo.per_window_min.get(w_key) or 0)
        effective_min = max(slo.min_per_window, w_min_specific)
        if effective_min > 0 and count < effective_min:
            satisfied = False

    # Required factors
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
        wf_summary_path="",  # to be set by caller
        min_factors=slo.min_factors,
        min_factors_per_window=slo.min_per_window,
        per_window_min={k: int(v) for k, v in slo.per_window_min.items()},
        required_factors=list(slo.required_factors),
        total_factors=total_factors,
        windows=list(windows),
        per_window_counts=per_window_counts,
        missing_required_factors=missing_required,
        satisfied=satisfied,
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview factor gate-ready SLO coverage based on rules_factors.yaml and wf_summary.json.",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root directory (default: current directory).",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default="rules_factors.yaml",
        help="Path to rules_factors.yaml (relative to --root if not absolute).",
    )
    parser.add_argument(
        "--wf-summary",
        type=str,
        default="reports/wf_summary.json",
        help="Path to wf_summary.json (relative to --root if not absolute).",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Optional profile name (e.g., dev/test/live).",
    )
    parser.add_argument(
        "--engine",
        type=str,
        default="classic",
        help="Engine name, e.g., classic or ai (default: classic).",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default=None,
        help="Comma-separated list of windows in months (e.g., 6,12,24). If omitted, inferred.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full result as JSON instead of human-readable text.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    root = Path(args.root).resolve()
    rules_path = Path(args.rules)
    if not rules_path.is_absolute():
        rules_path = root / rules_path

    wf_path = Path(args.wf_summary)
    if not wf_path.is_absolute():
        wf_path = root / wf_path

    # Parse windows if provided
    cli_windows: Optional[List[int]] = None
    if args.windows:
        try:
            cli_windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
        except ValueError:
            print(f"[ERROR] Invalid --windows value: {args.windows}", file=sys.stderr)
            return 2

    slo = load_factor_slo_config(rules_path, profile=args.profile, engine=args.engine)

    try:
        wf = _load_wf_summary(wf_path)
    except FileNotFoundError as exc:
        # If no constraints at all, treat as satisfied (nothing to check)
        has_any_constraint = (
            slo.min_factors > 0
            or slo.min_per_window > 0
            or bool(slo.required_factors)
            or bool(slo.per_window_min)
        )
        if not has_any_constraint:
            if not args.json:
                print(f"[WARN] {exc}; no gate_ready constraint configured, treated as satisfied.")
            result = FactorSloResult(
                name="factor_gate_ready",
                profile=slo.profile,
                engine=slo.engine,
                source=slo.source,
                wf_summary_path=str(wf_path),
                min_factors=slo.min_factors,
                min_factors_per_window=slo.min_per_window,
                per_window_min={k: int(v) for k, v in slo.per_window_min.items()},
                required_factors=list(slo.required_factors),
                total_factors=0,
                windows=cli_windows or [6, 12, 24],
                per_window_counts={},
                missing_required_factors=[],
                satisfied=True,
            )
            payload = asdict(result)
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print("[INFO] factor_gate_ready SLO satisfied (nothing to check).")
            return 0
        else:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 1
    except Exception as exc:
        print(f"[ERROR] Failed to load wf_summary.json: {exc}", file=sys.stderr)
        return 1

    windows = _infer_windows(slo, wf, cli_windows)
    result = evaluate_factor_slo(wf, slo, windows)
    # Fill wf_summary_path field now
    result.wf_summary_path = str(wf_path)

    payload = asdict(result)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("== Factor Gate-Ready SLO Preview ==")
        print(f"root     : {root}")
        print(f"rules    : {rules_path}")
        print(f"wf       : {wf_path}")
        print(f"profile  : {result.profile or '-'}")
        print(f"engine   : {result.engine}")
        print(f"source   : {result.source}")
        print()
        print(f"total_factors         : {result.total_factors}")
        print(f"min_factors           : {result.min_factors}")
        print(f"min_per_window        : {result.min_factors_per_window}")
        print(f"per_window_min        : {result.per_window_min}")
        print(f"windows               : {result.windows}")
        print(f"per_window_counts     : {result.per_window_counts}")
        print(f"required_factors      : {result.required_factors}")
        print(f"missing_required      : {result.missing_required_factors}")
        print(f"SLO satisfied         : {result.satisfied}")
    return 0 if result.satisfied else 1


if __name__ == "__main__":
    raise SystemExit(main())
