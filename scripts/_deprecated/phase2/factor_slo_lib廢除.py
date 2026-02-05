# C:\AI\tw-alpha-stack\scripts\factor_slo_check.py
#!/usr/bin/env python
"""
factor_slo_check.py

Single-entry CLI to evaluate factor gate-ready SLO coverage based on
rules_factors.yaml and reports/wf_summary.json.

設計重點：
- 所有 SLO 規則與計算一律委託給 factor_slo_lib（SSOT）。
- 預設為「觀察模式」：只印結果，不因 SLO 未達標而回傳非 0。
- 加上 --strict 時才會以 exit code 表示 PASS/FAIL，提供 Gate / CI 使用。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, List, Mapping, Optional

from alpha_core.factor_slo_lib import (  # ★ 這行改成 alpha_core 版本
    FactorSloResult,
    load_factor_slo_config,
    evaluate_factor_slo,
    slo_result_to_json,
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse CLI arguments for factor_slo_check.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate factor gate-ready SLO coverage based on "
            "rules_factors.yaml and wf_summary.json."
        )
    )

    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root directory (default: current directory).",
    )
    parser.add_argument(
        "--rules-file",
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
        help=(
            "Comma-separated list of windows in months (e.g., 6,12,24). "
            "If omitted, windows are inferred from SLO and wf_summary."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full FactorSloResult as JSON instead of human-readable text.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Strict mode: exit code reflects SLO satisfaction "
            "(0 = satisfied, 1 = violated, 2+ = errors)."
        ),
    )

    return parser.parse_args(argv)


def _load_wf_summary(path: Path) -> Mapping[str, Any]:
    """
    Load wf_summary.json from given path and perform basic validation.

    Raises:
        FileNotFoundError: if path does not exist.
        ValueError: if file is empty or JSON is not an object.
        json.JSONDecodeError: if JSON cannot be parsed.
    """
    if not path.exists():
        raise FileNotFoundError(f"wf_summary.json not found: {path}")
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError(f"wf_summary.json is empty: {path}")
    data = json.loads(text)
    if not isinstance(data, Mapping):
        raise ValueError("wf_summary.json must contain an object at top level")
    return data


def _parse_windows_arg(raw: Optional[str]) -> Optional[List[int]]:
    """
    Parse the --windows argument (comma-separated string) into List[int].

    Returns:
        List[int] if raw is not None, otherwise None.

    Raises:
        ValueError: if any item cannot be parsed as int.
    """
    if raw is None:
        return None
    items = [x.strip() for x in raw.split(",") if x.strip()]
    windows: List[int] = []
    for item in items:
        try:
            windows.append(int(item))
        except ValueError:
            raise ValueError(f"Invalid window value in --windows: {item!r}") from None
    return windows


def run_check(args: argparse.Namespace) -> FactorSloResult:
    """
    Core logic for factor SLO check.

    Steps:
      1) Resolve paths (root, rules_file, wf_summary).
      2) Load SLO config from rules_factors.yaml via factor_slo_lib.
      3) Attempt to load wf_summary.json.
         - If missing and no SLO is configured → treated as satisfied.
         - If missing and SLO is configured → error propagated to caller.
      4) Evaluate SLO via factor_slo_lib.evaluate_factor_slo.

    Returns:
        FactorSloResult describing the evaluation outcome.

    Raises:
        Exceptions from _load_wf_summary() if SLO is configured and wf_summary
        cannot be loaded.
    """
    root = Path(args.root).resolve()

    rules_path = Path(args.rules_file)
    if not rules_path.is_absolute():
        rules_path = root / rules_path

    wf_path = Path(args.wf_summary)
    if not wf_path.is_absolute():
        wf_path = root / wf_path

    # Parse windows argument
    explicit_windows: Optional[List[int]] = None
    if args.windows:
        explicit_windows = _parse_windows_arg(args.windows)

    # Load SLO config from rules_factors.yaml
    slo = load_factor_slo_config(
        rules_path=rules_path,
        profile=args.profile,
        engine=args.engine,
    )

    # Decide whether any constraint is actually configured
    has_any_constraint = (
        slo.min_factors > 0
        or slo.min_per_window > 0
        or bool(slo.required_factors)
        or bool(slo.per_window_min)
    )

    # Try loading wf_summary.json
    try:
        wf_summary = _load_wf_summary(wf_path)
    except FileNotFoundError:
        # No wf_summary found. If there is no constraint, treat as satisfied.
        if not has_any_constraint:
            windows = explicit_windows or [6, 12, 24]
            return FactorSloResult(
                name="factor_gate_ready",
                profile=slo.profile,
                engine=slo.engine,
                source=slo.source,
                wf_summary_path=str(wf_path),
                min_factors=slo.min_factors,
                min_factors_per_window=slo.min_per_window,
                per_window_min=dict(slo.per_window_min),
                required_factors=list(slo.required_factors),
                total_factors=0,
                windows=list(windows),
                per_window_counts={w: 0 for w in windows},
                missing_required_factors=[],
                satisfied=True,
            )
        # Constraints exist → let caller handle the error
        raise
    except Exception:
        # Other errors (empty / malformed JSON etc.) are forwarded to caller
        raise

    # Evaluate SLO using shared library
    result = evaluate_factor_slo(
        wf_summary=wf_summary,
        slo=slo,
        windows=explicit_windows,
        wf_summary_path=str(wf_path),
    )
    return result


def print_human_readable(
    result: FactorSloResult,
    root: Path,
    rules_path: Path,
    wf_path: Path,
) -> None:
    """
    Print a concise, human-readable summary of the SLO evaluation.
    """
    print("== Factor Gate-Ready SLO Check ==")
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


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    root = Path(args.root).resolve()
    rules_path = Path(args.rules_file)
    if not rules_path.is_absolute():
        rules_path = root / rules_path
    wf_path = Path(args.wf_summary)
    if not wf_path.is_absolute():
        wf_path = root / wf_path

    try:
        result = run_check(args)
    except FileNotFoundError as exc:
        # wf_summary missing while SLO is configured → treated as error
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        # Empty or structurally invalid wf_summary.json
        print(f"[ERROR] Invalid wf_summary.json: {exc}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Failed to parse wf_summary.json: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"[ERROR] Unexpected error during SLO check: {exc}", file=sys.stderr)
        return 3

    # Output
    if args.json:
        # Use shared helper to ensure consistent JSON encoding
        text = slo_result_to_json(result, indent=2)
        print(text)
    else:
        print_human_readable(
            result=result,
            root=root,
            rules_path=rules_path,
            wf_path=wf_path,
        )

    # Exit code policy
    if not args.strict:
        # Non-strict: never fail the process just because SLO is not satisfied.
        return 0

    # Strict mode: SLO violation → exit 1, else 0
    return 0 if result.satisfied else 1


if __name__ == "__main__":
    raise SystemExit(main())
