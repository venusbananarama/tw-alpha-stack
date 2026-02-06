# C:\AI\tw-alpha-stack\scripts\factor_capacity.py
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

LOG = logging.getLogger("factor_capacity")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate factor capacity SLO for selected factor combos. "
            "This script consumes the JSON produced by scripts/factor_combo.py "
            "and emits a reports/factor_capacity.<as_of>.json summary."
        )
    )
    default_root = Path(__file__).resolve().parents[1]

    parser.add_argument(
        "--root",
        type=Path,
        default=default_root,
        help="Repository root (default: parent of scripts directory).",
    )
    parser.add_argument(
        "--as-of",
        required=True,
        help="As-of date (YYYY-MM-DD). Should match the combo plan as_of.",
    )
    parser.add_argument(
        "--windows",
        type=int,
        nargs="+",
        default=None,
        help=(
            "One or more walk-forward windows to evaluate (e.g. 6 12 24). "
            "If omitted, uses all windows present in the combo plan."
        ),
    )
    parser.add_argument(
        "--combo-plan",
        type=Path,
        default=None,
        help=(
            "Path to factor_combo.<as_of>.json. "
            "Default: <root>/reports/factor_combo.<as_of>.json"
        ),
    )
    parser.add_argument(
        "--rules-file",
        type=Path,
        default=None,
        help="Path to rules_factors.yaml. Default: <root>/rules_factors.yaml",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output JSON path. "
            "Default: <root>/reports/factor_capacity.<as_of>.json"
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "If set, exit with code 1 when capacity all_pass is False. "
            "By default, the script exits with 0 as long as it runs successfully."
        ),
    )

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    root: Path = args.root.resolve()
    # Make sure we can import the local alpha_core package
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    try:
        from alpha_core.phase2.corelib.capacity_lib import (  # type: ignore
            build_inputs_from_combo,
            capacity_summary_to_json,
            evaluate_capacity,
            load_capacity_config,
            load_combo_plan,
        )
    except Exception as exc:  # pragma: no cover - defensive
        LOG.error("Failed to import alpha_core.phase2.corelib.capacity_lib: %s", exc)
        return 1

    as_of: str = str(args.as_of)

    combo_plan_path: Path = args.combo_plan or (
        root / "reports" / f"factor_combo.{as_of}.json"
    )
    if not combo_plan_path.is_file():
        LOG.error("Combo plan not found: %s", combo_plan_path)
        return 1

    rules_path: Path = args.rules_file or (root / "rules_factors.yaml")
    output_path: Path = args.output or (
        root / "reports" / f"factor_capacity.{as_of}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    LOG.info("factor_capacity: root=%s as_of=%s", root, as_of)
    LOG.info("  combo_plan = %s", combo_plan_path)
    LOG.info("  rules_file = %s", rules_path)
    LOG.info("  output     = %s", output_path)

    config = load_capacity_config(root=root, rules_path=rules_path, overrides=None)
    plan = load_combo_plan(combo_plan_path)

    plan_as_of, windows_from_plan, inputs = build_inputs_from_combo(
        plan,
        windows_filter=args.windows,
    )

    if plan_as_of != as_of:
        LOG.warning(
            "Mismatch between CLI as-of (%s) and combo plan as_of (%s). "
            "Continuing with CLI as-of.",
            as_of,
            plan_as_of,
        )

    if not windows_from_plan:
        LOG.warning("No windows found in combo plan; nothing to evaluate.")
    if not inputs:
        LOG.warning("No factor inputs found for capacity evaluation.")

    summary = evaluate_capacity(
        as_of=as_of,
        windows=windows_from_plan,
        config=config,
        inputs=inputs,
    )

    meta = {
        "spec_version": "factor_capacity.v1",
        "root": str(root),
        "combo_plan_path": str(combo_plan_path),
        "rules_path": str(rules_path),
    }
    payload = capacity_summary_to_json(summary, meta=meta)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    LOG.info(
        "factor_capacity done: as_of=%s windows=%s all_pass=%s num_factors=%d",
        summary.as_of,
        ",".join(str(w) for w in summary.windows),
        summary.all_pass,
        len(summary.per_factor),
    )

    if args.strict and not summary.all_pass:
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


