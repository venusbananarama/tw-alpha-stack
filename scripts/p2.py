from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpha_core.phase2 import GateFailError, MissingInputsError, Phase2RunConfig, RulesSchemaError  # noqa: E402
from alpha_core.phase2 import contracts as p2_contracts  # noqa: E402
from alpha_core.phase2 import pipeline as p2_pipeline  # noqa: E402
from alpha_core.phase2 import paths as p2_paths  # noqa: E402


def _parse_date(value: Optional[str]) -> date:
    if value:
        return date.fromisoformat(value)
    return p2_paths.default_as_of_date()


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--as-of", default="", help="YYYY-MM-DD (default: latest W-FRI)")
    parser.add_argument(
        "--engine",
        required=True,
        help="Phase-2 factor category selector: classic|ai|other (NOT impl engine like ta_mom_v1)",
    )
    parser.add_argument("--profile", required=True, help="profile name (e.g., dev/test/live)")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="p2")
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="build Phase-2 status")
    _add_common_args(p_status)

    p_plan = sub.add_parser("plan", help="build Phase-2 plan")
    _add_common_args(p_plan)

    p_run = sub.add_parser("run", help="run Phase-2 pipeline")
    p_run.add_argument("--as-of", default="", help="YYYY-MM-DD (default: latest W-FRI)")
    p_run.add_argument(
        "--engine",
        default="classic",
        help="Phase-2 factor category selector: classic|ai|other (NOT impl engine like ta_mom_v1)",
    )
    p_run.add_argument("--profile", default="test", help="profile name (default: test)")
    p_run.add_argument("--mode", choices=["dry-run", "apply"], default="dry-run")
    p_run.add_argument("--preset", default="full", help="preset name (default: full)")
    p_run.add_argument("--force", action="store_true", help="force recompute")
    p_run.add_argument("--run-id", default="", help="override run_id")
    p_run.add_argument(
        "--p1-policy",
        choices=["ignore", "require_pass", "auto_run_core"],
        default="require_pass",
        help="Phase-1 WFGate policy (auto_run_core not implemented)",
    )
    p_run.add_argument(
        "--gate-policy",
        choices=["require_pass", "allow_fail"],
        default=None,
        help="Phase-2 gate policy (default: allow_fail for dev/test, require_pass for live)",
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    root = ROOT
    rules_path = p2_paths.rules_factors_path(root)

    try:
        if args.command == "status":
            as_of = _parse_date(args.as_of).isoformat()
            out = p2_pipeline.run_status(
                root=root,
                rules_path=rules_path,
                as_of=as_of,
                engine=args.engine,
                profile=args.profile,
            )
            print(f"[p2] status={out}")
            return 0

        if args.command == "plan":
            as_of = _parse_date(args.as_of).isoformat()
            out, _plan = p2_pipeline.run_plan(
                root=root,
                rules_path=rules_path,
                as_of=as_of,
                engine=args.engine,
                profile=args.profile,
                preset="full",
                force=False,
            )
            print(f"[p2] plan={out}")
            return 0

        if args.command == "run":
            as_of_date = _parse_date(args.as_of)
            run_id = p2_paths.build_run_id(
                as_of_date.isoformat(),
                args.engine,
                args.profile,
                args.preset,
                args.run_id or None,
            )
            gate_policy = p2_contracts.resolve_gate_policy(args.profile, args.gate_policy)
            cfg = Phase2RunConfig(
                root=root,
                rules_path=rules_path,
                as_of=as_of_date,
                engine=args.engine,
                profile=args.profile,
                mode=args.mode,
                preset=args.preset,
                force=bool(args.force),
                run_id=run_id,
                p1_policy=args.p1_policy,
                gate_policy=gate_policy,
            )
            result = p2_pipeline.run_phase2(cfg)
            print(f"[p2] status={result.status} gate_pass={result.gate_pass} run_id={result.run_id}")
            if result.gate_pass is False and gate_policy == "allow_fail":
                print("[p2] gate=FAIL (non-blocking; gate_policy=allow_fail)")
            if result.gate_pass is False and gate_policy == "require_pass":
                return 2
            return 0
    except (RulesSchemaError, MissingInputsError, GateFailError) as exc:
        print(f"[p2] error={exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"[p2] error={exc}", file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())


