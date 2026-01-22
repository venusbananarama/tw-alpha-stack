# scripts/exec/validate_exec_logs.py
from __future__ import annotations

# --- sys.path bootstrap: ensure repo root is importable when running by file path ---
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# --- end bootstrap ---

import argparse
import json
from typing import Any, Dict, List

import pandas as pd

from alpha_core.execution.validator import (
    ValidationError,
    validate_account_snapshot,
    validate_cross_artifacts,
)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def to_report(ok: bool, errors: List[ValidationError]) -> Dict[str, Any]:
    return {
        "ok": ok,
        "errors": [{"code": e.code, "message": e.message, "context": e.context} for e in errors],
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Validate Phase-3 exec logs (schema + invariants + cross checks)"
    )
    ap.add_argument("--orders", required=True, help="Path to orders.csv")
    ap.add_argument("--trades", default=None, help="Path to trades.csv (optional)")
    ap.add_argument("--positions", default=None, help="Path to positions.csv (optional)")
    ap.add_argument("--account", default=None, help="Path to account_snapshot.json (optional)")
    ap.add_argument("--summary", default=None, help="Path to exec_summary.json (optional)")
    args = ap.parse_args()

    orders = pd.read_csv(args.orders)

    trades = pd.read_csv(args.trades) if args.trades else None
    positions = pd.read_csv(args.positions) if args.positions else None

    account_obj = None
    account_errors: List[ValidationError] = []
    if args.account:
        raw = load_json(Path(args.account))
        if isinstance(raw, list):
            for rec in raw:
                vr = validate_account_snapshot(rec)
                if not vr.ok:
                    account_errors.extend(vr.errors)
            account_obj = None
        elif isinstance(raw, dict):
            account_obj = raw
        else:
            account_obj = None
            account_errors.append(
                ValidationError(
                    code="E_ACCOUNT_JSON_TYPE",
                    message="account_snapshot must be dict or list[dict]",
                    context={"type": str(type(raw))},
                )
            )

    summary_obj = load_json(Path(args.summary)) if args.summary else None

    vr = validate_cross_artifacts(
        orders=orders,
        trades=trades,
        positions=positions,
        account=account_obj,
        summary=summary_obj,
    )

    all_errors = list(vr.errors) + account_errors
    ok = vr.ok and (len(account_errors) == 0)

    report = {"ok": ok, "cross": to_report(vr.ok, vr.errors)}
    if account_errors:
        report["account_snapshot_extra"] = to_report(False, account_errors)

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
