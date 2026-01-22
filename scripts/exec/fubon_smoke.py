from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List


def _format_error(e: BaseException) -> str:
    msg = f"{type(e).__name__}: {e}"
    cause = getattr(e, "__cause__", None)
    if cause:
        msg += f" | cause={type(cause).__name__}: {cause}"
    return msg


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / "alpha_core").exists():
            return parent
    return Path.cwd().resolve()


def _bootstrap_sys_path() -> None:
    root = _repo_root_from_here()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    # also allow running under scripts/exec
    exec_dir = (root / "scripts" / "exec")
    if exec_dir.exists() and str(exec_dir) not in sys.path:
        sys.path.insert(0, str(exec_dir))


def parse_args():
    ap = argparse.ArgumentParser(description="Fubon Smoke Test (connect/close only)")
    ap.add_argument("--provider-module", default=None, help="Override provider module import path")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--account-id", default=None)
    ap.add_argument("--no-keyring", action="store_true")
    ap.add_argument("--timeout-s", type=int, default=30)
    return ap.parse_args()


def main() -> int:
    _bootstrap_sys_path()

    from alpha_core.execution.fubon_broker_adapter import FubonAdapterConfig, FubonBrokerAdapter

    args = parse_args()

    cfg = FubonAdapterConfig(
        provider_module=args.provider_module,
        profile=args.profile,
        account_id=args.account_id,
        use_keyring=(not args.no_keyring),
        timeout_s=args.timeout_s,
    )

    ad = FubonBrokerAdapter(cfg)

    try:
        ad.connect()
        print("CONNECT_OK")
    except Exception as e:
        print(f"CONNECT_FAIL: {_format_error(e)}")
        return 60

    try:
        ad.close()
        ad.close()  # idempotent
        print("CLOSE_OK")
    except Exception as e:
        print(f"CLOSE_FAIL: {_format_error(e)}")
        return 61

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
