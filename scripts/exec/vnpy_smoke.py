# scripts/exec/vnpy_smoke.py
from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------
# sys.path bootstrap: allow "python scripts/exec/vnpy_smoke.py" to import alpha_core
# ---------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.execution.schemas import ExecMode  # noqa: E402
from alpha_core.execution.vnpy_broker_adapter import VnpyBrokerAdapter  # noqa: E402


def _parse_modules(arg: Optional[str]) -> List[str]:
    if not arg:
        return ["vnpy", "vnpy.trader"]
    parts = [p.strip() for p in arg.split(",")]
    return [p for p in parts if p]


def _missing_modules(modules: List[str]) -> List[str]:
    missing: List[str] = []
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception:
            missing.append(name)
    return missing


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="VnpyBrokerAdapter connect/close smoke test")
    p.add_argument(
        "--modules",
        default=None,
        help="Comma-separated module list (default: vnpy,vnpy.trader)",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    modules = _parse_modules(args.modules)
    missing = _missing_modules(modules)

    adapter = VnpyBrokerAdapter(mode=ExecMode.LIVE)
    exit_code = 0

    try:
        if missing:
            print("VNPY_IMPORT_FAILED: missing modules: " + ", ".join(missing))
            exit_code = 60
            return exit_code

        adapter.connect()
        print("CONNECT_OK")
    except Exception as exc:
        msg = str(exc)
        if "VNPY_IMPORT_FAILED:" in msg and "missing modules" in msg:
            print(msg)
            exit_code = 60
        else:
            print(f"CONNECT_FAILED: {msg}")
            exit_code = 61
    finally:
        try:
            adapter.close()
            adapter.close()
            print("CLOSE_OK")
        except Exception as exc:
            print(f"CLOSE_FAILED: {exc}")
            if exit_code == 0:
                exit_code = 61

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
