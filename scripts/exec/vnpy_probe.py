# scripts/exec/vnpy_probe.py
from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from importlib import metadata

# ---------------------------------------------------------------------
# sys.path bootstrap: allow "python scripts/exec/vnpy_probe.py" to import alpha_core
# ---------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _parse_modules(arg: Optional[str]) -> List[str]:
    if not arg:
        return ["vnpy", "vnpy.trader"]
    parts = [p.strip() for p in arg.split(",")]
    return [p for p in parts if p]


def _missing_modules(modules: Iterable[str]) -> List[str]:
    missing: List[str] = []
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception:
            missing.append(name)
    return missing


def _vnpy_version() -> str:
    try:
        vnpy_mod = importlib.import_module("vnpy")
        v = getattr(vnpy_mod, "__version__", None)
        if v:
            return str(v)
    except Exception:
        pass
    try:
        return str(metadata.version("vnpy"))
    except Exception:
        return "unknown"


def _gateway_packages() -> List[str]:
    pkgs: List[str] = []
    for dist in metadata.distributions():
        name = dist.metadata.get("Name", "") if dist.metadata else ""
        if not name:
            continue
        lname = name.lower()
        if lname.startswith("vnpy") and "gateway" in lname:
            pkgs.append(name)
    return sorted(set(pkgs))


def _gateway_entry_points() -> List[str]:
    entries: List[str] = []
    try:
        eps = metadata.entry_points()
        if isinstance(eps, dict):
            group = eps.get("vnpy.gateway", [])
        else:
            group = eps.select(group="vnpy.gateway")
        for ep in group:
            entries.append(f"{ep.name}={ep.value}")
    except Exception:
        return []
    return sorted(set(entries))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="vn.py environment probe (no orders, no loop)")
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

    if missing:
        print("MODULE_MISSING=" + json.dumps(missing, ensure_ascii=False))
        return 60

    print(f"PY={sys.executable}")
    print(f"PYVER={sys.version.split()[0]}")
    print("SYS_PATH=" + json.dumps(sys.path, ensure_ascii=False))
    print(f"VNPY_VERSION={_vnpy_version()}")
    print("MODULE_OK=" + json.dumps(modules, ensure_ascii=False))
    print("GATEWAY_PACKAGES=" + json.dumps(_gateway_packages(), ensure_ascii=False))
    print("GATEWAY_ENTRY_POINTS=" + json.dumps(_gateway_entry_points(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
