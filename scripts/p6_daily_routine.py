from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase6.core import run_phase6  # noqa: E402
from alpha_core.phase6.errors import ExitCode, Phase6Error  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Phase-6 daily routine (pre-trade gate).")
    p.add_argument("--root", default=str(_REPO_ROOT))
    p.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    p.add_argument("--mode", default="pretrade")
    p.add_argument("--snapshot-source", default="exec")
    p.add_argument("--prev-exec-dir", default=None)
    p.add_argument("--out-dir", default=None)
    p.add_argument("--benchmark-file", default=None)
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_phase6(
            root_dir=args.root,
            as_of=str(args.as_of).strip(),
            mode=str(args.mode).strip(),
            snapshot_source=str(args.snapshot_source).strip(),
            prev_exec_dir=args.prev_exec_dir,
            out_dir=args.out_dir,
            benchmark_file=args.benchmark_file,
        )
        return int(result.exit_code)
    except Phase6Error as exc:
        return int(exc.exit_code)
    except Exception:
        return int(ExitCode.FAIL_RUNTIME)


if __name__ == "__main__":
    raise SystemExit(main())
