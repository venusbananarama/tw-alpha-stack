from __future__ import annotations

import argparse
from typing import Sequence


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--exec-run-id", required=True)
    ap.add_argument("--exec-trades-path", default=None)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--bronze-root", default="datahub/bronze/fubon/trades")
    ap.add_argument("--exec-root", default="reports/exec")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--mode", default="all", choices=["all", "replay", "drift", "wf"])
    ap.add_argument("--ignore-incomplete", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--calendar-csv", default="datahub/ref/trading_days.csv")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--ref-price-mode", default="last_trade_before")
    ap.add_argument("--window-sec", type=int, default=5)
    ap.add_argument("--tolerance-ms", type=int, default=None)
    ap.add_argument(
        "--print-resolved-paths",
        action="store_true",
        help="Print resolved paths (also always logged) for audit/debug.",
    )
    ap.add_argument("--on-insufficient-data", choices=["fail", "skip", "force"], default="fail")
    ap.add_argument("--min-symbol-coverage", type=float, default=0.6)
    ap.add_argument("--min-trade-count", type=int, default=10)
    ap.add_argument("--profile", choices=["prod", "dev"], default="prod")
    return ap


def _cli_main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    from alpha_core.phase4.runner import run

    return run(args)


main = _cli_main


if __name__ == "__main__":
    raise SystemExit(_cli_main())
