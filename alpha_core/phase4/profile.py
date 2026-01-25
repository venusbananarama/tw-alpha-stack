from __future__ import annotations

from typing import Tuple


def apply_profile(args) -> None:
    profile = getattr(args, "profile", "prod")
    if profile == "dev":
        if args.on_insufficient_data == "fail":
            args.on_insufficient_data = "force"
        if args.min_trade_count == 10:
            args.min_trade_count = 1
        if args.min_symbol_coverage == 0.6:
            args.min_symbol_coverage = 0.0


def should_write_ok(profile: str, status: str) -> bool:
    return profile == "prod" and status == "PASS"
