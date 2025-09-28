# -*- coding: utf-8 -*-
"""
backtest_patch_core_weekly_fri.py
---------------------------------
Patch `backtest/core.py` so that WEEKLY rebalance uses **W-FRI** (week ending Friday)
and avoids picking weekend timestamps that produce zero rows.

Usage
-----
python backtest_patch_core_weekly_fri.py backtest/core.py
"""

from __future__ import annotations
import sys, re
from pathlib import Path
from datetime import datetime

BANNER = "[weekly-fix]"

def patch_core_weekly(target: str) -> None:
    p = Path(target)
    if not p.exists():
        print(f"{BANNER} ERROR: not found -> {p}")
        sys.exit(1)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = p.with_suffix(f".py.bak_{ts}")
    p.replace(bak)  # move as backup
    s = bak.read_text(encoding="utf-8")

    # Normalize newlines for easier regex
    s = s.replace("\r\n", "\n")

    # We will replace the weekly branch body to an implementation that:
    #   1) Ensures we only operate on weekdays (Mon-Fri)
    #   2) Uses to_period('W-FRI') and picks the max trading date per week
    weekly_impl = (
        'elif cfg.rebalance.upper() == "W":\n'
        '        # Week-ending Friday. Filter out weekends then take the last trading day of each week.\n'
        '        _df = df[df["date"].dt.weekday <= 4]\n'
        '        rb_dates = (\n'
        '            _df.groupby(_df["date"].dt.to_period("W-FRI"))["date"]\n'
        '               .max()\n'
        '               .sort_values()\n'
        '        )\n'
    )

    # Patterns we will try to replace (covering several historical variants)
    patterns = [
        # Variant with to_timestamp()
        r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*rb_dates\s*=\s*df\.groupby\(\s*df\["date"\]\.dt\.to_period\("W"\)\s*\)\["date"\]\.max\(\)\.sort_values\(\)\.dt\.[^\n]+\n',
        r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*\n\s*rb_dates\s*=\s*df\.groupby\(\s*df\["date"\]\.dt\.to_period\("W"\)\s*\)\["date"\]\.max\(\)\.sort_values\(\)\.dt\.[^\n]+\n',
        # Variant without to_timestamp()
        r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*\n\s*rb_dates\s*=\s*df\.groupby\(\s*df\["date"\]\.dt\.to_period\("W"\)\s*\)\["date"\]\.max\(\)\.sort_values\(\)\s*\n',
        r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*rb_dates\s*=\s*df\.groupby\(\s*df\["date"\]\.dt\.to_period\("W"\)\s*\)\["date"\]\.max\(\)\.sort_values\(\)\s*\n',
        # Generic "weekly branch" block from elif until next elif/else
        r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*\n(?:.+?\n)(?=(?:\s*elif|\s*else:))',
    ]

    replaced = False
    for pat in patterns:
        new_s, n = re.subn(pat, weekly_impl, s, flags=re.S)
        if n > 0:
            s = new_s
            replaced = True
            break

    if not replaced:
        # Fallback: try to locate the weekly branch header and replace its body manually.
        m = re.search(r'(\n\s*elif\s+cfg\.rebalance\.upper\(\)\s*==\s*"W"\s*:\s*\n)', s)
        if m:
            start = m.end()
            # Find next elif/else at same indentation
            tail = re.search(r'\n\s*(elif\s+cfg\.rebalance\.upper\(\)\s*==|else:)\s*', s[start:], flags=re.S)
            end = start + (tail.start() if tail else 0)
            s = s[:m.start()] + "\n" + weekly_impl + s[end:]
            replaced = True

    # Minor safety improvement for monthly branch in some older versions that used .dt.end_time
    s = re.sub(
        r'else\s*:\s*rb_dates\s*=\s*\(\s*df\.groupby\(df\["date"\]\.dt\.to_period\("M"\)\)\["date"\]\s*\.max\(\)\s*\.sort_values\(\)\s*\.dt\.to_period\("M"\)\s*\.dt\.end_time\s*\)',
        'else:\n        rb_dates = df.groupby(df["date"].dt.to_period("M"))["date"].max().sort_values()',
        s, flags=re.S
    )

    if not replaced:
        # Restore original if nothing changed and report
        bak.replace(p)  # move back
        print(f"{BANNER} WARN: no weekly pattern matched. Restored original file: {p}")
        sys.exit(2)

    p.write_text(s, encoding="utf-8")
    print(f"{BANNER} Patched OK -> {p}")
    print(f"{BANNER} Backup     -> {bak}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest_patch_core_weekly_fri.py backtest/core.py")
        sys.exit(1)
    patch_core_weekly(sys.argv[1])
