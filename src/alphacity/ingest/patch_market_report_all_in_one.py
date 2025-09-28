# -*- coding: utf-8 -*-
"""A small, safe patcher for ingest/market_report_all_in_one.py.

Fixes:
- Replace "out = g.apply(per_symbol)" with
  "out = df.groupby('symbol', group_keys=False).apply(per_symbol)" (two occurrences expected)
- Ensure "matplotlib.use('Agg')" is set to avoid GUI backend issues when saving charts.

Usage:
    python patch_market_report_all_in_one.py --file G:\AI\tw-alpha-stack\ingest\market_report_all_in_one.py
"""
import argparse
import io
import os
import re
import sys

REPLACEMENTS = [
    (r'^\s*out\s*=\s*g\.apply\(\s*per_symbol\s*\)\s*$', 'out = df.groupby("symbol", group_keys=False).apply(per_symbol)'),
]

def ensure_matplotlib_backend(src: str) -> str:
    lines = src.splitlines()
    has_backend = any("matplotlib.use(" in ln for ln in lines)
    if has_backend:
        return src
    # insert after the last import line near the top
    insert_idx = 0
    for i, ln in enumerate(lines[:80]):
        if re.match(r'^\s*(import|from)\s+\w+', ln):
            insert_idx = i + 1
    snippet = 'import matplotlib\nmatplotlib.use("Agg")'
    lines.insert(insert_idx, snippet)
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to market_report_all_in_one.py")
    args = ap.parse_args()

    path = args.file
    if not os.path.isfile(path):
        print(f"[ERROR] File not found: {path}", file=sys.stderr)
        sys.exit(2)

    with io.open(path, "r", encoding="utf-8") as f:
        src = f.read()

    original = src

    # apply replacements
    for pat, rep in REPLACEMENTS:
        src = re.sub(pat, rep, src, flags=re.MULTILINE)

    # ensure matplotlib backend
    src = ensure_matplotlib_backend(src)

    if src == original:
        print("[WARN] No changes made (file may already be patched).")
        return

    bak = path + ".bak"
    with io.open(bak, "w", encoding="utf-8") as f:
        f.write(original)

    with io.open(path, "w", encoding="utf-8") as f:
        f.write(src)

    print(f"[INFO] Patched. Backup saved to {bak}")

if __name__ == "__main__":
    main()
