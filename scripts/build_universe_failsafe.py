#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_universe_failsafe.py
- Tries build_universe.py first.
- If the output is empty, falls back to configs/universe.tw_all.txt (if present),
  otherwise writes a small sane default basket.
- Always exits with code 0 after attempting best-effort output.
"""
from __future__ import annotations
import os, sys, shutil
from pathlib import Path

DEFAULT_FALLBACK = [
    # Broad, liquid TW tickers as ultimate fallback (comment lines allowed)
    "2330.TW", "2317.TW", "2412.TW", "2303.TW", "2454.TW",
    "6505.TW", "2881.TW", "2882.TW", "1301.TW", "1303.TW",
    "2002.TW", "3711.TW", "2308.TW", "2382.TW", "2603.TW",
    "2609.TW", "2615.TW", "2891.TW", "2886.TW", "2884.TW",
    "2892.TW", "5871.TW", "1216.TW", "1101.TW", "1102.TW",
    "1590.TW", "3034.TW", "3037.TW", "2324.TW", "8046.TW",
    "6669.TW", "2379.TW", "3008.TW", "3714.TW", "2383.TW",
    "0050.TW", "0056.TW", "00878.TW", "006208.TW", "00881.TW",
    "1210.TW", "9904.TW", "2207.TW", "5876.TW", "1402.TW",
    "2890.TW", "2885.TW", "3481.TW", "2327.TW", "2357.TW",
]

def _count_symbols(p: Path) -> int:
    if not p.exists():
        return 0
    cnt = 0
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        cnt += 1
    return cnt

def _write_list(p: Path, symbols):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as w:
        for s in symbols:
            w.write(s.strip() + "\n")

def main(argv):
    import argparse, subprocess
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="rules.yaml")
    ap.add_argument("--out", default=str(Path("configs") / "investable_universe.txt"))
    ap.add_argument("--root", default=".")
    ap.add_argument("--fallback", default=str(Path("configs") / "universe.tw_all.txt"))
    args = ap.parse_args(argv)

    root = Path(args.root).resolve()
    os.environ.pop("PYTHONSTARTUP", None)
    os.environ.setdefault("ALPHACITY_ALLOW", "1")

    out = (root / args.out).resolve()
    fall = (root / args.fallback).resolve()

    # 1) Try the official builder
    builder = root / "scripts" / "build_universe.py"
    if builder.exists():
        try:
            cmd = [sys.executable, "-S", str(builder), "--rules", args.rules, "--out", str(out)]
            print(">>", " ".join(cmd))
            r = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
            sys.stdout.write(r.stdout or "")
            sys.stderr.write(r.stderr or "")
        except Exception as e:
            print(f"[WARN] build_universe.py failed: {e!r}")
    else:
        print(f"[WARN] missing {builder}")

    cnt = _count_symbols(out)
    if cnt == 0:
        # 2) Fallback from tw_all if present
        if fall.exists():
            try:
                shutil.copyfile(fall, out)
                cnt = _count_symbols(out)
                print(f"[FAILSAFE] copied {fall} -> {out}  ({cnt} symbols)")
            except Exception as e:
                print(f"[WARN] fallback copy failed: {e!r}")
        # 3) Ultimate fallback: baked list
        if cnt == 0:
            _write_list(out, DEFAULT_FALLBACK)
            cnt = _count_symbols(out)
            print(f"[FAILSAFE] wrote baked default list -> {out}  ({cnt} symbols)")
    print(f"UNIVERSE_FAILSAFE: {cnt} symbols -> {out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
