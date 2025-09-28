#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
emit_metrics_v63_live.py
- 即時串流 finmind_backfill.py 的輸出到主控台
- 不產生任何檔案
- 可選唯讀掃描 parquet 變更並在結尾印出 SUMMARY
"""
from __future__ import annotations
import argparse, os, sys, subprocess, signal
from pathlib import Path
from datetime import datetime, date

def is_trading_day(d: date, calendar_csv: str | None) -> bool:
    if calendar_csv and os.path.exists(calendar_csv):
        try:
            with open(calendar_csv, "r", encoding="utf-8") as f:
                return d.strftime("%Y-%m-%d") in f.read()
        except Exception:
            pass
    return d.weekday() < 5  # Mon–Fri fallback

def list_parquets(base_dirs: list[str]) -> dict[str, tuple[int, float]]:
    out = {}
    for bd in base_dirs:
        p = Path(bd)
        if not p.exists():
            continue
        for f in p.rglob("*.parquet"):
            try:
                st = f.stat()
                out[str(f)] = (st.st_size, st.st_mtime)
            except FileNotFoundError:
                continue
    return out

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--symbol")
    ap.add_argument("--datasets", nargs="*", default=["TaiwanStockPrice", "TaiwanStockInstitutionalInvestorsBuySell"])
    ap.add_argument("--universe", default="TSE")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--qps", type=float, default=1.6)
    ap.add_argument("--calendar-csv")
    ap.add_argument("--python-exe", default=r".\.venv\Scripts\python.exe")
    ap.add_argument("--finmind-script", default=r"scripts\finmind_backfill.py")
    ap.add_argument("--no-fs-scan", action="store_true", help="不要掃描 parquet 變更")
    args = ap.parse_args()

    root = Path(".").resolve()

    landing = [
        str(root / "datahub" / "silver" / "alpha" / "prices"),
        str(root / "datahub" / "silver" / "alpha" / "chip"),
    ]
    before = {} if args.no_fs_scan else list_parquets(landing)

    py = str((root / args.python_exe).resolve())
    fm = str((root / args.finmind_script).resolve())
    cmd = [py, "-u", "-X", "utf8", fm, "--start", args.start, "--end", args.end]
    if args.symbol:
        cmd += ["--symbols", args.symbol.replace(".TW", "")]
    else:
        cmd += ["--universe", args.universe]
    if args.datasets:
        cmd += ["--datasets"] + list(args.datasets)
    cmd += ["--workers", str(args.workers), "--qps", str(args.qps)]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    creationflags = 0x00000200 if os.name == "nt" else 0  # CREATE_NEW_PROCESS_GROUP

    print(">>> RUN:", " ".join(cmd), flush=True)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
            creationflags=creationflags
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="", flush=True)
        rc = proc.wait()
    except KeyboardInterrupt:
        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        try:
            proc.terminate()
        except Exception:
            pass
        print("SUMMARY status=FAIL reason=cancelled changed=0", flush=True)
        return 130

    changed = 0
    if not args.no_fs_scan:
        after = list_parquets(landing)
        for p, meta in after.items():
            if p not in before or before[p] != meta:
                changed += 1

    try:
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
        trading = is_trading_day(end_dt, args.calendar_csv)
    except Exception:
        trading = True

    if rc == 0:
        if changed > 0:
            print(f"SUMMARY status=PASS reason=write changed={changed}", flush=True)
            return 0
        else:
            reason = "api_empty" if trading else "end_is_non_trading_day"
            print(f"SUMMARY status=PASS_NOOP reason={reason} changed=0", flush=True)
            return 0
    else:
        print(f"SUMMARY status=FAIL reason=child_exit_code_{rc} changed={changed}", flush=True)
        return rc

if __name__ == "__main__":
    sys.exit(main())
