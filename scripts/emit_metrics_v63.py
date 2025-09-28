#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrap finmind_backfill.py, emit concise JSON summary, and log stdout/stderr safely.
"""
import argparse, json, os, sys, subprocess
from datetime import datetime, date
from pathlib import Path

def is_trading_day(d: date, calendar_csv: str | None) -> bool:
    if calendar_csv and os.path.exists(calendar_csv):
        try:
            with open(calendar_csv, "r", encoding="utf-8") as f:
                txt = f.read()
            return d.strftime("%Y-%m-%d") in txt
        except Exception:
            pass
    return d.weekday() < 5  # Mon-Fri fallback

def list_parquets(base_dirs):
    files = []
    for bd in base_dirs:
        for p in Path(bd).rglob("*.parquet"):
            try:
                st = p.stat()
                files.append({"path": str(p), "mtime": st.st_mtime, "size": st.st_size})
            except FileNotFoundError:
                continue
    return files

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--skip-full", action="store_true")
    ap.add_argument("--symbol")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--qps", type=float, default=1.6)
    ap.add_argument("--calendar-csv")
    ap.add_argument("--summary-json-path", default="metrics/verify_summary_latest.json")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--finmind-script", default="scripts/finmind_backfill.py")
    ap.add_argument("--datasets", nargs="*", default=["TaiwanStockPrice","TaiwanStockInstitutionalInvestorsBuySell"])
    ap.add_argument("--universe", default="TSE")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    (root / "metrics").mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(parents=True, exist_ok=True)

    landing = {
        "prices": str(root / "datahub" / "silver" / "alpha" / "prices"),
        "chip":   str(root / "datahub" / "silver" / "alpha" / "chip"),
    }
    before = list_parquets(landing.values())

    py = str(root / ".venv" / "Scripts" / "python.exe")
    fm = str(root / args.finmind_script)
    cmd = [py, "-X", "utf8", fm, "--start", args.start, "--end", args.end]
    if args.symbol:
        cmd += ["--symbols", args.symbol.replace(".TW","")]
    else:
        cmd += ["--universe", args.universe]
    if args.datasets:
        cmd += ["--datasets"] + args.datasets
    cmd += ["--workers", str(args.workers), "--qps", str(args.qps)]

    log_path = root / "logs" / f"finmind_v63_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root), timeout=60*60)
        stdout, stderr = proc.stdout, proc.stderr
        with open(log_path, "w", encoding="utf-8", errors="ignore") as f:
            f.write("=== CMD ===\n")
            f.write(" ".join(cmd) + "\n")
            f.write("\n=== STDOUT ===\n")
            f.write(stdout or "")
            f.write("\n\n=== STDERR ===\n")
            f.write(stderr or "")
    except Exception as e:
        summary = {
            "status":"FAIL","reason":"wrapper_error","noop":True,"rows":0,"landing":landing,
            "results":{"single":{"csv":None,"parquetFiles":[], "rows":0, "passReason":"wrapper_error"}},
            "meta":{"error":repr(e),"cmd":cmd,"log":str(log_path)}
        }
        Path(args.summary_json_path).parent.mkdir(parents=True, exist_ok=True)
        with open(args.summary_json_path,"w",encoding="utf-8") as f:
            json.dump(summary,f,ensure_ascii=False,indent=2)
        print("[FAIL] wrapper_error:", e, file=sys.stderr)
        sys.exit(1)

    end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
    on_trading_day = is_trading_day(end_dt, args.calendar_csv)
    after = list_parquets(landing.values())

    before_map = {x["path"]:(x["size"],x["mtime"]) for x in before}
    changed = [x["path"] for x in after if x["path"] not in before_map or before_map[x["path"]]!=(x["size"],x["mtime"])]

    if not on_trading_day:
        status, reason, noop = "PASS_NOOP", "end_is_non_trading_day", True
    elif changed:
        status, reason, noop = "PASS", "write", False
    else:
        status, reason, noop = "PASS_NOOP", "api_empty", True

    summary = {
        "status":status,"reason":reason,"noop":noop,"rows":None,"landing":landing,
        "results":{"single":{"csv":None,"parquetFiles":changed,"rows":None,"passReason":reason}},
        "meta":{"cmd":cmd,"stdout_tail":(stdout or "")[-2000:], "stderr_tail":(stderr or "")[-2000:], "log":str(log_path)}
    }
    Path(args.summary_json_path).parent.mkdir(parents=True, exist_ok=True)
    with open(args.summary_json_path,"w",encoding="utf-8") as f:
        json.dump(summary,f,ensure_ascii=False,indent=2)
    print(f"[{status}] reason={reason} changed_files={len(changed)} summary={args.summary_json_path}")

if __name__ == "__main__":
    main()
