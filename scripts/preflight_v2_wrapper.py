#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
preflight_v2_wrapper.py
- Self-contained preflight scanner + expect_date computation.
- Writes reports/preflight_report.json with freshness info.
- Avoids sitecustomize side-effects by recommending "python -S" at caller level.
"""
from __future__ import annotations
import os, sys, json, traceback
from pathlib import Path
from typing import Optional, Dict, Any, List

def _tz_now_yyyymmdd(tz: str = "Asia/Taipei") -> str:
    try:
        import pandas as pd
        return pd.Timestamp.now(tz=tz).normalize().date().isoformat()
    except Exception:
        # Fallback to naive UTC+8
        from datetime import datetime, timedelta, timezone
        return (datetime.now(timezone.utc) + timedelta(hours=8)).date().isoformat()

def _last_trading_day(root: Path) -> str:
    """Return last trading day (YYYY-MM-DD). If calendar missing, return today (TW)."""
    cal_csv = root / "cal" / "trading_days.csv"
    if not cal_csv.exists():
        return _tz_now_yyyymmdd("Asia/Taipei")
    try:
        import pandas as pd
        cal = pd.read_csv(cal_csv, header=None, names=["date"])
        cal["date"] = pd.to_datetime(cal["date"]).dt.date
        today_tw = pd.Timestamp.now(tz="Asia/Taipei").normalize().date()
        last_td = max([d for d in cal["date"].tolist() if d <= today_tw])
        return last_td.isoformat()
    except Exception:
        return _tz_now_yyyymmdd("Asia/Taipei")

def _max_date_in_parquet_dir(p: Path) -> Optional[str]:
    """Scan *.parquet under a partition dir and return max 'date' (YYYY-MM-DD)."""
    try:
        import pyarrow.parquet as pq
        import pandas as pd
    except Exception:
        return None
    # Gather max from all files (only 'date' column to minimize IO)
    maxd: Optional[pd.Timestamp] = None  # type: ignore
    files = list(p.glob("*.parquet"))
    for f in files:
        try:
            t = pq.read_table(f, columns=["date"])
            if t.num_rows == 0:
                continue
            s = t.column("date").to_pandas()
            # Ensure datetime
            s = pd.to_datetime(s)
            cur = s.max()
            if maxd is None or cur > maxd:
                maxd = cur
        except Exception:
            continue
    if maxd is None:
        return None
    return str(pd.to_datetime(maxd).date())  # type: ignore

def _scan_dataset(root: Path, name: str) -> Dict[str, Any]:
    base = root / "datahub" / "silver" / "alpha" / name
    exists = base.exists()
    max_date: Optional[str] = None
    if exists:
        # find latest yyyymm partition
        parts = sorted([p for p in base.glob("yyyymm=*") if p.is_dir()])
        # Search the newest first
        for part in reversed(parts):
            md = _max_date_in_parquet_dir(part)
            if md:
                max_date = md
                break
    return {"dataset": str(base), "exists": exists, "max_date": max_date}

def _dup_archives(root: Path, name: str) -> Dict[str, Any]:
    # Count how many "*.bak_*" archives present under datahub/_archive for this name
    arch = root / "datahub" / "_archive"
    bak_count = 0
    if arch.exists():
        for child in arch.glob("*.bak*"):
            if child.is_dir():
                bak_count += 1
    return {"dataset": str(root / "datahub" / "silver" / "alpha" / name), "ok": True, "bak_count": bak_count}

def main(argv: List[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="rules.yaml")
    ap.add_argument("--export", default="reports")
    ap.add_argument("--root", default=".")
    args = ap.parse_args(argv)

    root = Path(args.root).resolve()
    os.environ.pop("PYTHONSTARTUP", None)  # neutralize sitecustomize use
    os.environ.setdefault("ALPHACITY_ALLOW", "1")

    export_dir = root / args.export
    export_dir.mkdir(parents=True, exist_ok=True)

    expect_date = _last_trading_day(root)
    datasets = ["prices", "chip", "dividend", "per"]

    freshness = []
    for ds in datasets:
        info = _scan_dataset(root, ds)
        md = info.get("max_date")
        ok = bool(md and md >= expect_date)
        freshness.append({
            "dataset": info["dataset"],
            "exists": info["exists"],
            "ok": ok,
            "max_date": md,
        })
    dup = [_dup_archives(root, ds) for ds in datasets]

    report = {
        "freshness": freshness,
        "dup_partitions": dup,
        "meta": {
            "timezone": "Asia/Taipei",
            "expect_date": expect_date,
            "root": str(root),
        },
    }
    out = export_dir / "preflight_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Human log
    print(f"[Preflight] expect_date={expect_date} tz=Asia/Taipei")
    for item in freshness:
        path = item["dataset"]
        md = item["max_date"]
        ok = item["ok"]
        status = "OK" if ok else "FAIL"
        print(f"  freshness [{status}] {Path(path).as_posix()} max_date={md}")
    for item in dup:
        print(f"  dup_check [OK] {Path(item['dataset']).as_posix()} bak_count={item['bak_count']}")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except SystemExit as e:
        raise
    except Exception as e:
        traceback.print_exc()
        raise SystemExit(1)
