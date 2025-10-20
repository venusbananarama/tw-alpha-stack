# -*- coding: utf-8 -*-
import argparse
import os
import sys
import time

def main():
    p = argparse.ArgumentParser(description="Cleanup old generated reports safely.")
    p.add_argument("--dir", default="G:/AI/datahub/reports", help="Reports folder")
    p.add_argument("--keep-days", type=int, default=10, help="Keep files modified within N days")
    p.add_argument("--pattern", nargs="*", default=["*.csv", "*.xlsx", "*.parquet", "*.zip"], help="Glob patterns to remove")
    p.add_argument("--dry-run", action="store_true", help="Show actions without deleting")
    args = p.parse_args()

    if not os.path.isdir(args.dir):
        print(f"[ERROR] Directory not found: {args.dir}")
        sys.exit(2)

    import glob
    cutoff = time.time() - args.keep_days * 86400
    total = 0
    deleted = 0
    print(f"[INFO] Scanning {args.dir} (keep last {args.keep_days} days)")
    for pat in args.pattern:
        for path in glob.glob(os.path.join(args.dir, pat)):
            total += 1
            try:
                mtime = os.path.getmtime(path)
            except FileNotFoundError:
                continue
            if mtime < cutoff:
                age_days = (time.time() - mtime) / 86400.0
                if args.dry_run:
                    print(f"[DRY] Would delete: {path}  ({age_days:.1f} days old)")
                else:
                    try:
                        os.remove(path)
                        deleted += 1
                        print(f"[DEL] {path}")
                    except Exception as e:
                        print(f"[WARN] Failed to delete {path}: {e}")

    print(f"[INFO] Done. Scanned {total} files; deleted {deleted} (older than {args.keep_days} days).")

if __name__ == "__main__":
    main()
