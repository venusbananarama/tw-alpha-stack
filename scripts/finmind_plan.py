# -*- coding: utf-8 -*-
import argparse, json, os, sys
from _finmind_common import load_groups, month_chunks

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2015-01-01")
    ap.add_argument("--datasets", default="prices,chip,fund")
    ap.add_argument("--cfg", default="configs/datasets.yaml")
    ap.add_argument("--dryrun", action="store_true")
    args = ap.parse_args()

    groups = load_groups(args.cfg)
    reqs = []
    for key in [x.strip() for x in args.datasets.split(",") if x.strip()]:
        if key in groups:
            for ds in groups[key]:
                reqs.append(("group", key, ds))
        else:
            reqs.append(("raw", key, key))

    # Coarse plan: monthly chunking from since..today
    from datetime import date
    today = date.today().isoformat()
    chunks = month_chunks(args.since, today)

    plan = {
        "since": args.since,
        "until": today,
        "datasets": [r[2] for r in reqs],
        "total_months": len(chunks),
        "estimated_calls": len(chunks) * len(reqs),
        "notes": "Monthly chunking to respect rate limits; actual API calls occur in backfill/daily scripts."
    }
    print(json.dumps(plan, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
