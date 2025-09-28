# -*- coding: utf-8 -*-
import argparse, os, sys, logging
import pandas as pd

ROOT = os.path.join("data", "finmind", "raw")
REPORT_DIR = os.path.join("data", "finmind", "reports")

def scan_dataset(ds: str):
    dirp = os.path.join(ROOT, ds)
    files = []
    if os.path.isdir(dirp):
        for f in os.listdir(dirp):
            if f.endswith(".parquet"):
                files.append(os.path.join(dirp, f))
    return files

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--datasets", default="prices,chip,fundamentals")
    ap.add_argument("--cfg", default="configs/datasets.yaml")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    from _finmind_common import load_groups
    groups = load_groups(args.cfg)

    # expand
    ds_list = []
    for key in [x.strip() for x in args.datasets.split(",") if x.strip()]:
        if key in groups:
            ds_list += groups[key]
        else:
            ds_list.append(key)
    ds_list = list(dict.fromkeys(ds_list))

    rows = []
    for ds in ds_list:
        files = scan_dataset(ds)
        total_rows = 0
        schemas = set()
        for fp in files:
            try:
                df = pd.read_parquet(fp)
            except Exception as ex:
                logging.error("Read failed: %s → %s", fp, ex)
                continue
            total_rows += len(df)
            schemas.add(tuple(df.columns))
        rows.append({"dataset": ds, "files": len(files), "rows": total_rows, "schema_variants": len(schemas)})

    os.makedirs(REPORT_DIR, exist_ok=True)
    out_csv = os.path.join(REPORT_DIR, "verify_summary.csv")
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[REPORT] {out_csv}")

if __name__ == "__main__":
    main()
