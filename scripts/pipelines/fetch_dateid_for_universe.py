# scripts/pipelines/fetch_dateid_for_universe.py
from __future__ import annotations
import argparse, subprocess, sys, time, math, pathlib, yaml
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[2]
PY   = ROOT / ".venv" / "Scripts" / "python.exe"
BACKFILL = ROOT / "scripts" / "finmind_backfill.py"

def load_lines(fp: pathlib.Path) -> list[str]:
    return [x.strip() for x in fp.read_text(encoding="utf-8").splitlines() if x.strip() and not x.strip().startswith("#")]

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe-file", required=True)
    ap.add_argument("--datasets-config", required=True)
    ap.add_argument("--start", required=True)     # YYYY-MM-DD
    ap.add_argument("--end",   required=True)     # YYYY-MM-DD (inclusive for user; we'll +1d)
    ap.add_argument("--datahub-root", default="datahub")
    ap.add_argument("--batch-size", type=int, default=120)
    ap.add_argument("--batch-sleep", type=int, default=0)  # seconds between batches
    ap.add_argument("--jobs", type=int, default=1)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--include", nargs="*", default=None, help="Only run these datasets (optional)")
    ap.add_argument("--exclude", nargs="*", default=None, help="Skip these datasets (optional)")
    args = ap.parse_args()

    symbols = load_lines(pathlib.Path(args.universe_file))
    cfg = yaml.safe_load(pathlib.Path(args.datasets_config).read_text(encoding="utf-8"))
    start = args.start
    end_exclusive = (datetime.fromisoformat(args.end) + timedelta(days=1)).strftime("%Y-%m-%d")

    ds_list = cfg["datasets"]
    if args.include:
        keep = set(args.include)
        ds_list = [d for d in ds_list if d["table"] in keep]
    if args.exclude:
        ban = set(args.exclude)
        ds_list = [d for d in ds_list if d["table"] not in ban]

    print(f"[fetch] symbols={len(symbols)} datasets={len(ds_list)} start={start} end_excl={end_exclusive}")

    # NOTE: jobs>1 可用 multiprocessing/joblib 改寫；先以串行穩定供應商限流。
    for ds in ds_list:
        name   = ds["table"]
        id_key = ds.get("id_key", "stock_id")
        print(f"\n=== Dataset {name} (id_key={id_key}) ===")
        batches = list(chunked(symbols, args.batch_size))
        for bi, batch in enumerate(batches, 1):
            ok = False
            for attempt in range(1, args.retries+2):
                cmd = [
                    str(PY), str(BACKFILL),
                    "--datasets", name,
                    "--symbols", ",".join(batch),
                    "--start", start,
                    "--end", end_exclusive,
                    "--datahub-root", args.datahub_root,
                    "--id-key", id_key,            # 舊腳本小補丁（無此參數也不會壞）
                ]
                print(f"[{name}] batch {bi}/{len(batches)} attempt {attempt} (n={len(batch)})")
                rc = subprocess.call(cmd)
                if rc == 0:
                    ok = True; break
                sleep_s = 3 * attempt
                print(f"[WARN] rc={rc} sleeping {sleep_s}s before retry...")
                time.sleep(sleep_s)
            if not ok:
                print(f"[WARN] {name} batch {bi} failed after retries", file=sys.stderr)
            if args.batch_sleep > 0:
                time.sleep(args.batch_sleep)

if __name__ == "__main__":
    main()
