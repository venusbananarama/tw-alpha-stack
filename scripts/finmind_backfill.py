# -*- coding: utf-8 -*-
# FinMind Backfill (API, Strict Fix + Day-Index Gating)
import os, sys, json, time, math, argparse, glob, datetime
from urllib import request, parse
import pandas as pd

BASE  = os.environ.get("FINMIND_BASE_URL", "https://api.finmindtrade.com/api/v4/data")
TOKEN = (os.environ.get("FINMIND_TOKEN") or "").strip()
if not TOKEN:
    print("ERROR: FINMIND_TOKEN 未設定", file=sys.stderr); sys.exit(2)

def alias_to_dataset(name: str) -> str:
    m = {
        "prices":"TaiwanStockPrice","price":"TaiwanStockPrice","taiwanstockprice":"TaiwanStockPrice",
        "chip":"TaiwanStockInstitutionalInvestorsBuySell","institutional":"TaiwanStockInstitutionalInvestorsBuySell",
        "per":"TaiwanStockPER","taiwanstockper":"TaiwanStockPER",
        "dividend":"TaiwanStockDividend","taiwanstockdividend":"TaiwanStockDividend"
    }
    k = (name or "").strip().lower()
    return m.get(k, name)

def dataset_to_kind(ds: str) -> str:
    a = ds.lower()
    if "price" in a: return "prices"
    if "buysell" in a or "institutional" in a: return "chip"
    if "per" in a and "taiwanstockper" in a or a.endswith("per"): return "per"
    if "dividend" in a: return "dividend"
    return "prices"

def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()

def http_get(dataset: str, data_id: str, start: str, end: str) -> pd.DataFrame:
    qs = parse.urlencode({"dataset":dataset,"data_id":data_id,"start_date":start,"end_date":end})
    req = request.Request(f"{BASE}?{qs}", headers={"Authorization": f"Bearer {TOKEN}"})
    with request.urlopen(req, timeout=30) as r:
        obj = json.loads(r.read().decode("utf-8"))
        data = obj.get("data") or []
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str)
        df["symbol"] = df.get("stock_id", "").astype(str).str.replace(".TW","", regex=False)
        return df

def build_day_index(root: str, kind: str, day: str) -> tuple[set,int]:
    """一次掃描 day 所在月（必要時+前月），回傳該日已存在的 symbol 集合"""
    base = os.path.join(root, "silver", "alpha", kind)
    d    = parse_date(day)
    ym   = f"{d.year:04d}{d.month:02d}"
    # 若 day=月初才需要前月；否則僅掃當月（加速）
    pats = [os.path.join(base, f"yyyymm={ym}", "**", "*.parquet")]
    if d.day == 1:
        prev = (d.replace(day=1) - datetime.timedelta(days=1))
        pats.append(os.path.join(base, f"yyyymm={prev.year:04d}{prev.month:02d}", "**", "*.parquet"))
    files = []
    for p in pats: files.extend(glob.glob(p, recursive=True))
    seen = set()
    for f in files:
        try:
            df = pd.read_parquet(f, columns=["date","stock_id"])
            if df.empty: continue
            s = df["date"].astype(str) == day
            if s.any():
                sy = df.loc[s, "stock_id"].astype(str).str.replace(".TW","", regex=False).tolist()
                seen.update(sy)
        except Exception:
            pass
    return seen, len(files)

def write_silver(df: pd.DataFrame, root: str, kind: str) -> int:
    if df is None or df.empty: return 0
    df = df.copy()
    df["yyyymm"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m")
    total = 0
    for ym, g in df.groupby("yyyymm"):
        outdir = os.path.join(root, "silver", "alpha", kind, f"yyyymm={ym}")
        os.makedirs(outdir, exist_ok=True)
        out = os.path.join(outdir, f"ing_{kind}_{ym}_{int(time.time()*1000)}.parquet")
        g.drop(columns=["yyyymm"], errors="ignore").to_parquet(out, index=False)
        total += len(g)
    return total

def load_pool(root="."):
    for p in [os.path.join(root,"configs","investable_universe.txt"),
              os.path.join(root,"universe.tw_all.txt")]:
        if os.path.exists(p):
            syms = []
            with open(p,"r",encoding="utf-8",errors="ignore") as fh:
                for line in fh:
                    x = line.strip().replace(".TW","")
                    if x and len(x)==4 and x.isdigit(): syms.append(x)
            return sorted(set(syms))
    return []

def chunked(seq, n):
    for i in range(0, len(seq), n): yield seq[i:i+n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", required=True)
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)   # 不含
    ap.add_argument("--datahub-root", default="datahub")
    ap.add_argument("--force", action="store_true", help="跳過覆蓋判斷，直接打 API")
    args = ap.parse_args()

    ds_list = []
    for token in args.datasets:
        for part in (token.split(",") if "," in token else [token]):
            ds_list.append(alias_to_dataset(part))
    ds_list = list(dict.fromkeys(ds_list))

    syms = []
    if args.symbols:
        for s in args.symbols:
            for p in (s.split(",") if "," in s else [s]):
                q = p.strip().replace(".TW","")
                if q and len(q)==4 and q.isdigit(): syms.append(q)
        syms = sorted(set(syms))
    if not syms: syms = load_pool(".")

    print("=== FinMind Backfill (API, Strict Fix + DayIndex) ===")
    print(f"Start={args.start} End={args.end} Universe={'TSE' if syms else 'N/A'}")
    print(f"Datasets={','.join([dataset_to_kind(d) for d in ds_list])}")
    mode = ("單股 指定 %d 檔" % len(syms)) if syms else "全市場（本地投資池）"
    print(f"Mode={mode}")
    qps = float(os.environ.get("FINMIND_QPS","1.5")); print(f"QPS={qps:.3f}")
    sleep_sec = max(0.0, 1.0/qps)
    t0 = (parse_date(args.end) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    totals = []
    for ds in ds_list:
        kind = dataset_to_kind(ds)
        rows_written = files_out = 0
        sink_dir = os.path.join(args.datahub_root,"silver","alpha",kind)

        if args.force:
            todo = list(syms)
            print(f"== Phase: {kind}  FORCE mode todo={len(todo)}")
        else:
            day_syms, fcnt = build_day_index(args.datahub_root, kind, t0)
            todo = [s for s in syms if s not in day_syms]
            print(f"== Phase: {kind}  index(day={t0}) files={fcnt} covered={len(day_syms)} todo={len(todo)}")

        for s in todo:
            try:
                df = http_get(ds, s, args.start, args.end)
                if df is not None and not df.empty:
                    w = write_silver(df, args.datahub_root, kind)
                    rows_written += w
                    files_out    += 1 if w>0 else 0
                time.sleep(sleep_sec)
            except Exception as e:
                print(f"[WARN] {kind} {s}: {e}", file=sys.stderr)

        print(f"OK {kind}: rows_written={rows_written} files_out={files_out}")
        totals.append(dict(dataset=kind, mode=mode, estcalls=len(todo), rows_written=rows_written, files_out=files_out, sink_dir=sink_dir))

    os.makedirs("metrics", exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    outcsv = os.path.join("metrics", f"ingest_summary_{ts}_finmind.csv")
    pd.DataFrame(totals).to_csv(outcsv, index=False, encoding="utf-8")
    print(f"=== Backfill Done ===  metrics: {os.path.abspath(outcsv)}")

if __name__ == "__main__":
    try:
        if hasattr(sys.stdout,"reconfigure"): sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    main()
