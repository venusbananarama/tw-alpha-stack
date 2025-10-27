# -*- coding: utf-8 -*-
import os, sys, json, time, argparse, datetime
from urllib import request, parse
from urllib.error import HTTPError, URLError
import pandas as pd

BASE  = os.environ.get("FINMIND_BASE_URL", "https://api.finmindtrade.com/api/v4/data")
TOKEN = (os.environ.get("FINMIND_TOKEN") or "").strip()
if not TOKEN:
    print("ERROR: FINMIND_TOKEN not set", file=sys.stderr); sys.exit(2)

def norm_ids(ids):
    out = []
    for s in ids or []:
        parts = s.split(",") if isinstance(s, str) and "," in s else [s]
        for p in parts:
            q = (p or "").strip()
            if not q:
                continue
            q = q.replace(".TW","").replace(".TWO","").replace(".TPEX","")
            out.append(q)
    return sorted(set(out))

def alias_to_list(tokens):
    parts = []
    for x in tokens or []:
        if isinstance(x, str) and "," in x:
            parts.extend([p.strip() for p in x.split(",") if p.strip()])
        else:
            parts.append(x)
    return parts

def http_get_one(dataset: str, data_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Robust FinMind fetcher for both KBar and non-KBar datasets.

    KBar tries the following shapes in order:
      A) stock_id + start_time/end_time (+ time_interval if provided)
      B) stock_id + date (+ time_interval if provided)
      C) data_id  + start_date
    Others:
      dataset + data_id + start_date + end_date (end is exclusive)
    """
    ds  = (dataset or "").lower()
    sid = (str(data_id) or "").strip().replace(".TW","").replace(".TWO","").replace(".TPEX","")
    headers = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

    def _call(params: dict):
        qs  = parse.urlencode(params)
        req = request.Request(f"{BASE}?{qs}", headers=headers)
        try:
            with request.urlopen(req, timeout=20) as r:
                obj = json.loads(r.read().decode("utf-8", errors="ignore"))
            return obj, None
        except HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            return None, (e.code, body, params)
        except URLError as e:
            return None, (0, f"URLError: {e.reason}", params)

    if "kbar" in ds:
        shapes = []
        interval = os.environ.get("FINMIND_KBAR_INTERVAL", "").strip()
        st = f"{start} 00:00:00"; et = f"{start} 23:59:59"

        # A) legacy form first (explicit intraday window)
        if interval:
            shapes.append({"dataset": dataset, "stock_id": sid, "start_time": st, "end_time": et, "time_interval": interval})
        shapes.append({"dataset": dataset, "stock_id": sid, "start_time": st, "end_time": et})

        # B) daily date form
        if interval:
            shapes.append({"dataset": dataset, "stock_id": sid, "date": start, "time_interval": interval})
        shapes.append({"dataset": dataset, "stock_id": sid, "date": start})

        # C) official data_id + start_date
        shapes.append({"dataset": dataset, "data_id": sid, "start_date": start})

        last_err = None
        for p in shapes:
            obj, err = _call(p)
            if not err:
                data = (obj or {}).get("data") or []
                # treat empty as success (some days have no kbar)
                if not data:
                    return pd.DataFrame()
                df = pd.DataFrame(data)
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                if "stock_id" in df.columns:
                    df["stock_id"] = df["stock_id"].astype(str)
                return df
            else:
                last_err = err
        code, body, p = last_err
        raise RuntimeError(f"KBar HTTP {code} for {sid} on {start}. Params={p} Detail={body[:500]}")
    else:
        obj, err = _call({"dataset": dataset, "data_id": sid, "start_date": start, "end_date": end})
        if err:
            code, body, p = err
            raise RuntimeError(f"{dataset} HTTP {code} for {sid}. Params={p} Detail={body[:500]}")
        data = (obj or {}).get("data") or []
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str)
        return df

def write_extra(df: pd.DataFrame, out_root: str, dataset: str) -> int:
    if df is None or df.empty:
        return 0
    gdf = df.copy()
    gdf["yyyymm"] = pd.to_datetime(gdf["date"], errors="coerce").dt.strftime("%Y%m")
    total = 0
    for ym, g in gdf.groupby("yyyymm"):
        outdir = os.path.join(out_root, "silver", "alpha", "extra", dataset, f"yyyymm={ym}")
        os.makedirs(outdir, exist_ok=True)
        out = os.path.join(outdir, f"ing_extra_{dataset}_{ym}_{int(time.time()*1000)}.parquet")
        g.drop(columns=["yyyymm"], errors="ignore").to_parquet(out, index=False)
        total += len(g)
    return total

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", required=True, help="FinMind datasets (comma or multi)")
    ap.add_argument("--ids",       nargs="+", required=True, help="symbols (comma or multi)")
    ap.add_argument("--date",      required=True, help="yyyy-MM-dd (single day)")
    ap.add_argument("--end",       required=False, help="yyyy-MM-dd (exclusive); default date+1")
    ap.add_argument("--out-root",  default="datahub")
    args = ap.parse_args()

    ds_list = alias_to_list(args.datasets)
    ids     = norm_ids(alias_to_list(args.ids))
    start   = datetime.date.fromisoformat(args.date).strftime("%Y-%m-%d")
    end_ex  = args.end or (datetime.date.fromisoformat(args.date) + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    rpm = float(os.environ.get("FINMIND_THROTTLE_RPM") or os.environ.get("FINMIND_QPS","10"))
    if rpm <= 2.0:
        rpm = max(6.0, rpm * 60.0)
    sleep_sec = max(0.0, 60.0 / rpm)

    print(f"== DateID extras == {start} → {end_ex} (end exclusive)  ids={len(ids)}  rpm≈{rpm:.1f}/min")
    totals = []
    for ds in ds_list:
        rows = 0; calls = 0
        for sid in ids:
            try:
                df = http_get_one(ds, sid, start, end_ex)
                if df is not None and not df.empty:
                    try:
                        di = pd.to_datetime(df['date'], errors='coerce')
                        df = df[di < pd.to_datetime(end_ex)]
                    except Exception:
                        pass
                    rows += write_extra(df, args.out_root, ds)
                calls += 1
                time.sleep(sleep_sec)
            except Exception as e:
                print(f"[WARN] {ds} {sid}: {e}", file=sys.stderr)
        print(f"OK {ds}: rows_written={rows} calls={calls}")
        totals.append((ds, rows, calls))
    print("DONE extras:", totals)

if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
