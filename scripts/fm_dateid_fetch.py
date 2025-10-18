# -*- coding: utf-8 -*-
"""
fm_dateid_fetch.py (hardened for KBar)
- 通用 Date+ID 抓取（FinMind v4 /api/v4/data）
- KBar 特別處理：
  * 自動帶入 time_interval（ENV: FINMIND_KBAR_INTERVAL，預設 1 分）
  * 區間模式(start_date/end_date)失敗 → 單日(date) fallback 逐日抓取
- 落地：datahub/silver/alpha/extra/<Dataset>/yyyymm=YYYYMM/*.parquet
"""
import os, sys, time, argparse, pathlib
from datetime import datetime, timedelta
import requests
import pandas as pd

API_URL = "https://api.finmindtrade.com/api/v4/data"

def _ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)

def _http(params: dict, timeout: int, max_retries: int) -> dict:
    last = None
    for i in range(max_retries + 1):
        r = requests.get(API_URL, params=params, timeout=timeout)
        last = (r.status_code, r.text[:200])
        if r.ok:
            try:
                j = r.json()
            except Exception:
                j = {}
            # v4: {"status":200,"data":[...]} or {"success":true,"data":[...]}
            ok = (j.get("status") == 200) or (j.get("success") is True)
            if ok:
                return j
        time.sleep(min(2 * (i + 1), 10))
    raise RuntimeError(f"http fail: {last}")

def _norm_date(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    # 常見日期欄位
    for c in ("date","trading_date","report_date","month"):
        if c in x.columns:
            try:
                x["date"] = pd.to_datetime(x[c], errors="coerce")
            except Exception:
                pass
            break
    if "date" not in x.columns:
        if "time" in x.columns:  # KBar 可能是時間戳 -> 取日期
            try:
                t = pd.to_datetime(x["time"], errors="coerce")
                x["date"] = pd.to_datetime(t.dt.date)
            except Exception:
                pass
    if "date" not in x.columns:
        return pd.DataFrame()
    return x.dropna(subset=["date"])

def _write(df: pd.DataFrame, root: pathlib.Path, dataset: str, symbol: str, start: str, end: str) -> int:
    if df is None or df.empty:
        return 0
    x = _norm_date(df)
    if x.empty:
        return 0
    out_root = root / "silver" / "alpha" / "extra" / dataset
    x["_yyyymm"] = x["date"].dt.strftime("%Y%m")
    n = 0
    for yyyymm, part in x.groupby("_yyyymm", dropna=False):
        dest = out_root / f"yyyymm={yyyymm if isinstance(yyyymm,str) else 'unknown'}"
        _ensure_dir(dest)
        fn = f"{dataset}_{symbol}_{yyyymm}.parquet"
        part.drop(columns=["_yyyymm"], errors="ignore").to_parquet(dest / fn, index=False)
        n += len(part)
    return n

def _kbar_fetch(id_key: str, sym: str, start: str, end_excl: str, token: str, timeout: int, max_retries: int) -> pd.DataFrame:
    """先試「區間模式」；失敗/空 → 逐日 fallback（date 模式）。"""
    interval = os.getenv("FINMIND_KBAR_INTERVAL", "1")
    # 1) 區間模式（end_excl 的前一天做 end_inclusive）
    end_inclusive = (datetime.strptime(end_excl, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {"dataset": "TaiwanStockKBar", id_key: sym, "start_date": start, "end_date": end_inclusive, "time_interval": interval}
    if token: params["token"] = token
    try:
        j = _http(params, timeout, max_retries)
        df = pd.DataFrame(j.get("data", []))
        if not df.empty:
            return df
    except Exception:
        pass  # 進 fallback

    # 2) 單日 fallback（date 模式；逐日拼接）
    all_parts = []
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end_excl, "%Y-%m-%d")
    d = s
    while d < e:
        day = d.strftime("%Y-%m-%d")
        p = {"dataset": "TaiwanStockKBar", id_key: sym, "date": day, "time_interval": interval}
        if token: p["token"] = token
        try:
            j = _http(p, timeout, max_retries)
            part = pd.DataFrame(j.get("data", []))
            if not part.empty:
                all_parts.append(part)
        except Exception:
            pass
        d += timedelta(days=1)
        time.sleep(0.4)  # 溫和節流
    if all_parts:
        return pd.concat(all_parts, ignore_index=True)
    return pd.DataFrame()

def fetch_once(dataset: str, id_key: str, sym: str, start: str, end_excl: str, token: str, timeout: int, max_retries: int) -> pd.DataFrame:
    if dataset == "TaiwanStockKBar":
        return _kbar_fetch(id_key, sym, start, end_excl, token, timeout, max_retries)
    params = {"dataset": dataset, id_key: sym, "start_date": start, "end_date": end_excl}  # end_excl 由 orchestrator 處理 +1d
    if token: params["token"] = token
    j = _http(params, timeout, max_retries)
    return pd.DataFrame(j.get("data", []))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--id-key", choices=["data_id","stock_id"], required=True)
    ap.add_argument("--symbols", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)   # 已是 +1d 後的「不含」終點
    ap.add_argument("--datahub-root", default="datahub")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--rpm", type=int, default=int(os.getenv("FINMIND_THROTTLE_RPM","12")))
    args = ap.parse_args()

    token = os.getenv("FINMIND_TOKEN","")
    root = pathlib.Path(args.datahub_root)
    syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not syms:
        print("No symbols provided.", file=sys.stderr); sys.exit(2)

    # 節流
    rpm = max(1, args.rpm); pause = 60.0 / rpm
    total_rows = 0
    for i, sym in enumerate(syms, 1):
        try:
            df = fetch_once(args.dataset, args.id_key, sym, args.start, args.end, token, args.timeout, args.max_retries)
            n = _write(df, root, args.dataset, sym, args.start, args.end)
            print(f"OK {args.dataset} {sym}: rows={len(df)} files={1 if n>0 else 0}")
            total_rows += len(df)
        except Exception as e:
            print(f"FAIL {args.dataset} {sym}: {e}", file=sys.stderr)
            pass
        if i < len(syms):
            time.sleep(pause)
    print(f"DONE {args.dataset}: symbols={len(syms)} total_rows={total_rows}")
if __name__ == "__main__":
    main()

