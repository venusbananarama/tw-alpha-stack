# -*- coding: utf-8 -*-
import os, time, sys, math, json, textwrap, logging
from typing import Dict, List, Optional, Tuple
import datetime as dt

import pandas as pd
import requests

BASE_URL = "https://api.finmindtrade.com/api/v4/data"

def get_token() -> str:
    tok = os.environ.get("FINMIND_TOKEN") or os.environ.get("FINMINDTOKEN")
    if not tok:
        raise RuntimeError("FINMIND_TOKEN not set in environment.")
    return tok

def session_with_retries(max_retries: int = 3, timeout: int = 30) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "AlphaCity-FinMind/1.0"})
    s.request = _wrap_request_with_retries(s.request, max_retries=max_retries, timeout=timeout)  # type: ignore
    return s

def _wrap_request_with_retries(orig, max_retries: int, timeout: int):
    def wrapped(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        backoff = 1.0
        last_exc = None
        for i in range(max_retries):
            try:
                resp = orig(method, url, **kwargs)
                if resp.status_code >= 500:
                    raise RuntimeError(f"Server error {resp.status_code}: {resp.text[:200]}")
                return resp
            except Exception as e:
                last_exc = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
        raise last_exc  # type: ignore
    return wrapped

def fetch_dataset(dataset: str, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
    token = get_token()
    s = session_with_retries()
    params = {
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date,
        "token": token,
    }
    params.update(kwargs or {})
    r = s.get(BASE_URL, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
    obj = r.json()
    data = obj.get("data", [])
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response shape: {obj.keys()}")
    df = pd.DataFrame(data)
    if not df.empty:
        # Normalize common date fields
        for c in ("date", "trade_date", "Time"):
            if c in df.columns:
                try:
                    df[c] = pd.to_datetime(df[c])
                except Exception:
                    pass
    return df

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def write_parquet_part(df: pd.DataFrame, out_dir: str, dataset: str, part_name: str):
    ensure_dir(out_dir)
    path = os.path.join(out_dir, f"{dataset}__{part_name}.parquet")
    if df is None or df.empty:
        logging.info("Empty frame, skip write: %s", path)
        return
    df.to_parquet(path, index=False)
    print(f"[WRITE] {path} rows={len(df)}")

def month_chunks(start: str, end: str) -> List[Tuple[str,str]]:
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)
    out = []
    cur = dt.date(s.year, s.month, 1)
    while cur <= e:
        nxt_y = cur.year + (cur.month // 12)
        nxt_m = (cur.month % 12) + 1
        nxt = dt.date(nxt_y, nxt_m, 1) - dt.timedelta(days=1)
        if nxt > e: nxt = e
        out.append((cur.isoformat(), nxt.isoformat()))
        cur = dt.date(nxt_y, nxt_m, 1)
    return out

def load_groups(cfg_path: str) -> Dict[str, List[str]]:
    import yaml
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("groups", {})
