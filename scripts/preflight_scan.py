#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import re, pandas as pd
import pyarrow.parquet as pq

_DATE_FIELDS = ("date", "trade_date")
_TS_FIELDS = ("tsns", "ts")

def _norm_date(df: pd.DataFrame) -> pd.Series | None:
    for c in _DATE_FIELDS:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors='coerce', utc=True)
            if s.notna().any():
                return s.dt.tz_convert("Asia/Taipei").dt.normalize()
    if "tsns" in df.columns:
        s = pd.to_datetime(df["tsns"], unit="ns", errors='coerce', utc=True)
        return s.dt.tz_convert("Asia/Taipei").dt.normalize()
    if "ts" in df.columns:
        s = pd.to_datetime(df["ts"], unit="s", errors='coerce', utc=True)
        return s.dt.tz_convert("Asia/Taipei").dt.normalize()
    return None

def _iter_files(ds_dir: Path):
    pat = re.compile(r"^yyyymm=(\d{6})$")
    if not ds_dir.exists(): return
    for d in sorted(ds_dir.iterdir()):
        if d.is_dir() and pat.match(d.name):
            for f in d.glob("*.parquet"):
                yield f

def dataset_max_date(ds_path: Path):
    cols = list(_DATE_FIELDS)+list(_TS_FIELDS)
    mx = None
    for f in _iter_files(ds_path):
        try:
            t = pq.read_table(f, columns=[c for c in cols if c])
            s = _norm_date(t.to_pandas())
            if s is not None and len(s):
                cur = s.max()
                mx = cur if mx is None or cur>mx else mx
        except Exception:
            continue
    return mx

def scan_alpha_root(alpha_root: Path):
    names = ["prices","chip","dividend","per"]
    out=[]
    for n in names:
        p = alpha_root / n
        ex = p.exists()
        md = dataset_max_date(p) if ex else None
        out.append({"dataset": str(p), "exists": bool(ex), "max_date": md.strftime("%Y-%m-%d") if md is not None else None})
    return {"freshness": out}
