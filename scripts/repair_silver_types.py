#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
repair_silver_types.py (supports nested schema)
- Supports dataset schema in two styles:
  A) flat: {col: type}
  B) nested: {required: [...], types: {col: type}}
- Coerces types, dedups by PK (date,symbol|series), atomic replace, optional backup.
"""
from __future__ import annotations

import argparse, sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd

try:
    import yaml
except Exception:
    yaml = None

def load_schema(schema_path: Path) -> Dict[str, dict]:
    if yaml is None:
        print("❌ 未安裝 PyYAML：pip install pyyaml", file=sys.stderr); raise SystemExit(2)
    data = yaml.safe_load(schema_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        print("❌ schema 格式錯誤", file=sys.stderr); raise SystemExit(2)
    return data

def normalize_dataset_schema(s: dict) -> Tuple[Dict[str,str], List[str]]:
    if "types" in s or "required" in s:
        t = dict(s.get("types", {}))
        req = list(s.get("required", []))
        return {k:str(v) for k,v in t.items()}, req
    flat = {k:str(v) for k,v in s.items()}
    return flat, list(flat.keys())

def coerce(df: pd.DataFrame, types_map: Dict[str,str]) -> pd.DataFrame:
    out = df.copy()
    for col, typ in types_map.items():
        if col not in out.columns: continue
        try:
            if   typ=="date": out[col] = pd.to_datetime(out[col])
            elif typ in ("int","int64"): out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif typ in ("float","float64","double"): out[col] = pd.to_numeric(out[col], errors="coerce")
            elif typ in ("string","str","category"): out[col] = out[col].astype("string")
        except Exception:
            pass
    return out

def primary_keys(cols: List[str]) -> List[str]:
    if "symbol" in cols: return ["date","symbol"]
    if "series" in cols: return ["date","series"]
    return ["date"]

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--schema-path", required=True)
    ap.add_argument("--datasets", nargs="*", default=None)
    ap.add_argument("--backup", action="store_true")
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args(argv)

    root = Path(args.datahub_root)
    silver = root / "silver" / "alpha"
    schema_map_raw = load_schema(Path(args.schema_path))
    targets = list(schema_map_raw.keys()) if not args.datasets else [d for d in args.datasets if d in schema_map_raw]

    any_fail = False
    for dset in targets:
        ddir = silver / dset
        if not ddir.exists():
            print(f"⚠️ 跳過 {dset}（沒有資料夾）"); continue
        files = sorted(f for f in ddir.rglob("*.parquet") if "_bak" not in f.parts and "_old" not in f.parts)
        if not files:
            print(f"⚠️ 跳過 {dset}（沒有 parquet）"); continue

        tmap, _req = normalize_dataset_schema(schema_map_raw[dset])
        print(f"=== 修復 {dset}：{len(files)} 檔 ===")
        for f in files:
            try:
                df = pd.read_parquet(f)
                df = coerce(df, tmap)
                keys = primary_keys(list(df.columns))
                df = df.drop_duplicates(subset=keys, keep="last")

                if args.backup:
                    bak = f.parent / "_bak"
                    bak.mkdir(parents=True, exist_ok=True)
                    (bak / f.name).write_bytes(f.read_bytes())

                tmp = f.with_suffix(".parquet.tmp")
                df.to_parquet(tmp, index=False)
                tmp.replace(f)
                print(f"  ✅ {f.name} rows={len(df)}")
            except Exception as e:
                any_fail = True
                print(f"  ❌ {f.name} -> {e}")
    if args.strict and any_fail:
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
