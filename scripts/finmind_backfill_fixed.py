#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
finmind_backfill_fixed.py

Drop-in replacement for scripts/finmind_backfill.py with the following fixes:
1) Safe schema loading (PyYAML optional; guard when missing).
2) Dataset alias mapping (e.g., "TaiwanStockPrice" -> "prices").
3) Silver layer monthly partitioning (group by date's YYYYMM).
4) Unique filenames using UUID to avoid collisions under concurrency.
5) Thread-safe progress/metrics writes via a shared threading.Lock().

This file keeps the same CLI interface expected by the PowerShell wrappers:
  --start, --end, --datasets, --universe, --symbols, --workers, --qps,
  --datahub-root, --datasets-yaml (optional).

NOTE:
- Fetchers here generate small fake frames for smoke tests. Replace _fake_* with
  real FinMind API calls when wiring production.
- Global QPS is still "per worker". If you want global throttling, put a shared
  rate limiter.
"""
from __future__ import annotations

import argparse
import sys
import os
import threading
import queue
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# Optional PyYAML (only needed when using --datasets-yaml or schemas)
try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # sentinel

# Project layout:
#   <project_root>/scripts/finmind_backfill_fixed.py (this file)
#   <project_root>/schemas/datasets_schema.yaml
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# -------- Alias mapping -------------
ALIASES: Dict[str, str] = {
    "TaiwanStockPrice": "prices",
    # Add more FinMind -> logical dataset mappings here:
    # "TaiwanStockInstitutionalInvestorsBuySell": "chip",
    # "SomethingMacro": "macro_others",
}

def _normalize_dataset_name(name: str) -> str:
    return ALIASES.get(name, name)

# --------- Ingest utils (import from your repo) ---------
# Expecting scripts/ingest_utils.py in sys.path (same folder as this file)
sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from ingest_utils import ensure_dir, ProgressTracker, MetricsWriter
except Exception as e:  # Fallback minimal helpers if import fails (for local testing)
    def ensure_dir(p: Path) -> None:
        p.mkdir(parents=True, exist_ok=True)
    class ProgressTracker:
        def __init__(self, path: Path):
            self.path = path
            self.data = {"symbols": {}, "datasets": {}, "last_updated": None}
        def mark_symbol(self, date: str, symbol: str, dataset: str, status: str) -> None:
            self.data["symbols"].setdefault(symbol, {})[f"{dataset}:{date}"] = status
            self.data["last_updated"] = datetime.utcnow().isoformat()
        def mark_dataset_date(self, date: str, dataset: str, status: str) -> None:
            self.data["datasets"].setdefault(dataset, {})[date] = status
            self.data["last_updated"] = datetime.utcnow().isoformat()
        def save(self) -> None:
            ensure_dir(self.path.parent)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
    class MetricsWriter:
        def __init__(self, out_csv: Path):
            self.out_csv = out_csv
            self.rows = []
        def add(self, **kwargs):
            self.rows.append(kwargs)
        def flush_csv(self):
            ensure_dir(self.out_csv.parent)
            df = pd.DataFrame(self.rows)
            if len(df):
                if self.out_csv.exists():
                    df0 = pd.read_csv(self.out_csv)
                    df = pd.concat([df0, df], ignore_index=True)
                df.to_csv(self.out_csv, index=False)

# --------- Schema validator helpers ---------
class SchemaValidator:
    def __init__(self, schema: Dict[str, str]):
        self.schema = schema or {}

    def coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        for col, typ in self.schema.items():
            if col not in out.columns:
                continue
            if typ == "date":
                out[col] = pd.to_datetime(out[col]).dt.date.astype("datetime64[ns]")
            elif typ in ("int", "int64"):
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif typ in ("float", "float64", "double"):
                out[col] = pd.to_numeric(out[col], errors="coerce")
            elif typ in ("string", "str", "category"):
                out[col] = out[col].astype("string")
        return out

    def validate_columns(self, df: pd.DataFrame) -> List[str]:
        required = list(self.schema.keys())
        missing = [c for c in required if c not in df.columns]
        return missing

# --------- Paths ---------
def raw_path(root: Path, dataset: str, yyyymm: str) -> Path:
    return root / "raw" / "finmind" / dataset / yyyymm

def silver_path(root: Path, dataset: str, yyyymm: str) -> Path:
    return root / "silver" / "alpha" / dataset / yyyymm

# --------- Fake fetchers (replace with real API integration) ---------
def _fake_prices(date: str, symbols: Optional[List[str]]) -> pd.DataFrame:
    rng = np.random.default_rng(abs(hash((date, "prices"))) % (2**32))
    if symbols:
        rows = []
        for s in symbols:
            o = rng.uniform(50, 200)
            c = o * rng.uniform(0.95, 1.05)
            h = max(o, c) * rng.uniform(1.0, 1.03)
            l = min(o, c) * rng.uniform(0.97, 1.0)
            v = rng.integers(1000, 100000)
            rows.append([date, s, o, h, l, c, v])
        return pd.DataFrame(rows, columns=["date", "symbol", "open", "high", "low", "close", "volume"])
    # if no symbols, return empty
    return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])

def _fake_chip(date: str, symbols: Optional[List[str]]) -> pd.DataFrame:
    rng = np.random.default_rng(abs(hash((date, "chip"))) % (2**32))
    if symbols:
        rows = []
        for s in symbols:
            rows.append([date, s, rng.integers(-5000, 5000), rng.integers(-3000, 3000), rng.integers(-2000, 2000)])
        return pd.DataFrame(rows, columns=["date", "symbol", "foreign_net", "trust_net", "dealer_net"])
    return pd.DataFrame(columns=["date", "symbol", "foreign_net", "trust_net", "dealer_net"])

def _fake_macro_others(date: str, series_list: Optional[List[str]]) -> pd.DataFrame:
    # Example: just synthesize one series
    rng = np.random.default_rng(abs(hash((date, "macro_others"))) % (2**32))
    return pd.DataFrame([[date, "TW_USD_EXRATE", rng.uniform(28, 34)]], columns=["date", "series", "value"])

def fetch_dataset(dataset: str, date: str, symbols: Optional[List[str]]) -> pd.DataFrame:
    dataset = _normalize_dataset_name(dataset)
    if dataset == "prices":
        return _fake_prices(date, symbols)
    elif dataset == "chip":
        return _fake_chip(date, symbols)
    elif dataset == "macro_others":
        return _fake_macro_others(date, None)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")

# --------- Writers ---------
def _unique_file(prefix: str = "batch") -> str:
    return f"{prefix}-{int(time.time()*1000)}-{uuid.uuid4().hex}.parquet"

def write_raw(root: Path, dataset: str, df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    yyyymm = pd.to_datetime(df["date"]).dt.strftime("%Y%m").iloc[0]
    out_dir = raw_path(root, _normalize_dataset_name(dataset), yyyymm)
    ensure_dir(out_dir)
    out_file = out_dir / _unique_file("raw")
    df.to_parquet(out_file, index=False)
    return str(out_file)

def write_silver(root: Path, dataset: str, df: pd.DataFrame, schema_map: Dict[str, Dict[str, str]]) -> List[str]:
    """Validate, dedupe primary keys, and write monthly partitions."""
    out_files: List[str] = []
    if df is None or df.empty:
        return out_files
    dataset = _normalize_dataset_name(dataset)
    # Schema validation & coercion
    if dataset in schema_map and schema_map[dataset]:
        validator = SchemaValidator(schema_map[dataset])
        df = validator.coerce_types(df)
        missing = validator.validate_columns(df)
        if missing:
            raise ValueError(f"缺少必要欄位: {missing}")

    # Dedupe by primary keys
    keys = ["date", "symbol"] if "symbol" in df.columns else (["date", "series"] if "series" in df.columns else ["date"])
    df = df.drop_duplicates(subset=keys, keep="last")

    # Monthly partitioning
    df["_yyyymm"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m")
    for yyyymm, g in df.groupby("_yyyymm", as_index=False):
        out_dir = silver_path(root, dataset, str(yyyymm))
        ensure_dir(out_dir)
        out_file = out_dir / _unique_file("silver")
        g.drop(columns=["_yyyymm"]).to_parquet(out_file, index=False)
        out_files.append(str(out_file))
    return out_files

# --------- Worker ---------
def _worker_loop(q: "queue.Queue[Tuple[str,str,Optional[List[str]]]]",
                 lock: threading.Lock,
                 datahub_root: Path,
                 schema_map: Dict[str, Dict[str, str]],
                 prog: ProgressTracker,
                 metrics: MetricsWriter,
                 qps: float):
    while True:
        try:
            dataset, date, symbols = q.get(timeout=1.0)
        except queue.Empty:
            return
        t0 = time.time()
        status = "ok"
        n_rows = 0
        try:
            df = fetch_dataset(dataset, date, symbols)
            n_rows = 0 if df is None else len(df)
            write_raw(datahub_root, dataset, df)
            write_silver(datahub_root, dataset, df, schema_map)
        except Exception as e:
            status = f"error:{type(e).__name__}"
        finally:
            with lock:
                if symbols:
                    for s in symbols:
                        prog.mark_symbol(date, s, _normalize_dataset_name(dataset), status)
                else:
                    prog.mark_dataset_date(date, _normalize_dataset_name(dataset), status)
                prog.save()
                metrics.add(ts=datetime.utcnow().isoformat(),
                            dataset=_normalize_dataset_name(dataset),
                            date=date, rows=n_rows, status=status, elapsed=time.time()-t0)
                metrics.flush_csv()
            # naive per-worker throttle
            if qps and qps > 0:
                time.sleep(1.0 / qps)
            q.task_done()

# --------- CLI & main ---------
def _date_range(start: str, end: str) -> List[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    out = []
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out

def load_dataset_schema_map() -> Dict[str, Dict[str, str]]:
    schema_map: Dict[str, Dict[str, str]] = {}
    schema_path = PROJECT_ROOT / "schemas" / "datasets_schema.yaml"
    if schema_path.exists() and yaml is not None:
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            # Expect form: {prices: {date: 'date', symbol: 'string', ...}, ...}
            if isinstance(data, dict):
                schema_map = {k: (v or {}) for k, v in data.items()}
        except Exception:
            # fail soft: no schema
            schema_map = {}
    else:
        # skip if no yaml or no file
        schema_map = {}
    return schema_map

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--datasets", nargs="+", required=True, help="Logical dataset names or aliases")
    p.add_argument("--universe", default="TSE", help="Universe tag (for logging only here)")
    p.add_argument("--symbols", nargs="*", default=None, help="Symbols list; if omitted, dataset-level fetch is attempted")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--qps", type=float, default=1.0, help="Per-worker throttling")
    p.add_argument("--datahub-root", default=str(PROJECT_ROOT / "datahub"))
    p.add_argument("--datasets-yaml", default=None, help="Optional datasets mapping YAML (not used in fake mode)")
    args = p.parse_args(argv)

    datahub_root = Path(args.datahub_root)
    ensure_dir(datahub_root)

    dates = _date_range(args.start, args.end)
    datasets = [ _normalize_dataset_name(d) for d in args.datasets ]

    # Load schema map (safe)
    schema_map = load_dataset_schema_map()

    # Progress/Metrics
    prog = ProgressTracker(datahub_root / "_progress" / "backfill_progress.json")
    metrics = MetricsWriter(datahub_root / "_metrics" / "backfill_metrics.csv")

    # Task queue
    q: "queue.Queue[Tuple[str,str,Optional[List[str]]]]" = queue.Queue()
    for dset in datasets:
        for d in dates:
            # For simplicity: pass whole symbols list; real impl might split per-symbol
            q.put((dset, d, args.symbols))

    lock = threading.Lock()
    threads: List[threading.Thread] = []
    for _ in range(max(1, int(args.workers))):
        t = threading.Thread(target=_worker_loop,
                             args=(q, lock, datahub_root, schema_map, prog, metrics, float(args.qps)),
                             daemon=True)
        t.start()
        threads.append(t)

    # Wait
    q.join()
    # Threads exit when queue empty
    for t in threads:
        t.join(timeout=0.1)

    print(f"=== Backfill Done === rows_written≈ see metrics.csv at {metrics.out_csv}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
