
# ingest_utils.py
from __future__ import annotations
import json, time, os, csv
from datetime import datetime
from typing import Dict, Any, List, Optional

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

class ProgressTracker:
    def __init__(self, state_path: str):
        self.state_path = state_path
        _ensure_dir(os.path.dirname(state_path) or ".")
        self.state = {}  # type: Dict[str, Any]
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception:
                self.state = {}

    def save(self) -> None:
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)

    def mark_dataset_date(self, dataset: str, date_str: str, ok: bool) -> None:
        ds = self.state.setdefault(dataset, {})
        dates = ds.setdefault("dates", {})
        dates[date_str] = {"ok": bool(ok), "ts": datetime.utcnow().isoformat() + "Z"}
        if ok:
            ds["last_success_date"] = date_str

    def mark_symbol(self, dataset: str, symbol: str, date_str: str, ok: bool) -> None:
        ds = self.state.setdefault(dataset, {})
        syms = ds.setdefault("symbols", {})
        s = syms.setdefault(symbol, {})
        s["last_success_date"] = date_str if ok else s.get("last_success_date", None)
        s.setdefault("history", []).append({"date": date_str, "ok": bool(ok), "ts": datetime.utcnow().isoformat()+"Z"})

class MetricsWriter:
    def __init__(self, metrics_dir: str):
        self.metrics_dir = metrics_dir
        _ensure_dir(metrics_dir)
        self.rows = []  # type: List[Dict[str, Any]]

    def add(self, **kwargs: Any) -> None:
        self.rows.append(kwargs)

    def flush_csv(self, tag: Optional[str] = None) -> str:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        name = f"ingest_summary_{ts}{('_'+tag) if tag else ''}.csv"
        path = os.path.join(self.metrics_dir, name)
        if self.rows:
            keys = sorted({k for r in self.rows for k in r.keys()})
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for r in self.rows:
                    w.writerow(r)
        return path

class SchemaValidator:
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema

    def validate_columns(self, df) -> List[str]:
        missing = []
        req = self.schema.get("required", [])
        for col in req:
            if col not in df.columns:
                missing.append(col)
        return missing

    def coerce_types(self, df):
        import pandas as pd
        types = self.schema.get("types", {})
        for col, typ in types.items():
            if col in df.columns:
                try:
                    if typ == "date":
                        df[col] = pd.to_datetime(df[col]).dt.date
                    elif typ == "int":
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    elif typ == "float":
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif typ == "string":
                        df[col] = df[col].astype("string")
                except Exception:
                    pass
        return df
