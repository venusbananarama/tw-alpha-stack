from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

import pandas as pd

from .errors import IncompleteDayError, InputNotFoundError, SchemaValidationError


_TS_CANDIDATES = (
    "ts",
    "timestamp",
    "time",
    "event_ts",
    "trade_ts",
    "ts_event",
    "filled_at",
    "matched_at",
)
_SYMBOL_CANDIDATES = ("symbol", "ticker", "stock_id", "code", "instrument")
_PRICE_CANDIDATES = ("price", "px", "trade_price", "last_price")
_QTY_CANDIDATES = ("qty", "quantity", "size", "trade_qty", "volume")
_SIDE_CANDIDATES = ("side", "bs", "buy_sell", "direction", "sign")
_SYMBOL_STOPWORDS = {
    "trades",
    "trade",
    "fubon",
    "market",
    "bronze",
    "data",
    "tick",
    "ticks",
}
_FALLBACK_MIN_SYMBOLS = 1
_FALLBACK_MAX_FILES = 20
_FALLBACK_MAX_LINES = 200


def _normalize_fubon_trade_obj(obj: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Normalize fubon bronze trade envelope into schema-first columns:
    ts, symbol, price, qty.
    If obj["data"] is a dict, prefer data.time/price/size/symbol.
    """
    data = obj.get("data")
    src: Mapping[str, Any] = data if isinstance(data, dict) else obj

    symbol = (
        src.get("symbol")
        or obj.get("symbol")
        or obj.get("ticker")
        or obj.get("stock_id")
        or obj.get("code")
    )
    symbol = None if symbol is None else str(symbol).strip()

    ts_raw = (
        src.get("time")
        or src.get("ts")
        or src.get("timestamp")
        or obj.get("time")
        or obj.get("ts")
        or obj.get("timestamp")
    )

    price = src.get("price") or obj.get("price")
    qty = (
        src.get("size")
        or src.get("qty")
        or src.get("quantity")
        or obj.get("size")
        or obj.get("qty")
        or obj.get("quantity")
    )

    try:
        price = None if price is None else float(price)
    except Exception:
        price = None

    try:
        qty = None if qty is None else int(qty)
    except Exception:
        qty = None

    ts = None
    if ts_raw is not None:
        try:
            v = float(ts_raw)
            if not math.isfinite(v):
                raise ValueError("ts_raw is not finite")
            v_int = int(v)
            if v_int > 10**14:
                dt = pd.to_datetime(v_int, unit="us", utc=True)
            elif v_int > 10**11:
                dt = pd.to_datetime(v_int, unit="ms", utc=True)
            else:
                dt = pd.to_datetime(v_int, unit="s", utc=True)
            ts = dt.tz_convert("Asia/Taipei").tz_localize(None)
        except Exception:
            ts = None

    return {"ts": ts, "symbol": symbol, "price": price, "qty": qty}


def detect_incomplete_flag(bronze_day_dir: Path) -> bool:
    return (bronze_day_dir / "_INCOMPLETE").exists()


def _list_trade_files(bronze_day_dir: Path) -> List[Path]:
    files: List[Path] = []
    files.extend(sorted(bronze_day_dir.glob("*.jsonl")))
    files.extend(sorted(bronze_day_dir.glob("*.ndjson")))
    return sorted(files)


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        key = cand.lower()
        if key in cols:
            return cols[key]
    return None


def _normalize_side(value: Any) -> str:
    s = str(value).strip().upper()
    if s in ("B", "BUY", "1", "+1", "LONG"):
        return "BUY"
    if s in ("S", "SELL", "-1", "SHORT"):
        return "SELL"
    return s or "UNKNOWN"


def _parse_ts_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        vals = pd.to_numeric(series, errors="coerce")
        max_val = vals.max()
        if pd.isna(max_val):
            return pd.to_datetime(series, errors="coerce")
        if max_val > 1e12:
            return pd.to_datetime(vals, unit="ms", errors="coerce")
        if max_val > 1e10:
            return pd.to_datetime(vals, unit="ms", errors="coerce")
        if max_val > 1e9:
            return pd.to_datetime(vals, unit="s", errors="coerce")
        return pd.to_datetime(vals, errors="coerce")
    return pd.to_datetime(series, errors="coerce")


def load_bronze_trades(
    bronze_day_dir: Path,
    *,
    ignore_incomplete: bool,
    schema_hint: Mapping[str, str] | None = None,
    max_bad_lines: int = 20,
    max_bad_ratio: float = 0.01,
) -> pd.DataFrame:
    if not bronze_day_dir.exists():
        raise InputNotFoundError(f"bronze day dir not found: {bronze_day_dir}")

    if detect_incomplete_flag(bronze_day_dir) and not ignore_incomplete:
        raise IncompleteDayError(f"incomplete bronze day: {bronze_day_dir}")

    files = _list_trade_files(bronze_day_dir)
    if not files:
        raise InputNotFoundError(f"no bronze ndjson files: {bronze_day_dir}")

    rows: List[Dict[str, Any]] = []
    total_lines = 0
    bad_lines = 0
    for file_idx, path in enumerate(files):
        try:
            raw = path.read_text(encoding="utf-8").splitlines()
        except Exception:  # noqa: BLE001
            raw = []
        for line_idx, line in enumerate(raw, start=1):
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                bad_lines += 1
                continue
            if not isinstance(obj, dict):
                bad_lines += 1
                continue
            norm = _normalize_fubon_trade_obj(obj)
            norm["_source_file"] = path.name
            norm["_source_line"] = line_idx
            norm["_source_file_order"] = file_idx
            rows.append(norm)

    if total_lines > 0:
        ratio = bad_lines / max(total_lines, 1)
        if bad_lines > max_bad_lines or ratio > max_bad_ratio:
            raise SchemaValidationError(
                f"bronze parse failed: bad_lines={bad_lines} total={total_lines} ratio={ratio:.4f}"
            )

    df = pd.DataFrame(rows)
    df.attrs["bad_lines"] = bad_lines
    df.attrs["total_lines"] = total_lines
    if schema_hint:
        df.attrs["schema_hint"] = dict(schema_hint)
    return df


def canonicalize_bronze_trades(
    df_raw: pd.DataFrame,
    *,
    symbol: str | None = None,
    schema_hint: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=["ts", "symbol", "price", "qty", "side"])

    hint = dict(schema_hint or {})
    ts_col = hint.get("ts") or _pick_col(df_raw, _TS_CANDIDATES)
    sym_col = hint.get("symbol") or _pick_col(df_raw, _SYMBOL_CANDIDATES)
    price_col = hint.get("price") or _pick_col(df_raw, _PRICE_CANDIDATES)
    qty_col = hint.get("qty") or _pick_col(df_raw, _QTY_CANDIDATES)
    side_col = hint.get("side") or _pick_col(df_raw, _SIDE_CANDIDATES)

    if ts_col is None or sym_col is None or price_col is None or qty_col is None:
        raise SchemaValidationError(
            f"bronze schema missing required columns: ts={ts_col}, symbol={sym_col}, price={price_col}, qty={qty_col}"
        )

    out = pd.DataFrame()
    out["ts"] = _parse_ts_series(df_raw[ts_col])
    out["symbol"] = df_raw[sym_col].astype(str).str.strip()
    out["price"] = pd.to_numeric(df_raw[price_col], errors="coerce")
    out["qty"] = pd.to_numeric(df_raw[qty_col], errors="coerce")
    if side_col is not None and side_col in df_raw.columns:
        out["side"] = df_raw[side_col].apply(_normalize_side)
    else:
        out["side"] = "UNKNOWN"

    if symbol:
        out = out[out["symbol"] == str(symbol)]

    out["_source_file"] = df_raw.get("_source_file")
    out["_source_line"] = df_raw.get("_source_line")
    out["_source_file_order"] = df_raw.get("_source_file_order")

    out = out[out["ts"].notna()]
    out = out.sort_values(
        ["ts", "_source_file_order", "_source_line"],
        kind="mergesort",
    ).reset_index(drop=True)

    return out[["ts", "symbol", "price", "qty", "side"]]


def _infer_symbol_from_stem(stem: str) -> Optional[str]:
    if not stem:
        return None
    m = re.search(r"symbol[=_-]([A-Za-z0-9]+)", stem, flags=re.IGNORECASE)
    if m:
        cand = m.group(1)
        if re.fullmatch(r"\d{4,6}", cand):
            return cand
        return None
    tokens = re.split(r"[_\-.]", stem)
    tokens = [t for t in tokens if t and t.lower() not in _SYMBOL_STOPWORDS]
    if not tokens:
        return None
    for tok in tokens:
        if re.fullmatch(r"\d{4,6}", tok):
            return tok
    return None


def _extract_symbol_from_obj(obj: Mapping[str, Any]) -> Optional[str]:
    for key in _SYMBOL_CANDIDATES:
        if key in obj:
            val = obj.get(key)
            if val is None:
                continue
            sym = str(val).strip()
            if sym:
                return sym
    return None


def _fallback_symbols_from_files(files: List[Path]) -> Set[str]:
    symbols: Set[str] = set()
    for path in files[:_FALLBACK_MAX_FILES]:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:  # noqa: BLE001
            continue
        for line in lines[:_FALLBACK_MAX_LINES]:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            sym = _extract_symbol_from_obj(obj)
            if sym:
                symbols.add(sym)
        if len(symbols) >= _FALLBACK_MIN_SYMBOLS:
            break
    return symbols


def list_bronze_symbols(bronze_root: Path, as_of: str) -> Set[str]:
    bronze_day_dir = bronze_root / f"dt={as_of}"
    if not bronze_day_dir.exists():
        raise InputNotFoundError(f"bronze day dir not found: {bronze_day_dir}")
    files = _list_trade_files(bronze_day_dir)
    symbols: Set[str] = set()
    for path in files:
        sym = _infer_symbol_from_stem(path.stem)
        if sym:
            symbols.add(str(sym))
    if len(symbols) < _FALLBACK_MIN_SYMBOLS and files:
        symbols.update(_fallback_symbols_from_files(files))
    return symbols
