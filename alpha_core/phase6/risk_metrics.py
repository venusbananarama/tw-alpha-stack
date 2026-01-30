from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd


def _norm_symbol(value: object) -> str:
    s = str(value).strip()
    if s.isdigit() and len(s) < 4:
        return s.zfill(4)
    return s


def load_benchmark_returns(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"benchmark not found: {path}")
    if path.suffix.lower() in (".parquet", ".pq"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    if "date" not in df.columns or "ret" not in df.columns:
        raise ValueError("benchmark returns require columns: date, ret")
    out = df[["date", "ret"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ret"] = pd.to_numeric(out["ret"], errors="coerce")
    out = out[out["date"].notna() & out["ret"].notna()]
    return out.sort_values("date").reset_index(drop=True)


def _select_price_files(prices_path: Path, as_of: date, lookback_days: int) -> List[Path]:
    if prices_path.is_file():
        return [prices_path]
    if not prices_path.is_dir():
        return []
    start_date = as_of - timedelta(days=int(lookback_days))
    start_ym = start_date.strftime("%Y%m")
    as_of_ym = as_of.strftime("%Y%m")

    def _pick_partition_file(part_dir: Path, ym: str) -> Optional[Path]:
        data_path = part_dir / "data.parquet"
        if data_path.exists():
            return data_path
        preferred = part_dir / f"prices_{ym}_fromboss.parquet"
        if preferred.exists():
            return preferred
        candidates = sorted(part_dir.glob(f"prices_{ym}*.parquet"))
        candidates = [p for p in candidates if not p.name.startswith("ing_prices_")]
        if candidates:
            return candidates[0]
        candidates = sorted(part_dir.glob("*.parquet"))
        candidates = [p for p in candidates if not p.name.startswith("ing_prices_")]
        if candidates:
            return candidates[0]
        return None

    candidates: List[Tuple[str, Path]] = []
    for child in prices_path.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if not name.startswith("yyyymm="):
            continue
        ym = name.split("=", 1)[1]
        if len(ym) != 6 or not ym.isdigit():
            continue
        if ym < start_ym or ym > as_of_ym:
            continue
        pick = _pick_partition_file(child, ym)
        if pick is not None:
            candidates.append((ym, pick))
    return [p for _, p in sorted(candidates, key=lambda item: item[0])]


def load_price_returns(
    prices_path: Path,
    symbols: Iterable[str],
    as_of: date,
    max_window: int,
) -> pd.DataFrame:
    symbol_set = {_norm_symbol(s) for s in symbols}
    if not symbol_set:
        return pd.DataFrame()
    start_date = as_of - timedelta(days=int(max_window) * 2)
    files = _select_price_files(prices_path, as_of, lookback_days=max_window * 2)
    if not files:
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    selected_meta: List[Dict[str, str]] = []
    for path in files:
        selected_meta.append({"yyyymm": path.parent.name.split("=", 1)[-1], "file": path.name})
        df = pd.read_parquet(path)
        cols = [c for c in ["date", "symbol", "stock_id", "close"] if c in df.columns]
        if not cols:
            continue
        df = df[cols]
        has_symbol = "symbol" in df.columns
        has_stock_id = "stock_id" in df.columns
        if not has_symbol and not has_stock_id:
            continue
        if has_symbol:
            df["symbol"] = df["symbol"].astype(str).str.strip().apply(_norm_symbol)
        else:
            df["symbol"] = df["stock_id"].apply(_norm_symbol)
        df = df[df["symbol"].isin(symbol_set)]
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df[df["date"].notna() & df["close"].notna()]
        df = df[df["date"] <= as_of]
        df = df[df["date"] >= start_date]
        frames.append(df[["date", "symbol", "close"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["symbol", "date"])
    out["ret"] = out.groupby("symbol")["close"].pct_change()
    out = out.dropna(subset=["ret"])
    pivot = out.pivot(index="date", columns="symbol", values="ret").sort_index()
    pivot.attrs["price_partitions"] = selected_meta
    pivot.attrs["price_obs_count"] = int(len(pivot))
    return pivot


def compute_portfolio_returns(returns_df: pd.DataFrame, weights: Mapping[str, float]) -> pd.Series:
    if returns_df.empty:
        return pd.Series(dtype="float64")
    w = pd.Series(weights, dtype="float64")
    cols = [c for c in returns_df.columns if c in w.index]
    if not cols:
        return pd.Series(dtype="float64")
    r = returns_df[cols]
    w = w.reindex(cols).fillna(0.0)
    if r.empty:
        return pd.Series(dtype="float64")
    mask = r.notna()
    numerator = r.fillna(0.0).mul(w, axis=1).sum(axis=1)
    valid_weight = mask.mul(w, axis=1).sum(axis=1)
    port_ret = numerator.where(valid_weight != 0)
    port_ret = port_ret.replace([float("inf"), float("-inf")], pd.NA)
    return port_ret.dropna()


def compute_te_ir(
    returns_df: pd.DataFrame,
    weights: Mapping[str, float],
    bench_df: pd.DataFrame,
    windows: List[int],
    min_obs: int,
) -> Dict[str, object]:
    port_ret = compute_portfolio_returns(returns_df, weights)
    if port_ret.empty or bench_df.empty:
        return {
            "te": {},
            "ir": {},
            "active_return": {},
            "obs": {},
            "bench_last_date": None,
            "bench_obs": 0,
        }
    bench = bench_df.set_index("date")["ret"]
    combined = pd.concat([port_ret.rename("port"), bench.rename("bench")], axis=1, join="inner").dropna()
    if combined.empty:
        return {
            "te": {},
            "ir": {},
            "active_return": {},
            "obs": {},
            "bench_last_date": None,
            "bench_obs": int(len(bench_df)),
        }
    active = combined["port"] - combined["bench"]
    te: Dict[str, float] = {}
    ir: Dict[str, float] = {}
    active_return: Dict[str, float] = {}
    obs: Dict[str, int] = {}
    for w in windows:
        tail = active.tail(int(w))
        obs[str(w)] = int(len(tail))
        if len(tail) < int(min_obs):
            continue
        stdev = float(tail.std(ddof=1))
        te[str(w)] = float(stdev * (252.0**0.5))
        mean = float(tail.mean())
        active_return[str(w)] = float(mean * 252.0)
        if stdev > 0:
            ir[str(w)] = float((mean / stdev) * (252.0**0.5))
    bench_last = combined.index.max()
    return {
        "te": te,
        "ir": ir,
        "active_return": active_return,
        "obs": obs,
        "bench_last_date": bench_last.isoformat() if isinstance(bench_last, date) else None,
        "bench_obs": int(len(bench_df)),
    }
