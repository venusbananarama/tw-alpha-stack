from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Materialize monthly price shards into a single daily panel parquet."
    )
    p.add_argument("--as-of", required=True, help="As-of date (YYYY-MM-DD).")
    p.add_argument(
        "--in-root",
        default="datahub/silver/alpha/prices",
        help="Input root for monthly price shards.",
    )
    p.add_argument(
        "--out",
        default="datahub/silver/alpha/prices_daily.parquet",
        help="Output parquet path.",
    )
    p.add_argument(
        "--max-months",
        type=int,
        default=36,
        help="Max recent months (inclusive) to load.",
    )
    p.add_argument(
        "--date-col",
        default=None,
        help="Override date column name (auto-detect if omitted).",
    )
    p.add_argument(
        "--symbol-col",
        default=None,
        help="Override symbol column name (auto-detect if omitted).",
    )
    p.add_argument(
        "--close-col",
        default=None,
        help="Override close column name (auto-detect if omitted).",
    )
    p.add_argument(
        "--volume-col",
        default=None,
        help="Override volume column name (auto-detect if omitted).",
    )
    return p.parse_args()


def _parse_as_of(value: str) -> date:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        raise SystemExit(f"Invalid --as-of date: {value!r}")
    return ts.date()


def _month_keys(as_of: date, max_months: int) -> List[str]:
    if max_months <= 0:
        raise SystemExit("--max-months must be positive.")
    y = as_of.year
    m = as_of.month
    keys: List[str] = []
    for _ in range(max_months):
        keys.append(f"{y:04d}{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return keys


def _resolve_named(columns: List[str], name: str) -> Optional[str]:
    if name in columns:
        return name
    cols = {c.lower(): c for c in columns}
    return cols.get(name.lower())


def _pick_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in columns}
    for c in candidates:
        key = c.lower()
        if key in cols:
            return cols[key]
    return None


def _pick_required(
    columns: List[str], explicit: Optional[str], candidates: List[str]
) -> Optional[str]:
    if explicit:
        return _resolve_named(columns, explicit)
    return _pick_col(columns, candidates)


def _pick_optional(
    columns: List[str], explicit: Optional[str], candidates: List[str], label: str
) -> Optional[str]:
    if explicit:
        col = _resolve_named(columns, explicit)
        if not col:
            raise SystemExit(f"{label} column not found: {explicit!r}")
        return col
    return _pick_col(columns, candidates)


def _get_parquet_columns(
    path: Path,
) -> Tuple[Optional[List[str]], Optional[pd.DataFrame]]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        pq = None

    if pq is not None:
        try:
            pf = pq.ParquetFile(path)
            return list(pf.schema.names), None
        except Exception:
            pass

    try:
        df = pd.read_parquet(path)
    except Exception:
        return None, None
    return list(df.columns), df


def _read_parquet(
    path: Path, columns: List[str], preloaded: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if preloaded is not None:
        if not columns:
            return preloaded
        missing = [c for c in columns if c not in preloaded.columns]
        if missing:
            raise ValueError(f"Missing columns in preloaded data: {missing}")
        return preloaded[columns].copy()
    if not columns:
        return pd.read_parquet(path)
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception:
        return pd.read_parquet(path)


def _normalize_frame(df: pd.DataFrame, col_map: Dict[str, Optional[str]]) -> pd.DataFrame:
    required = ["date", "symbol", "close"]
    for key in required:
        src = col_map.get(key)
        if not src or src not in df.columns:
            return pd.DataFrame()

    data: Dict[str, pd.Series] = {}
    for out_col, src_col in col_map.items():
        if src_col is None:
            continue
        if src_col in df.columns:
            data[out_col] = df[src_col]

    out = pd.DataFrame(data)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["date"].notna()]
    out["symbol"] = out["symbol"].astype(str)
    for col in ["close", "open", "high", "low", "volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def main() -> None:
    args = _parse_args()
    as_of = _parse_as_of(args.as_of)
    as_of_ts = pd.Timestamp(as_of)
    months = _month_keys(as_of, args.max_months)

    repo_root = Path(__file__).resolve().parents[1]
    in_root = Path(args.in_root)
    if not in_root.is_absolute():
        in_root = repo_root / in_root
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = repo_root / out_path

    if not in_root.exists():
        raise SystemExit(f"Input root not found: {in_root}")

    month_dirs: List[Path] = []
    files: List[Path] = []
    for ym in months:
        d = in_root / f"yyyymm={ym}"
        if d.exists() and d.is_dir():
            month_dirs.append(d)
            files.extend(sorted(d.glob("*.parquet")))

    if not files:
        raise SystemExit(f"No parquet files found under: {in_root}")

    print(
        f"[INFO] as_of={as_of.isoformat()} months={len(month_dirs)} files={len(files)}"
    )

    frames: List[pd.DataFrame] = []
    read_files = 0
    skipped_files = 0
    skip_reasons: Dict[str, int] = {
        "missing_date": 0,
        "missing_symbol": 0,
        "missing_close": 0,
    }
    skipped_samples: List[str] = []
    for p in files:
        cols, preloaded = _get_parquet_columns(p)
        if not cols:
            skipped_files += 1
            skip_reasons["read_error"] = skip_reasons.get("read_error", 0) + 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        date_col = _pick_required(cols, args.date_col, ["date", "trade_date", "datetime"])
        if not date_col:
            skipped_files += 1
            skip_reasons["missing_date"] += 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        symbol_col = _pick_required(
            cols, args.symbol_col, ["symbol", "stock_id", "ticker", "code"]
        )
        if not symbol_col:
            skipped_files += 1
            skip_reasons["missing_symbol"] += 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        close_col = _pick_required(
            cols, args.close_col, ["adj_close", "close", "price", "last", "px_close"]
        )
        if not close_col:
            skipped_files += 1
            skip_reasons["missing_close"] += 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        volume_col = _pick_optional(
            cols,
            args.volume_col,
            ["Trading_Volume", "volume", "vol", "shares"],
            "volume",
        )
        open_col = _pick_col(cols, ["open"])
        high_col = _pick_col(cols, ["high", "max"])
        low_col = _pick_col(cols, ["low", "min"])

        col_map = {
            "date": date_col,
            "symbol": symbol_col,
            "close": close_col,
            "open": open_col,
            "high": high_col,
            "low": low_col,
            "volume": volume_col,
        }

        use_cols = sorted({c for c in col_map.values() if c})
        try:
            raw = _read_parquet(p, use_cols, preloaded)
        except Exception:
            skipped_files += 1
            skip_reasons["read_error"] = skip_reasons.get("read_error", 0) + 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        frame = _normalize_frame(raw, col_map)
        if frame.empty:
            skipped_files += 1
            skip_reasons["read_error"] = skip_reasons.get("read_error", 0) + 1
            if len(skipped_samples) < 5:
                skipped_samples.append(str(p))
            continue

        frame = frame[frame["date"] <= as_of_ts]
        frames.append(frame)
        read_files += 1

    if frames:
        panel = pd.concat(frames, ignore_index=True)
    else:
        panel = pd.DataFrame(columns=["date", "symbol", "close"])

    panel = panel.drop_duplicates(subset=["date", "symbol"], keep="last")
    panel = panel.sort_values(["date", "symbol"]).reset_index(drop=True)

    out_cols = [
        c
        for c in ["date", "symbol", "close", "open", "high", "low", "volume"]
        if c in panel.columns
    ]
    panel = panel[out_cols]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)

    if len(panel):
        print(
            f"[INFO] wrote rows={len(panel):,} date_range={panel['date'].min().date()}..{panel['date'].max().date()}"
        )
    else:
        print("[WARN] wrote empty panel.")
    print(f"[INFO] output={out_path}")
    print(f"[INFO] read_files={read_files}, skipped_files={skipped_files}")
    print(f"[INFO] skip_reasons={skip_reasons}")
    if skipped_samples:
        print("[INFO] skipped_samples:")
        for s in skipped_samples:
            print(f"  - {s}")


if __name__ == "__main__":
    main()
