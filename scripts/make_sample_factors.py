from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Materialize factor shards into a single sample factors parquet."
    )
    p.add_argument("--factor-id", default="adv_20d", help="Factor id to materialize.")
    p.add_argument("--root", default=".", help="Repo root path.")
    p.add_argument("--as-of", required=True, help="As-of date (YYYY-MM-DD).")
    p.add_argument(
        "--out",
        default="datahub/silver/alpha/factor/sample_factors.parquet",
        help="Output parquet path.",
    )
    p.add_argument(
        "--max-months",
        type=int,
        default=36,
        help="Max recent months (inclusive) to load.",
    )
    p.add_argument("--date-col", default="date", help="Date column name.")
    p.add_argument("--symbol-col", default="symbol", help="Symbol column name.")
    p.add_argument(
        "--value-col",
        default=None,
        help="Value column name. If omitted, it will be inferred.",
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


def _resolve_col(df: pd.DataFrame, name: str) -> Optional[str]:
    if name in df.columns:
        return name
    cols = {c.lower(): c for c in df.columns}
    return cols.get(name.lower())


def _infer_value_col(
    df: pd.DataFrame, preferred: Optional[str], date_col: str, symbol_col: str
) -> str:
    if preferred:
        col = _resolve_col(df, preferred)
        if not col:
            raise SystemExit(f"Value column not found: {preferred!r}")
        return col

    candidates = ["factor_value", "value", "score"]
    for name in candidates:
        col = _resolve_col(df, name)
        if col:
            return col

    for col in df.columns:
        if col in {date_col, symbol_col}:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            return col

    raise SystemExit(
        "No usable value column found. Provide --value-col explicitly."
    )


def _load_frame(
    path: Path,
    date_col: str,
    symbol_col: str,
    score_col: Optional[str],
    value_col: Optional[str],
) -> pd.DataFrame:
    use_col = score_col or value_col
    if not use_col:
        raise SystemExit("No score/value column selected.")

    df = pd.read_parquet(path, columns=[date_col, symbol_col, use_col])
    df = df[[date_col, symbol_col, use_col]].copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()]
    df[symbol_col] = df[symbol_col].astype(str)
    df[use_col] = pd.to_numeric(df[use_col], errors="coerce")
    df = df[df[use_col].notna()]
    df = df.rename(columns={date_col: "date", symbol_col: "symbol", use_col: "score"})
    df["factor_value"] = df["score"]
    return df[["date", "symbol", "score", "factor_value"]]


def main() -> None:
    args = _parse_args()
    root = Path(args.root).resolve()
    as_of = _parse_as_of(args.as_of)
    months = _month_keys(as_of, args.max_months)

    factor_root = root / "datahub" / "silver" / "alpha" / "factor" / args.factor_id
    if not factor_root.exists():
        raise SystemExit(f"Factor root not found: {factor_root}")

    files: List[Path] = []
    missing: List[str] = []
    for ym in months:
        p = factor_root / f"yyyymm={ym}" / "data.parquet"
        if p.exists():
            files.append(p)
        else:
            missing.append(ym)

    if not files:
        raise SystemExit(f"No parquet files found under: {factor_root}")

    print(f"[INFO] factor_id={args.factor_id} as_of={as_of.isoformat()}")
    print(f"[INFO] found {len(files)} parquet files (missing {len(missing)} months)")

    first = pd.read_parquet(files[0])
    date_col = _resolve_col(first, args.date_col)
    symbol_col = _resolve_col(first, args.symbol_col)
    if not date_col or not symbol_col:
        raise SystemExit(
            f"Missing required columns: date={args.date_col!r}, symbol={args.symbol_col!r}"
        )
    score_col = _resolve_col(first, "score")
    if score_col:
        value_col = None
    else:
        value_col = _infer_value_col(first, args.value_col, date_col, symbol_col)

    frames: List[pd.DataFrame] = [
        _load_frame(files[0], date_col, symbol_col, score_col, value_col)
    ]
    for p in files[1:]:
        frames.append(_load_frame(p, date_col, symbol_col, score_col, value_col))

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["date", "symbol"], keep="last")
    merged = merged.sort_values(["date", "symbol"]).reset_index(drop=True)

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = root / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, index=False)
    print(f"[INFO] wrote {len(merged):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
