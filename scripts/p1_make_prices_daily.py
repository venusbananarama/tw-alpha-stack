from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

STANDARD_COLS = ["date", "symbol", "open", "high", "low", "close", "volume"]
REGRESSION_RATIO = 0.9


@dataclass(frozen=True)
class InputFile:
    path: Path
    month: str
    priority: int
    source: str


@dataclass(frozen=True)
class CoverageStats:
    rows: int
    unique_dates: int
    unique_symbols: int
    min_date: Optional[date]
    max_date: Optional[date]


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
    include_group = p.add_mutually_exclusive_group()
    include_group.add_argument(
        "--include-fromboss",
        dest="include_fromboss",
        action="store_true",
        help="Include prices_*_fromboss.parquet when data.parquet is absent.",
    )
    include_group.add_argument(
        "--no-include-fromboss",
        dest="include_fromboss",
        action="store_false",
        help="Disable prices_*_fromboss.parquet fallback discovery.",
    )
    p.set_defaults(include_fromboss=True)
    p.add_argument(
        "--allow-regression",
        action="store_true",
        default=False,
        help="Allow overwriting output even if coverage regresses.",
    )
    p.add_argument(
        "--archive-dir",
        default="datahub/silver/alpha/_ing_archive/prices_daily",
        help="Archive directory for old output parquet.",
    )
    p.add_argument(
        "--tmp-dir",
        default="datahub/silver/alpha/_prices_build_tmp",
        help="Temporary directory for atomic writes.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Compute stats only, do not write output.",
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
    columns: List[str],
    explicit: Optional[str],
    candidates: List[str],
    label: str,
) -> Optional[str]:
    if explicit:
        col = _resolve_named(columns, explicit)
        if not col:
            raise SystemExit(f"{label} column not found: {explicit!r}")
        return col
    col = _pick_col(columns, candidates)
    if not col:
        raise SystemExit(f"{label} column not found in candidates: {candidates}")
    return col


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


def _normalize_frame(
    df: pd.DataFrame,
    col_map: Dict[str, str],
    symbol_is_stock_id: bool,
) -> pd.DataFrame:
    missing = [key for key, src in col_map.items() if src not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    data: Dict[str, pd.Series] = {out: df[src] for out, src in col_map.items()}
    out = pd.DataFrame(data)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["date"].notna()]
    out["symbol"] = out["symbol"].astype(str)
    if symbol_is_stock_id:
        out["symbol"] = out["symbol"].str.replace(".TW", "", regex=False)
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _discover_inputs(
    in_root: Path,
    months: List[str],
    include_fromboss: bool,
) -> Tuple[List[InputFile], Dict[str, int]]:
    month_dirs: List[Path] = []
    inputs: List[InputFile] = []
    data_parquet_used = 0
    fromboss_used = 0

    for ym in months:
        d = in_root / f"yyyymm={ym}"
        if not (d.exists() and d.is_dir()):
            continue
        month_dirs.append(d)
        data_path = d / "data.parquet"
        if data_path.is_file():
            inputs.append(InputFile(path=data_path, month=ym, priority=0, source="data_parquet"))
            data_parquet_used += 1
            continue
        if include_fromboss:
            candidates = sorted(
                [
                    p
                    for p in d.glob("prices_*_fromboss.parquet")
                    if ".bak" not in p.name.lower()
                ],
                key=lambda p: p.name,
            )
            for p in candidates:
                inputs.append(InputFile(path=p, month=ym, priority=1, source="fromboss"))
            fromboss_used += len(candidates)

    inputs.sort(key=lambda item: (item.month, item.priority, str(item.path)))
    stats = {
        "months_scanned": len(month_dirs),
        "files_used": len(inputs),
        "data_parquet_used": data_parquet_used,
        "fromboss_used": fromboss_used,
    }
    return inputs, stats


def _compute_coverage(df: pd.DataFrame) -> CoverageStats:
    if df is None or df.empty:
        return CoverageStats(
            rows=0,
            unique_dates=0,
            unique_symbols=0,
            min_date=None,
            max_date=None,
        )
    dates = pd.to_datetime(df["date"], errors="coerce")
    dates = dates[dates.notna()]
    min_date = dates.min().date() if not dates.empty else None
    max_date = dates.max().date() if not dates.empty else None
    unique_dates = int(dates.dt.date.nunique()) if not dates.empty else 0
    unique_symbols = int(df["symbol"].nunique(dropna=True))
    return CoverageStats(
        rows=int(len(df)),
        unique_dates=unique_dates,
        unique_symbols=unique_symbols,
        min_date=min_date,
        max_date=max_date,
    )


def _format_coverage(label: str, cov: CoverageStats) -> str:
    min_date = cov.min_date.isoformat() if cov.min_date else "NONE"
    max_date = cov.max_date.isoformat() if cov.max_date else "NONE"
    return (
        f"[INFO] {label} rows={cov.rows} unique_dates={cov.unique_dates} "
        f"unique_symbols={cov.unique_symbols} min_date={min_date} max_date={max_date}"
    )


def _load_existing_panel(path: Path, as_of_ts: pd.Timestamp) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path, columns=["date", "symbol"])
    except Exception:
        df = pd.read_parquet(path)
    if "date" not in df.columns or "symbol" not in df.columns:
        raise SystemExit("Existing output missing required columns: date/symbol")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    df = df[df["date"] <= as_of_ts]
    return df


def _regression_reasons(new: CoverageStats, old: CoverageStats) -> List[str]:
    reasons: List[str] = []
    if old.max_date and new.max_date:
        if new.max_date < old.max_date:
            reasons.append("MAX_DATE_REGRESSION")
    elif old.max_date and not new.max_date:
        reasons.append("MAX_DATE_REGRESSION")

    if old.unique_dates and new.unique_dates < old.unique_dates * REGRESSION_RATIO:
        reasons.append("UNIQUE_DATES_REGRESSION")
    if old.rows and new.rows < old.rows * REGRESSION_RATIO:
        reasons.append("ROWS_REGRESSION")
    if old.unique_symbols and new.unique_symbols < old.unique_symbols * REGRESSION_RATIO:
        reasons.append("UNIQUE_SYMBOLS_REGRESSION")
    return reasons


def _should_write(
    new_cov: CoverageStats,
    old_cov: Optional[CoverageStats],
    allow_regression: bool,
) -> Tuple[bool, str]:
    if old_cov is None or old_cov.rows == 0:
        return True, "NO_OLD"
    reasons = _regression_reasons(new_cov, old_cov)
    if reasons and not allow_regression:
        return False, "|".join(reasons)
    if reasons and allow_regression:
        return True, "ALLOW_REGRESSION:" + "|".join(reasons)
    return True, "OK"


def _emit_verdict(verdict: str, reason: str) -> None:
    reason_text = reason.replace(" ", "_")
    print(f"[RESULT] verdict={verdict} reason={reason_text}")


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
    tmp_dir = Path(args.tmp_dir)
    if not tmp_dir.is_absolute():
        tmp_dir = repo_root / tmp_dir
    archive_dir = Path(args.archive_dir)
    if not archive_dir.is_absolute():
        archive_dir = repo_root / archive_dir

    if not in_root.exists():
        scan_stats = {
            "months_scanned": 0,
            "files_used": 0,
            "data_parquet_used": 0,
            "fromboss_used": 0,
        }
        print(
            f"[INFO] as_of={as_of.isoformat()} months={scan_stats['months_scanned']} files={scan_stats['files_used']}"
        )
        print(
            "[INFO] inputs months_scanned=%s files_used=%s data_parquet_used=%s fromboss_used=%s"
            % (
                scan_stats["months_scanned"],
                scan_stats["files_used"],
                scan_stats["data_parquet_used"],
                scan_stats["fromboss_used"],
            )
        )
        _emit_verdict("FAIL_SCHEMA", "INPUT_ROOT_MISSING")
        raise SystemExit(f"Input root not found: {in_root}")

    inputs, scan_stats = _discover_inputs(in_root, months, args.include_fromboss)
    print(
        f"[INFO] as_of={as_of.isoformat()} months={scan_stats['months_scanned']} files={scan_stats['files_used']}"
    )
    print(
        "[INFO] inputs months_scanned=%s files_used=%s data_parquet_used=%s fromboss_used=%s"
        % (
            scan_stats["months_scanned"],
            scan_stats["files_used"],
            scan_stats["data_parquet_used"],
            scan_stats["fromboss_used"],
        )
    )
    if not inputs:
        _emit_verdict("FAIL_SCHEMA", "NO_INPUTS")
        raise SystemExit(f"No parquet files found under: {in_root}")

    frames: List[pd.DataFrame] = []
    read_files = 0
    for item in inputs:
        p = item.path
        cols, preloaded = _get_parquet_columns(p)
        if not cols:
            _emit_verdict("FAIL_SCHEMA", f"READ_ERROR:{p}")
            raise SystemExit(f"Failed to read parquet schema: {p}")

        try:
            date_col = _pick_required(
                cols, args.date_col, ["date", "trade_date", "datetime"], "date"
            )
            symbol_col = _pick_required(
                cols, args.symbol_col, ["symbol", "stock_id", "ticker", "code"], "symbol"
            )
            close_col = _pick_required(
                cols,
                args.close_col,
                ["adj_close", "close", "price", "last", "px_close"],
                "close",
            )
            open_col = _pick_required(cols, None, ["open"], "open")
            high_col = _pick_required(cols, None, ["high", "max"], "high")
            low_col = _pick_required(cols, None, ["low", "min"], "low")
            volume_col = _pick_required(
                cols,
                args.volume_col,
                ["Trading_Volume", "volume", "vol", "shares"],
                "volume",
            )
        except SystemExit as exc:
            _emit_verdict("FAIL_SCHEMA", f"MISSING_COLUMN:{p}:{exc}")
            raise

        col_map = {
            "date": date_col,
            "symbol": symbol_col,
            "open": open_col,
            "high": high_col,
            "low": low_col,
            "close": close_col,
            "volume": volume_col,
        }

        use_cols = sorted({c for c in col_map.values() if c})
        try:
            raw = _read_parquet(p, use_cols, preloaded)
            frame = _normalize_frame(
                raw, col_map, symbol_is_stock_id=symbol_col.lower() == "stock_id"
            )
        except Exception as exc:
            _emit_verdict("FAIL_SCHEMA", f"READ_ERROR:{p}")
            raise SystemExit(f"Failed to normalize {p}: {exc}") from exc

        frame = frame[frame["date"] <= as_of_ts]
        frames.append(frame)
        read_files += 1

    if frames:
        panel = pd.concat(frames, ignore_index=True)
    else:
        panel = pd.DataFrame(columns=STANDARD_COLS)

    panel = panel.drop_duplicates(subset=["date", "symbol"], keep="last")
    panel = panel.sort_values(["date", "symbol"]).reset_index(drop=True)
    missing_cols = [c for c in STANDARD_COLS if c not in panel.columns]
    if missing_cols:
        _emit_verdict("FAIL_SCHEMA", f"MISSING_OUTPUT:{','.join(missing_cols)}")
        raise SystemExit(f"Missing output columns: {missing_cols}")
    panel = panel[STANDARD_COLS]

    new_cov = _compute_coverage(panel)
    print(_format_coverage("coverage_new", new_cov))

    old_cov: Optional[CoverageStats] = None
    if out_path.exists():
        try:
            old_panel = _load_existing_panel(out_path, as_of_ts)
        except SystemExit as exc:
            _emit_verdict("FAIL_SCHEMA", f"OLD_SCHEMA:{exc}")
            raise
        old_cov = _compute_coverage(old_panel)
        print(_format_coverage("coverage_old", old_cov))
    else:
        print("[INFO] coverage_old NONE")

    should_write, reason = _should_write(new_cov, old_cov, args.allow_regression)
    if not should_write:
        _emit_verdict("SKIP_REGRESSION", reason)
        raise SystemExit(f"Regression guard failed: {reason}")

    if args.dry_run:
        _emit_verdict("DRY_RUN", reason)
        print(f"[INFO] output={out_path}")
        print(f"[INFO] read_files={read_files}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
    tmp_path = tmp_dir / f"prices_daily.{run_id}.parquet"
    panel.to_parquet(tmp_path, index=False)

    if out_path.exists():
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"prices_daily.{run_id}.parquet"
        out_path.replace(archive_path)

    tmp_path.replace(out_path)

    if len(panel):
        min_date = new_cov.min_date.isoformat() if new_cov.min_date else "NONE"
        max_date = new_cov.max_date.isoformat() if new_cov.max_date else "NONE"
        print(f"[INFO] wrote rows={new_cov.rows} date_range={min_date}..{max_date}")
    else:
        print("[WARN] wrote empty panel.")
    print(f"[INFO] output={out_path}")
    print(f"[INFO] read_files={read_files}")
    _emit_verdict("WRITE", reason)


if __name__ == "__main__":
    main()

# CLI added: --include-fromboss/--no-include-fromboss, --allow-regression, --archive-dir, --tmp-dir, --dry-run
# non-regression: max_date regression or unique_dates/rows/unique_symbols < 0.9x old blocks write unless --allow-regression
# defaults: include_fromboss=True, allow_regression=False, max_months=36
