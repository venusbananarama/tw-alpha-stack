# scripts/build_investable_universe.py
"""
Build an investable universe from silver/alpha/prices, with classification
rules and manual overrides.

- Single entrypoint: main() / run_pipeline(config)
- Deterministic & idempotent: same inputs => same outputs; rerun overwrites files.
- Schema-first: requires 'date' and 'stock_id' in prices, and 'stock_id',
  'market', 'type' in security master when classification is enabled.

Outputs:
- reports/universe_stats.csv: per-stock stats + rule decisions.
- investable_universe.txt: list of eligible stock_id (one per line).
- reports/universe_manifest.json: run metadata & summary (optional evidence).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Config & data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UniverseConfig:
    """Immutable configuration for universe building."""

    prices_root: Path
    refdata_path: Optional[Path]
    output_dir: Path

    lookback_years: int
    min_total_days: int
    min_recent_days: int
    min_median_turnover: Optional[float]

    include_list_path: Optional[Path]
    exclude_list_path: Optional[Path]

    strict_refdata: bool
    write_manifest: bool = True


@dataclass
class StockStats:
    """Per-stock statistics and rule decisions."""

    stock_id: str
    first_date: datetime
    last_date: datetime
    total_days: int
    recent_days: int
    median_turnover_1y: Optional[float]

    passed_activity: bool = False
    passed_liquidity: bool = False
    passed_classification: bool = True  # default permissive; refined later
    forced_include: bool = False
    forced_exclude: bool = False
    eligible: bool = False


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------


def log(message: str) -> None:
    """Log with timestamp to stdout."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")


# ---------------------------------------------------------------------------
# CLI & config
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> UniverseConfig:
    parser = argparse.ArgumentParser(
        description="Build investable_universe.txt from silver/alpha/prices."
    )

    parser.add_argument(
        "--prices-root",
        type=Path,
        required=True,
        help="Root directory of prices silver data (e.g. datahub/silver/alpha/prices).",
    )
    parser.add_argument(
        "--refdata-path",
        type=Path,
        default=None,
        help=(
            "Path to security master (parquet or CSV) providing stock_id→market/type. "
            "If omitted, classification rules are disabled unless strict_refdata is set."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports"),
        help="Directory to write stats CSV and manifest. Default: reports",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=5,
        help="Lookback window (years) for recent activity. Default: 5",
    )
    parser.add_argument(
        "--min-total-days",
        type=int,
        default=250,
        help="Minimum total trading days across full history. Default: 250",
    )
    parser.add_argument(
        "--min-recent-days",
        type=int,
        default=120,
        help="Minimum trading days in the recent window. Default: 120",
    )
    parser.add_argument(
        "--min-median-turnover",
        type=float,
        default=5_000_000.0,
        help=(
            "Minimum median daily turnover (TWD) in the last 1 year. "
            "Use 0 or negative value to disable liquidity check. Default: 5,000,000"
        ),
    )
    parser.add_argument(
        "--include-list",
        type=Path,
        default=None,
        help="Optional path to manual include list (one stock_id per line).",
    )
    parser.add_argument(
        "--exclude-list",
        type=Path,
        default=None,
        help="Optional path to manual exclude list (one stock_id per line).",
    )
    parser.add_argument(
        "--strict-refdata",
        action="store_true",
        help=(
            "Treat missing classification data as failure for classification rules. "
            "By default, missing refdata is treated as pass_classification=True."
        ),
    )
    parser.add_argument(
        "--no-manifest",
        action="store_true",
        help="Disable writing reports/universe_manifest.json.",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    prices_root = args.prices_root.resolve()
    refdata_path = args.refdata_path.resolve() if args.refdata_path else None
    output_dir = args.output_dir.resolve()

    # Interpret non-positive turnover threshold as "disabled"
    min_median_turnover: Optional[float]
    if args.min_median_turnover is not None and args.min_median_turnover > 0:
        min_median_turnover = float(args.min_median_turnover)
    else:
        min_median_turnover = None

    cfg = UniverseConfig(
        prices_root=prices_root,
        refdata_path=refdata_path,
        output_dir=output_dir,
        lookback_years=int(args.lookback_years),
        min_total_days=int(args.min_total_days),
        min_recent_days=int(args.min_recent_days),
        min_median_turnover=min_median_turnover,
        include_list_path=args.include_list.resolve() if args.include_list else None,
        exclude_list_path=args.exclude_list.resolve() if args.exclude_list else None,
        strict_refdata=bool(args.strict_refdata),
        write_manifest=not bool(args.no_manifest),
    )

    log(f"prices_root = {cfg.prices_root}")
    log(f"output_dir  = {cfg.output_dir}")
    log(
        "config      = "
        f"lookback_years={cfg.lookback_years}, "
        f"min_total_days={cfg.min_total_days}, "
        f"min_recent_days={cfg.min_recent_days}, "
        f"min_median_turnover={cfg.min_median_turnover}, "
        f"strict_refdata={cfg.strict_refdata}"
    )
    if cfg.refdata_path:
        log(f"refdata_path = {cfg.refdata_path}")
    else:
        log("refdata_path = <none> (classification rules may be disabled)")

    if cfg.include_list_path:
        log(f"include_list = {cfg.include_list_path}")
    if cfg.exclude_list_path:
        log(f"exclude_list = {cfg.exclude_list_path}")

    return cfg


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_prices(prices_root: Path) -> pd.DataFrame:
    """Load all prices parquet files under prices_root."""
    if not prices_root.is_dir():
        raise FileNotFoundError(f"prices root not found: {prices_root}")

    files = sorted(prices_root.glob("yyyymm=*/data.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files under {prices_root}")

    log(f"Found {len(files)} data.parquet files under {prices_root}")

    dfs: List[pd.DataFrame] = []
    for i, f in enumerate(files, start=1):
        df = pd.read_parquet(f)

        if "date" not in df.columns:
            raise ValueError(f"File {f} missing required column 'date'")
        if "stock_id" not in df.columns:
            raise ValueError(f"File {f} missing required column 'stock_id'")

        # Normalize schema: ensure datetime, keep all numeric columns for later use
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"])

        dfs.append(df)

        if i % 50 == 0:
            log(f"  ... loaded {i} files")

    all_df = pd.concat(dfs, ignore_index=True)
    log(f"Concatenated {len(all_df):,} rows from prices")
    return all_df


def pick_turnover_column(df: pd.DataFrame) -> Optional[str]:
    """
    Pick a reasonable turnover column for liquidity checks.

    Uses a list of common candidate names, case-insensitive.
    Returns the actual column name in df, or None if not found.
    """
    candidates = ["trading_money", "trading_value", "turnover", "value", "amount"]
    lower_map: Dict[str, str] = {c.lower(): c for c in df.columns}
    for key in candidates:
        if key in lower_map:
            return lower_map[key]
    return None


def load_security_master(refdata_path: Optional[Path]) -> Optional[pd.DataFrame]:
    """Load security master with at least stock_id, market, type."""
    if refdata_path is None:
        return None

    if not refdata_path.is_file():
        raise FileNotFoundError(f"security master not found: {refdata_path}")

    suffix = refdata_path.suffix.lower()
    if suffix == ".parquet":
        df = pd.read_parquet(refdata_path)
    elif suffix in (".csv", ".txt"):
        df = pd.read_csv(refdata_path)
    else:
        raise ValueError(f"Unsupported refdata file type: {refdata_path}")

    required = {"stock_id", "market", "type"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            f"security master {refdata_path} missing required columns: {sorted(missing)}"
        )

    # Drop duplicate stock_id rows deterministically: keep first occurrence
    before = len(df)
    df = df.drop_duplicates(subset=["stock_id"], keep="first")
    after = len(df)
    if after < before:
        log(
            f"[WARN] security master had {before - after} duplicate stock_id rows; "
            f"kept first occurrence for each id."
        )

    log(f"Loaded security master with {after} distinct stock_id from {refdata_path}")
    return df[["stock_id", "market", "type"]]


def load_id_list(path: Optional[Path]) -> Set[str]:
    """Load include/exclude list from plain text (one stock_id per line)."""
    ids: Set[str] = set()
    if path is None:
        return ids
    if not path.is_file():
        log(f"[WARN] id list file not found, treated as empty: {path}")
        return ids

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        ids.add(stripped)
    log(f"Loaded {len(ids)} ids from {path}")
    return ids


# ---------------------------------------------------------------------------
# Stats engine
# ---------------------------------------------------------------------------


def extract_global_dates(prices_df: pd.DataFrame) -> Tuple[datetime, datetime]:
    """Return (min_date, max_date) from prices data."""
    min_date = prices_df["date"].min()
    max_date = prices_df["date"].max()
    # Convert to Python datetime
    return (min_date.to_pydatetime(), max_date.to_pydatetime())


def compute_activity_and_liquidity_stats(
    prices_df: pd.DataFrame,
    cfg: UniverseConfig,
    turnover_col: Optional[str],
) -> Dict[str, StockStats]:
    """
    Compute per-stock activity & liquidity stats and initial rule results
    (passed_activity & passed_liquidity).
    """
    _, max_date = extract_global_dates(prices_df)
    lookback_start = max_date - timedelta(days=365 * cfg.lookback_years)
    one_year_start = max_date - timedelta(days=365)

    log(f"Global max_date in prices: {max_date.date()}")
    log(
        f"Recent window start (activity): {lookback_start.date()} "
        f"(last {cfg.lookback_years} years)"
    )
    log(f"Liquidity window start (1y): {one_year_start.date()}")

    if turnover_col:
        log(f"Using '{turnover_col}' as turnover column for liquidity checks")
    else:
        if cfg.min_median_turnover is not None:
            log(
                "[WARN] No turnover-like column found; "
                "liquidity check will be disabled despite min_median_turnover being set."
            )
        else:
            log("[INFO] No turnover-like column found; liquidity check disabled.")
        # If there is no turnover column, treat as disabled
        turnover_col = None

    # Precompute filtered windows for efficiency
    df_recent = prices_df[prices_df["date"] >= lookback_start]
    df_1y = prices_df[prices_df["date"] >= one_year_start] if turnover_col else None

    stats_by_id: Dict[str, StockStats] = {}

    grouped = prices_df.groupby("stock_id", sort=False)
    for stock_id, g in grouped:
        first_ts = g["date"].min()
        last_ts = g["date"].max()
        total_days = int(g["date"].nunique())

        g_recent = df_recent[df_recent["stock_id"] == stock_id]
        recent_days = int(g_recent["date"].nunique())

        if turnover_col and df_1y is not None:
            g_1y = df_1y[df_1y["stock_id"] == stock_id]
            if not g_1y.empty:
                median_turnover_1y = float(g_1y[turnover_col].median())
            else:
                median_turnover_1y = None
        else:
            median_turnover_1y = None

        passed_activity = (total_days >= cfg.min_total_days) and (
            recent_days >= cfg.min_recent_days
        )

        if cfg.min_median_turnover is not None:
            if median_turnover_1y is None:
                passed_liquidity = False
            else:
                passed_liquidity = median_turnover_1y >= cfg.min_median_turnover
        else:
            passed_liquidity = True

        stats_by_id[stock_id] = StockStats(
            stock_id=str(stock_id),
            first_date=first_ts.to_pydatetime(),
            last_date=last_ts.to_pydatetime(),
            total_days=total_days,
            recent_days=recent_days,
            median_turnover_1y=median_turnover_1y,
            passed_activity=passed_activity,
            passed_liquidity=passed_liquidity,
            # other flags will be filled later
        )

    log(f"Computed stats for {len(stats_by_id)} distinct stock_id")
    return stats_by_id


# ---------------------------------------------------------------------------
# Rules engine
# ---------------------------------------------------------------------------


def _is_allowed_by_classification(market: str, sec_type: str) -> bool:
    """
    Classification rule for investable universe.

    This is a simple, opinionated default:
    - market: TWSE/TSE/TPEX (case-insensitive)
    - type: EQUITY / STOCK / 普通股 (case-insensitive for ASCII, raw compare for others)

    This can be adjusted in future when you have a stricter security master schema.
    """
    if market is None or sec_type is None:
        return False

    m = str(market).strip().upper()
    t = str(sec_type).strip().upper()

    allowed_markets = {"TWSE", "TSE", "TPEX"}
    allowed_types = {"EQUITY", "STOCK"}

    if m not in allowed_markets:
        return False

    # Simple handling of Chinese "普通股"
    if t in allowed_types or "普通股" in str(sec_type):
        return True
    return False


def evaluate_classification_rules(
    stats_by_id: Dict[str, StockStats],
    security_master: Optional[pd.DataFrame],
    cfg: UniverseConfig,
) -> None:
    """
    Evaluate classification rules and update passed_classification for each stock.

    Behaviour:
    - If security_master is None:
      - strict_refdata=True  -> passed_classification=False for all.
      - strict_refdata=False -> passed_classification=True for all.
    - If security_master is present:
      - For stock without classification row:
        - strict_refdata=True  -> passed_classification=False.
        - strict_refdata=False -> passed_classification=True.
      - For stock with classification row:
        - Apply _is_allowed_by_classification().
    """
    if security_master is None:
        if cfg.strict_refdata:
            log(
                "[WARN] strict_refdata=True but no security master provided; "
                "passed_classification=False for all ids."
            )
            for s in stats_by_id.values():
                s.passed_classification = False
        else:
            log(
                "[INFO] No security master provided; "
                "classification rules disabled (passed_classification=True for all)."
            )
            for s in stats_by_id.values():
                s.passed_classification = True
        return

    # Build simple mapping from stock_id to (market, type)
    sec_map: Dict[str, Tuple[Any, Any]] = {}
    for _, row in security_master.iterrows():
        sid = str(row["stock_id"])
        sec_map[sid] = (row["market"], row["type"])

    missing_count = 0
    disallowed_count = 0

    for stock_id, s in stats_by_id.items():
        info = sec_map.get(stock_id)
        if info is None:
            if cfg.strict_refdata:
                s.passed_classification = False
                missing_count += 1
            else:
                s.passed_classification = True
            continue

        market, sec_type = info
        allowed = _is_allowed_by_classification(market, sec_type)
        s.passed_classification = allowed
        if not allowed:
            disallowed_count += 1

    if missing_count:
        log(
            f"[INFO] classification: {missing_count} ids missing in security master "
            f"(strict_refdata={cfg.strict_refdata})"
        )
    log(f"[INFO] classification: {disallowed_count} ids failed classification rule")


def apply_manual_overrides(
    stats_by_id: Dict[str, StockStats],
    include_ids: Set[str],
    exclude_ids: Set[str],
) -> None:
    """Mark forced include/exclude flags based on manual lists."""
    if not include_ids and not exclude_ids:
        return

    # Detect conflicts
    conflicted = include_ids.intersection(exclude_ids)
    if conflicted:
        log(
            f"[WARN] {len(conflicted)} ids appear in both include and exclude lists; "
            "exclude will take precedence."
        )

    for stock_id, s in stats_by_id.items():
        if stock_id in include_ids:
            s.forced_include = True
        if stock_id in exclude_ids:
            s.forced_exclude = True


def finalize_eligibility(stats_by_id: Dict[str, StockStats]) -> None:
    """
    Compute final eligibility based on rule flags with precedence:

    1) forced_exclude -> eligible=False
    2) forced_include -> eligible=True
    3) Otherwise: passed_activity & passed_liquidity & passed_classification
    """
    for s in stats_by_id.values():
        if s.forced_exclude:
            s.eligible = False
        elif s.forced_include:
            s.eligible = True
        else:
            s.eligible = (
                s.passed_activity and s.passed_liquidity and s.passed_classification
            )


# ---------------------------------------------------------------------------
# Outputs & audit
# ---------------------------------------------------------------------------


def write_universe_stats(
    stats_by_id: Dict[str, StockStats],
    security_master: Optional[pd.DataFrame],
    cfg: UniverseConfig,
) -> Path:
    """Write reports/universe_stats.csv with stats + classification info."""
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    stats_path = cfg.output_dir / "universe_stats.csv"

    # Build classification lookup
    market_map: Dict[str, Any] = {}
    type_map: Dict[str, Any] = {}
    if security_master is not None:
        for _, row in security_master.iterrows():
            sid = str(row["stock_id"])
            market_map[sid] = row["market"]
            type_map[sid] = row["type"]

    rows: List[Mapping[str, Any]] = []
    for stock_id, s in stats_by_id.items():
        base = asdict(s)
        base["market"] = market_map.get(stock_id)
        base["type"] = type_map.get(stock_id)
        rows.append(base)

    df = pd.DataFrame(rows)
    df = df.sort_values("stock_id")
    df.to_csv(stats_path, index=False, encoding="utf-8-sig")
    log(f"Wrote stats to {stats_path}")
    return stats_path


def write_investable_universe(stats_by_id: Dict[str, StockStats]) -> Path:
    """Write eligible stock_id to investable_universe.txt in repo root."""
    eligible_ids = sorted(
        s.stock_id for s in stats_by_id.values() if s.eligible  # type: ignore[truthy-function]
    )
    path = Path("investable_universe.txt")
    path.write_text("\n".join(eligible_ids), encoding="utf-8")
    log(f"Wrote investable universe ({len(eligible_ids)} ids) to {path}")
    return path


def write_universe_manifest(
    stats_by_id: Dict[str, StockStats],
    cfg: UniverseConfig,
    meta: Mapping[str, Any],
) -> Path:
    """Write a JSON manifest describing this universe build."""
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cfg.output_dir / "universe_manifest.json"

    raw_size = len(stats_by_id)
    eligible_ids = [s for s in stats_by_id.values() if s.eligible]
    eligible_size = len(eligible_ids)

    activity_pass = sum(1 for s in stats_by_id.values() if s.passed_activity)
    liquidity_pass = sum(1 for s in stats_by_id.values() if s.passed_liquidity)
    classification_pass = sum(
        1 for s in stats_by_id.values() if s.passed_classification
    )

    manifest: Dict[str, Any] = {
        "run_ts": datetime.now().isoformat(timespec="seconds"),
        "prices_root": str(cfg.prices_root),
        "refdata_path": str(cfg.refdata_path) if cfg.refdata_path else None,
        "output_dir": str(cfg.output_dir),
        "config": {
            "lookback_years": cfg.lookback_years,
            "min_total_days": cfg.min_total_days,
            "min_recent_days": cfg.min_recent_days,
            "min_median_turnover": cfg.min_median_turnover,
            "strict_refdata": cfg.strict_refdata,
        },
        "universe": {
            "raw_size": raw_size,
            "eligible_size": eligible_size,
            "activity_pass": activity_pass,
            "liquidity_pass": liquidity_pass,
            "classification_pass": classification_pass,
        },
        "meta": dict(meta),
    }

    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"Wrote universe manifest to {manifest_path}")
    return manifest_path


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run_pipeline(cfg: UniverseConfig) -> None:
    """Main pipeline: load → stats → rules → outputs."""
    # Load core data
    prices_df = load_prices(cfg.prices_root)
    min_date, max_date = extract_global_dates(prices_df)
    log(f"prices date range: {min_date.date()} .. {max_date.date()}")

    turnover_col = pick_turnover_column(prices_df)

    security_master = load_security_master(cfg.refdata_path)
    include_ids = load_id_list(cfg.include_list_path)
    exclude_ids = load_id_list(cfg.exclude_list_path)

    # Compute stats
    stats_by_id = compute_activity_and_liquidity_stats(
        prices_df=prices_df,
        cfg=cfg,
        turnover_col=turnover_col,
    )

    # Rules
    evaluate_classification_rules(
        stats_by_id=stats_by_id,
        security_master=security_master,
        cfg=cfg,
    )
    apply_manual_overrides(
        stats_by_id=stats_by_id,
        include_ids=include_ids,
        exclude_ids=exclude_ids,
    )
    finalize_eligibility(stats_by_id)

    raw_size = len(stats_by_id)
    eligible_size = sum(1 for s in stats_by_id.values() if s.eligible)
    log(f"Raw universe size    : {raw_size}")
    log(f"Eligible universe size: {eligible_size}")

    # Outputs
    stats_path = write_universe_stats(
        stats_by_id=stats_by_id,
        security_master=security_master,
        cfg=cfg,
    )
    uni_path = write_investable_universe(stats_by_id)
    manifest_path: Optional[Path] = None

    if cfg.write_manifest:
        manifest_meta = {
            "stats_path": str(stats_path),
            "universe_path": str(uni_path),
        }
        manifest_path = write_universe_manifest(
            stats_by_id=stats_by_id,
            cfg=cfg,
            meta=manifest_meta,
        )

    log("Universe build complete.")
    log(f"Stats CSV : {stats_path}")
    log(f"Universe  : {uni_path}")
    if manifest_path is not None:
        log(f"Manifest  : {manifest_path}")


def main(argv: Optional[Iterable[str]] = None) -> int:
    try:
        cfg = parse_args(argv)
        run_pipeline(cfg)
        return 0
    except Exception as exc:
        # Centralized error handling: log and signal non-zero exit code.
        log(f"[ERROR] {exc!r}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
