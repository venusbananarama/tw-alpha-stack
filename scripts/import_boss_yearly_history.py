# scripts/import_boss_yearly_history.py
"""
Import boss-provided yearly parquet files into tw-alpha-stack silver/alpha layout.

- Input:  yearly parquet files per dataset, e.g.
    C:\\AI\\historydate\\TaiwanStockPrice\\TaiwanStockPrice_1992.parquet
    C:\\AI\\historydate\\TaiwanStockDividend\\TaiwanStockDividend_2005.parquet
- Output: monthly-partitioned parquet files under:
    <repo_root>\\datahub\\silver\\alpha\\<dataset>\\yyyymm=YYYYMM\\*.parquet

Datasets currently supported (boss_name → dataset_id):

  # 原本 4 個日期線
    TaiwanStockPrice                          → prices
    TaiwanStockInstitutionalInvestorsBuySell  → chip
    TaiwanStockPER                            → per
    TaiwanStockDividend                       → dividend

  # 新增 6 個 DateID 線
    TaiwanStockFinancialStatements            → finstmt
    TaiwanStockBalanceSheet                   → bs
    TaiwanStockCashFlowsStatement             → cfs
    TaiwanStockShareholding                   → shareholding
    TaiwanStockTotalInstitutionalInvestors    → inst_total
    TaiwanStockGovernmentBankBuySell          → gov_bank

Usage example:

    # 一次匯入 4 表（日期線）
    python -m scripts.import_boss_yearly_history ^
      --boss-root C:\\AI\\historydate ^
      --repo-root C:\\AI\\tw-alpha-stack ^
      --datasets prices chip per dividend ^
      --start-year 1992 --end-year 2025

    # 只匯入 DateID 6 表
    python -m scripts.import_boss_yearly_history ^
      --boss-root C:\\AI\\historydate ^
      --repo-root C:\\AI\\tw-alpha-stack ^
      --datasets finstmt bs cfs shareholding inst_total gov_bank

Notes:
- Default: dry-run = False, wipe-target = False（不主動清空舊月分，只是覆寫同名 parquet）。
- 建議第一次使用時加上 --dry-run 看 log，再移除 --dry-run 正式執行。
- 非 dry-run 且未指定 --no-ledger 時，會將每個 dataset 的匯入日期範圍
  以 run_type="boss_import" 追加到 metrics\\ingest_ledger.jsonl，之後
  由 backfill_ingest_ok_from_ledger.py 產生 _state\\ingest\\<dataset>\\YYYY-MM-DD.ok。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetConfig:
    """Configuration for mapping boss dataset to silver dataset."""

    dataset_id: str          # internal dataset id: prices/chip/per/dividend/...
    boss_dir_name: str       # subdir under boss_root
    boss_file_prefix: str    # prefix of yearly parquet files
    date_column: str = "date"
    id_column: str = "stock_id"   # not used in logic, reserved for validation


@dataclass(frozen=True)
class ImportSummary:
    """Per-dataset summary for logging and ledger."""

    dataset_id: str
    date_start: date
    date_end: date
    total_rows: int
    total_files: int


DATASET_CONFIGS: Dict[str, DatasetConfig] = {
    # ---- 原本 4 表：prices / chip / per / dividend ----
    "prices": DatasetConfig(
        dataset_id="prices",
        boss_dir_name="TaiwanStockPrice",
        boss_file_prefix="TaiwanStockPrice_",
    ),
    "chip": DatasetConfig(
        dataset_id="chip",
        boss_dir_name="TaiwanStockInstitutionalInvestorsBuySell",
        boss_file_prefix="TaiwanStockInstitutionalInvestorsBuySell_",
    ),
    "per": DatasetConfig(
        dataset_id="per",
        boss_dir_name="TaiwanStockPER",
        boss_file_prefix="TaiwanStockPER_",
    ),
    "dividend": DatasetConfig(
        dataset_id="dividend",
        boss_dir_name="TaiwanStockDividend",
        boss_file_prefix="TaiwanStockDividend_",
    ),
    # ---- 新增 6 表：DateID 線 ----
    "finstmt": DatasetConfig(
        dataset_id="finstmt",
        boss_dir_name="TaiwanStockFinancialStatements",
        boss_file_prefix="TaiwanStockFinancialStatements_",
    ),
    "bs": DatasetConfig(
        dataset_id="bs",
        boss_dir_name="TaiwanStockBalanceSheet",
        boss_file_prefix="TaiwanStockBalanceSheet_",
    ),
    "cfs": DatasetConfig(
        dataset_id="cfs",
        boss_dir_name="TaiwanStockCashFlowsStatement",
        boss_file_prefix="TaiwanStockCashFlowsStatement_",
    ),
    "shareholding": DatasetConfig(
        dataset_id="shareholding",
        boss_dir_name="TaiwanStockShareholding",
        boss_file_prefix="TaiwanStockShareholding_",
    ),
    "inst_total": DatasetConfig(
        dataset_id="inst_total",
        boss_dir_name="TaiwanStockTotalInstitutionalInvestors",
        boss_file_prefix="TaiwanStockTotalInstitutionalInvestors_",
    ),
    "gov_bank": DatasetConfig(
        dataset_id="gov_bank",
        boss_dir_name="TaiwanStockGovernmentBankBuySell",
        boss_file_prefix="TaiwanStockGovernmentBankBuySell_",
    ),
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import boss yearly parquet files into tw-alpha-stack silver/alpha monthly layout."
    )
    parser.add_argument(
        "--boss-root",
        type=Path,
        required=True,
        help="Root directory of boss yearly datasets (contains subdirs like TaiwanStockPrice/...).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="Root directory of tw-alpha-stack repo (contains datahub/silver/alpha).",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(DATASET_CONFIGS.keys()),
        default=["prices", "chip", "per", "dividend"],
        help=(
            "Datasets to import, choose from: "
            + ", ".join(sorted(DATASET_CONFIGS.keys()))
            + ". Default: prices chip per dividend"
        ),
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="First year (inclusive) to import. If omitted, inferred from files.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year (inclusive) to import. If omitted, inferred from files.",
    )
    parser.add_argument(
        "--wipe-target",
        action="store_true",
        help=(
            "If set, remove existing monthly partitions for selected datasets under "
            "datahub/silver/alpha/<dataset> BEFORE importing. "
            "會把目標資料夾清空（請自行先做備份）。"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, only log actions without writing or deleting any files.",
    )
    parser.add_argument(
        "--no-ledger",
        action="store_true",
        help=(
            "If set, do NOT append boss_import entries to metrics/ingest_ledger.jsonl. "
            "預設會寫 ledger，方便 backfill_ingest_ok_from_ledger.py 產生 .ok。"
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print more detailed logs per month.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def find_year_files(
    cfg: DatasetConfig,
    boss_root: Path,
    start_year: Optional[int],
    end_year: Optional[int],
) -> List[Tuple[int, Path]]:
    """Discover yearly parquet files for a dataset.

    Expected boss folder layout:

        <boss_root>\\<boss_dir_name>\\<boss_file_prefix>YYYY.parquet

    Example:

        C:\\AI\\historydate\\TaiwanStockPrice\\TaiwanStockPrice_1992.parquet
    """

    src_dir = boss_root / cfg.boss_dir_name
    if not src_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found for {cfg.dataset_id}: {src_dir}")

    files: List[Tuple[int, Path]] = []
    for p in sorted(src_dir.glob(f"{cfg.boss_file_prefix}*.parquet")):
        # Expect filename like Prefix_YYYY.parquet
        stem = p.stem  # e.g. "TaiwanStockPrice_1992"
        try:
            year_str = stem.split("_")[-1]
            year = int(year_str)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Cannot parse year from file name: {p.name}") from exc

        files.append((year, p))

    if not files:
        raise FileNotFoundError(f"No yearly parquet files found in {src_dir}")

    # Infer year range if not specified
    all_years = [y for y, _ in files]
    min_year, max_year = min(all_years), max(all_years)

    effective_start = start_year if start_year is not None else min_year
    effective_end = end_year if end_year is not None else max_year

    if effective_start > effective_end:
        raise ValueError(
            f"Invalid year range: start_year={effective_start} > end_year={effective_end}"
        )

    # Filter files within range
    filtered = [(y, p) for y, p in files if effective_start <= y <= effective_end]
    if not filtered:
        raise FileNotFoundError(
            f"No yearly files within range {effective_start}-{effective_end} under {src_dir}"
        )

    return filtered


def wipe_target_dataset(
    dataset_id: str,
    repo_root: Path,
    dry_run: bool,
) -> None:
    """Remove existing monthly partitions for a single dataset.

    This function ONLY touches datahub/silver/alpha/<dataset> to avoid damaging other areas.
    """
    target_root = repo_root / "datahub" / "silver" / "alpha" / dataset_id
    if not target_root.exists():
        log(f"[{dataset_id}] target path does not exist, nothing to wipe: {target_root}")
        return

    if not target_root.is_dir():
        raise RuntimeError(f"Target path is not a directory: {target_root}")

    log(f"[{dataset_id}] wiping existing contents under {target_root} (monthly partitions).")
    if dry_run:
        return

    # We remove subdirectories and files under dataset root, but keep the root directory itself.
    for path in target_root.iterdir():
        if path.is_dir():
            # recursively delete subdir
            for sub in sorted(path.rglob("*"), reverse=True):
                if sub.is_file():
                    sub.unlink()
                elif sub.is_dir():
                    sub.rmdir()
            path.rmdir()
        elif path.is_file():
            path.unlink()


def write_month_partition(
    df_month: pd.DataFrame,
    dataset_id: str,
    yyyymm: str,
    repo_root: Path,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Write a single month partition parquet.

    Layout: <repo_root>/datahub/silver/alpha/<dataset>/yyyymm=YYYYMM/*.parquet
    File name: <dataset>_YYYYMM.parquet
    """

    target_dir = repo_root / "datahub" / "silver" / "alpha" / dataset_id / f"yyyymm={yyyymm}"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / f"{dataset_id}_{yyyymm}.parquet"

    if verbose:
        log(
            f"[{dataset_id}]  -> month {yyyymm}: "
            f"{len(df_month)} rows to {target_path}"
        )

    if dry_run:
        return

    # Ensure date is serialized properly
    df_month.to_parquet(target_path, index=False)


def import_dataset(
    cfg: DatasetConfig,
    boss_root: Path,
    repo_root: Path,
    start_year: Optional[int],
    end_year: Optional[int],
    wipe_target: bool,
    dry_run: bool,
    verbose: bool,
) -> ImportSummary:
    """Import one dataset (prices/chip/per/dividend/...).

    - 掃描 boss_root/<boss_dir_name> 之下所有 yearly parquet。
    - 依年份與 yyyymm 切成月檔，寫入銀河 monthly 路徑。
    - 回傳此 dataset 實際的日期區間與 row/file 數，用於 ledger。
    """

    log(f"[{cfg.dataset_id}] Discovering yearly files under {boss_root / cfg.boss_dir_name}...")
    year_files = find_year_files(cfg, boss_root, start_year, end_year)
    year_range = (min(y for y, _ in year_files), max(y for y, _ in year_files))
    log(
        f"[{cfg.dataset_id}] Found {len(year_files)} yearly files, "
        f"years {year_range[0]}–{year_range[1]}."
    )

    if wipe_target:
        wipe_target_dataset(cfg.dataset_id, repo_root, dry_run=dry_run)

    dataset_min: Optional[pd.Timestamp] = None
    dataset_max: Optional[pd.Timestamp] = None
    total_rows = 0
    total_files = 0

    for year, path in year_files:
        log(f"[{cfg.dataset_id}] Loading {path.name} (year {year})...")
        # 即使 dry-run 也讀一次 schema，早點發現欄位問題。
        df = pd.read_parquet(path)

        if cfg.date_column not in df.columns:
            raise KeyError(
                f"[{cfg.dataset_id}] date column '{cfg.date_column}' not found in {path}"
            )

        # Normalize date column to datetime64[ns]
        df[cfg.date_column] = pd.to_datetime(df[cfg.date_column])

        if df.empty:
            log(f"[{cfg.dataset_id}] WARNING: {path.name} has no rows, skip.")
            continue

        total_files += 1        # how many yearly files actually had rows
        total_rows += int(len(df))

        col_dates = df[cfg.date_column]
        year_min = col_dates.min()
        year_max = col_dates.max()
        if dataset_min is None or year_min < dataset_min:
            dataset_min = year_min
        if dataset_max is None or year_max > dataset_max:
            dataset_max = year_max

        # Derive yyyymm for grouping
        df["_yyyymm"] = df[cfg.date_column].dt.strftime("%Y%m")

        # ---- 放寬年份檢查：只警告，不擋跨年 ----
        df_years = sorted(df[cfg.date_column].dt.year.unique())
        if len(df_years) != 1 or df_years[0] != year:
            log(
                f"[{cfg.dataset_id}] WARNING: year mismatch in {path.name}: "
                f"file_year={year}, data_years={df_years}. "
                f"Cross-year filings are allowed; using actual dates for yyyymm."
            )

        # Group by month and write partitions（完全以 date → yyyymm 為準）
        for yyyymm, df_month in df.groupby("_yyyymm", sort=True):
            write_month_partition(
                df_month=df_month.drop(columns=["_yyyymm"]),
                dataset_id=cfg.dataset_id,
                yyyymm=yyyymm,
                repo_root=repo_root,
                dry_run=dry_run,
                verbose=verbose,
            )

    if dataset_min is None or dataset_max is None:
        raise RuntimeError(f"[{cfg.dataset_id}] No data rows were imported from boss files.")

    log(
        f"[{cfg.dataset_id}] Import completed. "
        f"rows={total_rows}, files={total_files}, "
        f"date_range={dataset_min.date()}..{dataset_max.date()}"
    )

    return ImportSummary(
        dataset_id=cfg.dataset_id,
        date_start=dataset_min.date(),
        date_end=dataset_max.date(),
        total_rows=total_rows,
        total_files=total_files,
    )


def append_ledger_entries(repo_root: Path, summaries: Iterable[ImportSummary]) -> None:
    """Append boss_import records for each dataset to metrics/ingest_ledger.jsonl.

    - run_type 固定為 "boss_import"
    - date_start / date_end 以實際 parquet 內容的最小/最大日期為準
    - run_id 格式：boss-import-YYYYMMDD-<dataset>
    """

    metrics_dir = repo_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = metrics_dir / "ingest_ledger.jsonl"

    now = datetime.now().astimezone()
    ts_iso = now.isoformat()
    run_date = now.strftime("%Y%m%d")

    with ledger_path.open("a", encoding="utf-8") as f:
        for summary in summaries:
            run_id = f"boss-import-{run_date}-{summary.dataset_id}"
            record = {
                "ts": ts_iso,
                "dataset": summary.dataset_id,
                "date_start": summary.date_start.isoformat(),
                "date_end": summary.date_end.isoformat(),
                "run_type": "boss_import",
                "run_id": run_id,
                "exit": "success",
                "source": "boss_yearly_parquet",
                "files": summary.total_files,
                "rows": summary.total_rows,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    boss_root = args.boss_root.resolve()
    repo_root = args.repo_root.resolve()

    log(f"Boss root: {boss_root}")
    log(f"Repo root: {repo_root}")
    log(f"Datasets: {', '.join(args.datasets)}")
    log(
        f"Year range: "
        f"{args.start_year if args.start_year is not None else '(auto)'}"
        f"–"
        f"{args.end_year if args.end_year is not None else '(auto)'}"
    )
    log(
        f"Options: wipe_target={args.wipe_target} dry_run={args.dry_run} "
        f"no_ledger={args.no_ledger} verbose={args.verbose}"
    )

    summaries: List[ImportSummary] = []

    for ds in args.datasets:
        cfg = DATASET_CONFIGS[ds]
        summary = import_dataset(
            cfg=cfg,
            boss_root=boss_root,
            repo_root=repo_root,
            start_year=args.start_year,
            end_year=args.end_year,
            wipe_target=args.wipe_target,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
        summaries.append(summary)

    if args.dry_run:
        log("Dry-run mode: skip appending boss_import entries to ingest_ledger.jsonl.")
    elif args.no_ledger:
        log("--no-ledger specified: skip appending boss_import entries to ingest_ledger.jsonl.")
    else:
        append_ledger_entries(repo_root=repo_root, summaries=summaries)
        log("Boss-import ranges appended to metrics/ingest_ledger.jsonl.")

    log("All datasets completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
