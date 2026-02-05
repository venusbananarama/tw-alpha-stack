# scripts/p1_backfill_ingest_ok_from_ledger.py
"""
Read boss_import ranges from metrics/ingest_ledger.jsonl and create
_state/ingest/<dataset>/YYYY-MM-DD.ok files for each dataset.

This is designed to be run AFTER import_boss_yearly_history.py has imported
boss-provided yearly parquet into silver/alpha, and appended boss_import
records to the ingest ledger.

Typical usage:

    # 從 ledger 自動推回 boss_import 的日期範圍（4 個原始 dataset）
    python -m scripts.p1_backfill_ingest_ok_from_ledger ^
      --repo-root C:\\AI\\tw-alpha-stack ^
      --datasets prices chip per dividend ^
      --from-ledger

    # DateID 6 表也一起補齊 .ok
    python -m scripts.p1_backfill_ingest_ok_from_ledger ^
      --repo-root C:\\AI\\tw-alpha-stack ^
      --datasets finstmt bs cfs shareholding inst_total gov_bank ^
      --from-ledger

    # 或是手動指定日期區間（不看 ledger）
    python -m scripts.p1_backfill_ingest_ok_from_ledger ^
      --repo-root C:\\AI\\tw-alpha-stack ^
      --datasets prices chip per dividend finstmt bs cfs shareholding inst_total gov_bank ^
      --start-date 1992-01-01 --end-date 2025-11-21

Notes:
- .ok 檔是一種「最小 checkpoint」，用來告訴 FullMarket / CodeD / others：
  某個 dataset 在某一天的銀河資料已經「補齊」。
- 這個腳本只負責補 .ok，不觸碰銀河 parquet 本身。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


DATASETS_SUPPORTED = (
    # 日期線 4 表
    "prices",
    "chip",
    "per",
    "dividend",
    # DateID 線 6 表
    "finstmt",
    "bs",
    "cfs",
    "shareholding",
    "inst_total",
    "gov_bank",
)


@dataclass(frozen=True)
class DateRange:
    """Inclusive date range [start, end]."""

    start: date
    end: date

    def iter_days(self) -> Iterable[date]:
        current = self.start
        while current <= self.end:
            yield current
            current = current + timedelta(days=1)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def parse_iso_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {s}") from exc


# ---------------------------------------------------------------------------
# Ledger / range building
# ---------------------------------------------------------------------------


def load_boss_import_ranges_from_ledger(
    repo_root: Path,
    datasets: Iterable[str],
) -> Dict[str, DateRange]:
    """Load boss_import ranges for given datasets from ingest_ledger.jsonl.

    Rules:
    - 只看 run_type == "boss_import" 的紀錄。
    - 同一 dataset 若有多筆 boss_import，以「最後一筆」為準（假設 ts 遞增）。
    - 必須同時有 date_start / date_end，且滿足 start <= end。
    """

    ledger_path = repo_root / "metrics" / "ingest_ledger.jsonl"
    if not ledger_path.is_file():
        raise FileNotFoundError(f"Ledger file not found: {ledger_path}")

    wanted = set(datasets)
    latest_by_dataset: Dict[str, Tuple[datetime, DateRange]] = {}

    with ledger_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # ignore non-JSON lines defensively
                continue

            if obj.get("run_type") != "boss_import":
                continue
            ds = obj.get("dataset")
            if ds not in wanted:
                continue

            ds_start = obj.get("date_start")
            ds_end = obj.get("date_end")
            if not ds_start or not ds_end:
                raise ValueError(
                    f"boss_import record for dataset={ds} at line {line_no} "
                    f"missing date_start/date_end"
                )

            start_d = parse_iso_date(ds_start)
            end_d = parse_iso_date(ds_end)
            if start_d > end_d:
                raise ValueError(
                    f"boss_import date range invalid for {ds}: {ds_start} > {ds_end}"
                )

            # 若同一 dataset 有多筆 boss_import，以最後一筆為準（通常也是最新 ts）
            ts_raw = obj.get("ts")
            try:
                ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.min
            except Exception:
                ts = datetime.min

            prev = latest_by_dataset.get(ds)
            if prev is None or ts >= prev[0]:
                latest_by_dataset[ds] = (ts, DateRange(start=start_d, end=end_d))

    if not latest_by_dataset:
        raise RuntimeError(
            f"No boss_import ranges found in ledger for datasets: {', '.join(sorted(wanted))}"
        )

    # Check all requested datasets are covered
    missing = wanted - set(latest_by_dataset.keys())
    if missing:
        raise RuntimeError(
            f"No boss_import ranges found in ledger for datasets: {', '.join(sorted(missing))}"
        )

    return {ds: rng for ds, (_, rng) in latest_by_dataset.items()}


def build_ranges_from_args(
    datasets: Iterable[str],
    start_date_str: Optional[str],
    end_date_str: Optional[str],
) -> Dict[str, DateRange]:
    if not start_date_str or not end_date_str:
        raise ValueError(
            "When --from-ledger is not used, both --start-date and --end-date must be provided."
        )

    start_d = parse_iso_date(start_date_str)
    end_d = parse_iso_date(end_date_str)
    if start_d > end_d:
        raise ValueError(f"Invalid manual date range: {start_d} > {end_d}")

    return {ds: DateRange(start=start_d, end=end_d) for ds in datasets}


# ---------------------------------------------------------------------------
# OK file creation
# ---------------------------------------------------------------------------


def backfill_ok_files_for_dataset(
    repo_root: Path,
    dataset: str,
    dr: DateRange,
) -> None:
    """Create YYYY-MM-DD.ok files for all days in the given range (inclusive)."""

    ok_root = repo_root / "_state" / "ingest" / dataset
    ok_root.mkdir(parents=True, exist_ok=True)

    total = 0
    created = 0

    for d in dr.iter_days():
        total += 1
        name = d.strftime("%Y-%m-%d") + ".ok"
        path = ok_root / name
        if path.exists():
            continue

        path.write_text("0", encoding="utf-8")
        created += 1

    log(
        f"[{dataset}] backfilled {created} new .ok files "
        f"(range {dr.start}..{dr.end}, total days={total})"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill _state/ingest/<dataset>/YYYY-MM-DD.ok from boss_import ledger ranges.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="Root directory of tw-alpha-stack repo.",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=DATASETS_SUPPORTED,
        required=True,
        help=(
            "Datasets to backfill .ok for. "
            "Choose from: " + ", ".join(DATASETS_SUPPORTED)
        ),
    )
    parser.add_argument(
        "--from-ledger",
        action="store_true",
        help=(
            "If set, read date ranges from metrics/ingest_ledger.jsonl "
            "(run_type='boss_import'). "
            "If not set, --start-date and --end-date must be provided."
        ),
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Manual start date (YYYY-MM-DD), used when --from-ledger is not set.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Manual end date (YYYY-MM-DD), used when --from-ledger is not set.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    repo_root = args.repo_root.resolve()
    datasets: List[str] = list(args.datasets)

    log(f"Repo root: {repo_root}")
    log(f"Datasets: {', '.join(datasets)}")

    if args.from_ledger:
        log("Mode: from-ledger (run_type='boss_import').")
        ranges = load_boss_import_ranges_from_ledger(repo_root, datasets)
    else:
        log("Mode: manual range (start-date / end-date).")
        ranges = build_ranges_from_args(
            datasets=datasets,
            start_date_str=args.start_date,
            end_date_str=args.end_date,
        )

    for ds in datasets:
        dr = ranges[ds]
        backfill_ok_files_for_dataset(repo_root=repo_root, dataset=ds, dr=dr)

    log("All datasets completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
