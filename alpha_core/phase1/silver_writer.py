from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import time

import pandas as pd


@dataclass
class WriteStats:
    output_path: Path
    rows_in: int
    rows_before: int
    rows_after: int

    @property
    def rows_added(self) -> int:
        return max(0, self.rows_after - self.rows_before)


def compute_hhf_path(dataset: str, day_str: str, datahub_root: Path) -> Path:
    yyyymm = day_str.replace("-", "")[:6]
    out_dir = datahub_root / "silver" / "alpha" / dataset / f"yyyymm={yyyymm}"
    return out_dir / "data.parquet"


def compute_dateid_path(
    dataset: str,
    day_str: str,
    datahub_root: Path,
    output_root: Path,
) -> Path:
    yyyymm = day_str.replace("-", "")[:6]
    base_root = datahub_root / output_root
    out_dir = base_root / f"yyyymm={yyyymm}"
    return out_dir / f"{dataset}_{day_str}.parquet"


def _tmp_path(out_path: Path) -> Path:
    stamp = f"{int(time.time())}.{os.getpid()}"
    return out_path.with_name(f"{out_path.name}.tmp.{stamp}")


def write_parquet_atomic(
    df: pd.DataFrame,
    out_path: Path,
    *,
    dedupe_keys: list[str] | None = None,
    sort_keys: list[str] | None = None,
) -> WriteStats:
    if df is None or df.empty:
        return WriteStats(output_path=out_path, rows_in=0, rows_before=0, rows_after=0)

    rows_in = int(len(df))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_before = 0
    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
        except Exception:
            existing = pd.DataFrame()
        if existing is not None and not existing.empty:
            rows_before = int(len(existing))
            df = pd.concat([existing, df], ignore_index=True)

    if dedupe_keys:
        df = df.drop_duplicates(subset=dedupe_keys, keep="last")
    else:
        df = df.drop_duplicates()

    if sort_keys:
        sort_cols = [c for c in sort_keys if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols)

    rows_after = int(len(df))
    tmp_path = _tmp_path(out_path)
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)

    return WriteStats(
        output_path=out_path,
        rows_in=rows_in,
        rows_before=rows_before,
        rows_after=rows_after,
    )


def validate_minimum_output(out_path: Path, rows_written: int) -> None:
    if rows_written <= 0:
        return
    if not out_path.exists():
        raise RuntimeError(f"expected output missing: {out_path}")
