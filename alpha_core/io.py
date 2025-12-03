# C:\AI\tw-alpha-stack\alpha_core\io.py
from __future__ import annotations

"""
alpha_core.io

共用 I/O 小工具（不碰任何商業邏輯）：

- 目錄建立：ensure_dir
- yyyymm 分區：yyyymm_from_date, factor_partition_dir
- 因子 parquet 寫入（依 yyyymm 分區，覆寫式、去重）：write_factor_parquet
- JSONL ledger 追加：append_jsonlines
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

import json

import pandas as pd


# ---------------------------------------------------------------------------
# 基本工具
# ---------------------------------------------------------------------------


def ensure_dir(path: Path) -> None:
    """
    Create directory if not exists (parents=True, exist_ok=True).
    """
    path.mkdir(parents=True, exist_ok=True)


def yyyymm_from_date(d: date) -> str:
    """
    Convert a date to 'YYYYMM' string.
    """
    return f"{d.year:04d}{d.month:02d}"


def factor_partition_dir(factor_root: Path, factor_id: str, d: date) -> Path:
    """
    Get factor partition directory:

        <factor_root>/<factor_id>/yyyymm=YYYYMM
    """
    return factor_root / factor_id / f"yyyymm={yyyymm_from_date(d)}"


# ---------------------------------------------------------------------------
# Parquet 寫入（因子層用）
# ---------------------------------------------------------------------------


def _normalize_date_series(s: pd.Series) -> pd.Series:
    """
    把欄位轉成 datetime64[ns] 後再取 .dt.date。
    """
    if not pd.api.types.is_datetime64_any_dtype(s):
        s = pd.to_datetime(s, errors="raise")
    return s.dt.date


def write_factor_parquet(
    df: pd.DataFrame,
    factor_root: Path,
    factor_id: str,
    run_id: str,
    date_column: str = "date",
) -> Tuple[int, List[Path]]:
    """
    依 yyyymm 分區寫入因子 parquet 檔案。

    規則：
    - 必須含有 date_column（預設 'date'）；會被轉成 datetime.date。
    - 分區路徑：<factor_root>/<factor_id>/yyyymm=YYYYMM/data.parquet
    - 若 data.parquet 已存在：
        - 讀舊檔 → 與新資料 concat → drop_duplicates → 覆寫。
    - 若原始 df 為空，不寫任何檔案，回傳 (0, []).

    回傳：
        (寫入的列數（原始 df 的列數）, [實際被觸及的檔案路徑列表])
    """
    if df is None or df.empty:
        return 0, []

    if date_column not in df.columns:
        raise ValueError(f"DataFrame must contain column {date_column!r} for partitioning.")

    df = df.copy()

    # 正規化 date 欄位 → datetime.date
    df[date_column] = _normalize_date_series(df[date_column])
    df["_yyyymm"] = df[date_column].apply(yyyymm_from_date)

    written_paths: List[Path] = []
    total_rows = len(df)

    # 一個 yyyymm 一個檔（覆寫式，保證 idempotent + 去重）
    for yyyymm, sub in df.groupby("_yyyymm"):
        part_dir = factor_root / factor_id / f"yyyymm={yyyymm}"
        ensure_dir(part_dir)
        path = part_dir / "data.parquet"

        new_data = sub.drop(columns=["_yyyymm"])

        if path.exists():
            old = pd.read_parquet(path)
            combined = pd.concat([old, new_data], axis=0, ignore_index=True)
            combined = combined.drop_duplicates()
        else:
            combined = new_data

        combined.to_parquet(path, index=False)
        written_paths.append(path)

    return total_rows, written_paths


# ---------------------------------------------------------------------------
# JSONL / ledger 寫入
# ---------------------------------------------------------------------------


def append_jsonlines(path: Path, records: Sequence[Mapping[str, Any]]) -> None:
    """
    Append records to a JSON Lines (jsonl) file.

    - 每個 record 會被 json.dumps 後加上換行。
    - 目錄不存在會自動建立。
    - date / datetime 等無法直接序列化的型別，透過 default=str 處理。
    """
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            line = json.dumps(rec, ensure_ascii=False, default=str)
            f.write(line)
            f.write("\n")
