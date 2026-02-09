# alpha_core/io.py
from __future__ import annotations

"""
alpha_core.phase2.corelib.io

共用 I/O 小工具（不碰任何商業邏輯）：

- 目錄建立：ensure_dir
- yyyymm 分區：yyyymm_from_date, factor_partition_dir
- 銀河資料讀取：load_silver_data (New!)
- 因子 parquet 寫入：write_factor_parquet (Fix: no double factor_id nesting)
- JSONL ledger 追加：append_jsonlines
"""

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import json
import os
import pandas as pd


# ---------------------------------------------------------------------------
# 基本工具
# ---------------------------------------------------------------------------


def ensure_dir(path: Path) -> None:
    """Create directory if not exists."""
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    os.replace(tmp, path)


def atomic_write_json(
    path: Path,
    obj: Any,
    *,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
    indent: int = 2,
    sort_keys: bool = False,
) -> None:
    payload = json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent, sort_keys=sort_keys)
    atomic_write_text(path, payload, encoding=encoding)


def yyyymm_from_date(d: date) -> str:
    """Convert a date to 'YYYYMM' string."""
    return f"{d.year:04d}{d.month:02d}"


def factor_partition_dir(factor_root: Path, factor_id: str, d: date) -> Path:
    """
    Get factor partition directory.
    Note: Assuming factor_root is the base folder (e.g. .../alpha/factor).
    If using this helper, ensure inputs are correct.
    """
    return factor_root / factor_id / f"yyyymm={yyyymm_from_date(d)}"


def load_factor_panel(
    factor_root: Path,
    factor_id: str,
    as_of: date,
    window_months: int,
) -> pd.DataFrame:
    """
    Load factor panel from parquet for dependency injection.

    Args:
        factor_root: base path of factor outputs (.../alpha/factor)
        factor_id: dependency factor id (e.g., size_log_mktcap)
        as_of: end date (inclusive)
        window_months: window in months; used to approximate lookback span

    Returns:
        DataFrame with columns at least date, stock_id, factor_value.

    Raises:
        FileNotFoundError / ValueError with context on missing/empty data.
    """
    # Approximate start date by months (~32 days per month)
    approx_days = max(1, int(window_months) * 32)
    start_date = as_of - timedelta(days=approx_days)

    factor_dir = factor_root / factor_id
    if not factor_dir.exists():
        raise FileNotFoundError(
            f"factor panel not found: factor_id={factor_id} dir={factor_dir} as_of={as_of} window_months={window_months}"
        )

    parts = _daterange_to_yyyymm(start_date, as_of)
    frames: List[pd.DataFrame] = []
    for ym in parts:
        part_dir = factor_dir / f"yyyymm={ym}"
        if not part_dir.exists():
            continue
        for p in part_dir.glob("*.parquet"):
            try:
                df_part = pd.read_parquet(p)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"failed to read parquet {p} for factor_id={factor_id} as_of={as_of} window_months={window_months}: {exc}"
                ) from exc
            frames.append(df_part)

    if not frames:
        raise FileNotFoundError(
            f"no parquet partitions for factor_id={factor_id} in {factor_dir} covering yyyymm={parts} as_of={as_of} window_months={window_months}"
        )

    df = pd.concat(frames, ignore_index=True)
    if "date" not in df.columns:
        raise ValueError(
            f"factor_id={factor_id} parquet missing 'date' column as_of={as_of} window_months={window_months}"
        )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    df = df[df["date"] <= pd.Timestamp(as_of)]
    df = df[df["date"] >= pd.Timestamp(start_date)]

    if df.empty:
        raise ValueError(
            f"factor_id={factor_id} panel empty after filtering to start={start_date} end={as_of} window_months={window_months}"
        )

    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].astype(str)

    # Normalize factor_value column
    if "factor_value" not in df.columns:
        if "value" in df.columns:
            df = df.rename(columns={"value": "factor_value"})
        else:
            raise ValueError(
                f"factor_id={factor_id} panel missing factor_value column as_of={as_of} window_months={window_months}"
            )

    return df.reset_index(drop=True)[["date", "stock_id", "factor_value"]]


# ---------------------------------------------------------------------------
# 銀河資料讀取 (Silver Reader)
# ---------------------------------------------------------------------------


def _daterange_to_yyyymm(start_date: date, end_date: date) -> List[str]:
    """產生從 start_date 到 end_date 涵蓋的所有 yyyymm 字串列表。"""
    months = []
    # 簡單迭代：從 start 的第一天開始，每次加 32 天取下個月，直到超過 end
    curr = date(start_date.year, start_date.month, 1)
    # 轉成 YYYYMM int 比較比較簡單，或者用 date 比較
    # 這裡用 date 迭代邏輯
    while True:
        # 當前月份加入
        months.append(yyyymm_from_date(curr))
        
        # 檢查是否已經超過 end_date 的月份
        # 產生下個月 1 號
        next_month = curr + timedelta(days=32)
        next_month = date(next_month.year, next_month.month, 1)
        
        if curr > end_date:
            break
        if yyyymm_from_date(curr) == yyyymm_from_date(end_date):
            break
            
        curr = next_month
    
    return sorted(list(set(months)))


def load_silver_data(
    root: Path,
    dataset: str,
    start_date: date,
    end_date: date,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    從 datahub/silver/alpha/<dataset> 讀取指定日期範圍的資料。
    
    Returns:
        Flat DataFrame (不設 Index)，包含 date (datetime.date) 欄位，已篩選 >= start_date 且 <= end_date。
    """
    silver_root = root / "datahub" / "silver" / "alpha" / dataset
    if not silver_root.exists():
        return pd.DataFrame()

    needed_ym = _daterange_to_yyyymm(start_date, end_date)
    frames = []

    for ym in needed_ym:
        part_dir = silver_root / f"yyyymm={ym}"
        if not part_dir.exists():
            continue
            
        for p_file in part_dir.glob("*.parquet"):
            try:
                # 傳入 columns 進行 IO 裁剪
                df_part = pd.read_parquet(p_file, columns=columns)
                if not df_part.empty:
                    frames.append(df_part)
            except Exception:
                pass

    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)
    
    # 標準化 date 並過濾
    if "date" in df_all.columns:
        # 確保轉成 datetime.date 進行比較
        date_series = pd.to_datetime(df_all["date"], errors="coerce").dt.date
        mask = (date_series >= start_date) & (date_series <= end_date)
        df_all = df_all.loc[mask].copy()
        # 確保回傳的 date column 是 object(date) 或 datetime，方便後續處理
        # 這裡保持 original dtype 或是 datetime64
        # 但為了 impl 方便，通常維持讀進來的 datetime64[ns]
    
    # 確保 stock_id 為字串
    if "stock_id" in df_all.columns:
        df_all["stock_id"] = df_all["stock_id"].astype(str)

    return df_all.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Parquet 寫入
# ---------------------------------------------------------------------------


def _normalize_date_series(s: pd.Series) -> pd.Series:
    """把欄位轉成 datetime64[ns] 後再取 .dt.date。"""
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

    Args:
        factor_root: 因子特定目錄 (例如 .../silver/alpha/factor/mom_6m)
        factor_id: 因子名稱 (用作 metadata 或檔名)
    
    Returns:
        (rows_written, written_paths)
    """
    if df is None or df.empty:
        return 0, []

    df_to_write = df.copy()
    # 確保是 Flat DataFrame
    if isinstance(df_to_write.index, (pd.MultiIndex, pd.DatetimeIndex)):
        df_to_write.reset_index(inplace=True)

    if date_column not in df_to_write.columns:
        raise ValueError(f"DataFrame must contain column {date_column!r}")

    df_to_write[date_column] = _normalize_date_series(df_to_write[date_column])
    df_to_write["_yyyymm"] = df_to_write[date_column].apply(yyyymm_from_date)

    written_paths: List[Path] = []
    total_rows = len(df_to_write)

    # 寫入邏輯：直接在 factor_root 下建立 yyyymm=...
    for yyyymm, sub in df_to_write.groupby("_yyyymm"):
        part_dir = factor_root / f"yyyymm={yyyymm}"
        ensure_dir(part_dir)
        
        # 檔名使用固定 data.parquet 方便讀取，或使用 factor_id 避免混淆
        # 這裡採用標準 data.parquet (符合銀河 Data Lake 規範)
        path = part_dir / "data.parquet"

        new_data = sub.drop(columns=["_yyyymm"])

        # 簡單的覆蓋邏輯 (Idempotent: 讀舊+新 -> 去重 -> 寫)
        if path.exists():
            try:
                old = pd.read_parquet(path)
                combined = pd.concat([old, new_data], axis=0, ignore_index=True)
                # 假設 date + stock_id 是 unique key
                subset = ["date", "stock_id"] if "stock_id" in combined.columns else ["date"]
                combined = combined.drop_duplicates(subset=subset, keep="last")
            except Exception:
                # 舊檔壞掉就覆蓋
                combined = new_data
        else:
            combined = new_data

        combined.to_parquet(path, index=False)
        written_paths.append(path)

    return total_rows, written_paths


def append_jsonlines(path: Path, records: Sequence[Mapping[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            line = json.dumps(rec, ensure_ascii=False, default=str)
            f.write(line)
            f.write("\n")
