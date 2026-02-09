from __future__ import annotations

"""
alpha_core.phase2.corelib.factor_eval

Phase-2 因子評估核心函式庫（SSOT）。

職責：
- 定義因子評估 JSON 的資料結構（FactorEval / WindowMetrics）。
- 產生 / 更新 reports/factor_eval/<factor_id>_summary.json。
- 提供 evaluate_single_factor / evaluate_factors 兩個 API 給 CLI 或其他工具呼叫。

特性：
- 不處理 CLI / argparse / sys.exit。
- 盡量維持 deterministic / idempotent：同一組輸入重跑結果一致。
"""

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCHEMA_VERSION = "factor_eval.v1.1"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class WindowMetrics:
    """單一 WF 視窗下的指標集合。

    註：seed 階段大多欄位仍可為 None，
    但欄位名稱要先固定，供後續 Gate / SLO 使用。
    """

    rank_ic: Optional[float] = None
    rank_ic_std: Optional[float] = None
    ic: Optional[float] = None
    ic_std: Optional[float] = None
    psr: Optional[float] = None
    dsr: Optional[float] = None
    turnover: Optional[float] = None
    coverage: Optional[float] = None
    coverage_ratio: Optional[float] = None
    coverage_count: Optional[float] = None
    max_corr: Optional[float] = None
    max_dd: Optional[float] = None
    sample_days: Optional[int] = None

    # 過渡期兼容欄位（舊版可能使用 *_mean）
    rank_ic_mean: Optional[float] = None
    ic_mean: Optional[float] = None


@dataclass
class FactorEval:
    """單一因子的評估結果外框結構。"""

    factor_id: str
    schema_version: str
    as_of: Optional[str]
    created_at: str
    updated_at: str
    windows: Dict[str, Dict[str, Any]]  # window -> metrics dict


# ---------------------------------------------------------------------------
# Helpers (generic)
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_window_block() -> Dict[str, Any]:
    """建立單一視窗的欄位結構，預設全為 None。"""
    return asdict(WindowMetrics())


def build_factor_eval_skeleton(
    factor_id: str,
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
    schema_version: str = SCHEMA_VERSION,
) -> FactorEval:
    """建立單一因子的 eval JSON skeleton。"""
    now = _utc_now_iso()
    windows: Dict[str, Dict[str, Any]] = {}

    for w in wf_windows:
        key = str(int(w))
        windows[key] = init_window_block()

    return FactorEval(
        factor_id=factor_id,
        schema_version=schema_version,
        as_of=as_of,
        created_at=now,
        updated_at=now,
        windows=windows,
    )


def load_existing_eval_if_any(path: Path) -> Optional[Dict[str, Any]]:
    """若 eval JSON 已存在，讀取後回傳 dict；若不存在則回傳 None。"""
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # 保守處理：讀不到既有檔案就當作不存在，由新 skeleton 覆蓋。
        return None


def merge_existing_into_skeleton(
    skeleton: FactorEval,
    existing: Mapping[str, Any],
) -> FactorEval:
    """在合理範圍內，將既有資料 merge 回新 skeleton。

    規則（保守）：
    - 若 existing.schema_version 與目前不同，仍嘗試帶入 windows 內的數值。
    - 只對 windows 區塊嘗試比對與覆蓋，不處理其他自訂欄位。
    - 若 existing.windows 使用 "6" / "12" 這類 key，會直接複製其內部欄位；
      seed 階段不做進一步轉換，由 compose_factors_to_wf 的 fallback 處理即可。
    """
    sk_dict = asdict(skeleton)
    windows = sk_dict.get("windows", {})

    existing_windows = existing.get("windows", {}) if isinstance(existing, Mapping) else {}
    if isinstance(existing_windows, Mapping):
        for k, v in existing_windows.items():
            if not isinstance(v, Mapping):
                continue
            if k in windows:
                merged = windows[k].copy()
                for mk, mv in v.items():
                    merged[mk] = mv
                windows[k] = merged
            else:
                windows[k] = dict(v)

    sk_dict["windows"] = windows
    sk_dict["updated_at"] = _utc_now_iso()
    return FactorEval(**sk_dict)


# ---------------------------------------------------------------------------
# Internal helpers for metrics computation
# ---------------------------------------------------------------------------

_HORIZON_DAYS_DEFAULT = 5  # 約一週的交易日數（以 trading-day shift 為主）


def _parse_as_of(as_of: Optional[str], factor_df: Optional[pd.DataFrame]) -> Optional[pd.Timestamp]:
    """將 as_of 字串轉成 Timestamp；若沒給則用 factor_df 的最大日期。"""
    if as_of:
        try:
            ts = pd.to_datetime(as_of)
            return pd.Timestamp(ts.date())
        except Exception:
            pass

    if factor_df is not None and not factor_df.empty and "date" in factor_df.columns:
        ts = pd.to_datetime(factor_df["date"]).max()
        return pd.Timestamp(ts.date())

    return None


def _load_factor_frame(root: Path, factor_id: str) -> pd.DataFrame:
    """載入指定因子的 parquet，並標準化欄位名稱。"""
    factor_root = root / "datahub" / "silver" / "alpha" / "factor" / factor_id
    files = sorted(factor_root.glob("yyyymm=*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"no factor parquet found for {factor_id!r} under {factor_root}")

    frames: List[pd.DataFrame] = []
    for p in files:
        df = pd.read_parquet(p)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        raise ValueError(f"factor parquet files for {factor_id!r} are all empty")

    df_all = pd.concat(frames, ignore_index=True)

    if "date" not in df_all.columns:
        raise ValueError(f"factor {factor_id!r} is missing 'date' column: {list(df_all.columns)}")
    df_all["date"] = pd.to_datetime(df_all["date"])

    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in df_all.columns:
            stock_col = cand
            break
    if stock_col is None:
        raise ValueError(f"factor {factor_id!r} is missing stock-id column: {list(df_all.columns)}")
    if stock_col != "stock_id":
        df_all.rename(columns={stock_col: "stock_id"}, inplace=True)

    if "factor_value" not in df_all.columns:
        for cand in ("value", "factor", factor_id):
            if cand in df_all.columns:
                df_all.rename(columns={cand: "factor_value"}, inplace=True)
                break
    if "factor_value" not in df_all.columns:
        raise ValueError(
            f"factor {factor_id!r} is missing 'factor_value' column: {list(df_all.columns)}"
        )

    df_all = df_all.dropna(subset=["date", "stock_id", "factor_value"])
    return df_all[["date", "stock_id", "factor_value"]]


def _load_prices(root: Path) -> pd.DataFrame:
    """載入 Phase-1 prices 銀河表並標準化欄位名稱。"""
    data_root = root / "datahub" / "silver" / "alpha" / "prices"
    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"no prices parquet found under {data_root}")

    frames: List[pd.DataFrame] = []
    for p in files:
        df = pd.read_parquet(p)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        raise ValueError("prices parquet files are all empty")

    prices = pd.concat(frames, ignore_index=True)

    if "date" not in prices.columns:
        raise ValueError(f"prices is missing 'date' column: {list(prices.columns)}")
    prices["date"] = pd.to_datetime(prices["date"])

    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in prices.columns:
            stock_col = cand
            break
    if stock_col is None:
        raise ValueError(f"prices is missing stock-id column: {list(prices.columns)}")
    if stock_col != "stock_id":
        prices.rename(columns={stock_col: "stock_id"}, inplace=True)

    price_col = None
    for cand in ("adj_close", "close", "Close", "price"):
        if cand in prices.columns:
            price_col = cand
            break
    if price_col is None:
        raise ValueError(
            f"prices is missing price column (adj_close/close/...): {list(prices.columns)}"
        )
    if price_col != "adj_close":
        prices.rename(columns={price_col: "adj_close"}, inplace=True)

    prices = prices.dropna(subset=["date", "stock_id", "adj_close"])

    return prices[["date", "stock_id", "adj_close"]]


def _compute_target_returns(
    prices: pd.DataFrame,
    horizon_days: int,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """
    以「交易日數 horizon_days」計算 horizon return：

        target_return = log( P_{t+h} / P_t )

    並確保未使用 as_of 之後的未來價格：
      - future_date = date.shift(-horizon_days)
      - 僅保留 future_date <= as_of
    """
    if prices.empty:
        raise ValueError("prices is empty, cannot compute target_return")

    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"])

    frames: List[pd.DataFrame] = []
    for stock_id, g in prices.groupby("stock_id", group_keys=False):
        g = g.sort_values("date").copy()
        g["future_price"] = g["adj_close"].shift(-horizon_days)
        g["future_date"] = g["date"].shift(-horizon_days)
        mask = (
            (g["adj_close"] > 0)
            & (g["future_price"] > 0)
            & g["future_date"].notna()
            & (g["future_date"] <= as_of)
        )
        g = g.loc[mask].copy()
        if g.empty:
            continue
        g["target_return"] = np.log(g["future_price"] / g["adj_close"])
        frames.append(g[["date", "stock_id", "target_return"]])

    if not frames:
        raise ValueError("no valid target_return samples after filtering by horizon/as_of")

    df_ret = pd.concat(frames, ignore_index=True)
    df_ret = df_ret.dropna(subset=["target_return"])

    return df_ret[["date", "stock_id", "target_return"]]


def _compute_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    對每個 date 計算：
      - ic        : Pearson corr(factor_value, target_return)
      - rank_ic   : Spearman corr
      - coverage_ratio: 有效樣本占比
      - coverage_count: 橫斷面有效樣本數
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    records: List[Dict[str, Any]] = []

    for dt, g in df.groupby("date", as_index=False):
        base_n = len(g)
        g = g.dropna(subset=["factor_value", "target_return"])
        n = len(g)
        coverage_ratio = float(n / base_n) if base_n > 0 else 0.0
        coverage_ratio = max(0.0, min(coverage_ratio, 1.0))
        coverage_count = int(n)

        if n < 2:
            ic = np.nan
            rank_ic = np.nan
        else:
            x = g["factor_value"].astype("float64")
            y = g["target_return"].astype("float64")
            # 常數列視為無效
            if x.nunique(dropna=True) <= 1 or y.nunique(dropna=True) <= 1:
                ic = np.nan
                rank_ic = np.nan
            else:
                ic = x.corr(y)
                rx = x.rank(method="average")
                ry = y.rank(method="average")
                rank_ic = rx.corr(ry)

        records.append(
            {
                "date": dt,
                "ic": float(ic) if ic is not None else np.nan,
                "rank_ic": float(rank_ic) if rank_ic is not None else np.nan,
                "coverage_ratio": coverage_ratio,
                "coverage_count": coverage_count,
            }
        )

    if not records:
        return pd.DataFrame(columns=["date", "ic", "rank_ic", "coverage_ratio", "coverage_count"])

    out = pd.DataFrame.from_records(records)
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values("date").reset_index(drop=True)
    return out


def _aggregate_window(
    daily: pd.DataFrame,
    window_months: int,
    as_of: pd.Timestamp,
) -> Dict[str, Any]:
    """
    將 daily stats 在「最後 window_months 個月」的區間內彙總。

    這裡用粗略換算：1 個月 ≈ 21 交易日。
    """
    def _empty_window() -> Dict[str, Any]:
        return {
            "ic_mean": None,
            "ic_std": None,
            "rank_ic_mean": None,
            "rank_ic_std": None,
            "coverage_ratio": None,
            "coverage": None,
            "coverage_count": None,
            "sample_days": 0,
        }

    if daily.empty:
        return _empty_window()

    as_of_ts = pd.to_datetime(as_of)
    approx_days = int(21 * window_months)
    start_ts = as_of_ts - timedelta(days=approx_days)

    win = daily.loc[(daily["date"] > start_ts) & (daily["date"] <= as_of_ts)].copy()
    if win.empty:
        return _empty_window()

    def _m(series: pd.Series) -> Optional[float]:
        s = series.dropna()
        if s.empty:
            return None
        return float(s.mean())

    def _s(series: pd.Series) -> Optional[float]:
        s = series.dropna()
        if s.empty:
            return None
        return float(s.std(ddof=1)) if len(s) > 1 else 0.0

    # 僅以有效 ic/rank_ic 的天數作為 sample_days
    valid_mask = win["rank_ic"].notna() | win["ic"].notna()
    sample_days = int(valid_mask.sum())
    if sample_days == 0:
        return _empty_window()

    coverage_ratio = _m(win["coverage_ratio"])
    if coverage_ratio is not None:
        coverage_ratio = max(0.0, min(coverage_ratio, 1.0))
    coverage_count_mean = _m(win["coverage_count"])

    return {
        "ic_mean": _m(win["ic"]),
        "ic_std": _s(win["ic"]),
        "rank_ic_mean": _m(win["rank_ic"]),
        "rank_ic_std": _s(win["rank_ic"]),
        "coverage_ratio": coverage_ratio,
        "coverage": coverage_ratio,
        "coverage_count": coverage_count_mean,
        "sample_days": sample_days,
    }


def _compute_factor_metrics(
    root: Path,
    factor_id: str,
    wf_windows: Sequence[int],
    as_of_str: Optional[str],
) -> Tuple[Dict[str, Dict[str, Any]], Optional[date]]:
    """
    實際計算因子在各個 WF 視窗下的統計指標。

    回傳：
        (metrics_by_window, resolved_as_of_date)

        metrics_by_window: { "6": {...}, "12": {...}, ... }
    """
    root = root.resolve()
    factor_df = _load_factor_frame(root, factor_id)
    as_of_ts = _parse_as_of(as_of_str, factor_df=factor_df)
    if as_of_ts is None:
        raise ValueError(f"cannot resolve as_of for factor {factor_id!r}")

    # 僅保留 as_of 之前的因子暴露
    factor_df = factor_df.loc[factor_df["date"] <= as_of_ts].copy()
    if factor_df.empty:
        raise ValueError(f"no factor samples before as_of for {factor_id!r}")

    prices = _load_prices(root)
    target_ret = _compute_target_returns(
        prices=prices,
        horizon_days=_HORIZON_DAYS_DEFAULT,
        as_of=as_of_ts,
    )

    merged = factor_df.merge(target_ret, on=["date", "stock_id"], how="inner")
    if merged.empty:
        raise ValueError(f"no overlap between factor and target_return for {factor_id!r}")

    daily = _compute_daily_stats(merged)

    metrics_by_window: Dict[str, Dict[str, Any]] = {}
    for w in wf_windows:
        w_int = int(w)
        key = str(w_int)
        agg = _aggregate_window(daily, window_months=w_int, as_of=as_of_ts)

        sample_days = agg.get("sample_days") or 0
        if sample_days <= 0:
            metrics_by_window[key] = {
                "ic": None,
                "ic_mean": None,
                "ic_std": None,
                "rank_ic": None,
                "rank_ic_mean": None,
                "rank_ic_std": None,
                "coverage": None,
                "coverage_ratio": None,
                "coverage_count": None,
                "sample_days": 0,
            }
            continue

        # 只填我們目前真的算出來的欄位，其餘維持 None
        metrics_by_window[key] = {
            "ic": agg["ic_mean"],
            "ic_mean": agg["ic_mean"],
            "ic_std": agg["ic_std"],
            "rank_ic": agg["rank_ic_mean"],
            "rank_ic_mean": agg["rank_ic_mean"],
            "rank_ic_std": agg["rank_ic_std"],
            "coverage": agg["coverage"],
            "coverage_ratio": agg.get("coverage_ratio", agg["coverage"]),
            "coverage_count": agg.get("coverage_count"),
            "sample_days": sample_days,
        }

    return metrics_by_window, as_of_ts.date()


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------


def evaluate_single_factor(
    root: Path,
    factor_id: str,
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
) -> FactorEval:
    """評估單一因子並回傳 FactorEval 物件。

    目前版本：
      - 先建立 FactorEval skeleton。
      - 若已有舊版 JSON，盡量 merge 其 windows 內容。
      - 嘗試計算實際指標（ic / rank_ic / coverage），填入各 window。
      - 若計算失敗（例如資料缺失），則保留 skeleton / 既有數值。
    """
    root = root.resolve()
    reports_dir = root / "reports" / "factor_eval"
    reports_dir.mkdir(parents=True, exist_ok=True)

    path = reports_dir / f"{factor_id}_summary.json"

    windows_list: List[int] = [int(w) for w in wf_windows]

    # 1) 建立 skeleton + merge 舊檔
    skeleton = build_factor_eval_skeleton(
        factor_id=factor_id,
        wf_windows=windows_list,
        as_of=as_of,
    )

    existing = load_existing_eval_if_any(path)
    if existing is not None:
        eval_obj = merge_existing_into_skeleton(skeleton, existing)
    else:
        eval_obj = skeleton

    eval_dict = asdict(eval_obj)

    # 2) 嘗試計算實際指標；若失敗則保留 skeleton / 既有值
    metrics_by_window: Dict[str, Dict[str, Any]] = {}
    resolved_as_of: Optional[date] = None

    try:
        metrics_by_window, resolved_as_of = _compute_factor_metrics(
            root=root,
            factor_id=factor_id,
            wf_windows=windows_list,
            as_of_str=as_of,
        )
    except Exception:
        # best-effort：不要讓整體流程炸掉，交給呼叫端決定是否檢查欄位為 None
        metrics_by_window = {}
        resolved_as_of = None

    if metrics_by_window:
        windows_block: Dict[str, Dict[str, Any]] = eval_dict.get("windows", {}) or {}
        for key, m in metrics_by_window.items():
            block = windows_block.get(key, init_window_block())
            # 只覆寫我們計算出的欄位，避免清掉舊檔中其他指標（例如 psr / dsr）
            for mk, mv in m.items():
                block[mk] = mv
            windows_block[key] = block
        eval_dict["windows"] = windows_block

    # 3) 更新 as_of / updated_at
    if resolved_as_of is not None and not as_of:
        eval_dict["as_of"] = resolved_as_of.isoformat()
    else:
        eval_dict["as_of"] = as_of

    eval_dict["schema_version"] = SCHEMA_VERSION
    eval_dict["updated_at"] = _utc_now_iso()

    result = FactorEval(**eval_dict)

    with path.open("w", encoding="utf-8") as f:
        json.dump(asdict(result), f, ensure_ascii=False, indent=2, sort_keys=True)

    return result


def evaluate_factors(
    root: Path,
    factor_ids: Iterable[str],
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
) -> Dict[str, Path]:
    """對多個 factor_id 進行評估，產生/更新 eval JSON。

    回傳:
        { factor_id: eval_json_path }
    """
    root = root.resolve()
    result: Dict[str, Path] = {}
    for factor_id in factor_ids:
        factor_id = str(factor_id).strip()
        if not factor_id:
            continue

        evaluate_single_factor(
            root=root,
            factor_id=factor_id,
            wf_windows=wf_windows,
            as_of=as_of,
        )
        eval_path = root / "reports" / "factor_eval" / f"{factor_id}_summary.json"
        result[factor_id] = eval_path

    return result
