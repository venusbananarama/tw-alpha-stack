# alpha_core/factor_impl/value_impl.py
# -*- coding: utf-8 -*-
"""
Value factor implementation (PE / CFY).

目標：
- value_pe：由 Phase-1 的 per 銀河表（含 date / stock_id / 某種 PE 欄位）計算 value 因子。
- value_cfy：由 prices / per / cfs 長表計算現金流殖利率類因子，供 Phase-2 使用。
- 不動 Gate / SLO 憲法，只把因子本身算得乾淨、穩定。

設計：
- value_pe：
    - 先從 per 裡找出「PE 類」欄位（自動偵測常見命名）。
    - 把 PE 映射成 value_raw = 1 / PE。
    - 依每個 date 做 winsorize（1% / 99%）+ 橫斷面 z-score，輸出 factor_value。
- value_cfy：
    - 優先路線：用 cfs 長表抽出 CFO，計算 CFO_TTM / 市值，做橫斷面 winsor + z-score。
    - 若缺 cfs 或 CFO，退而求其次用「每股現金流 / 價格」或「總額現金流 / shares / 價格」。
"""

from __future__ import annotations

import re
import logging
from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from alpha_core.factor_xform import apply_xsection_xform

# _get_shares_column 目前僅存在於 size_impl，但此模組在部分環境可能缺少該 helper。
# 以 try-import + fallback 方式確保 value_cfy 不因缺函式而無法載入。
try:
    from .size_impl import _get_shares_column  # type: ignore
except Exception:  # noqa: BLE001
    def _get_shares_column(df: pd.DataFrame) -> Optional[str]:
        """
        Fallback shares 欄位探測：嘗試從價格表中尋找股數或成交量 proxy 欄位。
        回傳欄位名稱或 None，不在 import 階段 raise，避免阻斷因子載入。
        """
        candidates = [
            "shares",
            "shares_outstanding",
            "share_outstanding",
            "shares_out",
            "outstanding_shares",
            "TotalShares",
            "total_shares",
            "Trading_Volume",
            "trading_volume",
            "volume",
        ]
        for col in candidates:
            if col in df.columns:
                return col
        return None


# ---------------------------------------------------------------------------
# 共用 helper：PE / 現金流 / 價格 / 市值 欄位解析
# ---------------------------------------------------------------------------

_PE_TOKEN_RE = re.compile(r"(^|[^a-z0-9])pe([^a-z0-9]|$)")
_PER_TOKEN_RE = re.compile(r"(^|[^a-z0-9])per([^a-z0-9]|$)")


def _pick_pe_column(df: pd.DataFrame) -> str:
    """
    Select a PE-like column from per dataframe.

    Priority:
    1) Exact candidates (case-insensitive)
    2) Fuzzy match with token-like 'pe' or 'per'
    """
    lower_map = {c.lower(): c for c in df.columns}

    candidates: List[str] = [
        "pe",
        "per",
        "pe_ttm",
        "per_ttm",
        "pe_ratio",
        "peratio",
        "pe1",
        "p/e",
        "本益比",
        "本益比(倍)",
    ]

    for name in candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]

    pe_like: List[str] = []
    for orig in df.columns:
        lower = orig.lower()
        if "%" in lower:
            continue
        if lower in {
            "open",
            "close",
            "high",
            "low",
            "adj_close",
            "price",
            "volume",
            "turnover",
            "amount",
        }:
            continue
        if "per_share" in lower or "per-share" in lower:
            continue
        if "percent" in lower or "percentage" in lower or "performance" in lower:
            continue
        if _PE_TOKEN_RE.search(lower) or _PER_TOKEN_RE.search(lower):
            pe_like.append(orig)

    if len(pe_like) == 1:
        return pe_like[0]
    if len(pe_like) > 1:
        pe_like_sorted = sorted(pe_like, key=lambda s: (len(s), s.lower()))
        return pe_like_sorted[0]

    raise ValueError(
        "Unable to locate PE-like column in per dataframe. "
        f"Columns={list(df.columns)}"
    )


def _resolve_pe_column(df: pd.DataFrame) -> str:
    """Backward-compatible alias for PE column selection."""
    return _pick_pe_column(df)


def _cs_winsorized_zscore(
    x: pd.Series,
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
) -> pd.Series:
    """
    單一橫斷面序列做 winsorize + z-score。

    - winsorize：依 quantile 截斷極端值。
    - z-score  ：(x - mean) / std；std 為 0 時回傳 0。
    """
    arr = x.to_numpy(dtype="float64")

    if arr.size == 0:
        return pd.Series(np.nan, index=x.index)

    lo = np.nanquantile(arr, lower_quantile)
    hi = np.nanquantile(arr, upper_quantile)

    arr = np.clip(arr, lo, hi)

    mu = np.nanmean(arr)
    sigma = np.nanstd(arr)

    if not np.isfinite(sigma) or sigma == 0.0:
        z = np.zeros_like(arr)
    else:
        z = (arr - mu) / sigma

    return pd.Series(z, index=x.index)


def _resolve_price_column(df: pd.DataFrame) -> str:
    """
    嘗試從價格表找出價格欄位。
    """
    for col in ["adj_close", "close", "Close", "price"]:
        if col in df.columns:
            return col
    raise KeyError(
        "Price column not found in dataframe. "
        f"columns={list(df.columns)}"
    )


def _resolve_cfy_column(df: pd.DataFrame) -> str:
    """
    嘗試從 per 資料找出「每股現金流」欄位。
    """
    lower_map = {c.lower(): c for c in df.columns}
    candidates = [
        "cashflowpershare",
        "cash_flow_per_share",
        "cashflow_per_share",
        "operating_cash_flow_per_share",
        "operatingcashflowpershare",
        "cf_ps",
        "cf_yield_ps",
        "cfps",
        "ocfps",
        "cfo_per_share",
        "ocf_per_share",
    ]
    for name in candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]

    raise KeyError(
        "Unable to locate cash-flow-per-share column. "
        f"Columns={list(df.columns)}"
    )


def _resolve_cf_total_column(df: pd.DataFrame) -> str:
    """
    嘗試從 per 或 cfs 資料找出「總額現金流」欄位（作為每股現金流的 fallback）。
    """
    lower_map = {c.lower(): c for c in df.columns}
    candidates = [
        "operating_cash_flow",
        "operating_cf",
        "ocf",
        "ocf_ttm",
        "cfo",
        "cash_from_operations",
        "cashflow_from_operations",
        "free_cash_flow",
        "fcf",
    ]
    for name in candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]

    raise KeyError(
        "Unable to locate cash-flow total column. "
        f"Columns={list(df.columns)}"
    )


def _resolve_market_cap_column(df: pd.DataFrame) -> Optional[str]:
    """
    嘗試從 per 資料找出市值欄位。
    找不到時回傳 None（讓上層 fallback 到 price * shares）。
    """
    lower_map = {c.lower(): c for c in df.columns}
    candidates = [
        "market_cap",
        "marketvalue",
        "market_value",
        "mkt_cap",
        "mv",
        "marketcapitalization",
        "market_capitalization",
        "MarketValue",
    ]
    for name in candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _resolve_cfo_column(cfs: pd.DataFrame) -> Optional[str]:
    """
    嘗試從「寬表」 cfs 資料找出營業活動現金流（CFO）欄位。

    回傳欄位名稱或 None；找不到時僅記錄警告，不 raise。
    """
    lower_map = {c.lower(): c for c in cfs.columns}

    # 先找 TTM 類欄位
    ttm_candidates = [
        "cfo_ttm",
        "net_operating_cf_ttm",
        "net_cash_from_operating_activities_ttm",
        "netcashflowsfromoperatingactivitiesttm",
        "net_cashflows_operating_ttm",
    ]
    for name in ttm_candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]

    # 再找單期 CFO 欄位
    cfo_candidates = [
        "net_cash_from_operating_activities",
        "netcashflowsfromoperatingactivities",
        "net_cashflows_from_operating_activities",
        "net_cashflows_operating",
        "net_cash_flow_from_operating_activities",
        "net_cash_flow_operating",
        "cfo",
        "operating_cash_flow",
        "cash_from_operations",
        "cashflow_from_operations",
        "net_operating_cash_flow",
        "net_cashflow_operating",
        "netcashflowsoperatingactivities",
        "NetCashFlowsOperatingActivities",
        "NetCashFlowsFromOperatingActivities",
    ]
    for name in cfo_candidates:
        key = name.lower()
        if key in lower_map:
            return lower_map[key]

    logging.getLogger(__name__).warning(
        "value_cfy: CFO column not found in cfs; returning empty."
    )
    return None


def _sort_by_date_stock(
    df: pd.DataFrame, date_col: str = "date", stock_col: str = "stock_id"
) -> pd.DataFrame:
    """
    將 DataFrame 依日期、股票代號排序，滿足 merge_asof 前置要求。
    """
    if date_col in df.columns and stock_col in df.columns:
        return df.sort_values([date_col, stock_col]).reset_index(drop=True)
    if date_col in df.columns:
        return df.sort_values([date_col]).reset_index(drop=True)
    return df.reset_index(drop=True)


def _extract_cfo_from_long_cfs(cfs: pd.DataFrame) -> pd.DataFrame:
    """
    從「長表」 cfs (date, stock_id, type, value, origin_name) 中擷取 CFO rows。
    回傳欄位：date, stock_id, cfo_value；找不到則回傳空表。
    """
    required = {"date", "stock_id", "type", "value"}
    if not required.issubset(cfs.columns):
        logging.getLogger(__name__).warning("value_cfy: cfs missing required columns %s", required - set(cfs.columns))
        return pd.DataFrame(columns=["date", "stock_id", "cfo_value"])

    cfo_types = {
        # 以 FinMind 實際欄位為主，全部小寫後比對
        "cashflowsfromoperatingactivities",
        "netcashinflowfromoperatingactivities",
        "netcashflowsoperatingactivities",
        "netcashflowfromoperatingactivities",
        "netcashflowsfromoperatingactivities",
        "netcashflowoperating",
        "netoperatingcashflow",
        "net_cash_flows_from_operating_activities",
        "net_cash_flows_operating_activities",
        "netcashflowsfromoperatingactivitiescontinuing",
        "cashflowfromoperations",
    }

    df = cfs.copy()
    df["type_lower"] = df["type"].astype(str).str.lower()
    df = df[df["type_lower"].isin(cfo_types)]
    if df.empty:
        logging.getLogger(__name__).warning("value_cfy: CFO rows not found in cfs long table; returning empty.")
        return pd.DataFrame(columns=["date", "stock_id", "cfo_value"])

    out = df[["date", "stock_id", "value"]].copy()
    out["cfo_value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["cfo_value"])
    if out.empty:
        logging.getLogger(__name__).warning("value_cfy: CFO rows found but all NaN; returning empty.")
        return pd.DataFrame(columns=["date", "stock_id", "cfo_value"])
    return out.sort_values(["stock_id", "date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# value_pe：由 PE 倒數構造 value 因子
# ---------------------------------------------------------------------------


def compute_value_pe(
    per: pd.DataFrame,
    *,
    min_pe: float = 0.1,
    max_pe: float = 100.0,
    eps: float = 1e-12,
) -> pd.DataFrame:
    """
    從 per 銀河表計算 value_pe 因子。

    參數：
    - per   ：必須包含 date / stock_id / 某個 PE 類欄位。
    - min_pe：小於等於這個值的 PE 視為不可信（設 NaN）。
    - max_pe：大於等於這個值的 PE 視為極端（設 NaN）。
    - eps   ：避免除以 0 的微小補值。

    回傳：
    - df_factor：含 (date, stock_id, factor_value) 的 DataFrame。
    """
    if "date" not in per.columns or "stock_id" not in per.columns:
        raise ValueError("per dataframe must contain 'date' and 'stock_id' columns")

    df = per.copy()

    # 嘗試找出正確的 PE 欄位
    pe_col = _pick_pe_column(df)

    df = df[["date", "stock_id", pe_col]].rename(columns={pe_col: "pe_raw"})
    df["pe_raw"] = pd.to_numeric(df["pe_raw"], errors="coerce")
    df["pe_raw"] = df["pe_raw"].replace([np.inf, -np.inf], np.nan)

    # 移除不合理或極端的 PE（虧損、超大倍數）
    df.loc[df["pe_raw"] <= 0, "pe_raw"] = np.nan
    df.loc[df["pe_raw"] <= min_pe, "pe_raw"] = np.nan
    df.loc[df["pe_raw"] >= max_pe, "pe_raw"] = np.nan

    # -log(PE)：越便宜 → 分數越高，且縮小極端值影響
    # 注意：這裡只對正 PE 計算，非正值已在上面被設為 NaN
    df["value_raw"] = -np.log(df["pe_raw"] + eps)

    # 依 date 做 winsor + z-score
    df["factor_value"] = (
        df.groupby("date", group_keys=False)["value_raw"]
        .transform(_cs_winsorized_zscore)
    )

    # 清理結果
    out = df[["date", "stock_id", "factor_value"]].copy()
    out = out.dropna(subset=["factor_value"])
    out = out.sort_values(["date", "stock_id"]).reset_index(drop=True)
    return out


# ---------------------------------------------------------------------------
# value_cfy：CFO_TTM / 市值 或 現金流 / 價格
# ---------------------------------------------------------------------------


def compute_value_cfy_panel(
    prices: pd.DataFrame,
    per: pd.DataFrame,
    cfs: Optional[pd.DataFrame] = None,
    *,
    winsor_limits: tuple[float, float] = (0.01, 0.99),
    clip_std: float = 3.0,
    min_valid_per_row: int = 30,
    eps: float = 1e-12,
) -> pd.DataFrame:
    """
    計算 cash flow yield (CFY) 因子，優先路線為 CFO_TTM / 市值：

      1) 若有 cfs 長表 → 抽 CFO → rolling 4 季得到 CFO_TTM
         → 以 per 的 market_cap 或 price * shares 做市值 → CFY = CFO_TTM / 市值
      2) 若缺 cfs 或 CFO，用 per 的每股現金流欄位：
         CFY = cash_flow_per_share / price
      3) 再退而求其次，用總額現金流 / shares / price。

    最後對 CFY 做橫斷面 winsor + z-score。
    """
    # 基本檢查
    if prices is None or per is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])
    if prices.empty or per.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    log = logging.getLogger(__name__)

    prices = prices.copy()
    per = per.copy()
    if cfs is not None:
        cfs = cfs.copy()

    # 日期標準化
    if not pd.api.types.is_datetime64_any_dtype(prices["date"]):
        prices["date"] = pd.to_datetime(prices["date"])
    if not pd.api.types.is_datetime64_any_dtype(per["date"]):
        per["date"] = pd.to_datetime(per["date"])
    if cfs is not None and not pd.api.types.is_datetime64_any_dtype(cfs["date"]):
        cfs["date"] = pd.to_datetime(cfs["date"])

    # 價格欄位解析
    try:
        price_col = _resolve_price_column(prices)
    except KeyError:
        price_col = None

    if price_col is None:
        # 嘗試用 market_cap / shares 推回價格
        shares_col = None
        try:
            shares_col = _get_shares_column(prices)
        except Exception:
            shares_col = None

        if shares_col and "market_cap" in prices.columns:
            prices = prices.copy()
            prices["price_proxy"] = (
                pd.to_numeric(prices["market_cap"], errors="coerce") /
                (pd.to_numeric(prices[shares_col], errors="coerce") + eps)
            )
            price_col = "price_proxy"

    if price_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df_price = prices[["date", "stock_id", price_col]].copy()
    df_price[price_col] = pd.to_numeric(df_price[price_col], errors="coerce")
    if "universe" in prices.columns:
        df_price["universe"] = prices["universe"]
        df_price = df_price[df_price["universe"].astype(bool)]
    df_price = _sort_by_date_stock(df_price.dropna(subset=[price_col]))
    if df_price.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # ------------------------------------------------------------------
    # Route 1：使用 cfs 計算 CFO_TTM / 市值
    # ------------------------------------------------------------------
    if cfs is not None and not cfs.empty:
        try:
            # 長表：有 type / value 欄位
            if {"type", "value"}.issubset(cfs.columns):
                cfo = _extract_cfo_from_long_cfs(cfs)
            else:
                # 寬表：直接找 CFO 欄位
                cfo_col = _resolve_cfo_column(cfs)
                if cfo_col is None:
                    return pd.DataFrame(columns=["date", "stock_id", "factor_value"])
                cfo = cfs[["date", "stock_id", cfo_col]].copy()
                cfo[cfo_col] = pd.to_numeric(cfo[cfo_col], errors="coerce")
                cfo = cfo.rename(columns={cfo_col: "cfo_value"})
                cfo = cfo.dropna(subset=["cfo_value"]).sort_values(
                    ["stock_id", "date"]
                )

            if not cfo.empty:
                # rolling 4 季得到 CFO_TTM
                cfo["cfo_ttm"] = (
                    cfo.groupby("stock_id")["cfo_value"]
                    .transform(lambda s: s.rolling(window=4, min_periods=4).sum())
                )
                cfo = cfo.dropna(subset=["cfo_ttm"])
                cfo_ttm = _sort_by_date_stock(cfo[["date", "stock_id", "cfo_ttm"]])

                if not cfo_ttm.empty:
                    df_cfy = pd.merge_asof(
                        df_price,
                        cfo_ttm,
                        on="date",
                        by="stock_id",
                        direction="backward",
                        allow_exact_matches=True,
                    )

                    # 市值：優先用 per 的 market_cap，否則 price * shares
                    mktcap_col = _resolve_market_cap_column(per)
                    if mktcap_col:
                        mkt = per[["date", "stock_id", mktcap_col]].copy()
                        mkt[mktcap_col] = pd.to_numeric(
                            mkt[mktcap_col],
                            errors="coerce",
                        )
                        if "universe" in per.columns:
                            mkt["universe"] = per["universe"]
                            mkt = mkt[mkt["universe"].astype(bool)]
                        mkt = _sort_by_date_stock(mkt.dropna(subset=[mktcap_col]))

                        if not mkt.empty:
                            df_cfy = pd.merge_asof(
                                _sort_by_date_stock(df_cfy),
                                _sort_by_date_stock(mkt[["date", "stock_id", mktcap_col]]),
                                on="date",
                                by="stock_id",
                                direction="backward",
                                allow_exact_matches=True,
                            )
                            df_cfy["market_cap"] = pd.to_numeric(
                                df_cfy[mktcap_col],
                                errors="coerce",
                            )

                    if (
                        "market_cap" not in df_cfy.columns
                        or df_cfy["market_cap"].isna().all()
                    ):
                        # 再退一步：用 shares proxy 做市值
                        shares_col = None
                        try:
                            shares_col = _get_shares_column(prices)
                        except Exception:
                            shares_col = None

                        if shares_col and shares_col in prices.columns:
                            df_shares = prices[["date", "stock_id", shares_col]].copy()
                            df_shares[shares_col] = pd.to_numeric(
                                df_shares[shares_col],
                                errors="coerce",
                            )
                            if "universe" in prices.columns:
                                df_shares["universe"] = prices["universe"]
                                df_shares = df_shares[
                                    df_shares["universe"].astype(bool)
                                ]
                            df_shares = _sort_by_date_stock(
                                df_shares.dropna(subset=[shares_col])
                            )

                            if not df_shares.empty:
                                df_cfy = pd.merge_asof(
                                    _sort_by_date_stock(df_cfy),
                                    df_shares,
                                    on="date",
                                    by="stock_id",
                                    direction="backward",
                                    allow_exact_matches=True,
                                )
                                df_cfy["market_cap"] = (
                                    pd.to_numeric(df_cfy[price_col], errors="coerce")
                                    * pd.to_numeric(df_cfy[shares_col], errors="coerce")
                                )

                    df_cfy = df_cfy.dropna(subset=["cfo_ttm", "market_cap"])
                    if not df_cfy.empty:
                        df_cfy["cfy_raw"] = df_cfy["cfo_ttm"].astype(float) / (
                            pd.to_numeric(df_cfy["market_cap"], errors="coerce") + eps
                        )
                        df_cfy["cfy_raw"] = df_cfy["cfy_raw"].replace(
                            [np.inf, -np.inf],
                            np.nan,
                        )
                        df_cfy = df_cfy.dropna(subset=["cfy_raw"])

                        if not df_cfy.empty:
                            wide = df_cfy.pivot(
                                index="date",
                                columns="stock_id",
                                values="cfy_raw",
                            )
                            wide = apply_xsection_xform(
                                wide,
                                strategy="zscore",
                                winsor_limits=winsor_limits,
                                clip_std=clip_std,
                                min_valid_per_row=min_valid_per_row,
                            )
                            long = wide.stack(dropna=True).reset_index()
                            long.columns = ["date", "stock_id", "factor_value"]
                            long = long.sort_values(
                                ["date", "stock_id"]
                            ).reset_index(drop=True)
                            if not long.empty:
                                return long
        except Exception:
            # CFO 路線有問題時，不中斷流程，退回 per-based CFY 路線
            log.exception("value_cfy: CFO-based route failed; fallback to per-based CFY.")

    # ------------------------------------------------------------------
    # Route 2：per 每股現金流 / 價格
    # ------------------------------------------------------------------
    cfy_series_name: Optional[str] = None
    cfy_per: Optional[pd.DataFrame] = None

    try:
        cfy_col = _resolve_cfy_column(per)
        cfy_per = per[["date", "stock_id", cfy_col]].copy()
        cfy_per[cfy_col] = pd.to_numeric(cfy_per[cfy_col], errors="coerce")
        if "universe" in per.columns:
            cfy_per["universe"] = per["universe"]
            cfy_per = cfy_per[cfy_per["universe"].astype(bool)]
        cfy_per = cfy_per.dropna(subset=[cfy_col]).sort_values(
            ["stock_id", "date"]
        ).reset_index(drop=True)
        if not cfy_per.empty:
            cfy_series_name = cfy_col
    except KeyError:
        cfy_per = None

    # ------------------------------------------------------------------
    # Route 3：總額現金流 / shares / 價格
    # ------------------------------------------------------------------
    if cfy_per is None or cfy_per.empty or cfy_series_name is None:
        cf_source: Optional[pd.DataFrame] = None
        cf_col: Optional[str] = None

        # 先看 per，再看 cfs（若有）
        for df_candidate in (per, cfs) if cfs is not None else (per,):
            try:
                cf_col = _resolve_cf_total_column(df_candidate)
                cf_source = df_candidate[["date", "stock_id", cf_col]].copy()
                cf_source[cf_col] = pd.to_numeric(
                    cf_source[cf_col],
                    errors="coerce",
                )
                if "universe" in df_candidate.columns:
                    cf_source["universe"] = df_candidate["universe"]
                    cf_source = cf_source[cf_source["universe"].astype(bool)]
                cf_source = _sort_by_date_stock(cf_source.dropna(subset=[cf_col]))
                if not cf_source.empty:
                    break
            except KeyError:
                continue

        if cf_source is not None and not cf_source.empty:
            shares_col = None
            try:
                shares_col = _get_shares_column(prices)
            except Exception:
                shares_col = None

            if shares_col and shares_col in prices.columns:
                df_shares = prices[["date", "stock_id", shares_col]].copy()
                df_shares[shares_col] = pd.to_numeric(
                    df_shares[shares_col],
                    errors="coerce",
                )
                if "universe" in prices.columns:
                    df_shares["universe"] = prices["universe"]
                    df_shares = df_shares[df_shares["universe"].astype(bool)]
                df_shares = _sort_by_date_stock(
                    df_shares.dropna(subset=[shares_col])
                )

                if not df_shares.empty:
                    cf_merged = pd.merge_asof(
                        cf_source,
                        df_shares,
                        on="date",
                        by="stock_id",
                        direction="backward",
                        allow_exact_matches=True,
                    )
                    cf_merged = cf_merged.dropna(subset=[cf_col, shares_col])
                    if not cf_merged.empty:
                        cf_merged["cf_per_share"] = cf_merged[cf_col].astype(float) / (
                            cf_merged[shares_col].astype(float) + eps
                        )
                        cf_merged = cf_merged.dropna(subset=["cf_per_share"])
                        if not cf_merged.empty:
                            cfy_per = cf_merged[["date", "stock_id", "cf_per_share"]]
                            cfy_per = cfy_per.rename(
                                columns={"cf_per_share": "cfy_ps"}
                            )
                            cfy_per = cfy_per.sort_values(
                                ["stock_id", "date"]
                            ).reset_index(drop=True)
                            cfy_series_name = "cfy_ps"

    # 若三條路線都失敗 → 回傳空表
    if cfy_per is None or cfy_per.empty or cfy_series_name is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 對齊最近可得的每股現金流與價格
    df_price = _sort_by_date_stock(df_price)
    cfy_per = _sort_by_date_stock(cfy_per)
    df = pd.merge_asof(
        df_price,
        cfy_per,
        on="date",
        by="stock_id",
        direction="backward",
        allow_exact_matches=True,
    )

    df = df.dropna(subset=[cfy_series_name, price_col])
    df["cfy_raw"] = df[cfy_series_name].astype(float) / df[price_col].astype(float)
    df["cfy_raw"] = df["cfy_raw"].replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["cfy_raw"])

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    wide = df.pivot(index="date", columns="stock_id", values="cfy_raw")
    wide = apply_xsection_xform(
        wide,
        strategy="zscore",
        winsor_limits=winsor_limits,
        clip_std=clip_std,
        min_valid_per_row=min_valid_per_row,
    )

    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]
    long = long.sort_values(["date", "stock_id"]).reset_index(drop=True)
    return long


def run_value_cfy_factor(
    *,
    prices: pd.DataFrame,
    per: pd.DataFrame,
    cfs: pd.DataFrame,
    window: int,  # 保留簽名以符合 factor_engine 介面，目前未直接使用
    end_date: date,  # 保留簽名以符合 factor_engine 介面，目前未直接使用
    calendar: Optional[pd.DataFrame] = None,  # noqa: ARG001
    universe: Optional[pd.DataFrame] = None,  # noqa: ARG001
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 value_cfy engine：CFO_TTM / 市值（或現金流 / 價格），正向 value。
    """
    if (
        prices is None
        or prices.empty
        or per is None
        or per.empty
        or cfs is None
        or cfs.empty
    ):
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    return compute_value_cfy_panel(
        prices=prices,
        per=per,
        cfs=cfs,
        winsor_limits=tuple(kwargs.get("winsor_limits", (0.01, 0.99))),  # type: ignore[arg-type]
        clip_std=float(kwargs.get("clip_std", 3.0)),
        min_valid_per_row=int(kwargs.get("min_valid_per_row", 30)),
        eps=float(kwargs.get("eps", 1e-12)),
    )


# ---------------------------------------------------------------------------
# factor_engine 路由入口
# ---------------------------------------------------------------------------


def run_value_factor(
    *,
    per: pd.DataFrame,
    prices: Optional[pd.DataFrame] = None,  # 必須存在於 value_cfy，保留可選以兼容舊參數
    cfs: Optional[pd.DataFrame] = None,
    factor_id: Optional[str] = None,
    window: int,
    end_date: date,
    calendar: Optional[pd.DataFrame] = None,
    universe: Optional[pd.DataFrame] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 引擎用的入口函式。

    簽名設計重點：
    - 保持與 factor_engine._route_and_compute 呼叫相容。
    - 顯式接收 window / end_date。
    - prices 為 Optional，避免 TypeError。

    實作：
    - value_pe：沿用 PE 倒數定義。
    - value_cfy：現金流殖利率（CFO_TTM / 市值 或 CF_per_share / price）。
    """
    fid = (factor_id or kwargs.get("factor_id") or "value_pe").lower()

    if fid == "value_cfy":
        return run_value_cfy_factor(
            prices=prices if prices is not None else pd.DataFrame(),
            per=per if per is not None else pd.DataFrame(),
            cfs=cfs if cfs is not None else pd.DataFrame(),
            window=window,
            end_date=end_date,
            calendar=calendar,
            universe=universe,
            **kwargs,
        )

    # 預設回到 value_pe
    min_pe = float(kwargs.get("min_pe", 0.1))
    max_pe = float(kwargs.get("max_pe", 100.0))

    df_factor = compute_value_pe(per, min_pe=min_pe, max_pe=max_pe)
    return df_factor


def run_value_pe_factor(
    *,
    per: pd.DataFrame,
    prices: Optional[pd.DataFrame] = None,
    cfs: Optional[pd.DataFrame] = None,
    factor_id: Optional[str] = None,  # noqa: ARG001
    window: int,
    end_date: date,
    calendar: Optional[pd.DataFrame] = None,
    universe: Optional[pd.DataFrame] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Thin alias for value_pe to keep dispatcher binding deterministic.
    """
    return run_value_factor(
        per=per,
        prices=prices,
        cfs=cfs,
        factor_id="value_pe",
        window=window,
        end_date=end_date,
        calendar=calendar,
        universe=universe,
        **kwargs,
    )


__all__ = [
    "compute_value_pe",
    "compute_value_cfy_panel",
    "run_value_cfy_factor",
    "run_value_factor",
    "run_value_pe_factor",
]
