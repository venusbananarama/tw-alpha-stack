#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl

Phase-2 因子實作模組（impl_module）。

角色：
- 提供 compute_factor(...) 單一入口，給 factor_engine 呼叫。
- 依 rules_factors.yaml 的 engine 欄位做路由（ta_mom_v1 / ta_vol_v1 / fundamental_value_v1 ...）。
- 各 engine 不做 parquet / ledger，只回傳 DataFrame（date, stock_id, factor_value）。

目前實作狀態：
- ta_mom_v1：動能（mom_6m / mom_12m）
- ta_vol_v1：波動（vol_20d）
- ta_beta_v1：β（beta_252d 等）
- fundamental_value_v1：估值（value_pe → earnings_yield）
- microstructure_v1：流動性 / 市值（liq_turnover_20d / size_log_mktcap）
- fundamental_quality_v1：品質（quality_roeq：TTM-ROE）

骨架（尚未實作，回傳空 DataFrame）：
- ai_xgb_v1
"""

from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

LoggerLike = logging.Logger

__all__ = ["compute_factor"]


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _get_logger(logger: Optional[LoggerLike]) -> LoggerLike:
    """
    若呼叫端未提供 logger，使用 module-level logger。
    """
    if logger is not None:
        return logger
    return logging.getLogger("alpha_core.factor_impl")


def _empty_frame() -> pd.DataFrame:
    """
    標準空 DataFrame：僅保留 (date, stock_id, factor_value) 欄位。
    """
    return pd.DataFrame(columns=["date", "stock_id", "factor_value"])


def _normalize_spec(spec: Any) -> Mapping[str, Any]:
    """
    因子 spec 正規化成 mapping，允許三種來源型別：
    - dict/Mapping
    - dataclass
    - 具 __dict__ 的一般物件
    """
    from collections.abc import Mapping as _Mapping

    if spec is None:
        return {}
    if isinstance(spec, _Mapping):
        return spec
    if is_dataclass(spec):
        return asdict(spec)
    if hasattr(spec, "__dict__"):
        return dict(spec.__dict__)
    raise TypeError(f"Unsupported factor spec type: {type(spec)!r}")


def _get_engine_id(spec: Optional[Mapping[str, Any]]) -> Optional[str]:
    """
    從 spec["engine"] 讀取 engine id，並做 strip。
    """
    if not spec:
        return None
    eng = spec.get("engine")
    if not eng:
        return None
    s = str(eng).strip()
    return s or None


# ---------------------------------------------------------------------------
# 讀取交易日曆
# ---------------------------------------------------------------------------


def _load_trading_calendar(root: Path, logger: LoggerLike) -> pd.DataFrame:
    cal_path = root / "datahub" / "ref" / "trading_days.csv"
    if not cal_path.exists():
        logger.error("找不到交易日曆：%s", cal_path)
        return pd.DataFrame(columns=["date"])

    cal = pd.read_csv(cal_path)
    col = (
        "date"
        if "date" in cal.columns
        else "trading_date"
        if "trading_date" in cal.columns
        else None
    )
    if col is None:
        logger.error("交易日曆缺少 date/trading_date 欄位，columns=%s", list(cal.columns))
        return pd.DataFrame(columns=["date"])

    cal = cal[[col]].copy()
    cal.rename(columns={col: "date"}, inplace=True)
    cal["date"] = pd.to_datetime(cal["date"])
    cal = (
        cal.dropna()
        .drop_duplicates()
        .sort_values("date")
        .reset_index(drop=True)
    )
    cal["cal_idx"] = np.arange(len(cal), dtype="int64")
    return cal


# ---------------------------------------------------------------------------
# prices：給 momentum 用
# ---------------------------------------------------------------------------


def _load_prices_for_momentum(
    root: Path,
    end_date: Optional[date],
    logger: LoggerLike,
) -> pd.DataFrame:
    data_root = root / "datahub" / "silver" / "alpha" / "prices"
    if not data_root.exists():
        logger.error("prices 資料不存在：%s", data_root)
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        logger.warning("prices 無分區 parquet")
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    frames = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("讀取失敗略過：%s (%r)", p, exc)
            continue
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    prices = pd.concat(frames, ignore_index=True)
    if "date" not in prices.columns:
        raise ValueError("prices 缺 date 欄位")

    prices["date"] = pd.to_datetime(prices["date"])
    prices.dropna(subset=["date"], inplace=True)

    # stock_id
    stock_col = None
    for c in ("stock_id", "stock", "code", "symbol"):
        if c in prices.columns:
            stock_col = c
            break
    if not stock_col:
        raise ValueError("prices 找不到股票代碼欄位")

    prices.rename(columns={stock_col: "stock_id"}, inplace=True)

    # price
    px_col = None
    for c in ("adj_close", "close", "Close", "price"):
        if c in prices.columns:
            px_col = c
            break
    if not px_col:
        raise ValueError("prices 找不到收盤價欄位")
    prices.rename(columns={px_col: "adj_close"}, inplace=True)

    if end_date:
        prices = prices.loc[prices["date"] < pd.to_datetime(end_date)]

    prices = prices.dropna(subset=["stock_id", "adj_close"])
    return prices[["date", "stock_id", "adj_close"]]


# ---------------------------------------------------------------------------
# momentum engine：ta_mom_v1
# ---------------------------------------------------------------------------


def _compute_ta_mom_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    if end_date is None:
        log.error("mom: end_date 不可為 None")
        return _empty_frame()

    spec_map = _normalize_spec(spec)
    params = spec_map.get("params") or {}
    if not isinstance(params, Mapping):
        params = {}

    lb = params.get("lookback_days") or spec_map.get("lookback_days")
    if lb is None:
        fid = str(factor_id).lower()
        lb = 126 if "6m" in fid else 252

    try:
        lb = int(lb)
    except Exception:
        lb = 252

    root = root.resolve()
    universe = spec_map.get("universe")
    log.info("mom: %s lookback=%d universe=%s", factor_id, lb, universe)

    cal = _load_trading_calendar(root, log)
    if cal.empty:
        return _empty_frame()
    cal_map = {d: int(i) for d, i in zip(cal["date"], cal["cal_idx"])}

    prices = _load_prices_for_momentum(root, end_date, log)
    if prices.empty:
        return _empty_frame()

    prices["date"] = pd.to_datetime(prices["date"])
    prices["cal_idx"] = prices["date"].map(cal_map)
    prices.dropna(subset=["cal_idx"], inplace=True)
    prices["cal_idx"] = prices["cal_idx"].astype("int64")

    start_ts = pd.to_datetime(start_date) if start_date else None
    end_ts = pd.to_datetime(end_date)

    def _per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("cal_idx").copy()
        g["lookback_idx"] = g["cal_idx"] - lb
        start_view = g[["cal_idx", "adj_close"]].rename(
            columns={"cal_idx": "lookback_idx", "adj_close": "P_start"},
        )
        m = g.merge(start_view, on="lookback_idx", how="left")
        m.dropna(subset=["P_start"], inplace=True)
        m = m.loc[(m["adj_close"] > 0) & (m["P_start"] > 0)].copy()
        if m.empty:
            return pd.DataFrame(columns=["date", "stock_id", "factor_value"])
        m["factor_value"] = np.log(m["adj_close"] / m["P_start"])
        out = m[["date", "stock_id", "factor_value"]]
        if start_ts is not None:
            out = out.loc[out["date"] >= start_ts]
        out = out.loc[out["date"] < end_ts]
        return out

    frames = [_per_stock(g) for _, g in prices.groupby("stock_id")]
    if not frames:
        return _empty_frame()

    result = pd.concat(frames, ignore_index=True)
    if result.empty:
        return _empty_frame()

    # universe 過濾（若有設定）
    result = _maybe_filter_by_universe(result, root, universe, log)
    if result.empty:
        return _empty_frame()

    return (
        result.sort_values(["date", "stock_id"])
        .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
    )


# ---------------------------------------------------------------------------
# OHLCV / microstructure 共用 helper
# ---------------------------------------------------------------------------


def _load_prices_for_ohlcv(
    root: Path,
    end_date: Optional[date],
    logger: LoggerLike,
) -> pd.DataFrame:
    log = _get_logger(logger)
    data_root = root / "datahub" / "silver" / "alpha" / "prices"
    if not data_root.exists():
        log.error("找不到 prices：%s", data_root)
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        log.warning("prices 分區為空")
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    frames = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            log.warning("讀取 parquet 失敗略過：%s (%r)", p, exc)
            continue
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["date", "stock_id", "adj_close"])

    df = pd.concat(frames, ignore_index=True)

    # 日期
    if "date" not in df.columns:
        raise ValueError("prices 缺 date 欄位")
    df["date"] = pd.to_datetime(df["date"])
    df.dropna(subset=["date"], inplace=True)

    # 股票代碼
    stock_col = None
    for c in ("stock_id", "stock", "code", "symbol"):
        if c in df.columns:
            stock_col = c
            break
    if not stock_col:
        raise ValueError("prices 無股票代碼欄位")
    df.rename(columns={stock_col: "stock_id"}, inplace=True)

    # 收盤價
    price_col = None
    for c in ("adj_close", "close", "Close", "price"):
        if c in df.columns:
            price_col = c
            break
    if not price_col:
        raise ValueError("prices 缺價格欄位")
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    # 可選欄位：volume
    for c in (
        "volume",
        "Volume",
        "vol",
        "Vol",
        "trading_volume",
        "Trading_Volume",
    ):
        if c in df.columns:
            df.rename(columns={c: "volume"}, inplace=True)
            break

    # turnover_value
    for c in (
        "turnover_value",
        "Trading_money",
        "trading_value",
        "amount",
        "total_turnover",
    ):
        if c in df.columns:
            df.rename(columns={c: "turnover_value"}, inplace=True)
            break

    # turnover_rate
    for c in (
        "turnover_rate",
        "Turnover_rate",
        "turnoverRatio",
        "turnover_ratio",
    ):
        if c in df.columns:
            df.rename(columns={c: "turnover_rate"}, inplace=True)
            break

    # 市值
    for c in ("market_cap", "MarketCap", "market_value", "Market_Value"):
        if c in df.columns:
            df.rename(columns={c: "market_cap"}, inplace=True)
            break

    if end_date:
        df = df.loc[df["date"] < pd.to_datetime(end_date)]

    df = df.dropna(subset=["stock_id", "adj_close"])
    keep = ["date", "stock_id", "adj_close"]
    for extra in ("volume", "turnover_value", "turnover_rate", "market_cap"):
        if extra in df.columns:
            keep.append(extra)
    return df[keep]


def _compute_log_returns(prices: pd.DataFrame, logger: LoggerLike) -> pd.DataFrame:
    log = _get_logger(logger)
    if prices.empty:
        return prices.assign(ret_log=pd.Series(dtype="float64"))

    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.dropna(subset=["date", "stock_id", "adj_close"], inplace=True)

    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df.dropna(subset=["adj_close"], inplace=True)

    invalid = (df["adj_close"] <= 0).sum()
    if invalid > 0:
        log.info("compute_log_returns: 非正價格=%d", int(invalid))
        df.loc[df["adj_close"] <= 0, "adj_close"] = np.nan

    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    df["prev_price"] = df.groupby("stock_id")["adj_close"].shift(1)

    df["ret_log"] = np.nan
    mask = (df["adj_close"] > 0) & (df["prev_price"] > 0)
    df.loc[mask, "ret_log"] = np.log(
        df.loc[mask, "adj_close"] / df.loc[mask, "prev_price"],
    )
    df.drop(columns=["prev_price"], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Value engine (value_pe)
# ---------------------------------------------------------------------------


def _load_per_for_value(
    root: Path,
    end_date: Optional[date],
    logger: LoggerLike,
) -> pd.DataFrame:
    data_root = root / "datahub" / "silver" / "alpha" / "per"
    if not data_root.exists():
        logger.error("找不到 per：%s", data_root)
        return pd.DataFrame(columns=["date", "stock_id"])

    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        return pd.DataFrame(columns=["date", "stock_id"])

    frames = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("讀取 per 失敗略過：%s (%r)", p, exc)
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue

        stock_col = None
        for c in ("stock_id", "stock", "code", "symbol"):
            if c in df.columns:
                stock_col = c
                break
        if not stock_col:
            continue

        tmp = df.copy()
        tmp["date"] = pd.to_datetime(tmp["date"])
        tmp.dropna(subset=["date"], inplace=True)
        tmp.rename(columns={stock_col: "stock_id"}, inplace=True)
        # 修正 bug：subset 需為參數名稱，值為 list
        tmp.dropna(subset=["stock_id"], inplace=True)
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["date", "stock_id"])

    per_df = pd.concat(frames, ignore_index=True)
    if end_date:
        per_df = per_df.loc[per_df["date"] < pd.to_datetime(end_date)]
    return per_df.sort_values(["date", "stock_id"]).reset_index(drop=True)


def _normalize_pe_column(per_df: pd.DataFrame, logger: LoggerLike) -> pd.DataFrame:
    if per_df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "pe"])

    candidates = (
        "pe",
        "PE",
        "pe_ratio",
        "PER",
        "per",
        "ttm_pe",
        "PER_ttm",
        "PE_ttm",
        "pe_raw",
    )
    src = None
    for c in candidates:
        if c in per_df.columns:
            src = c
            break
    if not src:
        logger.error("per 未找到 PE 欄位")
        return pd.DataFrame(columns=["date", "stock_id", "pe"])

    df = per_df[["date", "stock_id", src]].copy()
    df[src] = pd.to_numeric(df[src], errors="coerce")
    df.dropna(subset=[src], inplace=True)
    df.rename(columns={src: "pe"}, inplace=True)
    return df[["date", "stock_id", "pe"]]


def _compute_earnings_yield_frame(
    per_df: pd.DataFrame,
    logger: LoggerLike,
) -> pd.DataFrame:
    if per_df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = per_df.copy()
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df.dropna(subset=["pe"], inplace=True)

    valid = (df["pe"] > 0) & (df["pe"] < 5000)
    dropped = (~valid).sum()
    if dropped > 0:
        logger.info("earnings_yield: dropped=%d", int(dropped))
    df = df.loc[valid]

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df["factor_value"] = 1.0 / df["pe"]
    out = df[["date", "stock_id", "factor_value"]].copy()
    out["date"] = pd.to_datetime(out["date"])
    return out.dropna()


def _maybe_filter_by_universe(
    df: pd.DataFrame,
    root: Path,
    universe_name: Optional[str],
    logger: LoggerLike,
) -> pd.DataFrame:
    if df.empty or not universe_name:
        return df

    uname = str(universe_name).strip()
    if not uname:
        return df
    if uname != "tw_eq_investable":
        logger.info("未處理 universe=%s，直接通過", uname)
        return df

    path = root / "investable_universe.txt"
    if not path.exists():
        logger.warning("universe 檔不存在：%s", path)
        return df

    try:
        uni = {t.strip() for t in path.read_text(encoding="utf-8").splitlines() if t.strip()}
    except Exception as exc:  # noqa: BLE001
        logger.warning("讀取 universe 失敗：%r", exc)
        return df

    df2 = df[df["stock_id"].astype(str).isin(uni)]
    logger.info("universe 過濾：%d → %d", len(df), len(df2))
    return df2


# ---------------------------------------------------------------------------
# fundamental_value_v1 (value_pe)
# ---------------------------------------------------------------------------


def _compute_fundamental_value_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    if end_date is None:
        log.error("value: end_date 不可為 None")
        return _empty_frame()

    spec_map = _normalize_spec(spec)
    if str(factor_id).lower() != "value_pe":
        log.info("value engine 目前僅實作 value_pe")
        return _empty_frame()

    root = root.resolve()
    universe = spec_map.get("universe")
    log.info("value_pe: universe=%s", universe)

    per_df = _load_per_for_value(root, end_date, log)
    if per_df.empty:
        return _empty_frame()

    per_pe = _normalize_pe_column(per_df, log)
    if per_pe.empty:
        return _empty_frame()

    per_pe["date"] = pd.to_datetime(per_pe["date"])
    if start_date:
        per_pe = per_pe.loc[per_pe["date"] >= pd.to_datetime(start_date)]
    per_pe = per_pe.loc[per_pe["date"] < pd.to_datetime(end_date)]
    if per_pe.empty:
        return _empty_frame()

    value_df = _compute_earnings_yield_frame(per_pe, log)
    if value_df.empty:
        return _empty_frame()

    value_df = _maybe_filter_by_universe(value_df, root, universe, log)
    if value_df.empty:
        return _empty_frame()

    return (
        value_df.sort_values(["date", "stock_id"])
        .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
    )


# ---------------------------------------------------------------------------
# Volatility engine v1 (vol_20d)
# ---------------------------------------------------------------------------


def _compute_ta_vol_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    if end_date is None:
        log.error("vol: end_date 不可為 None")
        return _empty_frame()

    spec_map = _normalize_spec(spec)
    params = spec_map.get("params") or {}
    wd = params.get("window_days") or spec_map.get("window_days") or 20
    try:
        window_days = int(wd)
    except Exception:
        window_days = 20

    root = root.resolve()
    universe = spec_map.get("universe")

    prices = _load_prices_for_ohlcv(root, end_date, log)
    if prices.empty:
        return _empty_frame()

    df = _compute_log_returns(prices, log)
    if "ret_log" not in df.columns:
        return _empty_frame()

    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    group = df.groupby("stock_id", group_keys=False)

    df["vol"] = (
        group["ret_log"]
        .rolling(window_days, min_periods=window_days)
        .std()
        .reset_index(level=0, drop=True)
    )

    out = df[["date", "stock_id", "vol"]].dropna()
    if out.empty:
        return _empty_frame()

    if start_date:
        out = out.loc[out["date"] >= pd.to_datetime(start_date)]
    out = out.loc[out["date"] < pd.to_datetime(end_date)]
    if out.empty:
        return _empty_frame()

    out.rename(columns={"vol": "factor_value"}, inplace=True)
    out = _maybe_filter_by_universe(out, root, universe, log)
    if out.empty:
        return _empty_frame()

    return (
        out.sort_values(["date", "stock_id"])
        .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
    )


# ---------------------------------------------------------------------------
# Quality engine helper：載入 finstmt 並處理 long → wide
# ---------------------------------------------------------------------------


def _load_finstmt_for_quality(
    root: Path,
    end_date: Optional[date],
    logger: LoggerLike,
) -> pd.DataFrame:
    """
    載入財報：finstmt（損益表/綜合損益+權益）

    來源：
        datahub/silver/alpha/finstmt/yyyymm=YYYYMM/*.parquet

    支援兩種 schema：
        1) wide：date, stock_id, NetIncome, Equity, ...
        2) long：date, stock_id, type, value  → pivot 成 wide
    """
    data_root = root / "datahub" / "silver" / "alpha" / "finstmt"
    if not data_root.exists():
        logger.error("找不到 finstmt：%s", data_root)
        return pd.DataFrame()

    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        return pd.DataFrame()

    frames = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("讀取 finstmt 失敗略過：%s (%r)", p, exc)
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue

        stock_col = None
        for c in ("stock_id", "stock", "code", "symbol"):
            if c in df.columns:
                stock_col = c
                break
        if not stock_col:
            continue

        tmp = df.copy()
        tmp["date"] = pd.to_datetime(tmp["date"])
        tmp.rename(columns={stock_col: "stock_id"}, inplace=True)
        tmp.dropna(subset=["stock_id", "date"], inplace=True)
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    fin = pd.concat(frames, ignore_index=True)
    if end_date:
        fin = fin.loc[fin["date"] < pd.to_datetime(end_date)]

    fin = fin.sort_values(["date", "stock_id"]).reset_index(drop=True)

    # 支援 FinMind style 長表：date, stock_id, type, value
    if "type" in fin.columns and "value" in fin.columns:
        logger.info("finstmt: 偵測到 type/value 長表，執行 pivot → wide")
        try:
            pivot = (
                fin.pivot_table(
                    index=["date", "stock_id"],
                    columns="type",
                    values="value",
                    aggfunc="last",
                )
                .reset_index()
            )
            # columns 是 MultiIndex 時轉成單層字串
            pivot.columns = [str(c) for c in pivot.columns]
            fin = pivot
        except Exception as exc:  # noqa: BLE001
            logger.error("finstmt 長表 pivot 失敗，保留原始格式：%r", exc)

    return fin


# ---------------------------------------------------------------------------
# fundamental_quality_v1 (quality_roeq)
# ---------------------------------------------------------------------------


def _compute_fundamental_quality_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    fid = str(factor_id).lower()
    if fid != "quality_roeq":
        log.info("quality engine 僅實作 quality_roeq")
        return _empty_frame()

    if end_date is None:
        log.error("quality: end_date 不可為 None")
        return _empty_frame()

    root = root.resolve()
    spec_map = _normalize_spec(spec)
    universe = spec_map.get("universe")

    fin = _load_finstmt_for_quality(root, end_date, log)
    if fin.empty:
        return _empty_frame()

    # ------------------------------------------------------------------
    # 自動偵測 net_income / equity 欄位（支援舊版 + FinMind 2020+ 長表）
    # ------------------------------------------------------------------
    ni_col: Optional[str] = None
    eq_col: Optional[str] = None

    # 先對齊你實際看到的 FinMind type 名稱，優先順序從「最想用」排到舊 schema
    ni_candidates = (
        # FinMind 2020+ P/L type（本期淨利 / 稅後淨利 / 繼續營業單位）
        "IncomeAfterTaxes",                  # 本期淨利（淨損）
        "IncomeAfterTax",                    # 本期稅後淨利（淨損）
        "IncomeFromContinuingOperations",    # 繼續營業單位本期淨利（淨損）
        "IncomeBeforeTaxFromContinuingOperations",  # 繼續營業單位稅前淨利（淨損）
        "TotalConsolidatedProfitForThePeriod",      # 綜合損益 / 淨利總額
        # 也有部分「歸屬母公司業主」直接掛在 EquityAttributableToOwnersOfParent type
        "EquityAttributableToOwnersOfParent",
        # 舊 schema / 其他常見命名
        "NetIncomeLoss",
        "NetIncome",
        "net_income",
        "profit",
        "PAT",
        "NI",
    )

    eq_candidates = (
        # FinMind 權益類型（常見欄位）
        "EquityAttributableToOwnersOfParent",
        "EquityAttributableToOwnersOfParentCompany",
        "TotalEquity",
        "EquityTotal",
        # 其他常見欄位名稱
        "equity",
        "Equity",
        "shareholder_equity",
        "StockholdersEquity",
        "StockholdersEquityTotal",
        "book_value",
    )

    # 小小除錯資訊（看 log 確認實際抓到哪幾個候選）
    available_ni = [c for c in ni_candidates if c in fin.columns]
    available_eq = [c for c in eq_candidates if c in fin.columns]
    log.info("quality_roeq 可用 NI 欄位候選: %s", available_ni)
    log.info("quality_roeq 可用 Equity 欄位候選: %s", available_eq)

    for c in ni_candidates:
        if c in fin.columns:
            ni_col = c
            break
    for c in eq_candidates:
        if c in fin.columns:
            eq_col = c
            break

    if not ni_col or not eq_col:
        log.error(
            "quality_roeq 需要 net_income / equity 欄位，"
            "實際 columns=%s, ni_col=%r, eq_col=%r",
            list(fin.columns),
            ni_col,
            eq_col,
        )
        return _empty_frame()

    log.info("quality_roeq 使用欄位：net_income=%s, equity=%s", ni_col, eq_col)

    df = fin.copy()
    df["net_income"] = pd.to_numeric(df[ni_col], errors="coerce")
    df["equity"] = pd.to_numeric(df[eq_col], errors="coerce")
    df.dropna(subset=["net_income", "equity"], inplace=True)

    # ROE = NI / Equity
    df = df.loc[df["equity"] > 0]
    if df.empty:
        return _empty_frame()

    df["roe"] = df["net_income"] / df["equity"]

    # 目前先保留「以財報日期為主」的粗粒度，之後再視需要對齊每日。
    df = df[["date", "stock_id", "roe"]]
    df.rename(columns={"roe": "factor_value"}, inplace=True)

    if start_date:
        df = df.loc[df["date"] >= pd.to_datetime(start_date)]
    df = df.loc[df["date"] < pd.to_datetime(end_date)]
    if df.empty:
        return _empty_frame()

    df = _maybe_filter_by_universe(df, root, universe, log)
    if df.empty:
        return _empty_frame()

    return (
        df.sort_values(["date", "stock_id"])
        .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
    )


# ---------------------------------------------------------------------------
# bs helper：從 bs 推導市值基準（給 size_log_mktcap 用）
# ---------------------------------------------------------------------------


def _load_bs_for_market_cap(
    root: Path,
    end_date: Optional[date],
    logger: LoggerLike,
) -> pd.DataFrame:
    """
    從銀河 bs 資料（datahub/silver/alpha/bs）載入「資本基準」欄位，
    統一輸出為 (date, stock_id, capital_for_mktcap)。

    設計重點：
    - 支援 FinMind 長表格式：date, stock_id, type, value, ...
      → pivot 成 wide：每個 type 一欄。
    - 嘗試多種候選欄位當「資本基準」：
      CapitalStock / CommonStock / ShareCapital / StockholdersEquity /
      EquityAttributableToOwnersOfParent 等。
    - 單位不重要，之後 size_log_mktcap 只做 log(market_cap)。
    """
    log = _get_logger(logger)
    data_root = root / "datahub" / "silver" / "alpha" / "bs"
    if not data_root.exists():
        log.warning("size_log_mktcap: 找不到 bs 資料夾：%s", data_root)
        return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    files = sorted(data_root.glob("yyyymm=*/*.parquet"))
    if not files:
        log.warning("size_log_mktcap: bs 無分區檔")
        return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    frames: list[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            log.warning("size_log_mktcap: 讀取 bs 失敗略過：%s (%r)", p, exc)
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue

        stock_col = None
        for c in ("stock_id", "stock", "code", "symbol"):
            if c in df.columns:
                stock_col = c
                break
        if not stock_col:
            continue

        tmp = df.copy()
        tmp["date"] = pd.to_datetime(tmp["date"])
        tmp.rename(columns={stock_col: "stock_id"}, inplace=True)
        tmp.dropna(subset=["stock_id", "date"], inplace=True)
        frames.append(tmp)

    if not frames:
        log.warning("size_log_mktcap: bs 無有效資料")
        return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    bs = pd.concat(frames, ignore_index=True)
    if end_date:
        bs = bs.loc[bs["date"] < pd.to_datetime(end_date)]
    bs = bs.sort_values(["date", "stock_id"]).reset_index(drop=True)

    # 若是長表（有 type / value），先 pivot 成 wide
    if "type" in bs.columns and "value" in bs.columns:
        log.info("size_log_mktcap: 偵測到 bs 為長表，執行 pivot → wide")
        try:
            pivot = (
                bs.pivot_table(
                    index=["date", "stock_id"],
                    columns="type",
                    values="value",
                    aggfunc="last",
                )
                .reset_index()
            )
            pivot.columns = [str(c) for c in pivot.columns]
            bs = pivot
        except Exception as exc:  # noqa: BLE001
            log.error("size_log_mktcap: bs 長表 pivot 失敗：%r", exc)
            return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    # 嘗試多種候選欄位當「資本基準」
    cap_candidates = (
        "CapitalStock",
        "CommonStock",
        "ShareCapital",
        "PaidInCapital",
        "StockholdersEquity",
        "EquityAttributableToOwnersOfParent",
    )
    cap_col = None
    for c in cap_candidates:
        if c in bs.columns:
            cap_col = c
            break

    if not cap_col:
        log.warning(
            "size_log_mktcap: bs 缺少市值基準欄位，未找到候選欄位：%s",
            cap_candidates,
        )
        return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    log.info("size_log_mktcap: 使用 bs 欄位 %s 作為市值基準", cap_col)

    out = bs[["date", "stock_id", cap_col]].copy()
    out.rename(columns={cap_col: "capital_for_mktcap"}, inplace=True)
    out["capital_for_mktcap"] = pd.to_numeric(
        out["capital_for_mktcap"],
        errors="coerce",
    )
    out.dropna(subset=["capital_for_mktcap"], inplace=True)
    if out.empty:
        log.warning("size_log_mktcap: bs 資本基準欄位全部為空")
        return pd.DataFrame(columns=["date", "stock_id", "capital_for_mktcap"])

    return out.sort_values(["stock_id", "date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# microstructure_v1 (liq_turnover_20d / size_log_mktcap)
# ---------------------------------------------------------------------------


def _compute_microstructure_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    if end_date is None:
        log.error("microstructure: end_date 不可為 None")
        return _empty_frame()

    spec_map = _normalize_spec(spec)
    fid = str(factor_id).lower()
    universe = spec_map.get("universe")
    root = root.resolve()

    prices = _load_prices_for_ohlcv(root, end_date, log)
    if prices.empty:
        return _empty_frame()
    prices["date"] = pd.to_datetime(prices["date"])

    # ------------------------------------------------------------------
    # liq_turnover_20d：支援 fallback：
    #   1) 有 turnover_rate → 用 turnover_rate
    #   2) 否則有 volume → 用 volume 當 proxy，做 20D rolling mean
    # ------------------------------------------------------------------
    if fid == "liq_turnover_20d":
        params = spec_map.get("params") or {}
        wd = params.get("window_days") or spec_map.get("window_days") or 20
        try:
            window_days = int(wd)
        except Exception:
            window_days = 20

        base_col = None
        if "turnover_rate" in prices.columns:
            base_col = "turnover_rate"
            log.info("liq_turnover_20d: 使用 turnover_rate 作為基底")
        elif "volume" in prices.columns:
            base_col = "volume"
            log.info("liq_turnover_20d: 缺 turnover_rate，使用 volume 作為 proxy")
        else:
            log.warning(
                "liq_turnover_20d: 缺 turnover_rate / volume 欄位，無法計算流動性因子",
            )
            return _empty_frame()

        df = prices[["date", "stock_id", base_col]].copy()
        df[base_col] = pd.to_numeric(df[base_col], errors="coerce")
        df.dropna(subset=[base_col], inplace=True)
        if df.empty:
            return _empty_frame()

        df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
        grp = df.groupby("stock_id", group_keys=False)
        df["avg_turnover"] = (
            grp[base_col]
            .rolling(window_days, min_periods=window_days)
            .mean()
            .reset_index(level=0, drop=True)
        )

        out = df[["date", "stock_id", "avg_turnover"]].dropna()
        if start_date:
            out = out.loc[out["date"] >= pd.to_datetime(start_date)]
        out = out.loc[out["date"] < pd.to_datetime(end_date)]
        if out.empty:
            return _empty_frame()

        out.rename(columns={"avg_turnover": "factor_value"}, inplace=True)
        out = _maybe_filter_by_universe(out, root, universe, log)
        if out.empty:
            return _empty_frame()
        return (
            out.sort_values(["date", "stock_id"])
            .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
        )

    # ------------------------------------------------------------------
    # size_log_mktcap：
    #   Case 1：prices 有 market_cap → 直接用。
    #   Case 2：prices 無 market_cap → 從 bs 推「資本基準 × 股價」當市值 proxy。
    # ------------------------------------------------------------------
    if fid == "size_log_mktcap":
        root = root.resolve()

        # Case 1：prices 已有 market_cap 欄位（未來你在 ETL 補完時會走這條）
        if "market_cap" in prices.columns:
            log.info("size_log_mktcap: 直接使用 prices.market_cap")
            df = prices[["date", "stock_id", "market_cap"]].copy()
            df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
            df = df.loc[df["market_cap"] > 0]
        else:
            # Case 2：目前情況：沒有 market_cap → 由 bs 衍生市值 proxy
            log.info(
                "size_log_mktcap: prices 缺 market_cap，改由 bs 衍生市值 proxy",
            )
            cap = _load_bs_for_market_cap(root, end_date, log)
            if cap.empty:
                log.warning("size_log_mktcap: 無法由 bs 推得市值基準，回傳空")
                return _empty_frame()

            # 準備 prices：只需要 date / stock_id / adj_close
            px = prices[["date", "stock_id", "adj_close"]].copy()
            px["date"] = pd.to_datetime(px["date"])
            px["adj_close"] = pd.to_numeric(px["adj_close"], errors="coerce")
            px.dropna(subset=["date", "stock_id", "adj_close"], inplace=True)

            cap["date"] = pd.to_datetime(cap["date"])

            # ★關鍵修正：merge_asof 需要在 on=date 上全域排序
            px = px.sort_values(["date", "stock_id"]).reset_index(drop=True)
            cap = cap.sort_values(["date", "stock_id"]).reset_index(drop=True)

            # 對每檔股票，用「最近一次財報日」的資本基準往後填到每日
            merged = pd.merge_asof(
                px,
                cap,
                by="stock_id",
                left_on="date",
                right_on="date",
                direction="backward",
            )

            merged["capital_for_mktcap"] = pd.to_numeric(
                merged["capital_for_mktcap"],
                errors="coerce",
            )
            merged.dropna(subset=["capital_for_mktcap", "adj_close"], inplace=True)
            if merged.empty:
                log.warning("size_log_mktcap: prices + bs 合併後無有效資料")
                return _empty_frame()

            # 單位不重要：只用在 log()，常數倍數不影響 cross-section 排序
            merged["market_cap"] = (
                merged["adj_close"] * merged["capital_for_mktcap"]
            )

            df = merged[["date", "stock_id", "market_cap"]]

        if df.empty:
            return _empty_frame()

        df["date"] = pd.to_datetime(df["date"])
        if start_date:
            df = df.loc[df["date"] >= pd.to_datetime(start_date)]
        df = df.loc[df["date"] < pd.to_datetime(end_date)]
        if df.empty:
            return _empty_frame()

        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
        df = df.loc[df["market_cap"] > 0]
        if df.empty:
            return _empty_frame()

        df["factor_value"] = np.log(df["market_cap"])
        df = df[["date", "stock_id", "factor_value"]]

        df = _maybe_filter_by_universe(df, root, universe, log)
        if df.empty:
            return _empty_frame()

        return (
            df.sort_values(["date", "stock_id"])
            .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
        )

    log.info("microstructure 未實作：%s", factor_id)
    return _empty_frame()


# ---------------------------------------------------------------------------
# beta engine (ta_beta_v1) – rolling beta_252d
# ---------------------------------------------------------------------------


def _compute_ta_beta_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    """
    以全市場等權平均報酬作為 benchmark，計算 rolling beta。

    定義：
        x = 個股日 log return
        y = 市場日 log return (等權平均)
        beta = Cov(x, y) / Var(y) over window_days

    規則：
        - 必須提供 end_date，表示 [start_date, end_date) 的半開區間。
        - window_days 預設 252，可由 spec.params.window_days / spec.window_days 覆寫。
        - 只使用 Phase-1 的 prices + 交易日粒度，不依賴外部指數。
        - 輸出欄位固定為 (date, stock_id, factor_value)。
    """
    log = _get_logger(logger)

    if end_date is None:
        log.error("beta: end_date 不可為 None")
        return _empty_frame()

    spec_map = _normalize_spec(spec)
    params = spec_map.get("params") or {}
    if not isinstance(params, Mapping):
        params = {}

    # 決定 rolling window 長度
    wd = params.get("window_days") or spec_map.get("window_days") or 252
    try:
        window_days = int(wd)
    except Exception:
        window_days = 252

    root = root.resolve()
    universe = spec_map.get("universe")

    log.info(
        "beta (ta_beta_v1): factor_id=%s window_days=%d universe=%s",
        factor_id,
        window_days,
        universe,
    )

    # 1) 載入 prices，計算日 log return
    prices = _load_prices_for_ohlcv(root, end_date, log)
    if prices.empty:
        log.warning("beta: prices 資料為空，回傳空 DataFrame")
        return _empty_frame()

    df = _compute_log_returns(prices, log)
    if "ret_log" not in df.columns:
        log.warning("beta: _compute_log_returns 未產生 ret_log 欄位，回傳空 DataFrame")
        return _empty_frame()

    df = df[["date", "stock_id", "ret_log"]].copy()
    df.dropna(subset=["date", "stock_id", "ret_log"], inplace=True)
    if df.empty:
        log.warning("beta: 無有效日報酬資料，回傳空 DataFrame")
        return _empty_frame()

    df["date"] = pd.to_datetime(df["date"])

    # 2) 計算市場報酬（等權平均）
    mkt = (
        df.groupby("date", as_index=False)["ret_log"]
        .mean()
        .rename(columns={"ret_log": "ret_mkt"})
    )
    df = df.merge(mkt, on="date", how="inner")
    df.dropna(subset=["ret_log", "ret_mkt"], inplace=True)
    if df.empty:
        log.warning("beta: 合併市場報酬後無資料，回傳空 DataFrame")
        return _empty_frame()

    df.rename(columns={"ret_log": "ret_stock"}, inplace=True)

    # 3) 準備 rolling 統計量
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    grp = df.groupby("stock_id", group_keys=False)

    # 預先計算乘積以利 rolling
    df["xy"] = df["ret_stock"] * df["ret_mkt"]
    df["y2"] = df["ret_mkt"] * df["ret_mkt"]

    # rolling mean
    rolling_kwargs = dict(window=window_days, min_periods=window_days)

    df["mean_x"] = (
        grp["ret_stock"]
        .rolling(**rolling_kwargs)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["mean_y"] = (
        grp["ret_mkt"]
        .rolling(**rolling_kwargs)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["mean_xy"] = (
        grp["xy"]
        .rolling(**rolling_kwargs)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["mean_y2"] = (
        grp["y2"]
        .rolling(**rolling_kwargs)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # 4) 根據 rolling 統計量計算 beta
    cov_xy = df["mean_xy"] - df["mean_x"] * df["mean_y"]
    var_y = df["mean_y2"] - df["mean_y"] * df["mean_y"]

    eps = 1e-12
    df["beta"] = np.nan
    valid = var_y > eps
    df.loc[valid, "beta"] = cov_xy[valid] / var_y[valid]

    # 5) 時間範圍裁切 [start_date, end_date)
    if start_date:
        start_ts = pd.to_datetime(start_date)
        df = df.loc[df["date"] >= start_ts]
    end_ts = pd.to_datetime(end_date)
    df = df.loc[df["date"] < end_ts]

    # 只留 beta 有值的列
    out = df.loc[df["beta"].notna(), ["date", "stock_id", "beta"]].copy()
    if out.empty:
        log.warning("beta: 計算完成後無有效 beta 值，回傳空 DataFrame")
        return _empty_frame()

    out.rename(columns={"beta": "factor_value"}, inplace=True)

    # 6) universe 過濾
    out = _maybe_filter_by_universe(out, root, universe, log)
    if out.empty:
        log.info("beta: universe filter 後無資料，回傳空 DataFrame")
        return _empty_frame()

    out = (
        out.sort_values(["date", "stock_id"])
        .reset_index(drop=True)[["date", "stock_id", "factor_value"]]
    )

    log.info("beta: 完成，rows=%d", len(out))
    return out


# ---------------------------------------------------------------------------
# AI engine (尚未實作)
# ---------------------------------------------------------------------------


def _compute_ai_xgb_v1(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]],
    logger: Optional[LoggerLike],
) -> pd.DataFrame:
    log = _get_logger(logger)
    log.info("ai_xgb_v1 尚未實作：%s", factor_id)
    return _empty_frame()


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


_ENGINE_DISPATCH: Dict[str, Any] = {
    "ta_mom_v1": _compute_ta_mom_v1,
    "ta_vol_v1": _compute_ta_vol_v1,
    "ta_beta_v1": _compute_ta_beta_v1,
    "fundamental_value_v1": _compute_fundamental_value_v1,
    "fundamental_quality_v1": _compute_fundamental_quality_v1,
    "microstructure_v1": _compute_microstructure_v1,
    "ai_xgb_v1": _compute_ai_xgb_v1,
}


# ---------------------------------------------------------------------------
# compute_factor：唯一外部 API
# ---------------------------------------------------------------------------


def compute_factor(
    root: Path,
    factor_id: str,
    spec: Any,
    start_date: Optional[date],
    end_date: Optional[date],
    windows: Optional[Sequence[int]] = None,
    logger: Optional[LoggerLike] = None,
) -> pd.DataFrame:
    """
    Phase-2 因子計算的單一入口（由 factor_engine 呼叫）。

    參數：
        root       : repo root（C:\\AI\\tw-alpha-stack）
        factor_id  : 因子 ID（rules_factors.yaml.factors.* 的 key）
        spec       : 因子設定物件（通常來自 factor_registry.FactorConfig）
                      需至少包含 engine/universe/params 等欄位。
        start_date : 起始日期（含），None 表示依資料自動決定。
        end_date   : 結束日期（不含），必須為交易日之後一個曆日。
        windows    : WF 視窗列表，目前實作主要用在 per-window 門檻統計，
                     大部分 engine 可忽略此參數。
        logger     : 選用 logger，未提供時使用 module logger。

    回傳：
        pandas.DataFrame，欄位至少包含：
            - date      (datetime64[ns])
            - stock_id  (可轉為 str)
            - factor_value (float64)

        若任何錯誤或條件不足，回傳標準空 DataFrame。
    """
    log = _get_logger(logger)

    spec_map = _normalize_spec(spec)
    eng = _get_engine_id(spec_map)
    if not eng:
        log.warning("factor=%s 無 engine，回傳空", factor_id)
        return _empty_frame()

    handler = _ENGINE_DISPATCH.get(eng)
    if handler is None:
        log.warning("factor=%s engine=%s 未知，回傳空", factor_id, eng)
        return _empty_frame()

    log.info("compute_factor: %s (engine=%s)", factor_id, eng)

    return handler(
        root=root,
        factor_id=factor_id,
        spec=spec_map,
        start_date=start_date,
        end_date=end_date,
        windows=windows,
        logger=log,
    )
