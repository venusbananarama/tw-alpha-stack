#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Long-only TopN backtest (P2-MVP).

Inputs:
  --factors   factor data (parquet/feather/csv)
  --out-dir   output folder for this run
  --config    YAML config
  --as-of     optional override for as_of date

Outputs:
  nav_clean.csv, positions.csv, trades.csv, metrics.json, run_manifest.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml  # type: ignore


def _log(level: str, msg: str, current: str) -> None:
    levels = {"DEBUG": 10, "INFO": 20}
    if levels.get(level, 20) >= levels.get(current, 20):
        print(f"[{level}] {msg}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--factors", required=True, help="Path to factor data (parquet/feather/csv)")
    p.add_argument("--out-dir", required=True, help="Output directory for this run")
    p.add_argument("--config", required=True, help="Backtest config YAML")
    p.add_argument("--as-of", default=None, help="Override as_of date (YYYY-MM-DD)")
    p.add_argument("--log-level", default="INFO", choices=["INFO", "DEBUG"])
    return p.parse_args()


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".parquet"}:
        return pd.read_parquet(path)
    if path.suffix.lower() in {".feather"}:
        return pd.read_feather(path)
    if path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file format: {path}")


def _load_yaml(path: Path) -> Dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return raw


def _parse_date(s: Optional[str]) -> Optional[pd.Timestamp]:
    if s is None:
        return None
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in cols:
            return cols[c]
    return None


def _load_universe(path: Optional[str], log_level: str) -> Optional[List[str]]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        _log("INFO", f"Universe path not found, skipping: {p}", log_level)
        return None
    if p.suffix.lower() in {".txt"}:
        symbols = [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        return sorted(list(set(symbols)))
    df = _read_table(p)
    sym_col = _pick_col(df, ["symbol", "stock_id", "ticker"])
    if not sym_col:
        raise ValueError(f"Universe file missing symbol column: {p}")
    symbols = df[sym_col].astype(str).dropna().unique().tolist()
    return sorted(list(set(symbols)))


def _normalize_factors(
    df: pd.DataFrame,
    signal_cols: List[str],
    direction: str,
    weekly_anchor: str,
) -> pd.DataFrame:
    date_col = _pick_col(df, ["date", "datetime"])
    sym_col = _pick_col(df, ["symbol", "stock_id", "ticker"])
    if not date_col or not sym_col:
        raise ValueError("Factor data must have date + symbol columns.")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()]
    df[sym_col] = df[sym_col].astype(str)

    cols_lower = {c.lower(): c for c in df.columns}
    if "factor_id" in cols_lower and "factor_value" in cols_lower:
        fid_col = cols_lower["factor_id"]
        fval_col = cols_lower["factor_value"]
        if signal_cols:
            df = df[df[fid_col].isin(signal_cols)]
        df[fval_col] = pd.to_numeric(df[fval_col], errors="coerce")
        df = df[df[fval_col].notna()]
        grp = df.groupby([date_col, sym_col], as_index=False)[fval_col].mean()
        out = grp.rename(columns={date_col: "date", sym_col: "symbol", fval_col: "signal"})
    else:
        if not signal_cols:
            raise ValueError("signal.columns is required for wide factor inputs.")
        missing = [c for c in signal_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing signal columns in factor data: {missing}")
        vals = df[signal_cols].astype(float)
        signal = vals.mean(axis=1) if len(signal_cols) > 1 else vals.iloc[:, 0]
        out = pd.DataFrame(
            {
                "date": df[date_col],
                "symbol": df[sym_col].astype(str),
                "signal": pd.to_numeric(signal, errors="coerce"),
            }
        )

    out = out[out["signal"].notna()]
    if direction == "lower_is_better":
        out["signal"] = -out["signal"]

    out = out.sort_values(["symbol", "date"])
    out["week"] = out["date"].dt.to_period(weekly_anchor)
    aligned = out.groupby(["symbol", "week"], as_index=False).last()
    return aligned[["date", "symbol", "signal"]]


def _normalize_prices(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str]]:
    date_col = _pick_col(df, ["date", "datetime"])
    sym_col = _pick_col(df, ["symbol", "stock_id", "ticker"])
    if not date_col or not sym_col:
        raise ValueError("Price data must have date + symbol columns.")
    price_col = _pick_col(df, ["close", "adj_close", "price", "nav", "value"])
    if not price_col:
        raise ValueError("Price data must have a price-like column (close/adj_close/price/nav/value).")
    vol_col = _pick_col(df, ["volume", "vol", "turnover", "trading_volume"])
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()]
    df[sym_col] = df[sym_col].astype(str)
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    if vol_col:
        df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce")
    df = df[df[price_col].notna()]
    out = df.rename(columns={date_col: "date", sym_col: "symbol", price_col: "price"})
    if vol_col:
        out = out.rename(columns={vol_col: "volume"})
    return out, "price", "volume" if vol_col else None


def _infer_annualization_factor(dates: pd.Series) -> int:
    if len(dates) < 3:
        return 252
    s = dates.sort_values().diff().median()
    if pd.isna(s):
        return 252
    days = s / pd.Timedelta(days=1)
    if days <= 1.5:
        return 252
    if days <= 8:
        return 52
    if days <= 20:
        return 12
    return 252


def _max_drawdown(nav: pd.Series) -> float:
    peaks = nav.cummax()
    dd = nav / peaks - 1.0
    return float(dd.min()) if len(dd) else float("nan")


def _compute_metrics(nav: pd.Series, dates: pd.Series) -> Dict[str, float]:
    ret = nav.pct_change().fillna(0.0)
    ann = _infer_annualization_factor(dates)
    vol = ret.std(ddof=1) * np.sqrt(ann)
    sharpe = (ret.mean() * ann) / vol if vol > 0 else np.nan
    total_return = nav.iloc[-1] / nav.iloc[0] - 1.0 if len(nav) >= 2 else np.nan
    cagr = (nav.iloc[-1] / nav.iloc[0]) ** (ann / max(len(nav), 1)) - 1.0 if len(nav) >= 2 else np.nan
    return {
        "CAGR": float(cagr),
        "Sharpe": float(sharpe),
        "MaxDD": float(_max_drawdown(nav)),
        "total_return": float(total_return),
        "ann_factor": int(ann),
        "periods": int(len(nav)),
        "start": str(pd.to_datetime(dates.iloc[0]).date()) if len(dates) else None,
        "end": str(pd.to_datetime(dates.iloc[-1]).date()) if len(dates) else None,
    }


def _hash_config(cfg: Dict) -> str:
    dumped = json.dumps(cfg, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def _git_commit_hash(root: Path) -> Optional[str]:
    git_dir = root / ".git"
    if not git_dir.exists():
        return None
    head = git_dir / "HEAD"
    if not head.exists():
        return None
    head_text = head.read_text(encoding="utf-8").strip()
    if head_text.startswith("ref:"):
        ref = head_text.split(" ", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
        packed = git_dir / "packed-refs"
        if packed.exists():
            for line in packed.read_text(encoding="utf-8").splitlines():
                if line.startswith("#") or " " not in line:
                    continue
                sha, ref_name = line.split(" ", 1)
                if ref_name.strip() == ref:
                    return sha.strip()
        return None
    return head_text


def _select_topn(df: pd.DataFrame, topn: int) -> pd.DataFrame:
    if df.empty:
        return df
    ranked = df.sort_values("signal", ascending=False)
    return ranked.head(topn)


def _build_weights(
    factors: pd.DataFrame,
    prices: pd.DataFrame,
    topn: int,
    min_price: Optional[float],
    min_volume: Optional[float],
) -> Dict[pd.Timestamp, pd.DataFrame]:
    weights: Dict[pd.Timestamp, pd.DataFrame] = {}
    for d, g in factors.groupby("date"):
        day_prices = prices[prices["date"] == d]
        if min_price is not None and not day_prices.empty:
            g = g.merge(day_prices[["symbol", "price"]], on="symbol", how="left")
            g = g[g["price"].notna() & (g["price"] >= min_price)]
        if min_volume is not None and "volume" in prices.columns and not day_prices.empty:
            g = g.merge(day_prices[["symbol", "volume"]], on="symbol", how="left")
            g = g[g["volume"].notna() & (g["volume"] >= min_volume)]
        g = g[["symbol", "signal"]].dropna()
        picks = _select_topn(g, topn)
        if picks.empty:
            weights[d] = pd.DataFrame(columns=["symbol", "weight"])
            continue
        w = 1.0 / len(picks)
        weights[d] = pd.DataFrame({"symbol": picks["symbol"].values, "weight": w})
    return weights


def _map_exec_dates(signal_dates: List[pd.Timestamp], trading_dates: List[pd.Timestamp], delay: int) -> Dict[pd.Timestamp, pd.Timestamp]:
    tset = pd.Index(trading_dates)
    mapping: Dict[pd.Timestamp, pd.Timestamp] = {}
    for d in signal_dates:
        if d not in tset:
            idx = tset.searchsorted(d, side="right") - 1
        else:
            idx = tset.get_loc(d)
        if idx < 0:
            continue
        exec_idx = idx + int(delay)
        if exec_idx >= len(tset):
            continue
        mapping[d] = tset[exec_idx]
    return mapping


def main() -> None:
    args = _parse_args()
    log_level = args.log_level
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = _load_yaml(Path(args.config))
    if args.as_of:
        cfg.setdefault("calendar", {})
        cfg["calendar"]["as_of"] = args.as_of

    calendar_cfg = cfg.get("calendar", {})
    weekly_anchor = calendar_cfg.get("weekly_anchor", "W-FRI")
    exec_delay_days = int(calendar_cfg.get("exec_delay_days", 1))
    as_of = _parse_date(calendar_cfg.get("as_of"))

    universe_cfg = cfg.get("universe", {})
    universe_path = universe_cfg.get("path")
    min_price = universe_cfg.get("min_price")
    min_volume = universe_cfg.get("min_volume")

    data_cfg = cfg.get("data", {})
    prices_path = data_cfg.get("prices_path")
    if not prices_path:
        raise SystemExit("config.data.prices_path is required.")

    strategy_cfg = cfg.get("strategy", {})
    topn = int(strategy_cfg.get("topN", 0) or 0)
    if topn <= 0:
        raise SystemExit("config.strategy.topN must be > 0.")

    signal_cfg = cfg.get("signal", {})
    signal_cols = signal_cfg.get("columns", [])
    if isinstance(signal_cols, str):
        signal_cols = [signal_cols]
    direction = signal_cfg.get("direction", "higher_is_better")

    costs_cfg = cfg.get("costs", {})
    fees_bps = float(costs_cfg.get("fees_bps", 0.0))
    tax_bps = float(costs_cfg.get("tax_bps", 0.0))
    slip_bps = float(costs_cfg.get("slip_bps", 0.0))
    total_bps = fees_bps + tax_bps + slip_bps

    portfolio_cfg = cfg.get("portfolio", {})
    initial_capital = float(portfolio_cfg.get("initial_capital", 1_000_000.0))
    allow_cash = bool(portfolio_cfg.get("allow_cash", True))

    _log("INFO", f"Reading factors: {args.factors}", log_level)
    factors_raw = _read_table(Path(args.factors))
    factors = _normalize_factors(factors_raw, signal_cols, direction, weekly_anchor)

    if as_of is not None:
        factors = factors[factors["date"] <= as_of]

    universe = _load_universe(universe_path, log_level)
    if universe:
        factors = factors[factors["symbol"].isin(universe)]

    if factors.empty:
        raise SystemExit("No factor rows after filtering.")

    _log("INFO", f"Reading prices: {prices_path}", log_level)
    prices_raw = _read_table(Path(prices_path))
    prices, _, _ = _normalize_prices(prices_raw)
    if as_of is not None:
        prices = prices[prices["date"] <= as_of]

    prices = prices.sort_values(["symbol", "date"])
    trading_dates = sorted(prices["date"].dropna().unique().tolist())
    if not trading_dates:
        raise SystemExit("No trading dates available in price data.")

    weights_by_signal_date = _build_weights(factors, prices, topn, min_price, min_volume)
    signal_dates = sorted(weights_by_signal_date.keys())
    exec_map = _map_exec_dates(signal_dates, trading_dates, exec_delay_days)

    if not exec_map:
        raise SystemExit("No executable rebalance dates after applying exec_delay_days.")

    exec_weights: List[pd.DataFrame] = []
    positions_rows: List[Dict] = []
    trades_rows: List[Dict] = []
    turnover_rows: List[Dict] = []

    prev_weights: Dict[str, float] = {}
    for sig_date in signal_dates:
        if sig_date not in exec_map:
            continue
        exec_date = exec_map[sig_date]
        wdf = weights_by_signal_date[sig_date]
        wdf = wdf.copy()
        wdf["exec_date"] = exec_date
        exec_weights.append(wdf)

        curr_weights = {r["symbol"]: float(r["weight"]) for r in wdf.to_dict("records")}
        symbols = sorted(set(prev_weights.keys()) | set(curr_weights.keys()))
        turnover = 0.0
        for sym in symbols:
            prev = prev_weights.get(sym, 0.0)
            curr = curr_weights.get(sym, 0.0)
            delta = curr - prev
            turnover += abs(delta)
            trades_rows.append(
                {
                    "date": exec_date.date().isoformat(),
                    "symbol": sym,
                    "weight_delta": float(delta),
                    "turnover_est": 0.0,
                }
            )
        turnover = turnover * 0.5
        for row in trades_rows[-len(symbols) :]:
            row["turnover_est"] = float(turnover)
        turnover_rows.append({"date": exec_date, "turnover_est": float(turnover)})

        for sym, w in curr_weights.items():
            if w == 0:
                continue
            positions_rows.append(
                {
                    "date": exec_date.date().isoformat(),
                    "symbol": sym,
                    "weight": float(w),
                }
            )
        prev_weights = curr_weights

    if not exec_weights:
        raise SystemExit("No positions generated.")

    weights_df = pd.concat(exec_weights, ignore_index=True)
    weights_df = weights_df.rename(columns={"exec_date": "exec_date"})
    weights_df["exec_date"] = pd.to_datetime(weights_df["exec_date"])

    prices = prices.sort_values(["symbol", "date"])
    prices["ret"] = prices.groupby("symbol")["price"].pct_change().fillna(0.0)
    ret_df = prices[["date", "symbol", "ret"]].copy()

    exec_dates = sorted(weights_df["exec_date"].unique().tolist())
    date_map = pd.DataFrame({"date": sorted(ret_df["date"].unique())})
    date_map = date_map.sort_values("date")
    exec_map_df = pd.DataFrame({"exec_date": exec_dates}).sort_values("exec_date")
    date_map = pd.merge_asof(date_map, exec_map_df, left_on="date", right_on="exec_date", direction="backward")

    ret_df = ret_df.merge(date_map, on="date", how="left")
    ret_df = ret_df.merge(weights_df, on=["exec_date", "symbol"], how="left")
    ret_df["weight"] = ret_df["weight"].fillna(0.0)

    weight_sum = weights_df.groupby("exec_date")["weight"].sum().rename("weight_sum")
    ret_df = ret_df.merge(weight_sum, on="exec_date", how="left")
    ret_df["weight_sum"] = ret_df["weight_sum"].fillna(0.0)
    if not allow_cash:
        ret_df["weight"] = np.where(ret_df["weight_sum"] > 0, ret_df["weight"] / ret_df["weight_sum"], 0.0)
        ret_df["weight_sum"] = np.where(ret_df["weight_sum"] > 0, 1.0, 0.0)

    gross = ret_df.groupby("date").apply(lambda x: float((x["weight"] * x["ret"]).sum()))
    gross = gross.rename("ret_gross").reset_index()

    turnover_df = pd.DataFrame(turnover_rows)
    if turnover_df.empty:
        turnover_df = pd.DataFrame({"date": [], "turnover_est": []})
    turnover_df = turnover_df.rename(columns={"date": "exec_date"})
    turnover_df["exec_date"] = pd.to_datetime(turnover_df["exec_date"])
    turnover_df["cost_rate"] = turnover_df["turnover_est"] * (total_bps / 10000.0)

    cost_map = turnover_df.rename(columns={"exec_date": "date"})[["date", "cost_rate"]]
    nav_df = gross.merge(cost_map, on="date", how="left")
    nav_df["cost_rate"] = nav_df["cost_rate"].fillna(0.0)
    nav_df["ret_net"] = nav_df["ret_gross"] - nav_df["cost_rate"]
    nav_df = nav_df.sort_values("date")
    nav_df["nav_gross"] = (1.0 + nav_df["ret_gross"]).cumprod() * initial_capital
    nav_df["nav_net"] = (1.0 + nav_df["ret_net"]).cumprod() * initial_capital

    nav_clean = nav_df[["date", "nav_gross", "nav_net", "ret_gross", "ret_net"]].copy()
    nav_clean["date"] = nav_clean["date"].dt.date.astype(str)
    nav_clean.to_csv(out_dir / "nav_clean.csv", index=False)

    pd.DataFrame(positions_rows).to_csv(out_dir / "positions.csv", index=False)
    pd.DataFrame(trades_rows).to_csv(out_dir / "trades.csv", index=False)

    metrics_net = _compute_metrics(nav_df["nav_net"], nav_df["date"])
    metrics_gross = _compute_metrics(nav_df["nav_gross"], nav_df["date"])
    avg_turnover = float(np.nanmean(turnover_df["turnover_est"].values)) if not turnover_df.empty else 0.0
    metrics = {
        "metrics": {
            **metrics_net,
            "Turnover": avg_turnover,
        },
        "metrics_gross": metrics_gross,
        "costs": {
            "fees_bps": fees_bps,
            "tax_bps": tax_bps,
            "slip_bps": slip_bps,
            "total_bps": total_bps,
        },
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    manifest = {
        "as_of": str(as_of.date()) if as_of is not None else None,
        "config_hash": _hash_config(cfg),
        "inputs": {
            "factors": str(Path(args.factors)),
            "prices": str(Path(prices_path)),
            "universe": str(Path(universe_path)) if universe_path else None,
            "benchmarks": str(Path(data_cfg.get("benchmarks_path"))) if data_cfg.get("benchmarks_path") else None,
            "config": str(Path(args.config)),
        },
        "git_commit": _git_commit_hash(Path.cwd()),
    }
    (out_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    _log("INFO", f"Done. Outputs in {out_dir}", log_level)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(2)
