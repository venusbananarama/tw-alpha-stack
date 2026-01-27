from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from alpha_core.dates import parse_ymd
from alpha_core.io import load_silver_data

from .errors import InfeasibleError, InputNotFoundError, InsufficientDataError, SchemaInvalidError
from .schemas import ArtifactNames, TARGET_PORTFOLIO_COLUMNS


_DATE_COL_CANDS = ("date", "trade_date", "as_of", "ts", "datetime")
_SYMBOL_COL_CANDS = ("symbol", "stock_id", "ticker", "secid", "id")
_CLOSE_COL_CANDS = ("close", "adj_close", "close_price", "price", "px_close", "last")
_VOLUME_COL_CANDS = ("volume", "vol", "qty", "volume_shares", "share_volume")

_PORTFOLIO_STRATEGY_ID = "p5_blend"


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    version: str
    params: Dict[str, object]
    strategy_type: str = "long_only"


@dataclass
class StrategyEvalResult:
    metrics: pd.DataFrame
    returns: pd.DataFrame
    candidate_count: int
    strategy_outputs: Dict[str, pd.DataFrame]
    last_price_date: str


@dataclass
class DeCorrResult:
    corr_matrix: pd.DataFrame
    selected_strategy_ids: List[str]
    decision_trace: Dict[str, object]


@dataclass
class AllocResult:
    alloc_table: pd.DataFrame
    method: str
    constraints_check: Dict[str, object]


@dataclass
class TargetPortfolioResult:
    target_df: pd.DataFrame
    row_count: int
    skipped_symbols: List[str]
    schema_check: Dict[str, object]


def build_seed_registry(profile: str, as_of: str, topn: int) -> List[StrategySpec]:
    if topn <= 0:
        raise ValueError("topn must be positive")
    seed = [
        StrategySpec(
            strategy_id="mom_20d",
            version="v1",
            params={"lookback": 20, "topn": topn, "profile": profile, "as_of": as_of},
        ),
        StrategySpec(
            strategy_id="vol_20d",
            version="v1",
            params={"lookback": 20, "topn": topn, "profile": profile, "as_of": as_of},
        ),
    ]
    return seed


def _detect_col(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        key = cand.lower()
        if key in lowered:
            return lowered[key]
    return None


def _normalize_symbol(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _load_prices_from_dir(prices_dir: Path) -> pd.DataFrame:
    parquet_files = list(prices_dir.rglob("*.parquet"))
    csv_files = list(prices_dir.rglob("*.csv"))
    frames: List[pd.DataFrame] = []
    for p in parquet_files:
        frames.append(pd.read_parquet(p))
    for p in csv_files:
        frames.append(pd.read_csv(p))
    if not frames:
        raise InputNotFoundError(f"prices data not found under {prices_dir}")
    return pd.concat(frames, ignore_index=True)


def _standardize_prices(df: pd.DataFrame) -> pd.DataFrame:
    date_col = _detect_col(df.columns, _DATE_COL_CANDS)
    symbol_col = _detect_col(df.columns, _SYMBOL_COL_CANDS)
    close_col = _detect_col(df.columns, _CLOSE_COL_CANDS)
    volume_col = _detect_col(df.columns, _VOLUME_COL_CANDS)
    if not date_col or not symbol_col or not close_col:
        raise SchemaInvalidError(
            "prices data missing required columns",
            details={"date": date_col, "symbol": symbol_col, "close": close_col, "columns": list(df.columns)},
        )

    keep_cols = [date_col, symbol_col, close_col] + ([volume_col] if volume_col else [])
    df = df[keep_cols].copy()
    rename_map = {date_col: "date", symbol_col: "symbol", close_col: "close"}
    if volume_col:
        rename_map[volume_col] = "volume"
    df = df.rename(columns=rename_map)
    df["date"] = _coerce_date(df["date"])
    df = df[df["date"].notna()].copy()
    df["symbol"] = _normalize_symbol(df["symbol"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df[df["close"].notna()].copy()
    return df.reset_index(drop=True)


def _load_prices(prices_path: str, as_of: str, lookback_days: int) -> pd.DataFrame:
    path = Path(prices_path)
    if path.is_dir():
        df = _load_prices_from_dir(path)
    else:
        if not path.exists():
            raise InputNotFoundError(f"prices_path not found: {prices_path}")
        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        else:
            raise SchemaInvalidError(f"unsupported prices file type: {path.suffix}")
    if df.empty:
        raise InsufficientDataError("prices data empty")
    df = _standardize_prices(df)

    as_of_date = parse_ymd(as_of)
    df = df[df["date"] <= as_of_date]
    if df.empty:
        raise InsufficientDataError("prices data empty after as_of filter")

    dates = sorted(df["date"].unique())
    if len(dates) > lookback_days:
        keep = set(dates[-lookback_days:])
        df = df[df["date"].isin(keep)]
    return df.reset_index(drop=True)


def _load_prices_panel(prices_path: str, as_of: str, lookback_days: int) -> pd.DataFrame:
    path = Path(prices_path)
    if path.is_dir():
        root = _infer_repo_root(path)
        if root is not None:
            dataset = path.name
            start = parse_ymd(as_of) - timedelta(days=lookback_days)
            df = load_silver_data(root, dataset, start, parse_ymd(as_of), columns=None)
            if not df.empty:
                return _standardize_prices(df)
    return _load_prices(prices_path, as_of, lookback_days)


def _infer_repo_root(path: Path) -> Optional[Path]:
    current = path.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "alpha_core").exists():
            return parent
    return None


def _build_universe(universe_path: Optional[str], prices_df: pd.DataFrame) -> Tuple[List[str], bool]:
    if universe_path:
        p = Path(universe_path)
        if p.exists():
            lines = [line.strip() for line in p.read_text(encoding="utf-8").splitlines()]
            symbols = [s for s in lines if s]
            return sorted(set(symbols)), False
    last_date = prices_df["date"].max()
    symbols = sorted(set(prices_df.loc[prices_df["date"] == last_date, "symbol"].tolist()))
    return symbols, True


def _pivot_close(prices_df: pd.DataFrame) -> pd.DataFrame:
    return (
        prices_df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .sort_index(axis=1)
    )


def _pivot_volume(prices_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if "volume" not in prices_df.columns:
        return None
    return (
        prices_df.pivot_table(index="date", columns="symbol", values="volume", aggfunc="last")
        .sort_index()
        .sort_index(axis=1)
    )


def _strategy_weights_momentum(close_px: pd.DataFrame, lookback: int, topn: int) -> pd.DataFrame:
    window = close_px.tail(lookback + 1)
    if len(window) < lookback + 1:
        raise InsufficientDataError("insufficient data for momentum strategy")
    first = window.iloc[0]
    last = window.iloc[-1]
    score = (last / first) - 1.0
    df = score.reset_index()
    df.columns = ["symbol", "score"]
    df = df.dropna()
    df = df.sort_values(["score", "symbol"], ascending=[False, True], kind="mergesort")
    selected = df.head(topn).copy()
    if selected.empty:
        raise InsufficientDataError("no symbols for momentum strategy")
    weight = 1.0 / len(selected)
    selected.loc[:, "weight"] = weight
    return selected[["symbol", "weight"]].reset_index(drop=True)


def _strategy_weights_volume(
    close_px: pd.DataFrame,
    vol_px: Optional[pd.DataFrame],
    lookback: int,
    topn: int,
) -> pd.DataFrame:
    if vol_px is None or vol_px.dropna(how="all").empty:
        proxy = close_px.tail(lookback).mean()
    else:
        proxy = vol_px.tail(lookback).mean()
    df = proxy.reset_index()
    df.columns = ["symbol", "score"]
    df = df.dropna()
    df = df.sort_values(["score", "symbol"], ascending=[False, True], kind="mergesort")
    selected = df.head(topn).copy()
    if selected.empty:
        raise InsufficientDataError("no symbols for volume strategy")
    weight = 1.0 / len(selected)
    selected.loc[:, "weight"] = weight
    return selected[["symbol", "weight"]].reset_index(drop=True)


def _apply_universe(weights: pd.DataFrame, universe: List[str]) -> pd.DataFrame:
    if not universe:
        return weights
    filtered = weights[weights["symbol"].isin(set(universe))].copy()
    if filtered.empty:
        return filtered
    total = float(filtered["weight"].sum())
    if total > 0:
        filtered["weight"] = filtered["weight"] / total
    return filtered.reset_index(drop=True)


def evaluate_strategies(
    specs: List[StrategySpec],
    prices_path: str,
    universe_path: Optional[str],
    as_of: str,
    windows: List[int],
) -> StrategyEvalResult:
    lookback = max([int(spec.params.get("lookback", 20)) for spec in specs] + windows + [20])
    prices_df = _load_prices_panel(prices_path, as_of, lookback_days=lookback + 5)
    universe, _ = _build_universe(universe_path, prices_df)
    close_px = _pivot_close(prices_df)
    vol_px = _pivot_volume(prices_df)

    if close_px.empty or close_px.shape[0] < 2:
        raise InsufficientDataError("insufficient close data for returns")

    strategy_outputs: Dict[str, pd.DataFrame] = {}
    for spec in specs:
        params = spec.params
        topn = int(params.get("topn", 50))
        lb = int(params.get("lookback", 20))
        if spec.strategy_id == "mom_20d":
            weights = _strategy_weights_momentum(close_px, lb, topn)
        elif spec.strategy_id == "vol_20d":
            weights = _strategy_weights_volume(close_px, vol_px, lb, topn)
        else:
            raise SchemaInvalidError(f"unknown strategy_id: {spec.strategy_id}")
        weights = _apply_universe(weights, universe)
        if weights.empty:
            raise InsufficientDataError(f"strategy {spec.strategy_id} has no universe symbols")
        strategy_outputs[spec.strategy_id] = weights

    daily_returns = close_px.pct_change(fill_method=None).iloc[1:]
    eval_window = max(windows + [20])
    daily_returns = daily_returns.tail(eval_window)
    if daily_returns.empty:
        raise InsufficientDataError("insufficient returns window")

    returns_matrix: Dict[str, pd.Series] = {}
    metrics_rows: List[Dict[str, object]] = []
    for spec in specs:
        weights = strategy_outputs[spec.strategy_id].set_index("symbol")["weight"]
        common = [s for s in weights.index if s in daily_returns.columns]
        if not common:
            raise InsufficientDataError(f"strategy {spec.strategy_id} has no return symbols")
        w = weights.loc[common]
        panel = daily_returns[common].fillna(0.0)
        strat_returns = (panel * w).sum(axis=1)
        returns_matrix[spec.strategy_id] = strat_returns
        metrics_rows.append(
            {
                "strategy_id": spec.strategy_id,
                "n_obs": int(strat_returns.shape[0]),
                "mean_return": float(strat_returns.mean()),
                "volatility": float(strat_returns.std(ddof=0)),
            }
        )

    returns_df = pd.DataFrame(returns_matrix).dropna(how="all")
    metrics_df = pd.DataFrame(metrics_rows).set_index("strategy_id").sort_index()
    last_date = close_px.index.max()
    return StrategyEvalResult(
        metrics=metrics_df,
        returns=returns_df,
        candidate_count=len(specs),
        strategy_outputs=strategy_outputs,
        last_price_date=str(last_date),
    )


def decorrelate_strategies(
    returns_matrix: pd.DataFrame,
    threshold: float,
    min_pool_size: int,
) -> DeCorrResult:
    if returns_matrix.empty:
        raise InsufficientDataError("returns matrix empty")
    strategies = list(returns_matrix.columns)
    corr = returns_matrix.corr().fillna(0.0)
    selected = strategies.copy()
    trace: List[Dict[str, object]] = []
    for i, sid in enumerate(strategies):
        if sid not in selected:
            continue
        for j in range(i + 1, len(strategies)):
            other = strategies[j]
            if other not in selected:
                continue
            value = float(corr.loc[sid, other])
            if abs(value) > threshold:
                selected.remove(other)
                trace.append(
                    {
                        "keep": sid,
                        "drop": other,
                        "corr": value,
                        "threshold": threshold,
                        "reason": "corr_exceeds_threshold",
                    }
                )
    if len(selected) < min_pool_size:
        raise InfeasibleError(
            "min_pool_size not satisfied",
            details={"min_pool_size": min_pool_size, "selected": len(selected)},
        )
    decision_trace = {"decisions": trace, "selected": selected}
    return DeCorrResult(corr_matrix=corr, selected_strategy_ids=selected, decision_trace=decision_trace)


def allocate_strategies(selected_strategy_ids: List[str], method: str = "equal_weight") -> AllocResult:
    if not selected_strategy_ids:
        raise InfeasibleError("no strategies selected")
    if method != "equal_weight":
        raise SchemaInvalidError(f"unsupported alloc_method: {method}")
    weight = 1.0 / len(selected_strategy_ids)
    alloc = pd.DataFrame(
        {"strategy_id": selected_strategy_ids, "weight": [weight] * len(selected_strategy_ids)}
    ).sort_values("strategy_id", kind="mergesort")
    constraints = {"sum_weight": float(alloc["weight"].sum()), "method": method}
    return AllocResult(alloc_table=alloc.reset_index(drop=True), method=method, constraints_check=constraints)


def compile_target_portfolio(
    alloc: AllocResult,
    specs: List[StrategySpec],
    eval_result: StrategyEvalResult,
    prices_path: str,
    universe_path: Optional[str],
    as_of: str,
    notional: int,
) -> TargetPortfolioResult:
    if alloc.alloc_table.empty:
        raise InfeasibleError("empty allocation table")
    allocations = alloc.alloc_table.set_index("strategy_id")["weight"].to_dict()
    combined: Dict[str, float] = {}
    for sid, w in allocations.items():
        if sid not in eval_result.strategy_outputs:
            raise SchemaInvalidError(f"missing strategy output: {sid}")
        weights = eval_result.strategy_outputs[sid]
        for _, row in weights.iterrows():
            symbol = str(row["symbol"]).strip()
            if not symbol:
                continue
            combined[symbol] = combined.get(symbol, 0.0) + float(row["weight"]) * float(w)
    if not combined:
        raise InfeasibleError("combined weights empty")

    prices_df = _load_prices_panel(prices_path, as_of, lookback_days=5)
    close_px = _pivot_close(prices_df)
    if close_px.empty:
        raise InsufficientDataError("close data missing for target portfolio")
    last_date = close_px.index.max()
    last_close = close_px.loc[last_date]

    rows: List[Dict[str, object]] = []
    skipped: List[str] = []
    for symbol in sorted(combined.keys()):
        weight = combined[symbol]
        close = last_close.get(symbol)
        if close is None or pd.isna(close) or close <= 0:
            skipped.append(symbol)
            continue
        qty = int(math.floor(weight * notional / float(close)))
        if qty == 0:
            continue
        rows.append({"symbol": symbol, "target_qty": qty, "strategy_id": _PORTFOLIO_STRATEGY_ID})

    if not rows:
        raise InfeasibleError("target portfolio empty after sizing", details={"skipped": skipped})

    df = pd.DataFrame(rows, columns=TARGET_PORTFOLIO_COLUMNS)
    df = df.sort_values("symbol", kind="mergesort").reset_index(drop=True)
    schema_check = {"ok": True, "errors": []}
    return TargetPortfolioResult(
        target_df=df,
        row_count=int(df.shape[0]),
        skipped_symbols=skipped,
        schema_check=schema_check,
    )


def validate_target_portfolio(path: str) -> Dict[str, object]:
    p = Path(path)
    if not p.exists():
        return {"ok": False, "errors": ["target_portfolio_not_found"]}
    df = pd.read_csv(p)
    errors: List[str] = []
    cols = list(df.columns)
    if cols != TARGET_PORTFOLIO_COLUMNS:
        errors.append(f"invalid_columns:{cols}")
    if df.empty:
        errors.append("empty_target_portfolio")
    if "symbol" in df.columns:
        if df["symbol"].isna().any() or (df["symbol"].astype(str).str.strip() == "").any():
            errors.append("empty_symbol")
        if df["symbol"].duplicated().any():
            errors.append("duplicate_symbol")
    if "target_qty" in df.columns:
        try:
            df["target_qty"].astype(int)
        except Exception:
            errors.append("invalid_target_qty")
    return {"ok": not errors, "errors": errors}


def write_artifacts(
    out_dir: str,
    as_of: str,
    specs: List[StrategySpec],
    eval_result: StrategyEvalResult,
    decor_result: DeCorrResult,
    alloc_result: AllocResult,
    target_result: TargetPortfolioResult,
) -> Dict[str, str]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    artifacts: Dict[str, str] = {}
    pool_path = out_path / ArtifactNames.STRATEGY_POOL_JSON
    pool_payload = {
        "as_of": as_of,
        "strategy_specs": [
            {
                "strategy_id": spec.strategy_id,
                "version": spec.version,
                "params": spec.params,
                "strategy_type": spec.strategy_type,
            }
            for spec in specs
        ],
        "metrics": eval_result.metrics.reset_index().to_dict(orient="records"),
    }
    _write_json_atomic(pool_path, pool_payload)
    artifacts["strategy_pool"] = str(pool_path)

    corr_path = out_path / ArtifactNames.STRATEGY_CORR_FILE
    decor_result.corr_matrix.to_csv(corr_path, index=True)
    artifacts["strategy_corr"] = str(corr_path)

    trace_path = out_path / ArtifactNames.DECISION_TRACE_JSON
    _write_json_atomic(trace_path, decor_result.decision_trace)
    artifacts["decision_trace"] = str(trace_path)

    alloc_path = out_path / ArtifactNames.STRATEGY_ALLOC_CSV
    alloc_result.alloc_table.to_csv(alloc_path, index=False)
    artifacts["strategy_alloc"] = str(alloc_path)

    target_name = ArtifactNames.TARGET_PORTFOLIO_CSV_FMT.format(as_of=as_of)
    target_path = out_path / target_name
    _write_csv_atomic(target_path, target_result.target_df)
    artifacts["target_portfolio"] = str(target_path)

    summary_path = out_path / ArtifactNames.P5_SUMMARY_JSON
    artifacts["p5_summary"] = str(summary_path)
    return artifacts


def write_summary(summary_path: str, summary_obj: Dict[str, object]) -> None:
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(path, summary_obj)


def _write_json_atomic(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)
