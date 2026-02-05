from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Mapping, Optional, Tuple

import pandas as pd

from alpha_core.common.lockfile import FileLock, LockActiveError

from alpha_core.config import ConfigError, load_rules
from alpha_core.dates import parse_ymd
from alpha_core.phase4.calendar import is_trading_day, load_trading_days

from .errors import (
    ExitCode,
    Phase6Error,
    REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
    REASON_FAIL_BAD_TARGET_SCHEMA,
    REASON_FAIL_CASH_EXCEEDED,
    REASON_FAIL_CONCENTRATION_BREACH,
    REASON_FAIL_INTERNAL_ERROR,
    REASON_FAIL_LOCKED,
    REASON_FAIL_MISSING_PRICES,
    REASON_FAIL_MISSING_TARGET,
    REASON_FAIL_OVERLAY,
    REASON_FAIL_RISK_BUDGET,
    REASON_FAIL_RULES_INVALID,
    REASON_FAIL_TRACKING_ERROR,
    REASON_FAIL_TURNOVER_BREACH,
    REASON_OK,
    REASON_SKIP_NON_TRADING_DAY,
)
from .paths import LockError, build_out_dir, compute_run_id, resolve_phase6_paths
from .portfolio_construction import build_target_snapshot, scale_targets
from .risk_budget import compute_risk_budget
from .risk_metrics import compute_te_ir, load_benchmark_returns, load_price_returns
from .risk_overlay import build_adjustment_trace
from .schemas import (
    ArtifactNames,
    GateResult,
    P6_MANIFEST_SCHEMA_VERSION,
    P6_RISK_SCHEMA_VERSION,
    P6_SUMMARY_SCHEMA_VERSION,
    ResolvedPaths,
    RiskMetrics,
)


@dataclass(frozen=True)
class Phase6Result:
    status: str
    exit_code: int
    reason_code: str
    out_dir: Path
    summary_path: Path


LOCK_TTL_MINUTES = 1440


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _log_line(log_path: Optional[Path], message: str) -> None:
    ts = _now_iso()
    line = f"{ts} {message}"
    print(line)
    if log_path is None:
        return
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        return


def _emit_resolved_paths(log_path: Optional[Path], payload: ResolvedPaths) -> None:
    line = "resolved_paths=" + json.dumps(payload, ensure_ascii=True, sort_keys=True)
    _log_line(log_path, line)


def _write_json_atomic(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)
    tmp.replace(path)


def _write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_rules(rules: Mapping[str, object]) -> str:
    payload = json.dumps(rules, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_inputs(paths: ResolvedPaths) -> Tuple[str, Dict[str, str]]:
    items: Dict[str, Optional[Path]] = {
        "target_csv": Path(paths["target_csv"]) if paths.get("target_csv") else None,
        "prices_parquet": Path(paths["prices_parquet"]) if paths.get("prices_parquet") else None,
        "calendar_csv": Path(paths["calendar_csv"]) if paths.get("calendar_csv") else None,
        "prev_positions_csv": Path(paths["prev_positions_csv"]) if paths.get("prev_positions_csv") else None,
        "prev_account_json": Path(paths["prev_account_json"]) if paths.get("prev_account_json") else None,
        "benchmark_file": Path(paths["benchmark_file"]) if paths.get("benchmark_file") else None,
    }
    per_file: Dict[str, str] = {}
    h = hashlib.sha256()
    for key in sorted(items):
        path = items[key]
        if path is None:
            token = "missing:<none>"
        elif path.exists():
            if key == "prices_parquet":
                resolved = _resolve_prices_partition(path, paths["as_of"]) if path.is_dir() else path
                token = _hash_file(resolved) if resolved is not None and resolved.exists() else f"missing:{path}"
            elif path.is_dir():
                token = f"dir:{path}"
            else:
                token = _hash_file(path)
        else:
            token = f"missing:{path}"
        per_file[key] = token
        h.update(key.encode("utf-8"))
        h.update(b":")
        h.update(token.encode("utf-8"))
        h.update(b";")
    return h.hexdigest(), per_file


def _norm_symbol(value: object) -> str:
    s = str(value).strip()
    if s.isdigit() and len(s) < 4:
        return s.zfill(4)
    return s


def _resolve_prices_partition(path: Path, as_of: str) -> Optional[Path]:
    if not path.exists():
        return None
    if not path.is_dir():
        return path
    as_of_ym = as_of[:7].replace("-", "")
    candidates: list[tuple[str, Path]] = []
    for child in path.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if not name.startswith("yyyymm="):
            continue
        ym = name.split("=", 1)[1]
        if len(ym) != 6 or not ym.isdigit():
            continue
        if ym > as_of_ym:
            continue
        data_path = child / "data.parquet"
        if data_path.exists():
            candidates.append((ym, data_path))
    if not candidates:
        return None
    _, data_path = max(candidates, key=lambda item: item[0])
    return data_path


def _gate_result(status: str, observed: object, threshold: object, detail: Optional[Dict[str, object]] = None) -> GateResult:
    return {
        "status": status,
        "observed": observed,
        "threshold": threshold,
        "detail": detail or {},
    }


def _init_gate_results() -> Dict[str, GateResult]:
    return {
        "trading_day": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "target_schema": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "prices": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "tracking_error": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "information_ratio": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "risk_budget": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "overlay": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "cash_usage": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "concentration": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
        "turnover": _gate_result("SKIP", None, None, {"reason": "not_evaluated"}),
    }


def _empty_metrics() -> RiskMetrics:
    return {
        "schema_version": P6_RISK_SCHEMA_VERSION,
        "nav_estimate": None,
        "gross_exposure": None,
        "net_exposure": None,
        "cash_available": None,
        "cash_usage_ratio": None,
        "max_single_name_ratio": None,
        "topk_concentration_ratio": None,
        "turnover_ratio": None,
        "tracking_error": None,
        "information_ratio": None,
        "active_return": None,
        "tracking_error_obs": None,
        "benchmark_last_date": None,
        "benchmark_obs": None,
    }


def _empty_breakdown() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "target_qty",
            "current_qty",
            "delta_qty",
            "price",
            "target_notional",
            "abs_target_notional",
            "trade_notional",
            "nav_ratio",
            "side",
        ]
    )


def _get_rule(rules: Mapping[str, object], path: str) -> object:
    node: object = rules
    for key in path.split("."):
        if not isinstance(node, Mapping) or key not in node:
            raise Phase6Error(
                f"rules missing: {path}",
                REASON_FAIL_RULES_INVALID,
                ExitCode.FAIL_INPUT,
                {"missing_key": path},
            )
        node = node[key]
    return node


def _require_int(rules: Mapping[str, object], path: str) -> int:
    value = _get_rule(rules, path)
    if not isinstance(value, int):
        raise Phase6Error(
            f"rules invalid type for {path}",
            REASON_FAIL_RULES_INVALID,
            ExitCode.FAIL_INPUT,
            {"key": path, "type": type(value).__name__},
        )
    return value


def _require_float(rules: Mapping[str, object], path: str) -> float:
    value = _get_rule(rules, path)
    if not isinstance(value, (int, float)):
        raise Phase6Error(
            f"rules invalid type for {path}",
            REASON_FAIL_RULES_INVALID,
            ExitCode.FAIL_INPUT,
            {"key": path, "type": type(value).__name__},
        )
    return float(value)


def _extract_phase6_rules(raw_rules: Mapping[str, object]) -> Dict[str, object]:
    if "phase6" not in raw_rules or not isinstance(raw_rules["phase6"], Mapping):
        raise Phase6Error(
            "rules missing phase6 section",
            REASON_FAIL_RULES_INVALID,
            ExitCode.FAIL_INPUT,
            {"missing_key": "phase6"},
        )
    rules = raw_rules["phase6"]
    return {
        "cash": {
            "buffer_bps": _require_int(rules, "cash.buffer_bps"),
            "max_cash_usage_ratio": _require_float(rules, "cash.max_cash_usage_ratio"),
        },
        "concentration": {
            "max_single_name_ratio": _require_float(rules, "concentration.max_single_name_ratio"),
            "topk_k": _require_int(rules, "concentration.topk_k"),
            "max_topk_ratio": _require_float(rules, "concentration.max_topk_ratio"),
        },
        "turnover": {
            "max_turnover_ratio": _require_float(rules, "turnover.max_turnover_ratio"),
        },
        "freshness": {
            "max_price_age_days": _require_int(rules, "freshness.max_price_age_days"),
            "max_snapshot_age_days": _require_int(rules, "freshness.max_snapshot_age_days"),
        },
    }


def _extract_phase6_optional(raw_rules: Mapping[str, object]) -> Dict[str, object]:
    phase6 = raw_rules.get("phase6", {}) if isinstance(raw_rules, Mapping) else {}
    if not isinstance(phase6, Mapping):
        return {"benchmark": {}, "risk": {}, "overlay": {}}
    out: Dict[str, object] = {}
    for key in ("benchmark", "risk", "overlay"):
        val = phase6.get(key, {})
        out[key] = val if isinstance(val, Mapping) else {}
    return out


def _coerce_window_map(value: object) -> Dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    out: Dict[str, float] = {}
    for k, v in value.items():
        try:
            key = str(int(k))
            if isinstance(v, (int, float)):
                out[key] = float(v)
        except Exception:
            continue
    return out


def _parse_risk_config(phase6_rules: Mapping[str, object]) -> Dict[str, object]:
    raw = phase6_rules.get("risk", {}) if isinstance(phase6_rules, Mapping) else {}
    if not isinstance(raw, Mapping):
        return {
            "te_windows": [252],
            "min_obs": 10,
            "max_te": {},
            "min_ir": {},
            "max_single_name_rc": None,
            "max_topk_rc": None,
            "topk_k": 5,
            "enforce_te": False,
            "enforce_ir": False,
        }
    windows = raw.get("te_windows")
    if isinstance(windows, list) and windows:
        te_windows = [int(w) for w in windows if isinstance(w, (int, float))]
    else:
        te_windows = [252]
    min_obs = int(raw.get("min_obs", 10)) if isinstance(raw.get("min_obs", 10), (int, float)) else 10
    max_te = _coerce_window_map(raw.get("max_te"))
    min_ir = _coerce_window_map(raw.get("min_ir"))
    max_single_name_rc = raw.get("max_single_name_rc")
    max_topk_rc = raw.get("max_topk_rc")
    topk_k = raw.get("topk_k")
    enforce_te = raw.get("enforce_te", False)
    enforce_ir = raw.get("enforce_ir", False)
    return {
        "te_windows": te_windows,
        "min_obs": min_obs,
        "max_te": max_te,
        "min_ir": min_ir,
        "max_single_name_rc": float(max_single_name_rc)
        if isinstance(max_single_name_rc, (int, float))
        else None,
        "max_topk_rc": float(max_topk_rc) if isinstance(max_topk_rc, (int, float)) else None,
        "topk_k": int(topk_k) if isinstance(topk_k, (int, float)) else 5,
        "enforce_te": bool(enforce_te),
        "enforce_ir": bool(enforce_ir),
    }


def _parse_overlay_config(phase6_rules: Mapping[str, object]) -> Dict[str, object]:
    raw = phase6_rules.get("overlay", {}) if isinstance(phase6_rules, Mapping) else {}
    if not isinstance(raw, Mapping):
        return {"enabled": False, "policy": "none", "trigger_on_warn_te": False}
    enabled = raw.get("enabled", False)
    policy = raw.get("policy", "none")
    trigger_on_warn_te = raw.get("trigger_on_warn_te", False)
    return {
        "enabled": bool(enabled),
        "policy": str(policy).strip().lower() if policy is not None else "none",
        "trigger_on_warn_te": bool(trigger_on_warn_te),
    }


def _load_target(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise Phase6Error("target not found", REASON_FAIL_MISSING_TARGET, ExitCode.FAIL_INPUT, {"path": str(path)})
    df = pd.read_csv(path)
    required = ["symbol", "target_qty"]
    for col in required:
        if col not in df.columns:
            raise Phase6Error(
                "target schema missing required column",
                REASON_FAIL_BAD_TARGET_SCHEMA,
                ExitCode.FAIL_INPUT,
                {"missing": col},
            )
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().apply(_norm_symbol)
    if out["symbol"].isna().any() or (out["symbol"] == "").any():
        raise Phase6Error(
            "target schema invalid symbol",
            REASON_FAIL_BAD_TARGET_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    if out["symbol"].duplicated().any():
        raise Phase6Error(
            "target schema duplicated symbols",
            REASON_FAIL_BAD_TARGET_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    qty = pd.to_numeric(out["target_qty"], errors="coerce")
    if qty.isna().any():
        raise Phase6Error(
            "target schema invalid target_qty",
            REASON_FAIL_BAD_TARGET_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    if (qty % 1 != 0).any():
        raise Phase6Error(
            "target schema target_qty not integer-like",
            REASON_FAIL_BAD_TARGET_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    out["target_qty"] = qty.astype(int)
    keep_cols = ["symbol", "target_qty"]
    if "strategy_id" in out.columns:
        out["strategy_id"] = out["strategy_id"].astype(str)
        keep_cols.append("strategy_id")
    return out[keep_cols].reset_index(drop=True)


def _load_positions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise Phase6Error(
            "positions not found",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
            {"path": str(path)},
        )
    df = pd.read_csv(path)
    for col in ["symbol", "qty"]:
        if col not in df.columns:
            raise Phase6Error(
                "positions schema missing required column",
                REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
                ExitCode.FAIL_INPUT,
                {"missing": col},
            )
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().apply(_norm_symbol)
    if out["symbol"].isna().any() or (out["symbol"] == "").any():
        raise Phase6Error(
            "positions schema invalid symbol",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    if out["symbol"].duplicated().any():
        raise Phase6Error(
            "positions schema duplicated symbols",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    qty = pd.to_numeric(out["qty"], errors="coerce")
    if qty.isna().any():
        raise Phase6Error(
            "positions schema invalid qty",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    if (qty % 1 != 0).any():
        raise Phase6Error(
            "positions schema qty not integer-like",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
        )
    out["qty"] = qty.astype(int)
    return out[["symbol", "qty"]].reset_index(drop=True)


def _load_account_snapshot(path: Path) -> Dict[str, float]:
    def _coerce_float(v: object) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            try:
                return float(s)
            except Exception:
                return None
        if isinstance(v, dict):
            # common nested numeric shapes: {"value": 123}, {"amount": 123}
            for key in ("cash_available", "buying_power", "cash", "nav", "equity", "value", "amount"):
                if key in v:
                    out = _coerce_float(v.get(key))
                    if out is not None:
                        return out
        return None

    if not path.exists():
        raise Phase6Error(
            "account snapshot not found",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
            {"path": str(path)},
        )

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    nav_raw = _coerce_float(payload.get("nav"))
    if nav_raw is None:
        nav_raw = _coerce_float(payload.get("equity"))

    cash_av_raw = _coerce_float(payload.get("cash_available"))
    if cash_av_raw is None:
        cash_raw = _coerce_float(payload.get("cash"))
        bp_raw = _coerce_float(payload.get("buying_power"))
        cands = [v for v in [cash_raw, bp_raw] if v is not None]
        if cands:
            cash_av_raw = float(min(cands))

    if nav_raw is None or cash_av_raw is None:
        raise Phase6Error(
            "account snapshot missing required keys",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
            {
                "keys": sorted(list(payload.keys())),
                "nav": payload.get("nav"),
                "equity": payload.get("equity"),
                "cash_available": payload.get("cash_available"),
                "cash": payload.get("cash"),
                "buying_power": payload.get("buying_power"),
            },
        )

    # If broker snapshot reports negative cash/cash_available, treat it as not usable for pre-trade.
    # Prefer buying_power when available; otherwise clamp to 0 to let cash gate fail deterministically.
    if cash_av_raw is not None and float(cash_av_raw) < 0:
        bp = _coerce_float(payload.get("buying_power"))
        if bp is not None and bp >= 0:
            cash_av_raw = float(bp)
        else:
            cash_av_raw = 0.0

    nav_val = float(nav_raw)
    cash_val = float(cash_av_raw)
    if nav_val <= 0:
        raise Phase6Error(
            "account snapshot nav must be positive and cash non-negative",
            REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
            ExitCode.FAIL_INPUT,
            {"nav": nav_val, "cash_available": cash_val},
        )

    return {"nav": nav_val, "cash_available": cash_val}


def _load_prices(path: Path, as_of: str, max_price_age_days: int) -> Tuple[Dict[str, float], str, Dict[str, object]]:
    if not path.exists():
        raise Phase6Error(
            "prices not found",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {"path": str(path)},
        )
    resolved = _resolve_prices_partition(path, as_of)
    if resolved is None:
        raise Phase6Error(
            "prices partition missing on or before as_of",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {
                "as_of": as_of,
                "pricing_asof": None,
                "age_days": None,
                "max_price_age_days": int(max_price_age_days),
                "missing_symbols_sample": [],
            },
        )
    path = resolved
    df = pd.read_parquet(path)
    for col in ["date", "close"]:
        if col not in df.columns:
            raise Phase6Error(
                "prices schema missing required column",
                REASON_FAIL_MISSING_PRICES,
                ExitCode.FAIL_INPUT,
                {"missing": col},
            )
    has_symbol_col = "symbol" in df.columns
    has_stock_id_col = "stock_id" in df.columns
    if not has_symbol_col and not has_stock_id_col:
        raise Phase6Error(
            "prices schema missing required column",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {"missing": "symbol or stock_id"},
        )
    symbol_dtype = str(df["symbol"].dtype) if has_symbol_col else None
    stock_id_dtype = str(df["stock_id"].dtype) if has_stock_id_col else None
    sample_cols = [col for col in ["date", "symbol", "stock_id", "close"] if col in df.columns]
    sample_df = df[sample_cols].copy()
    sample_df["date"] = pd.to_datetime(sample_df["date"], errors="coerce").dt.date
    sample_df["close"] = pd.to_numeric(sample_df["close"], errors="coerce")
    key_series = sample_df["symbol"] if has_symbol_col else sample_df["stock_id"]
    sample_df["norm_symbol"] = key_series.apply(_norm_symbol)
    sample_df = sample_df[sample_df["norm_symbol"].isin(["0050", "0052", "0056"])].head(5)
    sample_rows = []
    if not sample_df.empty:
        for _, row in sample_df.iterrows():
            dval = row.get("date")
            sample_rows.append(
                {
                    "date": dval.isoformat() if isinstance(dval, date) else None,
                    "symbol": row.get("symbol") if has_symbol_col else None,
                    "stock_id": row.get("stock_id") if has_stock_id_col else None,
                    "close": row.get("close"),
                }
            )
    cols = ["date", "close", "symbol"] if has_symbol_col else ["date", "close", "stock_id"]
    out = df[cols].copy()
    if has_symbol_col:
        out["symbol"] = out["symbol"].astype(str).str.strip().apply(_norm_symbol)
    else:
        out["symbol"] = out["stock_id"].apply(_norm_symbol)
        out = out.drop(columns=["stock_id"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out[out["date"].notna() & out["close"].notna()]
    if out.empty:
        raise Phase6Error(
            "prices data empty",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
        )
    as_of_date = parse_ymd(as_of)
    eligible = out[out["date"] <= as_of_date]
    if eligible.empty:
        raise Phase6Error(
            "prices missing on or before as_of",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {
                "as_of": as_of,
                "pricing_asof": None,
                "age_days": None,
                "max_price_age_days": int(max_price_age_days),
                "missing_symbols_sample": [],
            },
        )
    pricing_asof_date = max(eligible["date"])
    age_days = (as_of_date - pricing_asof_date).days
    pricing_asof = pricing_asof_date.isoformat()
    if age_days > max_price_age_days:
        raise Phase6Error(
            "prices stale",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {
                "as_of": as_of,
                "pricing_asof": pricing_asof,
                "age_days": int(age_days),
                "max_price_age_days": int(max_price_age_days),
                "missing_symbols_sample": [],
                "stale": True,
            },
        )
    out = eligible[eligible["date"] == pricing_asof_date]
    if out["symbol"].duplicated().any():
        raise Phase6Error(
            "prices duplicated symbol on pricing_asof",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
        )
    detail = {
        "pricing_asof": pricing_asof,
        "age_days": int(age_days),
        "max_price_age_days": int(max_price_age_days),
        "row_count": int(len(out)),
        "missing_symbols_sample": [],
        "symbol_dtype": symbol_dtype,
        "stock_id_dtype": stock_id_dtype,
        "has_symbol_col": has_symbol_col,
        "sample_symbols_for_0050": sample_rows,
    }
    return dict(zip(out["symbol"], out["close"])), pricing_asof, detail


def _compute_risk(
    target_df: pd.DataFrame,
    positions_df: pd.DataFrame,
    prices: Dict[str, float],
    nav: float,
    cash_available: float,
    buffer_bps: int,
    topk_k: int,
) -> Tuple[RiskMetrics, pd.DataFrame, Dict[str, object]]:
    pos_map = dict(zip(positions_df["symbol"], positions_df["qty"]))
    out = target_df.copy()
    out["current_qty"] = out["symbol"].map(pos_map).fillna(0).astype(int)
    out["delta_qty"] = out["target_qty"] - out["current_qty"]
    out["price"] = out["symbol"].map(prices)
    if out["price"].isna().any():
        missing = out.loc[out["price"].isna(), "symbol"].tolist()
        raise Phase6Error(
            "prices missing for target symbols",
            REASON_FAIL_MISSING_PRICES,
            ExitCode.FAIL_INPUT,
            {"missing_symbols": missing[:20]},
        )
    out["target_notional"] = out["target_qty"] * out["price"]
    out["abs_target_notional"] = out["target_notional"].abs()
    out["trade_notional"] = out["delta_qty"].abs() * out["price"]
    out["nav_ratio"] = out["abs_target_notional"] / nav
    out["side"] = out["delta_qty"].apply(lambda x: "BUY" if x > 0 else "SELL" if x < 0 else "HOLD")

    gross_exposure = float(out["abs_target_notional"].sum())
    net_exposure = float(out["target_notional"].sum())
    buy_mask = out["delta_qty"] > 0
    required_cash = float((out.loc[buy_mask, "delta_qty"] * out.loc[buy_mask, "price"]).sum())
    required_cash *= 1.0 + (float(buffer_bps) / 10000.0)
    if cash_available == 0:
        cash_usage_ratio = float("inf") if required_cash > 0 else 0.0
    else:
        cash_usage_ratio = required_cash / cash_available
    turnover_notional = float(out["trade_notional"].sum())
    turnover_ratio = turnover_notional / nav
    max_single = float(out["nav_ratio"].max()) if not out.empty else 0.0

    topk = out.sort_values("abs_target_notional", ascending=False).head(max(topk_k, 0))
    topk_ratio = float(topk["abs_target_notional"].sum()) / nav if not topk.empty else 0.0

    metrics: RiskMetrics = {
        "schema_version": P6_RISK_SCHEMA_VERSION,
        "nav_estimate": float(nav),
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
        "cash_available": float(cash_available),
        "cash_usage_ratio": float(cash_usage_ratio),
        "max_single_name_ratio": max_single,
        "topk_concentration_ratio": topk_ratio,
        "turnover_ratio": float(turnover_ratio),
    }

    breakdown = out[
        [
            "symbol",
            "target_qty",
            "current_qty",
            "delta_qty",
            "price",
            "target_notional",
            "abs_target_notional",
            "trade_notional",
            "nav_ratio",
            "side",
        ]
    ].copy()
    breakdown = breakdown.sort_values("abs_target_notional", ascending=False).reset_index(drop=True)

    detail = {
        "topk_symbols": topk["symbol"].tolist(),
        "topk_notional": topk["abs_target_notional"].tolist(),
    }
    return metrics, breakdown, detail


def _evaluate_risk_gates(
    metrics: RiskMetrics,
    rules: Mapping[str, object],
    concentration_detail: Dict[str, object],
) -> Tuple[Dict[str, GateResult], str, int]:
    gate_results: Dict[str, GateResult] = {}
    reason_code = REASON_OK
    exit_code = int(ExitCode.PASS)

    cash_usage_ratio = float(metrics["cash_usage_ratio"] or 0.0)
    cash_threshold = float(rules["cash"]["max_cash_usage_ratio"])
    cash_status = "PASS"
    if cash_usage_ratio > cash_threshold:
        cash_status = "FAIL"
        reason_code = REASON_FAIL_CASH_EXCEEDED
        exit_code = int(ExitCode.FAIL_POLICY)
    gate_results["cash_usage"] = _gate_result(
        cash_status,
        {"cash_usage_ratio": cash_usage_ratio},
        {"max_cash_usage_ratio": cash_threshold, "buffer_bps": rules["cash"]["buffer_bps"]},
    )

    max_single = float(metrics["max_single_name_ratio"] or 0.0)
    topk_ratio = float(metrics["topk_concentration_ratio"] or 0.0)
    max_single_thr = float(rules["concentration"]["max_single_name_ratio"])
    topk_thr = float(rules["concentration"]["max_topk_ratio"])
    topk_k = int(rules["concentration"]["topk_k"])
    conc_status = "PASS"
    if max_single > max_single_thr or topk_ratio > topk_thr:
        conc_status = "FAIL"
        if reason_code == REASON_OK:
            reason_code = REASON_FAIL_CONCENTRATION_BREACH
            exit_code = int(ExitCode.FAIL_POLICY)
    gate_results["concentration"] = _gate_result(
        conc_status,
        {"max_single_name_ratio": max_single, "topk_concentration_ratio": topk_ratio},
        {"max_single_name_ratio": max_single_thr, "max_topk_ratio": topk_thr, "topk_k": topk_k},
        concentration_detail,
    )

    turnover_ratio = float(metrics["turnover_ratio"] or 0.0)
    turnover_thr = float(rules["turnover"]["max_turnover_ratio"])
    turnover_status = "PASS"
    if turnover_ratio > turnover_thr:
        turnover_status = "FAIL"
        if reason_code == REASON_OK:
            reason_code = REASON_FAIL_TURNOVER_BREACH
            exit_code = int(ExitCode.FAIL_POLICY)
    gate_results["turnover"] = _gate_result(
        turnover_status,
        {"turnover_ratio": turnover_ratio},
        {"max_turnover_ratio": turnover_thr},
    )

    return gate_results, reason_code, exit_code


def _build_manifest(
    paths: ResolvedPaths,
    per_file_hash: Dict[str, str],
    inputs_hash: str,
    rules_hash: str,
    rules_path: Optional[str],
) -> Dict[str, object]:
    inputs: Dict[str, object] = {}
    entries = {
        "target_csv": paths.get("target_csv"),
        "prices_parquet": paths.get("prices_parquet"),
        "calendar_csv": paths.get("calendar_csv"),
        "prev_positions_csv": paths.get("prev_positions_csv"),
        "prev_account_json": paths.get("prev_account_json"),
        "benchmark_file": paths.get("benchmark_file"),
        "rules_file": rules_path,
    }
    for key, raw_path in entries.items():
        if raw_path is None:
            inputs[key] = {"path": None, "exists": False}
            continue
        path = Path(raw_path)
        if not path.exists():
            inputs[key] = {"path": str(path), "exists": False}
            continue
        stat = path.stat()
        inputs[key] = {
            "path": str(path),
            "exists": True,
            "size": stat.st_size,
            "mtime": datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            "sha256": per_file_hash.get(key) if key in per_file_hash else _hash_file(path),
        }
    return {
        "schema_version": P6_MANIFEST_SCHEMA_VERSION,
        "inputs": inputs,
        "hashes": {"inputs_hash": inputs_hash, "rules_hash": rules_hash},
        "resolved_paths": paths,
        "versions": {"python": sys.version.split()[0], "pandas": pd.__version__},
    }


def run_phase6(
    *,
    root_dir: str | Path,
    as_of: str,
    mode: str = "pretrade",
    snapshot_source: str = "exec",
    prev_exec_dir: Optional[str] = None,
    out_dir: Optional[str] = None,
    benchmark_file: Optional[str] = None,
    rules: Optional[Mapping[str, object]] = None,
) -> Phase6Result:
    status = "FAIL"
    reason_code = REASON_FAIL_INTERNAL_ERROR
    exit_code = int(ExitCode.FAIL_RUNTIME)
    gate_results = _init_gate_results()
    metrics: RiskMetrics = _empty_metrics()
    breakdown = _empty_breakdown()
    inputs_hash = "missing_inputs"
    rules_hash = "missing_rules"
    per_file_hash: Dict[str, str] = {}
    resolved_paths: ResolvedPaths = resolve_phase6_paths(root_dir, as_of, prev_exec_dir, snapshot_source)
    rules_path: Optional[str] = None
    out_path = Path(build_out_dir(root_dir, as_of, "p6.unknown", out_dir))
    summary_path = out_path / ArtifactNames.P6_SUMMARY_JSON
    log_path: Optional[Path] = None
    lock_handle: Optional[FileLock] = None
    lock_path: Optional[Path] = None
    lock_acquired = False
    approved_target_path: Optional[str] = None
    pricing_asof = as_of
    benchmark_path: Optional[Path] = None
    targets_input_df: Optional[pd.DataFrame] = None
    targets_adjusted_df: Optional[pd.DataFrame] = None
    risk_budget_payload: Optional[Dict[str, object]] = None
    adjustment_trace: Optional[Dict[str, object]] = None

    try:
        parse_ymd(as_of)
        if mode.strip().lower() != "pretrade":
            raise Phase6Error(
                f"unsupported mode: {mode}",
                REASON_FAIL_INTERNAL_ERROR,
                ExitCode.FAIL_INPUT,
            )
        if snapshot_source.strip().lower() != "exec":
            raise Phase6Error(
                f"snapshot_source not supported: {snapshot_source}",
                REASON_FAIL_BAD_SNAPSHOT_SCHEMA,
                ExitCode.FAIL_INPUT,
            )

        if rules is None:
            rules_path = str(Path(root_dir).resolve() / "rules.yaml")
            resolved_paths["rules_path"] = rules_path
            try:
                rules = load_rules(rules_path)
            except (ConfigError, Exception) as exc:
                raise Phase6Error(
                    "rules load failed",
                    REASON_FAIL_RULES_INVALID,
                    ExitCode.FAIL_INPUT,
                    {"path": rules_path, "error": str(exc)},
                ) from exc
        elif not isinstance(rules, Mapping):
            raise Phase6Error(
                "rules must be a mapping",
                REASON_FAIL_RULES_INVALID,
                ExitCode.FAIL_INPUT,
            )

        phase6_rules = _extract_phase6_rules(rules)
        phase6_optional = _extract_phase6_optional(rules)
        phase6_rules.update(phase6_optional)
        raw_benchmark = (
            benchmark_file
            or phase6_optional.get("benchmark", {}).get("returns_file")
            or phase6_optional.get("risk", {}).get("benchmark_file")
        )
        if raw_benchmark and isinstance(raw_benchmark, str):
            bench_path = Path(raw_benchmark)
            if not bench_path.is_absolute():
                bench_path = Path(root_dir).resolve() / bench_path
            benchmark_path = bench_path.resolve()
        resolved_paths["benchmark_file"] = str(benchmark_path) if benchmark_path is not None else None
        rules_hash = _hash_rules(phase6_rules)

        inputs_hash, per_file_hash = _hash_inputs(resolved_paths)
        run_id = compute_run_id(inputs_hash, rules_hash, as_of)
        out_path = build_out_dir(root_dir, as_of, run_id, out_dir)
        resolved_paths["out_dir"] = str(out_path)
        summary_path = out_path / ArtifactNames.P6_SUMMARY_JSON
        log_path = out_path / ArtifactNames.P6_RUN_LOG

        _log_line(log_path, f"start as_of={as_of} run_id={run_id} mode={mode} snapshot_source={snapshot_source}")
        _emit_resolved_paths(log_path, resolved_paths)
        _log_line(log_path, f"inputs_hash={inputs_hash}")
        _log_line(log_path, f"rules_hash={rules_hash}")

        out_path.mkdir(parents=True, exist_ok=True)
        lock_path = Path(resolved_paths["lock_path"])
        command = " ".join(str(arg) for arg in sys.argv if arg is not None)
        lock_handle = FileLock(
            lock_path,
            ttl_minutes=LOCK_TTL_MINUTES,
            auto_break_stale=True,
            command=command,
        )
        try:
            lock_handle.acquire()
        except LockActiveError as exc:
            raise LockError(str(exc)) from exc
        lock_acquired = True

        calendar_path = Path(resolved_paths["calendar_csv"])
        if not calendar_path.exists():
            gate_results["trading_day"] = _gate_result(
                "FAIL",
                {"calendar_csv": str(calendar_path)},
                {"required": True},
                {"reason": "calendar_missing"},
            )
            raise Phase6Error(
                "calendar not found",
                REASON_FAIL_INTERNAL_ERROR,
                ExitCode.FAIL_INPUT,
                {"path": str(calendar_path)},
            )
        trading_days = load_trading_days(calendar_path)
        if not is_trading_day(as_of, trading_days):
            gate_results["trading_day"] = _gate_result(
                "SKIP",
                {"is_trading_day": False},
                {"skip_non_trading": True},
                {"reason": "non_trading_day"},
            )
            raise Phase6Error(
                "not trading day",
                REASON_SKIP_NON_TRADING_DAY,
                ExitCode.SKIP,
            )
        gate_results["trading_day"] = _gate_result(
            "PASS",
            {"is_trading_day": True},
            {"skip_non_trading": True},
        )

        try:
            target_df = _load_target(Path(resolved_paths["target_csv"]))
            gate_results["target_schema"] = _gate_result(
                "PASS",
                {"row_count": int(len(target_df))},
                {"required_columns": ["symbol", "target_qty"]},
            )
        except Phase6Error as exc:
            gate_results["target_schema"] = _gate_result(
                "FAIL",
                {"error": exc.reason_code},
                {"required_columns": ["symbol", "target_qty"]},
                {"message": exc.message},
            )
            raise

        positions_df = _load_positions(Path(resolved_paths["prev_positions_csv"]))
        account = _load_account_snapshot(Path(resolved_paths["prev_account_json"]))
        targets_input_df = target_df.copy()
        max_price_age_days = int(phase6_rules["freshness"]["max_price_age_days"])
        prices_detail: Dict[str, object] = {}
        try:
            prices, pricing_asof, prices_detail = _load_prices(
                Path(resolved_paths["prices_parquet"]),
                as_of,
                max_price_age_days,
            )
            gate_results["prices"] = _gate_result(
                "PASS",
                {"pricing_asof": pricing_asof, "age_days": prices_detail.get("age_days"), "missing_symbols": 0},
                {"max_price_age_days": max_price_age_days, "require_all_symbols": True},
                prices_detail,
            )
        except Phase6Error as exc:
            if exc.reason_code == REASON_FAIL_MISSING_PRICES:
                detail = exc.details or {}
                if detail.get("pricing_asof"):
                    pricing_asof = str(detail.get("pricing_asof"))
                detail.setdefault("max_price_age_days", max_price_age_days)
                detail.setdefault("missing_symbols_sample", [])
                gate_results["prices"] = _gate_result(
                    "FAIL",
                    {
                        "pricing_asof": detail.get("pricing_asof"),
                        "age_days": detail.get("age_days"),
                        "missing_symbols": len(detail.get("missing_symbols_sample") or []),
                    },
                    {"max_price_age_days": max_price_age_days, "require_all_symbols": True},
                    detail,
                )
            raise

        try:
            metrics, breakdown, concentration_detail = _compute_risk(
                target_df=target_df,
                positions_df=positions_df,
                prices=prices,
                nav=account["nav"],
                cash_available=account["cash_available"],
                buffer_bps=int(phase6_rules["cash"]["buffer_bps"]),
                topk_k=int(phase6_rules["concentration"]["topk_k"]),
            )
        except Phase6Error as exc:
            if exc.reason_code == REASON_FAIL_MISSING_PRICES:
                missing_sample = []
                if exc.details and isinstance(exc.details.get("missing_symbols"), list):
                    missing_sample = exc.details.get("missing_symbols", [])[:20]
                detail = dict(prices_detail)
                detail.update(
                    {
                        "pricing_asof": pricing_asof,
                        "age_days": prices_detail.get("age_days"),
                        "max_price_age_days": max_price_age_days,
                        "missing_symbols_sample": missing_sample,
                    }
                )
                gate_results["prices"] = _gate_result(
                    "FAIL",
                    {
                        "pricing_asof": pricing_asof,
                        "age_days": prices_detail.get("age_days"),
                        "missing_symbols": len(missing_sample),
                    },
                    {"max_price_age_days": max_price_age_days, "require_all_symbols": True},
                    detail,
                )
            raise

        target_snapshot = build_target_snapshot(target_df, prices, account["nav"])
        weights = dict(zip(target_snapshot["symbol"], target_snapshot["weight"]))
        risk_cfg = _parse_risk_config(phase6_rules)
        overlay_cfg = _parse_overlay_config(phase6_rules)
        max_window = max(risk_cfg["te_windows"]) if risk_cfg["te_windows"] else 0
        returns_df = pd.DataFrame()
        if max_window > 0:
            returns_df = load_price_returns(
                Path(resolved_paths["prices_parquet"]),
                target_snapshot["symbol"].tolist(),
                parse_ymd(as_of),
                max_window,
            )

        tracking_status = "SKIP"
        info_status = "SKIP"
        te_exceeded = False
        te_detail: Dict[str, object] = {
            "pricing_asof": pricing_asof,
            "benchmark_file": str(benchmark_path) if benchmark_path is not None else None,
        }
        ir_detail: Dict[str, object] = {
            "pricing_asof": pricing_asof,
            "benchmark_file": str(benchmark_path) if benchmark_path is not None else None,
        }
        te_result: Dict[str, object] = {
            "te": {},
            "ir": {},
            "active_return": {},
            "obs": {},
            "bench_last_date": None,
            "bench_obs": 0,
        }
        bench_df = pd.DataFrame()
        if benchmark_path is None:
            te_detail["reason"] = "missing_benchmark"
            ir_detail["reason"] = "missing_benchmark"
            gate_results["tracking_error"] = _gate_result(
                "SKIP",
                {"pricing_asof": pricing_asof},
                {"max_te": risk_cfg["max_te"], "min_obs": risk_cfg["min_obs"]},
                te_detail,
            )
            gate_results["information_ratio"] = _gate_result(
                "SKIP",
                {"pricing_asof": pricing_asof},
                {"min_obs": risk_cfg["min_obs"]},
                ir_detail,
            )
        else:
            try:
                bench_df = load_benchmark_returns(benchmark_path)
                bench_df = bench_df[bench_df["date"] <= parse_ymd(as_of)]
                te_result = compute_te_ir(
                    returns_df=returns_df,
                    weights=weights,
                    bench_df=bench_df,
                    windows=[int(w) for w in risk_cfg["te_windows"]],
                    min_obs=int(risk_cfg["min_obs"]),
                )
                te_detail.update(
                    {
                        "observations": te_result.get("obs", {}),
                        "bench_last_date": te_result.get("bench_last_date"),
                        "bench_obs": te_result.get("bench_obs"),
                    }
                )
                # only copy shared fields to avoid mixing TE-specific flags into IR detail
                for key in ["pricing_asof", "benchmark_file", "observations", "bench_last_date", "bench_obs"]:
                    if key in te_detail:
                        ir_detail[key] = te_detail[key]
                te_values = te_result.get("te", {})
                ir_values = te_result.get("ir", {})
                if te_values:
                    tracking_status = "PASS"
                    for win, thr in risk_cfg["max_te"].items():
                        if win in te_values and float(te_values[win]) > float(thr):
                            te_exceeded = True
                    if te_exceeded:
                        tracking_status = "FAIL" if risk_cfg["enforce_te"] else "WARN"
                        te_detail["te_exceeded"] = True
                    info_status = "PASS" if ir_values else "SKIP"
                    ir_below = False
                    for win, thr in risk_cfg["min_ir"].items():
                        if win in ir_values and float(ir_values[win]) < float(thr):
                            ir_below = True
                    if ir_below:
                        info_status = "FAIL" if risk_cfg["enforce_ir"] else "WARN"
                        ir_detail["ir_below"] = True
                else:
                    tracking_status = "SKIP"
                    info_status = "SKIP"
                    te_detail["reason"] = "insufficient_data"
                    ir_detail["reason"] = "insufficient_data"
            except Exception as exc:  # noqa: BLE001
                tracking_status = "SKIP"
                info_status = "SKIP"
                te_detail["reason"] = f"benchmark_error: {exc}"
                ir_detail["reason"] = f"benchmark_error: {exc}"

            gate_results["tracking_error"] = _gate_result(
                tracking_status,
                {
                    "tracking_error": te_result.get("te", {}),
                    "pricing_asof": pricing_asof,
                    "bench_last_date": te_result.get("bench_last_date"),
                },
                {"max_te": risk_cfg["max_te"], "min_obs": risk_cfg["min_obs"]},
                te_detail,
            )
            gate_results["information_ratio"] = _gate_result(
                info_status,
                {
                    "information_ratio": te_result.get("ir", {}),
                    "active_return": te_result.get("active_return", {}),
                },
                {"min_obs": risk_cfg["min_obs"]},
                ir_detail,
            )

        metrics["tracking_error"] = te_result.get("te", {})
        metrics["information_ratio"] = te_result.get("ir", {})
        metrics["active_return"] = te_result.get("active_return", {})
        metrics["tracking_error_obs"] = te_result.get("obs", {})
        metrics["benchmark_last_date"] = te_result.get("bench_last_date")
        metrics["benchmark_obs"] = te_result.get("bench_obs")

        risk_budget_status = "SKIP"
        if risk_cfg["max_single_name_rc"] is not None or risk_cfg["max_topk_rc"] is not None:
            budget = compute_risk_budget(
                returns_df=returns_df,
                weights=weights,
                topk_k=int(risk_cfg["topk_k"]),
                window=max_window if max_window > 0 else None,
            )
            risk_budget_payload = {
                "as_of": as_of,
                "pricing_asof": pricing_asof,
                "port_vol": budget.get("port_vol"),
                "obs_count": budget.get("obs_count"),
                "rc_by_symbol": budget.get("rc_by_symbol", {}),
                "max_single_rc": budget.get("max_single_rc"),
                "topk_rc": budget.get("topk_rc"),
                "topk_symbols": budget.get("topk_symbols", []),
            }
            obs_count = int(budget.get("obs_count") or 0)
            max_single_rc = float(budget.get("max_single_rc") or 0.0)
            topk_rc = float(budget.get("topk_rc") or 0.0)
            if obs_count < int(risk_cfg["min_obs"]):
                risk_budget_status = "SKIP"
            else:
                risk_budget_status = "PASS"
                if risk_cfg["max_single_name_rc"] is not None and max_single_rc > risk_cfg["max_single_name_rc"]:
                    risk_budget_status = "FAIL"
                if risk_cfg["max_topk_rc"] is not None and topk_rc > risk_cfg["max_topk_rc"]:
                    risk_budget_status = "FAIL"
            budget_detail = {
                "topk_symbols_sample": budget.get("topk_symbols", [])[:10],
                "obs_count": obs_count,
            }
            if risk_budget_status == "SKIP":
                budget_detail["reason"] = "insufficient_data"
            gate_results["risk_budget"] = _gate_result(
                risk_budget_status,
                {
                    "max_single_rc": max_single_rc,
                    "topk_rc": topk_rc,
                    "port_vol": budget.get("port_vol"),
                },
                {
                    "max_single_name_rc": risk_cfg["max_single_name_rc"],
                    "max_topk_rc": risk_cfg["max_topk_rc"],
                    "topk_k": risk_cfg["topk_k"],
                },
                budget_detail,
            )

        overlay_status = "SKIP"
        overlay_detail: Dict[str, object] = {"policy": overlay_cfg["policy"]}
        te_after_result: Optional[Dict[str, object]] = None
        trigger_level: Optional[str] = None
        trigger_on_warn_te_effective = bool(overlay_cfg.get("trigger_on_warn_te", False))
        te_exceeded_effective = bool(te_detail.get("te_exceeded", False))
        overlay_detail["trigger_on_warn_te_effective"] = trigger_on_warn_te_effective
        overlay_detail["te_exceeded_effective"] = te_exceeded_effective
        overlay_detail["tracking_status_effective"] = tracking_status
        triggered_by: list[str] = []
        should_trigger_te = te_exceeded_effective and (
            tracking_status == "FAIL"
            or (tracking_status == "WARN" and trigger_on_warn_te_effective)
        )
        if should_trigger_te:
            triggered_by.append("tracking_error")
            trigger_level = tracking_status
        if risk_budget_status == "FAIL":
            triggered_by.append("risk_budget")

        if overlay_cfg["enabled"]:
            if not triggered_by:
                overlay_status = "SKIP"
                overlay_detail["reason"] = "not_triggered"
                targets_adjusted_df = targets_input_df
            elif overlay_cfg["policy"] != "scale":
                overlay_status = "FAIL"
                overlay_detail["reason"] = "unsupported_policy"
            else:
                if trigger_level is not None:
                    overlay_detail["trigger_level"] = trigger_level
                scale_factor = 1.0
                if te_exceeded:
                    ratios = []
                    for win, thr in risk_cfg["max_te"].items():
                        te_val = te_result.get("te", {}).get(win)
                        if te_val and float(te_val) > 0:
                            ratios.append(float(thr) / float(te_val))
                    if ratios:
                        scale_factor = max(min(ratios), 0.0)
                targets_adjusted_df = scale_targets(target_df, scale_factor)
                adjustment_trace = build_adjustment_trace(
                    before_df=targets_input_df,
                    after_df=targets_adjusted_df,
                    prices=prices,
                    nav=account["nav"],
                    policy=overlay_cfg["policy"],
                    scale_factor=scale_factor,
                    triggered_by=triggered_by,
                    notes={"pricing_asof": pricing_asof},
                )
                overlay_detail["scale_factor"] = scale_factor
                if "risk_budget" in triggered_by:
                    overlay_detail["reason"] = "risk_budget_not_adjustable"
                if "tracking_error" in triggered_by and scale_factor < 1.0:
                    adjusted_snapshot = build_target_snapshot(targets_adjusted_df, prices, account["nav"])
                    weights_after = dict(zip(adjusted_snapshot["symbol"], adjusted_snapshot["weight"]))
                    te_after_result = compute_te_ir(
                        returns_df=returns_df,
                        weights=weights_after,
                        bench_df=bench_df,
                        windows=[int(w) for w in risk_cfg["te_windows"]],
                        min_obs=int(risk_cfg["min_obs"]),
                    )
                    te_after = te_after_result.get("te", {})
                    overlay_detail["tracking_error_before"] = te_result.get("te", {})
                    overlay_detail["tracking_error_after"] = te_after
                    pass_after = True
                    for win, thr in risk_cfg["max_te"].items():
                        if win in te_after and float(te_after[win]) > float(thr):
                            pass_after = False
                    if pass_after:
                        overlay_status = "PASS"
                    elif risk_cfg["enforce_te"] and trigger_level == "FAIL":
                        overlay_status = "FAIL"
                    else:
                        overlay_status = "WARN"
                        overlay_detail["reason"] = "not_reduced_to_threshold"
                else:
                    if tracking_status != "FAIL":
                        overlay_status = "PASS"
                    elif risk_cfg["enforce_te"] and trigger_level == "FAIL":
                        overlay_status = "FAIL"
                    else:
                        overlay_status = "WARN"
                        overlay_detail["reason"] = "not_reduced_to_threshold"
                if "risk_budget" in triggered_by:
                    overlay_status = "FAIL"
        else:
            overlay_detail["reason"] = "disabled"

        gate_results["overlay"] = _gate_result(
            overlay_status,
            {"triggered_by": triggered_by, "policy": overlay_cfg["policy"]},
            {"enabled": overlay_cfg["enabled"]},
            overlay_detail,
        )
        if overlay_status == "PASS" and "tracking_error" in triggered_by:
            gate_results["tracking_error"]["status"] = "PASS"
            gate_results["tracking_error"]["detail"]["adjusted"] = True
            gate_results["tracking_error"]["detail"]["tracking_error_before"] = te_result.get("te", {})
            gate_results["tracking_error"]["detail"]["tracking_error_after"] = overlay_detail.get(
                "tracking_error_after"
            )
            if te_after_result is not None:
                gate_results["tracking_error"]["observed"]["tracking_error"] = te_after_result.get("te", {})
                gate_results["tracking_error"]["observed"]["bench_last_date"] = te_after_result.get("bench_last_date")
                gate_results["information_ratio"]["observed"]["information_ratio"] = te_after_result.get("ir", {})
                gate_results["information_ratio"]["observed"]["active_return"] = te_after_result.get(
                    "active_return", {}
                )
                gate_results["information_ratio"]["detail"]["adjusted"] = True
                gate_results["information_ratio"]["status"] = "PASS" if te_after_result.get("ir") else "SKIP"
                metrics["tracking_error"] = te_after_result.get("te", {})
                metrics["information_ratio"] = te_after_result.get("ir", {})
                metrics["active_return"] = te_after_result.get("active_return", {})
                metrics["tracking_error_obs"] = te_after_result.get("obs", {})
                metrics["benchmark_last_date"] = te_after_result.get("bench_last_date")
                metrics["benchmark_obs"] = te_after_result.get("bench_obs")

        risk_gate_results, reason_code, exit_code = _evaluate_risk_gates(metrics, phase6_rules, concentration_detail)
        gate_results.update(risk_gate_results)

        if exit_code == int(ExitCode.PASS):
            if tracking_status == "FAIL" and risk_cfg["enforce_te"] and not overlay_cfg["enabled"]:
                exit_code = int(ExitCode.FAIL_POLICY)
                reason_code = REASON_FAIL_TRACKING_ERROR
            elif risk_budget_status == "FAIL" and not overlay_cfg["enabled"]:
                exit_code = int(ExitCode.FAIL_POLICY)
                reason_code = REASON_FAIL_RISK_BUDGET
            elif overlay_status == "FAIL" and overlay_cfg["enabled"]:
                exit_code = int(ExitCode.FAIL_POLICY)
                reason_code = REASON_FAIL_OVERLAY

        if exit_code == int(ExitCode.PASS):
            status = "PASS"
            reason_code = REASON_OK
            approved_target_path = str(out_path / ArtifactNames.APPROVED_TARGET_FMT.format(as_of=as_of))
            if targets_adjusted_df is None:
                targets_adjusted_df = targets_input_df
            _write_csv_atomic(Path(approved_target_path), targets_adjusted_df)
        else:
            status = "FAIL"

    except LockError as exc:
        raise Phase6Error(
            "lock already held",
            REASON_FAIL_LOCKED,
            ExitCode.FAIL_RUNTIME,
            {"path": str(lock_path) if lock_path is not None else None},
        ) from exc
    except Phase6Error as exc:
        reason_code = exc.reason_code
        exit_code = int(exc.exit_code)
        status = "SKIP" if exit_code == int(ExitCode.SKIP) else "FAIL"
    except Exception as exc:  # noqa: BLE001
        reason_code = REASON_FAIL_INTERNAL_ERROR
        exit_code = int(ExitCode.FAIL_RUNTIME)
        status = "FAIL"
        _log_line(log_path, f"runtime_error={exc}")
    finally:
        out_path.mkdir(parents=True, exist_ok=True)
        if log_path is None:
            log_path = out_path / ArtifactNames.P6_RUN_LOG
        if not resolved_paths.get("out_dir"):
            resolved_paths["out_dir"] = str(out_path)
        _log_line(log_path, "stage=write_artifacts begin")
        _write_json_atomic(out_path / ArtifactNames.RISK_METRICS_JSON, metrics)
        _write_csv_atomic(out_path / ArtifactNames.RISK_BREAKDOWN_CSV, breakdown)
        if targets_input_df is not None:
            _write_csv_atomic(out_path / ArtifactNames.TARGETS_INPUT_CSV, targets_input_df)
            if targets_adjusted_df is None:
                targets_adjusted_df = targets_input_df
        if targets_adjusted_df is not None:
            _write_csv_atomic(out_path / ArtifactNames.TARGETS_RISK_ADJUSTED_CSV, targets_adjusted_df)
        if risk_budget_payload is not None:
            _write_json_atomic(out_path / ArtifactNames.RISK_BUDGET_JSON, risk_budget_payload)
        if adjustment_trace is not None:
            _write_json_atomic(out_path / ArtifactNames.ADJUSTMENT_TRACE_JSON, adjustment_trace)
        manifest = _build_manifest(
            paths=resolved_paths,
            per_file_hash=per_file_hash,
            inputs_hash=inputs_hash,
            rules_hash=rules_hash,
            rules_path=rules_path,
        )
        _write_json_atomic(out_path / ArtifactNames.P6_MANIFEST_JSON, manifest)
        summary: Dict[str, object] = {
            "schema_version": P6_SUMMARY_SCHEMA_VERSION,
            "as_of": as_of,
            "pricing_asof": pricing_asof,
            "status": status,
            "reason_code": reason_code,
            "exit_code": int(exit_code),
            "gate_results": gate_results,
            "resolved_paths": resolved_paths,
            "hashes": {"inputs_hash": inputs_hash, "rules_hash": rules_hash},
            "created_at": _now_iso(),
            "approved_target_path": approved_target_path,
        }
        _write_json_atomic(summary_path, summary)
        for name, gate in gate_results.items():
            _log_line(
                log_path,
                "gate="
                + name
                + " status="
                + str(gate.get("status"))
                + " observed="
                + json.dumps(gate.get("observed"), ensure_ascii=True)
                + " threshold="
                + json.dumps(gate.get("threshold"), ensure_ascii=True),
            )
        _log_line(log_path, f"summary_path={summary_path}")
        _log_line(log_path, f"out_dir={out_path}")
        _log_line(log_path, f"final status={status} exit_code={exit_code} reason_code={reason_code}")
        _log_line(log_path, "stage=write_artifacts end")
        if lock_acquired and lock_handle is not None:
            lock_handle.release()

    return Phase6Result(
        status=status,
        exit_code=int(exit_code),
        reason_code=reason_code,
        out_dir=out_path,
        summary_path=summary_path,
    )
