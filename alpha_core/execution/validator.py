# alpha_core/execution/validator.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from . import schemas as _s


# ---------- datatypes ----------

@dataclass(frozen=True)
class ValidationError:
    code: str
    message: str
    context: Dict[str, Any] | None = None


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: List[ValidationError]


def _ok() -> ValidationResult:
    return ValidationResult(True, [])


def _fail(errors: List[ValidationError]) -> ValidationResult:
    return ValidationResult(False, errors)


def _add(errors: List[ValidationError], code: str, message: str, **context: Any) -> None:
    errors.append(ValidationError(code=code, message=message, context=context or None))


def _get(name: str, default: Any) -> Any:
    return getattr(_s, name, default)


# ---------- schema constants (best-effort, fallback-safe) ----------

ORDER_REQUIRED_COLUMNS = _get(
    "ORDER_REQUIRED_COLUMNS",
    (
        "run_id",
        "as_of",
        "ts_created",
        "cl_order_id",
        "symbol",
        "side",
        "order_type",
        "time_in_force",
        "qty",
        "filled_qty",
        "status",
        "strategy_id",
    ),
)

TRADE_REQUIRED_COLUMNS = _get(
    "TRADE_REQUIRED_COLUMNS",
    (
        "run_id",
        "as_of",
        "trade_id",
        "cl_order_id",
        "ts_filled",
        "symbol",
        "side",
        "price",
        "qty",
        "commission",
        "tax",
    ),
)

POSITION_REQUIRED_COLUMNS = _get(
    "POSITION_REQUIRED_COLUMNS",
    (
        "run_id",
        "as_of",
        "symbol",
        "qty",
        "avg_cost",
        "market_value",
        "source",
    ),
)

ACCOUNT_REQUIRED_KEYS = _get(
    "ACCOUNT_REQUIRED_KEYS",
    (
        "run_id",
        "as_of",
        "ts_snapshot",
        "currency",
        "cash",
        "buying_power",
        "equity",
        "nav",
        "source",
    ),
)

EXEC_SUMMARY_REQUIRED_KEYS = _get(
    "EXEC_SUMMARY_REQUIRED_KEYS",
    (
        "run_id",
        "as_of",
        "mode",
        "schema_version",
        "job_success",
        "started_at",
        "finished_at",
        "orders_count",
        "trades_count",
        "fill_rate",
        "reject_rate",
        "artefacts_manifest",
    ),
)

EXEC_SUMMARY_OPTIONAL_KEYS = _get("EXEC_SUMMARY_OPTIONAL_KEYS", tuple())


# ---------- enums (must exist; if not, import error should surface early) ----------
Side = _s.Side
OrderType = _s.OrderType
OrderStatus = _s.OrderStatus


# ---------- per-artifact validators ----------

def validate_orders(df: pd.DataFrame) -> ValidationResult:
    errors: List[ValidationError] = []

    # empty is allowed, but schema must be present
    missing = [c for c in ORDER_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        _add(errors, "E_ORDER_SCHEMA", "Missing required order columns", missing=missing)
        return _fail(errors)

    if df.empty:
        return _ok()

    # cl_order_id must be unique
    if df["cl_order_id"].isna().any():
        _add(errors, "E_ORDER_CLID_NULL", "cl_order_id contains null")
    else:
        dup = df["cl_order_id"][df["cl_order_id"].duplicated()].tolist()
        if dup:
            _add(errors, "E_ORDER_CLID_DUP", "Duplicate cl_order_id detected", duplicates=dup[:20])

    allowed_side = {Side.BUY.value, Side.SELL.value}
    allowed_type = {t.value for t in OrderType}
    allowed_status = {s.value for s in OrderStatus}
    allowed_tif = {"ROD", "IOC", "FOK"}  # schema hardening point

    for i, row in df.iterrows():
        # qty / filled_qty numeric constraints
        try:
            qty = int(pd.to_numeric(row["qty"], errors="raise"))
            if qty <= 0:
                _add(errors, "E_ORDER_QTY", "qty must be > 0", row=int(i), qty=row["qty"])
        except Exception:
            _add(errors, "E_ORDER_QTY_FMT", "qty invalid numeric format", row=int(i), qty=row.get("qty"))

        try:
            fqty = int(pd.to_numeric(row["filled_qty"], errors="raise"))
            if fqty < 0:
                _add(errors, "E_ORDER_FILLED_QTY", "filled_qty must be >= 0", row=int(i), filled_qty=row["filled_qty"])
        except Exception:
            _add(errors, "E_ORDER_FILLED_QTY_FMT", "filled_qty invalid numeric format", row=int(i), filled_qty=row.get("filled_qty"))
            fqty = None

        if "qty" in row and fqty is not None:
            try:
                qty = int(pd.to_numeric(row["qty"], errors="coerce"))
                if pd.notna(qty) and fqty > qty:
                    _add(errors, "E_ORDER_FILLED_GT_QTY", "filled_qty must be <= qty", row=int(i), qty=qty, filled_qty=fqty)
            except Exception:
                pass

        # enums
        if str(row["side"]) not in allowed_side:
            _add(errors, "E_ORDER_SIDE", "Invalid side", row=int(i), side=row.get("side"))
        if str(row["order_type"]) not in allowed_type:
            _add(errors, "E_ORDER_TYPE", "Invalid order_type", row=int(i), order_type=row.get("order_type"))
        if str(row["status"]) not in allowed_status:
            _add(errors, "E_ORDER_STATUS", "Invalid status", row=int(i), status=row.get("status"))

        # TIF
        if "time_in_force" in df.columns:
            if str(row["time_in_force"]) not in allowed_tif:
                _add(errors, "E_ORDER_TIF", "Invalid time_in_force", row=int(i), time_in_force=row.get("time_in_force"))

        # LIMIT requires limit_price > 0 (if column exists)
        if str(row["order_type"]) == OrderType.LIMIT.value:
            if "limit_price" not in df.columns:
                _add(errors, "E_ORDER_LIMIT_PRICE_MISSING", "LIMIT order missing limit_price column", row=int(i))
            else:
                lp = pd.to_numeric(row.get("limit_price"), errors="coerce")
                if not pd.notna(lp) or float(lp) <= 0:
                    _add(errors, "E_ORDER_LIMIT_PRICE", "limit_price must be > 0 for LIMIT order", row=int(i), limit_price=row.get("limit_price"))

        # REJECTED requires reject_reason (if column exists)
        if str(row["status"]) == OrderStatus.REJECTED.value:
            rr = None
            if "reject_reason" in df.columns:
                rr = row.get("reject_reason")
            if rr is None or str(rr).strip() == "" or str(rr).strip().lower() == "nan":
                _add(errors, "E_ORDER_REJECT_REASON", "REJECTED order must have reject_reason", row=int(i), cl_order_id=row.get("cl_order_id"))

    return _ok() if not errors else _fail(errors)


def validate_trades(df: pd.DataFrame) -> ValidationResult:
    errors: List[ValidationError] = []

    missing = [c for c in TRADE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        _add(errors, "E_TRADE_SCHEMA", "Missing required trade columns", missing=missing)
        return _fail(errors)

    if df.empty:
        return _ok()

    # trade_id unique
    if df["trade_id"].isna().any():
        _add(errors, "E_TRADE_ID_NULL", "trade_id contains null")
    else:
        dup = df["trade_id"][df["trade_id"].duplicated()].tolist()
        if dup:
            _add(errors, "E_TRADE_ID_DUP", "Duplicate trade_id detected", duplicates=dup[:20])

    allowed_side = {Side.BUY.value, Side.SELL.value}

    for i, row in df.iterrows():
        if str(row["side"]) not in allowed_side:
            _add(errors, "E_TRADE_SIDE", "Invalid side", row=int(i), side=row.get("side"))

        try:
            qty = int(pd.to_numeric(row["qty"], errors="raise"))
            if qty <= 0:
                _add(errors, "E_TRADE_QTY", "qty must be > 0", row=int(i), qty=row.get("qty"))
        except Exception:
            _add(errors, "E_TRADE_QTY_FMT", "qty invalid numeric format", row=int(i), qty=row.get("qty"))

        px = pd.to_numeric(row.get("price"), errors="coerce")
        if not pd.notna(px) or float(px) <= 0:
            _add(errors, "E_TRADE_PRICE", "price must be > 0", row=int(i), price=row.get("price"))

        comm = pd.to_numeric(row.get("commission"), errors="coerce")
        tax = pd.to_numeric(row.get("tax"), errors="coerce")
        if not pd.notna(comm) or float(comm) < 0:
            _add(errors, "E_TRADE_COMMISSION", "commission must be >= 0", row=int(i), commission=row.get("commission"))
        if not pd.notna(tax) or float(tax) < 0:
            _add(errors, "E_TRADE_TAX", "tax must be >= 0", row=int(i), tax=row.get("tax"))

        # buy should have tax=0 (tolerate tiny epsilon)
        if str(row["side"]) == Side.BUY.value and pd.notna(tax) and abs(float(tax)) > 1e-9:
            _add(errors, "E_TRADE_TAX_BUY", "BUY trade tax should be 0", row=int(i), tax=row.get("tax"))

    return _ok() if not errors else _fail(errors)


def validate_positions(df: pd.DataFrame, *, allow_short: bool = False) -> ValidationResult:
    errors: List[ValidationError] = []

    missing = [c for c in POSITION_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        _add(errors, "E_POSITION_SCHEMA", "Missing required position columns", missing=missing)
        return _fail(errors)

    if df.empty:
        return _ok()

    # unique symbol (within run)
    dup = df["symbol"][df["symbol"].duplicated()].tolist()
    if dup:
        _add(errors, "E_POSITION_SYMBOL_DUP", "Duplicate symbol in positions snapshot", duplicates=dup[:20])

    for i, row in df.iterrows():
        try:
            q = int(pd.to_numeric(row["qty"], errors="raise"))
            if (not allow_short) and q < 0:
                _add(errors, "E_POSITION_NEGATIVE", "Negative qty not allowed (long-only)", row=int(i), symbol=row.get("symbol"), qty=q)
        except Exception:
            _add(errors, "E_POSITION_QTY_FMT", "qty invalid numeric format", row=int(i), qty=row.get("qty"))

        mv = pd.to_numeric(row.get("market_value"), errors="coerce")
        if not pd.notna(mv):
            _add(errors, "E_POSITION_MV_FMT", "market_value invalid numeric format", row=int(i), market_value=row.get("market_value"))

        ac = pd.to_numeric(row.get("avg_cost"), errors="coerce")
        if not pd.notna(ac):
            _add(errors, "E_POSITION_AVG_COST_FMT", "avg_cost invalid numeric format", row=int(i), avg_cost=row.get("avg_cost"))

        src = str(row.get("source", "")).strip()
        if not src:
            _add(errors, "E_POSITION_SOURCE", "source must be non-empty", row=int(i))

    return _ok() if not errors else _fail(errors)


def validate_account_snapshot(account: Dict[str, Any]) -> ValidationResult:
    errors: List[ValidationError] = []

    missing = [k for k in ACCOUNT_REQUIRED_KEYS if k not in account]
    if missing:
        _add(errors, "E_ACCOUNT_SCHEMA", "Missing required account keys", missing=missing)
        return _fail(errors)

    # basic numeric sanity
    for k in ("cash", "buying_power", "equity", "nav"):
        v = account.get(k)
        try:
            fv = float(v)
            if k in ("buying_power",) and fv < 0:
                _add(errors, "E_ACCOUNT_BUYING_POWER", "buying_power must be >= 0", buying_power=v)
        except Exception:
            _add(errors, "E_ACCOUNT_NUM_FMT", f"{k} invalid numeric format", key=k, value=v)

    # nav should equal equity (single-currency snapshot)
    try:
        eq = float(account.get("equity"))
        nav = float(account.get("nav"))
        if abs(eq - nav) > 1e-6:
            _add(errors, "E_ACCOUNT_NAV", "nav must equal equity", equity=eq, nav=nav)
    except Exception:
        pass

    cur = str(account.get("currency", "")).strip()
    if not cur:
        _add(errors, "E_ACCOUNT_CURRENCY", "currency must be non-empty", currency=account.get("currency"))

    src = str(account.get("source", "")).strip()
    if not src:
        _add(errors, "E_ACCOUNT_SOURCE", "source must be non-empty", source=account.get("source"))

    return _ok() if not errors else _fail(errors)


def validate_exec_summary(summary: Dict[str, Any]) -> ValidationResult:
    errors: List[ValidationError] = []

    missing = [k for k in EXEC_SUMMARY_REQUIRED_KEYS if k not in summary]
    if missing:
        _add(errors, "E_SUMMARY_SCHEMA", "Missing required summary keys", missing=missing)

    allowed = set(EXEC_SUMMARY_REQUIRED_KEYS) | set(EXEC_SUMMARY_OPTIONAL_KEYS)
    allowed.update({"status", "reason_code"})
    unknown = [k for k in summary.keys() if k not in allowed]
    if unknown:
        _add(errors, "E_SUMMARY_UNKNOWN", "Unknown summary keys (not in whitelist)", unknown=unknown)

    return _ok() if not errors else _fail(errors)


def validate_cross_artifacts(
    orders: pd.DataFrame,
    trades: Optional[pd.DataFrame],
    positions: Optional[pd.DataFrame],
    account: Optional[Dict[str, Any]],
    summary: Optional[Dict[str, Any]],
) -> ValidationResult:
    """
    Cross-artifact reconcile:
      1) Orders <-> Trades FK + qty reconcile
      2) Positions <-> Account equity reconcile
      3) Summary <-> artefacts basic consistency (counts / totals if present)
    """
    errors: List[ValidationError] = []

    # 0) per-artifact baseline validation (best-effort)
    vr_o = validate_orders(orders)
    if not vr_o.ok:
        errors.extend(vr_o.errors)

    if trades is not None:
        vr_t = validate_trades(trades)
        if not vr_t.ok:
            errors.extend(vr_t.errors)

    allow_short = False
    if summary and "allow_short" in summary:
        try:
            allow_short = bool(summary.get("allow_short"))
        except Exception:
            allow_short = False

    if positions is not None:
        vr_p = validate_positions(positions, allow_short=allow_short)
        if not vr_p.ok:
            errors.extend(vr_p.errors)

    if account is not None:
        vr_a = validate_account_snapshot(account)
        if not vr_a.ok:
            errors.extend(vr_a.errors)

    if summary is not None:
        vr_s = validate_exec_summary(summary)
        if not vr_s.ok:
            errors.extend(vr_s.errors)

    # If schema already broken badly, still continue reconcile but expect failures.

    # 1) Orders <-> Trades (FK + filled reconcile)
    if trades is not None and (not trades.empty) and (not orders.empty):
        # FK: every trade.cl_order_id must exist in orders
        order_map = orders.set_index("cl_order_id", drop=False)

        # sum(trades.qty) per cl_order_id
        tq = trades.copy()
        tq["qty"] = pd.to_numeric(tq["qty"], errors="coerce").fillna(0).astype(int)
        grp = tq.groupby("cl_order_id")["qty"].sum()

        for clid, sum_qty in grp.items():
            if clid not in order_map.index:
                _add(errors, "E_CROSS_ORPHAN_TRADE", "Trade references unknown cl_order_id", cl_order_id=str(clid))
                continue

            o = order_map.loc[clid]
            try:
                o_filled = int(pd.to_numeric(o["filled_qty"], errors="coerce") or 0)
                if int(sum_qty) != o_filled:
                    _add(
                        errors,
                        "E_CROSS_FILL_MISMATCH",
                        "Order filled_qty != sum(trades.qty)",
                        cl_order_id=str(clid),
                        order_filled_qty=o_filled,
                        trades_qty_sum=int(sum_qty),
                    )
            except Exception:
                _add(errors, "E_CROSS_FILL_FMT", "Cannot parse filled_qty for reconcile", cl_order_id=str(clid))

            # trade symbol/side should match order (strict)
            try:
                o_sym = str(o["symbol"])
                o_side = str(o["side"])
                tsub = trades[trades["cl_order_id"] == clid]
                bad = tsub[(tsub["symbol"].astype(str) != o_sym) | (tsub["side"].astype(str) != o_side)]
                if not bad.empty:
                    _add(errors, "E_CROSS_TRADE_ATTR", "Trade symbol/side mismatch with order", cl_order_id=str(clid))
            except Exception:
                pass

    # 2) Positions <-> Account (equity reconcile)
    if (positions is not None) and (account is not None):
        try:
            cash = float(account.get("cash", 0.0))
            equity = float(account.get("equity", 0.0))
            mv_sum = 0.0
            if not positions.empty and "market_value" in positions.columns:
                mv_sum = float(pd.to_numeric(positions["market_value"], errors="coerce").fillna(0.0).sum())
            calc = cash + mv_sum
            if abs(calc - equity) > 1.0:  # tolerate rounding
                _add(
                    errors,
                    "E_CROSS_EQUITY",
                    "equity must equal cash + sum(market_value) within tolerance",
                    cash=cash,
                    market_value_sum=mv_sum,
                    equity=equity,
                    calc_equity=calc,
                )
        except Exception as e:
            _add(errors, "E_CROSS_EQUITY_CALC", "Failed equity reconcile", error=str(e))

    # 3) Summary <-> artefacts basic consistency (if provided)
    if summary is not None:
        try:
            if "orders_count" in summary and orders is not None:
                if int(summary["orders_count"]) != int(len(orders)):
                    _add(errors, "E_CROSS_SUMMARY_ORDERS_COUNT", "summary.orders_count mismatch", summary=int(summary["orders_count"]), actual=int(len(orders)))
        except Exception:
            pass

        try:
            if "trades_count" in summary and trades is not None:
                if int(summary["trades_count"]) != int(len(trades)):
                    _add(errors, "E_CROSS_SUMMARY_TRADES_COUNT", "summary.trades_count mismatch", summary=int(summary["trades_count"]), actual=int(len(trades)))
        except Exception:
            pass

        # cash_end consistency (Step-2 metric)
        if account is not None and "cash_end" in summary:
            try:
                if abs(float(summary["cash_end"]) - float(account.get("cash", 0.0))) > 0.01:
                    _add(errors, "E_CROSS_SUMMARY_CASH", "summary.cash_end mismatch account.cash", summary=float(summary["cash_end"]), account=float(account.get("cash", 0.0)))
            except Exception:
                pass

        # totals for commission/tax (Step-1 metric)
        if trades is not None and (not trades.empty):
            try:
                comm_sum = float(pd.to_numeric(trades["commission"], errors="coerce").fillna(0.0).sum())
                tax_sum = float(pd.to_numeric(trades["tax"], errors="coerce").fillna(0.0).sum())
                if "total_commission" in summary and abs(float(summary["total_commission"]) - comm_sum) > 0.01:
                    _add(errors, "E_CROSS_SUMMARY_COMMISSION", "summary.total_commission mismatch", summary=float(summary["total_commission"]), calc=comm_sum)
                if "total_tax" in summary and abs(float(summary["total_tax"]) - tax_sum) > 0.01:
                    _add(errors, "E_CROSS_SUMMARY_TAX", "summary.total_tax mismatch", summary=float(summary["total_tax"]), calc=tax_sum)
            except Exception:
                pass

    return _ok() if not errors else _fail(errors)
