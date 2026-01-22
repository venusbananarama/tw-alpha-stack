# scripts/exec/run_execution.py
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import sys
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# ---------------------------------------------------------------------
# sys.path bootstrap: allow "python scripts/exec/run_execution.py" to import alpha_core
# ---------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.execution.schemas import (  # noqa: E402
    SCHEMA_VERSION,
    ExecMode,
    EXEC_SUMMARY_REQUIRED_KEYS,
    EXEC_SUMMARY_OPTIONAL_KEYS,
    OrderStatus,
)
from alpha_core.execution.broker_adapter import FillEvent, OrderAck  # noqa: E402
from alpha_core.execution.mock_broker_adapter import MockBrokerAdapter  # noqa: E402
from alpha_core.execution.paper_broker_adapter import PaperBrokerAdapter  # noqa: E402
from alpha_core.execution.validator import (  # noqa: E402
    validate_cross_artifacts,
    validate_exec_summary,
)

EXIT_CALENDAR_NOT_FOUND = 40
EXIT_NOT_TRADING_DAY = 41
EXIT_OUTDIR_NOT_EMPTY = 44
EXIT_EXEC_LOCKED = 48

# -----------------------------
# Helpers
# -----------------------------
def _iso_parse(ts: str) -> datetime:
    # Accept "YYYY-MM-DDTHH:MM:SS" and "YYYY-MM-DD HH:MM:SS"
    ts = ts.strip().replace(" ", "T")
    return datetime.fromisoformat(ts)


def _iso_fmt(dt: datetime) -> str:
    # No timezone to keep deterministic + consistent with existing logs
    return dt.replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def preflight_live_or_die(*, repo_root: Path, as_of: str, target_path: Path) -> None:
    killswitch = (repo_root / "KILLSWITCH").resolve()
    if killswitch.exists():
        print(f"KILLSWITCH_ACTIVE: {killswitch}")
        raise SystemExit(45)

    errors: List[str] = []
    if not target_path.exists():
        errors.append(f"TARGET_NOT_FOUND: {target_path}")
    try:
        datetime.strptime(as_of, "%Y-%m-%d")
    except Exception:
        errors.append(f"AS_OF_INVALID: {as_of}")

    if target_path.exists():
        _ = sha256_file(target_path)

    for msg in errors:
        print(msg)

    print("LIVE_NOT_READY")
    raise SystemExit(46)


def write_json(path: Path, obj: Any) -> None:
    # JSON determinism: sort_keys=True is required for stable hashing
    s = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(s, encoding="utf-8")


def ensure_out_dir(out_dir: Path, *, force: bool) -> None:
    """
    Step-3 Gate:
      - If out_dir exists and non-empty and not force => fail fast
      - If force => clean slate (prevent zombie artifacts)
    """
    if out_dir.exists():
        try:
            has_any = any(out_dir.iterdir())
        except FileNotFoundError:
            has_any = False

        if has_any and not force:
            print(f"OUTDIR_NOT_EMPTY: {out_dir}")
            raise SystemExit(EXIT_OUTDIR_NOT_EMPTY)

        if force:
            shutil.rmtree(out_dir, ignore_errors=True)

    out_dir.mkdir(parents=True, exist_ok=True)


def _parse_trading_day(value: str) -> Optional[str]:
    s = value.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def load_trading_days(paths: List[Path]) -> Tuple[Optional[Set[str]], List[Path]]:
    tried: List[Path] = []
    for path in paths:
        tried.append(path)
        if not path.exists():
            continue
        days: Set[str] = set()
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    day = _parse_trading_day(str(cell))
                    if day:
                        days.add(day)
                        break
        return days, tried
    return None, tried


def enforce_trading_day(*, as_of: str, repo_root: Path) -> None:
    paths = [
        repo_root / "datahub" / "ref" / "trading_days.csv",
        repo_root / "cal" / "trading_days.csv",
    ]
    days, tried = load_trading_days(paths)
    if days is None:
        tried_s = "; ".join(str(p) for p in tried)
        print(f"CALENDAR_NOT_FOUND: {tried_s}")
        raise SystemExit(EXIT_CALENDAR_NOT_FOUND)

    try:
        as_of_norm = datetime.strptime(as_of, "%Y-%m-%d").date().isoformat()
    except Exception:
        as_of_norm = as_of

    if as_of_norm not in days:
        print(f"NOT_TRADING_DAY: {as_of}")
        raise SystemExit(EXIT_NOT_TRADING_DAY)


def acquire_exec_lock(*, repo_root: Path, run_id: str) -> Path:
    lock_dir = repo_root / "reports" / "exec" / "_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{run_id}.lock"
    try:
        with lock_path.open("x", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()}\n")
            f.write(f"run_id={run_id}\n")
    except FileExistsError:
        print(f"EXEC_LOCKED: {lock_path}")
        raise SystemExit(EXIT_EXEC_LOCKED)
    return lock_path


def release_exec_lock(lock_path: Optional[Path]) -> None:
    if lock_path is None:
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return
    except Exception:
        return


def infer_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def read_target_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    sym_col = infer_col(df, ["symbol", "ticker", "stock_id", "sid"])
    qty_col = infer_col(df, ["target_qty", "qty", "shares", "target", "target_shares"])
    strat_col = infer_col(df, ["strategy_id", "strategy", "signal", "signal_name"])

    if sym_col is None or qty_col is None:
        raise ValueError(f"Target CSV missing required columns: need symbol & target_qty (got: {list(df.columns)})")

    out = pd.DataFrame(
        {
            "symbol": df[sym_col].astype(str).str.strip(),
            "target_qty": pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int),
            "strategy_id": (df[strat_col].astype(str).str.strip() if strat_col else "unknown"),
        }
    )
    # Stable order for determinism
    out = out.sort_values(["symbol", "strategy_id"], kind="mergesort").reset_index(drop=True)
    return out


def read_current_positions(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    sym_col = infer_col(df, ["symbol", "ticker", "stock_id", "sid"])
    qty_col = infer_col(df, ["qty", "quantity", "shares", "position_qty"])
    cost_col = infer_col(df, ["avg_cost", "cost", "avg_price", "average_cost"])

    if sym_col is None or qty_col is None:
        raise ValueError(f"Current positions CSV missing required columns: need symbol & qty (got: {list(df.columns)})")

    out = pd.DataFrame(
        {
            "symbol": df[sym_col].astype(str).str.strip(),
            "qty": pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int),
            "avg_cost": (pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0) if cost_col else 0.0),
        }
    )
    out = out.sort_values(["symbol"], kind="mergesort").reset_index(drop=True)
    return out


def _side_from_delta(delta_qty: int) -> str:
    return "BUY" if delta_qty > 0 else "SELL"


def build_orders(
    *,
    run_id: str,
    as_of: str,
    ts_created: str,
    target_df: pd.DataFrame,
    current_pos_df: Optional[pd.DataFrame],
    strategy_id_default: str,
    order_type: str,
    tif: str,
    mock_price: float,
    allow_short: bool,
) -> pd.DataFrame:
    # Map current qty if provided
    cur_map: Dict[str, int] = {}
    if current_pos_df is not None and not current_pos_df.empty:
        cur_map = {r["symbol"]: int(r["qty"]) for _, r in current_pos_df.iterrows()}

    rows: List[Dict[str, Any]] = []
    seq = 0

    # Deterministic: sorted target_df already
    for _, r in target_df.iterrows():
        symbol = str(r["symbol"]).strip()
        tgt_qty = int(r["target_qty"])

        cur_qty = int(cur_map.get(symbol, 0))
        delta = tgt_qty - cur_qty if current_pos_df is not None else tgt_qty

        if delta == 0:
            continue

        # Prevent short if policy disallows and delta implies net short beyond holdings (simple check)
        if (not allow_short) and current_pos_df is not None:
            # If we sell more than current, we'd go short
            if delta < 0 and abs(delta) > cur_qty:
                raise RuntimeError(f"SHORT_NOT_ALLOWED: symbol={symbol} cur_qty={cur_qty} sell_qty={abs(delta)}")

        side = _side_from_delta(delta)
        qty = abs(int(delta))

        seq += 1
        cl_order_id = f"{run_id}_{symbol}_{side}_{seq:04d}"
        row = {
            "run_id": run_id,
            "as_of": as_of,
            "ts_created": ts_created,
            "cl_order_id": cl_order_id,
            "broker_order_id": None,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "time_in_force": tif,
            "qty": int(qty),
            "filled_qty": 0,
            "status": "NEW",
            "strategy_id": str(r.get("strategy_id", "")).strip() or strategy_id_default,
            "limit_price": float(mock_price) if order_type == "LIMIT" else None,
            "avg_price": None,
            "reject_reason": None,
            "ts_submitted": None,
            "ts_last_update": ts_created,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        # ensure stable columns even if empty
        df = pd.DataFrame(
            columns=[
                "run_id",
                "as_of",
                "ts_created",
                "cl_order_id",
                "broker_order_id",
                "symbol",
                "side",
                "order_type",
                "time_in_force",
                "qty",
                "filled_qty",
                "status",
                "strategy_id",
                "limit_price",
                "avg_price",
                "reject_reason",
                "ts_submitted",
                "ts_last_update",
            ]
        )
    return df


def mock_match_orders(
    *,
    orders: pd.DataFrame,
    ts_base: datetime,
    fill_rate: float,
    commission_bps: float,
    tax_bps: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deterministic mock matcher:
      NEW -> SUBMITTED -> (FILLED/PARTIALLY_FILLED)
      trades.trade_id = trd_{cl_order_id}_01
      broker_order_id = bk_{cl_order_id}
    """
    if orders.empty:
        trades = pd.DataFrame(
            columns=[
                "run_id",
                "as_of",
                "trade_id",
                "cl_order_id",
                "broker_order_id",
                "ts_filled",
                "symbol",
                "side",
                "price",
                "qty",
                "commission",
                "tax",
            ]
        )
        return orders.copy(), trades

    updated = orders.copy()
    trade_rows: List[Dict[str, Any]] = []

    # Deterministic processing order
    updated = updated.sort_values(["symbol", "side", "cl_order_id"], kind="mergesort").reset_index(drop=True)

    for i in range(len(updated)):
        row = updated.iloc[i].to_dict()
        clid = row["cl_order_id"]
        broker_order_id = f"bk_{clid}"

        ts_submitted = ts_base + timedelta(seconds=1 + i)
        ts_filled = ts_base + timedelta(seconds=2 + i)

        updated.at[i, "status"] = "SUBMITTED"
        updated.at[i, "ts_submitted"] = _iso_fmt(ts_submitted)
        updated.at[i, "ts_last_update"] = _iso_fmt(ts_submitted)
        updated.at[i, "broker_order_id"] = broker_order_id

        qty = int(row["qty"])
        # deterministic fill qty
        if fill_rate >= 1.0:
            fill_qty = qty
        elif fill_rate <= 0.0:
            fill_qty = 0
        else:
            fill_qty = int(math.floor(qty * float(fill_rate) + 1e-9))

        if fill_qty <= 0:
            # no fill: remain SUBMITTED (or could be CANCELLED); keep simple
            updated.at[i, "filled_qty"] = 0
            updated.at[i, "status"] = "SUBMITTED"
            updated.at[i, "ts_last_update"] = _iso_fmt(ts_submitted)
            continue

        price = float(row["limit_price"]) if row.get("limit_price") is not None else float(row.get("avg_price") or 0.0)

        gross = price * fill_qty
        commission = round(gross * (float(commission_bps) / 10000.0), 2)
        tax = 0.0
        if str(row["side"]).upper() == "SELL":
            tax = round(gross * (float(tax_bps) / 10000.0), 2)

        trade_id = f"trd_{clid}_01"
        trade_rows.append(
            {
                "run_id": row["run_id"],
                "as_of": row["as_of"],
                "trade_id": trade_id,
                "cl_order_id": clid,
                "broker_order_id": broker_order_id,
                "ts_filled": _iso_fmt(ts_filled),
                "symbol": row["symbol"],
                "side": row["side"],
                "price": float(price),
                "qty": int(fill_qty),
                "commission": float(commission),
                "tax": float(tax),
            }
        )

        updated.at[i, "filled_qty"] = int(fill_qty)
        updated.at[i, "avg_price"] = float(price)
        updated.at[i, "ts_last_update"] = _iso_fmt(ts_filled)
        updated.at[i, "status"] = "FILLED" if fill_qty == qty else "PARTIALLY_FILLED"

    trades = pd.DataFrame(trade_rows)
    if trades.empty:
        trades = pd.DataFrame(
            columns=[
                "run_id",
                "as_of",
                "trade_id",
                "cl_order_id",
                "broker_order_id",
                "ts_filled",
                "symbol",
                "side",
                "price",
                "qty",
                "commission",
                "tax",
            ]
        )
    else:
        trades = trades.sort_values(["symbol", "side", "trade_id"], kind="mergesort").reset_index(drop=True)

    return updated, trades


def apply_order_acks(orders: pd.DataFrame, acks: List[OrderAck]) -> pd.DataFrame:
    if orders is None or orders.empty or not acks:
        return orders

    updated = orders.copy()
    ack_map = {a.cl_order_id: a for a in acks}

    for i, row in updated.iterrows():
        clid = row.get("cl_order_id")
        if clid not in ack_map:
            continue

        ack = ack_map[clid]
        ts_event = ack.ts_event if isinstance(ack.ts_event, datetime) else _iso_parse(str(ack.ts_event))
        ts_str = _iso_fmt(ts_event)

        updated.at[i, "broker_order_id"] = ack.broker_order_id
        updated.at[i, "status"] = ack.status.value if hasattr(ack.status, "value") else str(ack.status)
        updated.at[i, "reject_reason"] = ack.reject_reason
        updated.at[i, "ts_last_update"] = ts_str
        if "ts_submitted" in updated.columns:
            updated.at[i, "ts_submitted"] = ts_str

    return updated


def fills_to_trades(fills: List[FillEvent], orders: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "run_id",
        "as_of",
        "trade_id",
        "cl_order_id",
        "broker_order_id",
        "ts_filled",
        "symbol",
        "side",
        "price",
        "qty",
        "commission",
        "tax",
    ]
    if not fills:
        return pd.DataFrame(columns=columns)

    if orders is None or orders.empty:
        raise RuntimeError("FILL_WITHOUT_ORDERS")

    order_map = orders.set_index("cl_order_id", drop=False)
    rows: List[Dict[str, Any]] = []

    for fill in fills:
        clid = fill.cl_order_id
        if clid not in order_map.index:
            raise RuntimeError(f"FILL_UNKNOWN_ORDER: {clid}")

        o = order_map.loc[clid]
        ts_filled = fill.ts_filled if isinstance(fill.ts_filled, datetime) else _iso_parse(str(fill.ts_filled))
        rows.append(
            {
                "run_id": o["run_id"],
                "as_of": o["as_of"],
                "trade_id": fill.trade_id,
                "cl_order_id": clid,
                "broker_order_id": o.get("broker_order_id"),
                "ts_filled": _iso_fmt(ts_filled),
                "symbol": fill.symbol,
                "side": fill.side,
                "price": float(fill.price),
                "qty": int(fill.qty),
                "commission": float(fill.commission),
                "tax": float(fill.tax),
            }
        )

    trades = pd.DataFrame(rows)
    trades = trades.sort_values(["symbol", "side", "trade_id"], kind="mergesort").reset_index(drop=True)
    return trades


def apply_trades_to_orders(orders: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if orders is None or orders.empty or trades is None or trades.empty:
        return orders

    updated = orders.copy()
    trade_groups = trades.groupby("cl_order_id", sort=False)

    for clid, grp in trade_groups:
        fill_qty = int(pd.to_numeric(grp["qty"], errors="coerce").fillna(0).sum())
        if fill_qty <= 0:
            continue

        idx = updated.index[updated["cl_order_id"] == clid]
        if idx.empty:
            continue
        i = idx[0]

        qty_val = pd.to_numeric(updated.at[i, "qty"], errors="coerce")
        qty = int(0 if pd.isna(qty_val) else qty_val)
        px = pd.to_numeric(grp["price"], errors="coerce").fillna(0.0)
        qv = pd.to_numeric(grp["qty"], errors="coerce").fillna(0.0)
        price = float((px * qv).sum() / float(fill_qty)) if fill_qty > 0 else 0.0

        ts_vals = [(_iso_parse(t) if isinstance(t, str) else t) for t in grp["ts_filled"].tolist()]
        ts_last = max(ts_vals) if ts_vals else None

        updated.at[i, "filled_qty"] = int(fill_qty)
        updated.at[i, "avg_price"] = float(price)
        if ts_last is not None:
            updated.at[i, "ts_last_update"] = _iso_fmt(ts_last)

        if fill_qty >= qty:
            updated.at[i, "status"] = OrderStatus.FILLED.value
        else:
            updated.at[i, "status"] = OrderStatus.PARTIALLY_FILLED.value

        if pd.isna(updated.at[i, "broker_order_id"]):
            updated.at[i, "broker_order_id"] = grp["broker_order_id"].iloc[0]

    return updated


def derive_positions(
    *,
    run_id: str,
    as_of: str,
    trades: pd.DataFrame,
    current_pos_df: Optional[pd.DataFrame],
    mark_price: float,
    allow_short: bool,
) -> pd.DataFrame:
    # build start map: symbol -> (qty, avg_cost)
    start_qty: Dict[str, int] = {}
    start_cost: Dict[str, float] = {}
    if current_pos_df is not None and not current_pos_df.empty:
        for _, r in current_pos_df.iterrows():
            s = str(r["symbol"]).strip()
            start_qty[s] = int(r["qty"])
            start_cost[s] = float(r.get("avg_cost", 0.0) or 0.0)

    # trades net
    buy_value: Dict[str, float] = {}
    buy_qty: Dict[str, int] = {}
    sell_qty: Dict[str, int] = {}
    symbols: set[str] = set(start_qty.keys())

    if trades is not None and not trades.empty:
        for _, t in trades.iterrows():
            sym = str(t["symbol"]).strip()
            symbols.add(sym)
            q = int(t["qty"])
            px = float(t["price"])
            side = str(t["side"]).upper()
            if side == "BUY":
                buy_qty[sym] = buy_qty.get(sym, 0) + q
                buy_value[sym] = buy_value.get(sym, 0.0) + (px * q)
            else:
                sell_qty[sym] = sell_qty.get(sym, 0) + q

    rows: List[Dict[str, Any]] = []
    for sym in sorted(symbols):
        sq = int(start_qty.get(sym, 0))
        bq = int(buy_qty.get(sym, 0))
        sv = int(sell_qty.get(sym, 0))
        nq = sq + bq - sv

        if (not allow_short) and nq < 0:
            raise RuntimeError(f"SHORT_NOT_ALLOWED_AFTER_TRADES: symbol={sym} qty={nq}")

        # avg_cost (simple):
        # - If nq <= 0 => avg_cost = 0
        # - Else weighted avg of remaining inventory:
        #   start_cost*start_qty + buy_value / (start_qty + buy_qty) then scaled by remaining qty.
        #   This is an approximation; Phase-4 will replace with real ledger.
        if nq <= 0:
            avg_cost = 0.0
        else:
            base_qty = max(sq + bq, 1)
            base_cost_value = float(start_cost.get(sym, 0.0)) * float(sq) + float(buy_value.get(sym, 0.0))
            avg_cost = float(base_cost_value) / float(base_qty)

        mv = float(nq) * float(mark_price)

        rows.append(
            {
                "run_id": run_id,
                "as_of": as_of,
                "symbol": sym,
                "qty": int(nq),
                "avg_cost": round(float(avg_cost), 6),
                "market_value": round(float(mv), 6),
                "source": "DERIVED",
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["run_id", "as_of", "symbol", "qty", "avg_cost", "market_value", "source"])
    return df


def derive_account(
    *,
    run_id: str,
    as_of: str,
    ts_snapshot: str,
    currency: str,
    initial_cash: float,
    trades: pd.DataFrame,
    positions: pd.DataFrame,
) -> Dict[str, Any]:
    cash = float(initial_cash)

    if trades is not None and not trades.empty:
        for _, t in trades.iterrows():
            side = str(t["side"]).upper()
            px = float(t["price"])
            q = int(t["qty"])
            commission = float(t.get("commission", 0.0) or 0.0)
            tax = float(t.get("tax", 0.0) or 0.0)
            gross = px * q
            if side == "BUY":
                cash -= (gross + commission)
            else:
                cash += (gross - commission - tax)

    mv_sum = 0.0
    if positions is not None and not positions.empty:
        mv_sum = float(pd.to_numeric(positions["market_value"], errors="coerce").fillna(0.0).sum())

    equity = cash + mv_sum
    nav = equity

    # For now: buying_power mirrors cash (future: broker mapped)
    buying_power = max(cash, 0.0)

    return {
        "run_id": run_id,
        "as_of": as_of,
        "ts_snapshot": ts_snapshot,
        "currency": currency,
        "cash": round(float(cash), 6),
        "buying_power": round(float(buying_power), 6),
        "equity": round(float(equity), 6),
        "nav": round(float(nav), 6),
        "source": "DERIVED",
    }


def sanitize_exec_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    allowed = set(EXEC_SUMMARY_REQUIRED_KEYS) | set(EXEC_SUMMARY_OPTIONAL_KEYS)
    return {k: summary[k] for k in summary.keys() if k in allowed}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-3 Execution Runner (MockExec Step-2 + Step-3 Gate)")

    p.add_argument("--as-of", required=True, help="Trading date YYYY-MM-DD")
    p.add_argument("--run-id", required=True, help="Unique run id")
    p.add_argument("--target", required=True, help="Path to target_portfolio CSV")
    p.add_argument("--out-dir", default=None, help="Output directory (default: reports/exec/<run_id>)")

    p.add_argument("--mode", default="MOCK", choices=[m.value for m in ExecMode], help="Execution mode")

    p.add_argument("--strategy-id", default="unknown", help="Default strategy id if target has none")
    p.add_argument("--order-type", default="LIMIT", choices=["MARKET", "LIMIT"], help="Order type")
    p.add_argument("--tif", default="ROD", choices=["ROD", "IOC", "FOK"], help="Time in force")

    p.add_argument("--ts-created", default=None, help="Deterministic created timestamp (default: <as_of>T09:00:00)")
    p.add_argument("--current-positions", default=None, help="Optional current positions CSV")

    # Step-2 arguments
    p.add_argument("--mock-fill-rate", type=float, default=1.0, help="Fill rate [0..1]")
    p.add_argument("--mock-commission-bps", type=float, default=14.25, help="Commission bps")
    p.add_argument("--mock-tax-bps", type=float, default=30.0, help="Tax bps (SELL only)")
    p.add_argument("--mock-price", type=float, default=100.0, help="Mock price (used for limit_price & mark price)")

    p.add_argument("--initial-cash", type=float, default=0.0, help="Starting cash for derived account snapshot")
    p.add_argument("--currency", type=str, default="TWD", help="Account currency")

    p.add_argument("--allow-short", action="store_true", help="Allow short positions (default: False)")

    # Step-3 gate
    p.add_argument("--force", action="store_true", help="If set, clear out_dir before running")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    as_of = args.as_of.strip()
    run_id = args.run_id.strip()
    mode = ExecMode(args.mode)

    target_path = Path(args.target)
    if mode != ExecMode.LIVE:
        if not target_path.exists():
            print(f"TARGET_NOT_FOUND: {target_path}")
            return 2

    enforce_trading_day(as_of=as_of, repo_root=_REPO_ROOT)

    lock_path: Optional[Path] = None
    try:
        lock_path = acquire_exec_lock(repo_root=_REPO_ROOT, run_id=run_id)

        if mode == ExecMode.LIVE:
            preflight_live_or_die(repo_root=_REPO_ROOT, as_of=as_of, target_path=target_path)

        if not target_path.exists():
            print(f"TARGET_NOT_FOUND: {target_path}")
            return 2

        out_dir = Path(args.out_dir) if args.out_dir else (Path("reports") / "exec" / run_id)

        # Step-3B: Idempotency Gate + Zombie prevention
        ensure_out_dir(out_dir, force=bool(args.force))

        # Deterministic timestamps
        ts_created = args.ts_created or f"{as_of}T09:00:00"
        t0 = _iso_parse(ts_created)
        started_at = _iso_fmt(t0)
        finished_at = _iso_fmt(t0 + timedelta(seconds=5))

        # Read inputs
        target_df = read_target_csv(target_path)
        current_pos_df = None
        if args.current_positions:
            cp = Path(args.current_positions)
            if not cp.exists():
                print(f"CURRENT_POSITIONS_NOT_FOUND: {cp}")
                return 2
            current_pos_df = read_current_positions(cp)

        # Build orders (intent)
        orders = build_orders(
            run_id=run_id,
            as_of=as_of,
            ts_created=started_at,
            target_df=target_df,
            current_pos_df=current_pos_df,
            strategy_id_default=args.strategy_id,
            order_type=args.order_type,
            tif=args.tif,
            mock_price=float(args.mock_price),
            allow_short=bool(args.allow_short),
        )

        trades = pd.DataFrame()
        positions = pd.DataFrame()
        account: Dict[str, Any] = {}

        # Execute
        if mode == ExecMode.MOCK:
            adapter = MockBrokerAdapter(
                mode=mode,
                ts_base=t0,
                fill_rate=float(args.mock_fill_rate),
                commission_bps=float(args.mock_commission_bps),
                tax_bps=float(args.mock_tax_bps),
                mock_price=float(args.mock_price),
            )
            adapter.connect()
            try:
                ack_result = adapter.send_orders(orders)
                orders = apply_order_acks(orders, ack_result.acks)

                fills_result = adapter.poll_fills()
                trades = fills_to_trades(fills_result.fills, orders)
                orders = apply_trades_to_orders(orders, trades)
            finally:
                adapter.close()
        elif mode == ExecMode.PAPER:
            adapter = PaperBrokerAdapter(
                mode=mode,
                ts_base=t0,
            )
            adapter.connect()
            try:
                ack_result = adapter.send_orders(orders)
                orders = apply_order_acks(orders, ack_result.acks)

                fills_result = adapter.poll_fills()
                trades = fills_to_trades(fills_result.fills, orders)
                orders = apply_trades_to_orders(orders, trades)
            finally:
                adapter.close()
        else:
            # LIVE: runner does not implement broker wiring yet (Phase-3 MockExec focus)
            # Keep deterministic: do nothing.
            trades = pd.DataFrame(
                columns=[
                    "run_id",
                    "as_of",
                    "trade_id",
                    "cl_order_id",
                    "broker_order_id",
                    "ts_filled",
                    "symbol",
                    "side",
                    "price",
                    "qty",
                    "commission",
                    "tax",
                ]
            )

        # Derive positions/account (Step-2)
        positions = derive_positions(
            run_id=run_id,
            as_of=as_of,
            trades=trades,
            current_pos_df=current_pos_df,
            mark_price=float(args.mock_price),
            allow_short=bool(args.allow_short),
        )

        account = derive_account(
            run_id=run_id,
            as_of=as_of,
            ts_snapshot=finished_at,
            currency=str(args.currency),
            initial_cash=float(args.initial_cash),
            trades=trades,
            positions=positions,
        )

        # Summary (keep keys within whitelist; avoid unknown keys)
        qty_total = int(pd.to_numeric(orders["qty"], errors="coerce").fillna(0).sum()) if not orders.empty else 0
        filled_qty_total = int(pd.to_numeric(orders["filled_qty"], errors="coerce").fillna(0).sum()) if not orders.empty else 0
        fill_rate_metric = float(filled_qty_total) / float(qty_total) if qty_total > 0 else 0.0

        total_commission = float(pd.to_numeric(trades["commission"], errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
        total_tax = float(pd.to_numeric(trades["tax"], errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0

        cash_start = float(args.initial_cash)
        cash_end = float(account.get("cash", 0.0))

        is_noop = int(len(orders)) == 0
        status = "NOOP" if is_noop else "OK"
        reason_code = "NO_TRADES" if is_noop else "EXECUTED"

        summary_raw: Dict[str, Any] = {
            "run_id": run_id,
            "as_of": as_of,
            "mode": mode.value,
            "schema_version": SCHEMA_VERSION,
            "job_success": True,  # may flip to False if cross validation fails
            "started_at": started_at,
            "finished_at": finished_at,
            "orders_count": int(len(orders)),
            "trades_count": int(len(trades)) if trades is not None else 0,
            "fill_rate": round(float(fill_rate_metric), 12),
            "reject_rate": 0.0,
            "artefacts_manifest": [],  # fill later

            # Optional keys (must be whitelisted in schemas.py)
            "total_commission": round(float(total_commission), 6),
            "total_tax": round(float(total_tax), 6),
            "qty_total": int(qty_total),
            "filled_qty_total": int(filled_qty_total),
            "cash_start": round(float(cash_start), 6),
            "cash_end": round(float(cash_end), 6),
            "allow_short": bool(args.allow_short),
            "status": status,
            "reason_code": reason_code,
        }

        summary_base = sanitize_exec_summary(summary_raw)

        # Cross validation must happen before writing artefacts (fail fast)
        cross_errors: List[Dict[str, Any]] = []

        r_sum = validate_exec_summary(summary_base)
        if not r_sum.ok:
            for e in r_sum.errors:
                d = {"code": getattr(e, "code", "E_UNKNOWN"), "message": getattr(e, "message", str(e))}
                if hasattr(e, "context"):
                    d["context"] = getattr(e, "context")
                cross_errors.append(d)

        r = validate_cross_artifacts(orders, trades, positions, account, summary_base)
        if not r.ok:
            for e in r.errors:
                d = {"code": getattr(e, "code", "E_UNKNOWN"), "message": getattr(e, "message", str(e))}
                if hasattr(e, "context"):
                    d["context"] = getattr(e, "context")
                cross_errors.append(d)

        if cross_errors:
            print(f"CROSS_VALIDATE_FAILED: {json.dumps(cross_errors, ensure_ascii=False)}")
            return 3

        summary_out = dict(summary_base)
        summary_out["status"] = status
        summary_out["reason_code"] = reason_code

        # Write artefacts
        p_orders = out_dir / "orders.csv"
        p_trades = out_dir / "trades.csv"
        p_positions = out_dir / "positions.csv"
        p_account = out_dir / "account_snapshot.json"
        p_summary = out_dir / "exec_summary.json"
        p_ledger = out_dir / "ledger.json"

        # deterministic CSV output (stable column order)
        orders.to_csv(p_orders, index=False, encoding="utf-8", lineterminator="\n")
        trades.to_csv(p_trades, index=False, encoding="utf-8", lineterminator="\n")
        positions.to_csv(p_positions, index=False, encoding="utf-8", lineterminator="\n")
        write_json(p_account, account)

        # Manifest (deterministic ordering)
        artefacts = [
            ("orders", p_orders),
            ("trades", p_trades),
            ("positions", p_positions),
            ("account_snapshot", p_account),
        ]
        manifest: List[Dict[str, Any]] = []
        for name, path in artefacts:
            manifest.append(
                {
                    "name": name,
                    "path": str(path.as_posix()),
                    "sha256": sha256_file(path),
                    "bytes": int(path.stat().st_size),
                }
            )

        # exec_summary depends on manifest
        summary_out["artefacts_manifest"] = manifest
        write_json(p_summary, summary_out)

        # include summary in ledger after writing
        ledger = {
            "run_id": run_id,
            "as_of": as_of,
            "schema_version": SCHEMA_VERSION,
            "python_version": sys.version.split()[0],
            "argv": list(sys.argv),
            "inputs": {
                "target_path": str(target_path.as_posix()),
                "target_sha256": sha256_file(target_path),
            },
            "outputs": [
                *manifest,
                {
                    "name": "exec_summary",
                    "path": str(p_summary.as_posix()),
                    "sha256": sha256_file(p_summary),
                    "bytes": int(p_summary.stat().st_size),
                },
            ],
        }
        write_json(p_ledger, ledger)

        return 0
    finally:
        release_exec_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
