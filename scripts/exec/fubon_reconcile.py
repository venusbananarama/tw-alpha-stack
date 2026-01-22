from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------
# sys.path bootstrap: allow "python scripts/exec/fubon_reconcile.py"
# ---------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.execution import schemas as exec_schemas  # noqa: E402

EXIT_OK = 0
EXIT_CONFIG = 60
EXIT_DATA = 70
EXIT_IO = 71


@dataclass(frozen=True)
class SourceRef:
    path: Path
    line_no: int
    date: str


class DataError(RuntimeError):
    pass


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text.strip(), "%Y-%m-%d")


def _iter_dates(start: str, end: str) -> List[str]:
    d0 = _parse_date(start)
    d1 = _parse_date(end)
    if d1 < d0:
        raise ValueError("end < start")
    days: List[str] = []
    cur = d0
    while cur <= d1:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def _ensure_out_dir(out_dir: Path, force: bool) -> None:
    if out_dir.exists():
        has_any = any(out_dir.iterdir())
        if has_any and not force:
            raise DataError(f"OUTDIR_NOT_EMPTY: {out_dir}")
        if force:
            shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)


def _parse_event_date(ts_event: str) -> str:
    text = str(ts_event).strip()
    if "T" in text:
        head = text.split("T", 1)[0]
    elif " " in text:
        head = text.split(" ", 1)[0]
    else:
        head = text
    _ = _parse_date(head)
    return head


def _safe_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        try:
            return _safe_json(value.to_dict())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return _safe_json(dict(value.__dict__))
        except Exception:
            pass
    return str(value)


def _get_value(payload: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in payload and payload[k] is not None:
            return payload[k]
    lower_map = {k.lower(): k for k in payload.keys()}
    for k in keys:
        lk = k.lower()
        if lk in lower_map:
            v = payload[lower_map[lk]]
            if v is not None:
                return v
    data = payload.get("data")
    if isinstance(data, dict):
        return _get_value(data, keys)
    return None


def _require_str(payload: Dict[str, Any], keys: Iterable[str], *, field: str, ref: SourceRef) -> str:
    v = _get_value(payload, keys)
    if v is None:
        raise DataError(f"MISSING_FIELD: {field} at {ref.path}:{ref.line_no}")
    s = str(v).strip()
    if not s:
        raise DataError(f"MISSING_FIELD: {field} at {ref.path}:{ref.line_no}")
    return s


def _require_int(payload: Dict[str, Any], keys: Iterable[str], *, field: str, ref: SourceRef) -> int:
    v = _get_value(payload, keys)
    if v is None:
        raise DataError(f"MISSING_FIELD: {field} at {ref.path}:{ref.line_no}")
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        raise DataError(f"INVALID_FIELD: {field} at {ref.path}:{ref.line_no}")


def _require_float(payload: Dict[str, Any], keys: Iterable[str], *, field: str, ref: SourceRef) -> float:
    v = _get_value(payload, keys)
    if v is None:
        raise DataError(f"MISSING_FIELD: {field} at {ref.path}:{ref.line_no}")
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        raise DataError(f"INVALID_FIELD: {field} at {ref.path}:{ref.line_no}")


def _map_side(raw: str, *, ref: SourceRef) -> str:
    s = str(raw).strip().upper()
    if s in {"B", "BUY", "1"}:
        return "BUY"
    if s in {"S", "SELL", "2"}:
        return "SELL"
    raise DataError(f"INVALID_FIELD: side at {ref.path}:{ref.line_no}")


def _map_order_type(raw: str, *, ref: SourceRef) -> str:
    s = str(raw).strip().upper()
    if "LIMIT" in s or s in {"LMT", "L"}:
        return "LIMIT"
    if "MARKET" in s or s in {"MKT", "M"}:
        return "MARKET"
    raise DataError(f"INVALID_FIELD: order_type at {ref.path}:{ref.line_no}")


def _map_tif(raw: str, *, ref: SourceRef) -> str:
    s = str(raw).strip().upper()
    if s in {"ROD", "IOC", "FOK"}:
        return s
    if s in {"DAY"}:
        return "ROD"
    raise DataError(f"INVALID_FIELD: time_in_force at {ref.path}:{ref.line_no}")


def _map_status(raw: str, *, ref: SourceRef) -> str:
    s = str(raw).strip().upper()
    if "REJECT" in s:
        return "REJECTED"
    if "CANCEL" in s:
        return "CANCELLED"
    if "PART" in s:
        return "PARTIALLY_FILLED"
    if "FILLED" in s or "FILL" in s:
        return "FILLED"
    if "SUBMIT" in s:
        return "SUBMITTED"
    if "NEW" in s:
        return "NEW"
    raise DataError(f"INVALID_FIELD: status at {ref.path}:{ref.line_no}")


def _load_ndjson(path: Path, *, kind: str, date: str) -> List[Tuple[Dict[str, Any], SourceRef]]:
    items: List[Tuple[Dict[str, Any], SourceRef]] = []
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            text = line.rstrip("\n")
            if text.strip() == "":
                raise DataError(f"INVALID_JSON: empty line at {path}:{idx}")
            try:
                obj = json.loads(text)
            except Exception:
                raise DataError(f"INVALID_JSON: {path}:{idx}")
            if not isinstance(obj, dict):
                raise DataError(f"INVALID_JSON: {path}:{idx} not object")
            event = obj.get("event")
            if event is not None and str(event).strip() != kind:
                raise DataError(f"EVENT_MISMATCH: {path}:{idx}")
            ts_event = obj.get("ts_event")
            if not ts_event:
                raise DataError(f"MISSING_FIELD: ts_event at {path}:{idx}")
            event_date = _parse_event_date(ts_event)
            if event_date != date:
                raise DataError(f"DATE_OUT_OF_RANGE: {path}:{idx} ts_event={event_date} dt={date}")
            run_meta = obj.get("run_meta")
            if isinstance(run_meta, dict) and run_meta.get("date") and str(run_meta["date"]) != date:
                raise DataError(f"DATE_MISMATCH: {path}:{idx} run_meta.date={run_meta.get('date')}")
            payload = obj.get("payload")
            if not isinstance(payload, dict):
                raise DataError(f"MISSING_FIELD: payload at {path}:{idx}")
            obj["payload"] = _safe_json(payload)
            items.append((obj, SourceRef(path=path, line_no=idx, date=date)))
    return items


def _collect_records(src_root: Path, dates: List[str], kind: str) -> List[Tuple[Dict[str, Any], SourceRef]]:
    all_items: List[Tuple[Dict[str, Any], SourceRef]] = []
    for date in dates:
        folder = src_root / kind / f"dt={date}"
        if not folder.exists():
            continue
        files = sorted(folder.glob("*.ndjson"))
        for path in files:
            all_items.extend(_load_ndjson(path, kind=kind[:-1], date=date))
    return all_items


def _map_order_record(
    obj: Dict[str, Any],
    ref: SourceRef,
    *,
    run_id: str,
    as_of: str,
) -> Dict[str, Any]:
    payload = obj["payload"]
    ts_event = str(obj.get("ts_event"))

    cl_order_id = _require_str(
        payload,
        ["cl_order_id", "clOrderId", "client_order_id", "clientOrderId", "order_no", "orderNo", "order_id", "orderId", "id"],
        field="cl_order_id",
        ref=ref,
    )
    symbol = _require_str(payload, ["symbol", "stock_no", "stockNo", "stock_id", "stockId", "code"], field="symbol", ref=ref)
    side_raw = _require_str(payload, ["side", "action", "bs", "bs_action", "bsAction", "buy_sell"], field="side", ref=ref)
    side = _map_side(side_raw, ref=ref)
    ot_raw = _require_str(payload, ["order_type", "orderType", "type", "price_type", "priceType"], field="order_type", ref=ref)
    order_type = _map_order_type(ot_raw, ref=ref)
    tif_raw = _require_str(payload, ["time_in_force", "tif", "timeInForce"], field="time_in_force", ref=ref)
    time_in_force = _map_tif(tif_raw, ref=ref)
    qty = _require_int(payload, ["qty", "quantity", "order_qty", "orderQty", "volume", "shares"], field="qty", ref=ref)
    if qty <= 0:
        raise DataError(f"INVALID_FIELD: qty at {ref.path}:{ref.line_no}")
    filled_qty = _require_int(payload, ["filled_qty", "filledQty", "filled_quantity", "filledQuantity", "fill_qty", "fillQty", "deal_qty"], field="filled_qty", ref=ref)
    if filled_qty < 0:
        raise DataError(f"INVALID_FIELD: filled_qty at {ref.path}:{ref.line_no}")
    status_raw = _require_str(payload, ["status", "order_status", "orderStatus", "state", "order_state"], field="status", ref=ref)
    status = _map_status(status_raw, ref=ref)
    ts_created = _get_value(payload, ["ts_created", "created_at", "order_time", "orderTime", "time"])
    if not ts_created:
        ts_created = ts_event
    ts_created = str(ts_created)

    strategy_id = _get_value(payload, ["strategy_id", "strategyId", "strategy"])
    if not strategy_id:
        strategy_id = "broker"

    limit_price = None
    lp_raw = _get_value(payload, ["limit_price", "limitPrice", "price", "order_price", "orderPrice"])
    if lp_raw is not None:
        try:
            limit_price = float(str(lp_raw).replace(",", "").strip())
        except Exception:
            raise DataError(f"INVALID_FIELD: limit_price at {ref.path}:{ref.line_no}")

    if order_type == "LIMIT":
        if limit_price is None or float(limit_price) <= 0:
            raise DataError(f"MISSING_FIELD: limit_price at {ref.path}:{ref.line_no}")

    reject_reason = None
    if status == "REJECTED":
        reject_reason = _get_value(payload, ["reject_reason", "rejectReason", "error", "message", "msg"])
        if not reject_reason:
            raise DataError(f"MISSING_FIELD: reject_reason at {ref.path}:{ref.line_no}")

    broker_order_id = _get_value(payload, ["broker_order_id", "brokerOrderId", "order_no", "orderNo", "order_id", "orderId"])
    ts_submitted = _get_value(payload, ["ts_submitted", "submitted_at", "submit_time", "submitTime"])
    ts_last_update = _get_value(payload, ["ts_last_update", "update_time", "updateTime", "last_update", "lastUpdate"])

    return {
        "run_id": run_id,
        "as_of": as_of,
        "ts_created": str(ts_created),
        "cl_order_id": str(cl_order_id),
        "symbol": str(symbol),
        "side": side,
        "order_type": order_type,
        "time_in_force": time_in_force,
        "qty": int(qty),
        "filled_qty": int(filled_qty),
        "status": status,
        "strategy_id": str(strategy_id),
        "broker_order_id": broker_order_id,
        "limit_price": limit_price,
        "reject_reason": reject_reason,
        "ts_submitted": (str(ts_submitted) if ts_submitted else None),
        "ts_last_update": (str(ts_last_update) if ts_last_update else None),
    }


def _map_trade_record(
    obj: Dict[str, Any],
    ref: SourceRef,
    *,
    run_id: str,
    as_of: str,
) -> Dict[str, Any]:
    payload = obj["payload"]
    ts_event = str(obj.get("ts_event"))

    cl_order_id = _require_str(
        payload,
        ["cl_order_id", "clOrderId", "client_order_id", "clientOrderId", "order_no", "orderNo", "order_id", "orderId"],
        field="cl_order_id",
        ref=ref,
    )
    trade_id = _get_value(payload, ["trade_id", "tradeId", "deal_id", "dealId", "match_id", "matchId", "filled_id", "filledId", "id"])
    if trade_id is None:
        trade_id = obj.get("dedup_key")
    if not trade_id:
        raise DataError(f"MISSING_FIELD: trade_id at {ref.path}:{ref.line_no}")

    symbol = _require_str(payload, ["symbol", "stock_no", "stockNo", "stock_id", "stockId", "code"], field="symbol", ref=ref)
    side_raw = _require_str(payload, ["side", "action", "bs", "bs_action", "bsAction", "buy_sell"], field="side", ref=ref)
    side = _map_side(side_raw, ref=ref)
    price = _require_float(payload, ["price", "trade_price", "match_price", "deal_price", "filled_price"], field="price", ref=ref)
    if price <= 0:
        raise DataError(f"INVALID_FIELD: price at {ref.path}:{ref.line_no}")
    qty = _require_int(payload, ["qty", "quantity", "trade_qty", "match_qty", "deal_qty", "filled_qty", "fill_qty"], field="qty", ref=ref)
    if qty <= 0:
        raise DataError(f"INVALID_FIELD: qty at {ref.path}:{ref.line_no}")
    commission = _require_float(payload, ["commission", "fee", "fee_amount", "commission_fee"], field="commission", ref=ref)
    if commission < 0:
        raise DataError(f"INVALID_FIELD: commission at {ref.path}:{ref.line_no}")
    tax = _require_float(payload, ["tax", "transaction_tax", "stamp_tax"], field="tax", ref=ref)
    if tax < 0:
        raise DataError(f"INVALID_FIELD: tax at {ref.path}:{ref.line_no}")

    ts_filled = _get_value(payload, ["ts_filled", "filled_time", "trade_time", "match_time", "time"])
    if not ts_filled:
        ts_filled = ts_event

    broker_trade_id = _get_value(payload, ["broker_trade_id", "brokerTradeId", "trade_no", "tradeNo", "deal_no", "dealNo"])

    return {
        "run_id": run_id,
        "as_of": as_of,
        "trade_id": str(trade_id),
        "cl_order_id": str(cl_order_id),
        "ts_filled": str(ts_filled),
        "symbol": str(symbol),
        "side": side,
        "price": float(price),
        "qty": int(qty),
        "commission": float(commission),
        "tax": float(tax),
        "broker_trade_id": broker_trade_id,
    }


def _write_csv(path: Path, columns: Tuple[str, ...], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconcile Fubon bronze NDJSON to Phase-3 orders/trades CSV.")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--run-id", required=True, help="Run id for output rows")
    p.add_argument("--out-dir", required=True, help="Output directory for orders.csv/trades.csv")
    p.add_argument("--src-root", default="datahub/bronze/fubon", help="Bronze root directory")
    p.add_argument("--force", action="store_true", help="Overwrite out-dir contents")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        dates = _iter_dates(args.start, args.end)
    except Exception as exc:
        print(f"DATE_RANGE_INVALID: {exc}")
        return EXIT_CONFIG

    out_dir = Path(args.out_dir)
    try:
        _ensure_out_dir(out_dir, force=bool(args.force))
    except DataError as exc:
        print(str(exc))
        return EXIT_DATA
    except Exception as exc:
        print(f"OUTDIR_FAIL: {exc}")
        return EXIT_IO

    src_root = Path(args.src_root)

    try:
        order_items = _collect_records(src_root, dates, "orders")
        trade_items = _collect_records(src_root, dates, "trades")
    except DataError as exc:
        print(str(exc))
        return EXIT_DATA
    except Exception as exc:
        print(f"READ_FAIL: {exc}")
        return EXIT_IO

    orders: List[Dict[str, Any]] = []
    trades: List[Dict[str, Any]] = []

    try:
        for obj, ref in order_items:
            orders.append(_map_order_record(obj, ref, run_id=args.run_id, as_of=ref.date))
        for obj, ref in trade_items:
            trades.append(_map_trade_record(obj, ref, run_id=args.run_id, as_of=ref.date))
    except DataError as exc:
        print(str(exc))
        return EXIT_DATA

    orders.sort(key=lambda r: (str(r.get("ts_created", "")), str(r.get("cl_order_id", ""))))
    trades.sort(key=lambda r: (str(r.get("ts_filled", "")), str(r.get("trade_id", ""))))

    order_ids = {o["cl_order_id"] for o in orders}
    for t in trades:
        if t["cl_order_id"] not in order_ids and len(trades) > 0:
            print(f"ORPHAN_TRADE: cl_order_id={t['cl_order_id']}")
            return EXIT_DATA

    orders_cols = exec_schemas.ORDERS.required_cols + exec_schemas.ORDERS.optional_cols
    trades_cols = exec_schemas.TRADES.required_cols + exec_schemas.TRADES.optional_cols

    _write_csv(out_dir / "orders.csv", orders_cols, orders)
    _write_csv(out_dir / "trades.csv", trades_cols, trades)

    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
