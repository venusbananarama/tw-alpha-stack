from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------
# sys.path bootstrap: allow "python scripts/exec/fubon_record_orders.py"
# ---------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.fubon import provider as fubon_provider  # noqa: E402


EXIT_OK = 0
EXIT_CONFIG = 60
EXIT_API_NOT_FOUND = 72
EXIT_EXCEPTION = 63


def _get_attr_or_key(obj: Any, keys: List[str]) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
        return None
    for k in keys:
        if hasattr(obj, k):
            v = getattr(obj, k)
            if v is not None:
                return v
    return None


def _summary_text(v: Any, *, limit: int = 160) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v)
    except Exception:
        return None
    s = s.replace("\r", " ").replace("\n", " ")
    lower = s.lower()
    if "token" in lower or "password" in lower or "secret" in lower:
        return "[REDACTED]"
    if len(s) > limit:
        s = s[:limit] + "..."
    return s


def _response_hint(resp: Any) -> Dict[str, Any]:
    hint: Dict[str, Any] = {"type": type(resp).__name__}
    flag = _get_attr_or_key(resp, ["is_success", "success", "ok"])
    if flag is not None:
        hint["is_success"] = flag
    hint["code"] = _summary_text(_get_attr_or_key(resp, ["code", "error_code", "status_code"]))
    hint["message"] = _summary_text(_get_attr_or_key(resp, ["message", "msg", "error", "err", "error_message"]))
    data = _get_attr_or_key(resp, ["data", "Data", "items", "Items"])
    if isinstance(data, list):
        hint["data_len"] = len(data)
    else:
        hint["data_len"] = 0 if data is None else 1
    return hint


def _is_response_failure(resp: Any) -> bool:
    flag = _get_attr_or_key(resp, ["is_success", "success", "ok"])
    if isinstance(flag, bool):
        return not flag
    if isinstance(flag, str) and flag.strip().lower() in {"false", "fail", "failed", "error", "no"}:
        return True
    return False


def _to_yyyymmdd(date_str: str) -> str:
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")


def _fallback_event_ts(date_str: str) -> str:
    return f"{date_str}T00:00:00"


def _write_ndjson(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for rec in records:
            handle.write(json.dumps(rec, ensure_ascii=True, sort_keys=True) + "\n")

def _safe_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_json(v) for v in value]
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
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _extract_event_ts(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for key in [
            "ts_event",
            "timestamp",
            "time",
            "order_time",
            "update_time",
            "last_update",
            "ts",
        ]:
            v = payload.get(key)
            if v:
                return str(v)
        data = payload.get("data")
        if isinstance(data, dict):
            return _extract_event_ts(data)
    return None


def _payload_items(resp: Any) -> List[Any]:
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        data = resp.get("data") or resp.get("Data") or resp.get("items") or resp.get("Items")
        if isinstance(data, list):
            return data
        if data is not None:
            return [data]
        return [resp]
    if hasattr(resp, "data"):
        try:
            data = getattr(resp, "data")
            if isinstance(data, list):
                return data
            if data is not None:
                return [data]
        except Exception:
            pass
    return [resp]


def _hash_payload(payload: Any) -> str:
    try:
        raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    except Exception:
        raw = json.dumps(_safe_json(payload), ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text.strip(), "%Y-%m-%d")


def _iter_dates(start: str, end: str) -> List[str]:
    d0 = _parse_date(start)
    d1 = _parse_date(end)
    if d1 < d0:
        raise ValueError("end < start")
    days = []
    cur = d0
    while cur <= d1:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def _resolve_dates(args: argparse.Namespace) -> List[str]:
    if not args.start or not args.end:
        raise ValueError("missing --start/--end")
    return _iter_dates(args.start, args.end)


def _open_output(out_root: str, date: str, run_id: str) -> Path:
    out_dir = Path(out_root) / "orders" / f"dt={date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"orders_{run_id}.ndjson"


def _call_with_variants(fn, account: Any, start: str, end: str) -> Any:
    variants: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((account, start, end), {}),
        ((account, start), {}),
        ((account,), {}),
        ((), {"account": account, "start_date": start, "end_date": end}),
        ((), {"account": account, "start": start, "end": end}),
        ((), {"account": account, "date": start}),
        ((), {"account": account}),
    ]
    last_type_error: Optional[TypeError] = None
    for args, kwargs in variants:
        try:
            return fn(*args, **kwargs)
        except TypeError as exc:
            last_type_error = exc
            continue
    if last_type_error is not None:
        raise last_type_error
    return fn(account)


def _invoke_order_api(stock: Any, account: Any, start: str, end: str) -> Any:
    fn = getattr(stock, "order_history", None)
    if not callable(fn):
        raise RuntimeError("FUBON_ORDER_API_NOT_FOUND: order_history")
    return _call_with_variants(fn, account, start, end)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Record Fubon orders to NDJSON (poll-only).")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--run-id", required=True, help="Output run id.")
    p.add_argument("--out-root", default="datahub/bronze/fubon", help="Output root directory.")
    p.add_argument("--account-no", default=None)
    p.add_argument("--account-index", type=int, default=None)
    return p.parse_args()


def _build_run_meta(args: argparse.Namespace, date: str) -> Dict[str, Any]:
    return {
        "date": date,
        "run_id": args.run_id,
    }


def _build_records(
    *,
    stock: Any,
    account: Any,
    date: str,
    run_meta: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    start_key = _to_yyyymmdd(date)
    end_key = start_key
    resp = _invoke_order_api(stock, account, start_key, end_key)
    if _is_response_failure(resp):
        return [], _response_hint(resp)

    items = _payload_items(resp)
    records: List[Dict[str, Any]] = []
    for item in items:
        payload = _safe_json(item)
        ts_event = _extract_event_ts(payload) or _fallback_event_ts(date)
        record = {
            "ts_event": ts_event,
            "source": "poll",
            "event": "order",
            "dedup_key": _hash_payload(payload),
            "payload": payload,
            "run_meta": run_meta,
        }
        records.append(record)

    records.sort(key=lambda r: (str(r.get("ts_event", "")), str(r.get("dedup_key", ""))))
    return records, None


def main() -> int:
    args = parse_args()

    try:
        dates = _resolve_dates(args)
    except Exception as exc:
        print(f"DATE_RANGE_INVALID: {exc}")
        return EXIT_CONFIG

    try:
        ctx = fubon_provider.connect(account_no=args.account_no, account_index=args.account_index)
    except Exception as exc:
        print(f"CONNECT_FAIL: {exc}")
        return EXIT_CONFIG

    sdk = ctx.sdk
    account = ctx.account
    stock = getattr(sdk, "stock", None)
    if stock is None:
        print("FUBON_STOCK_API_MISSING")
        fubon_provider.close(ctx)
        return EXIT_API_NOT_FOUND

    total = 0
    try:
        for date in dates:
            out_path = _open_output(args.out_root, date, args.run_id)
            run_meta = _build_run_meta(args, date)
            try:
                records, hint = _build_records(stock=stock, account=account, date=date, run_meta=run_meta)
                if hint is not None:
                    print("API_FAIL: " + json.dumps(hint, ensure_ascii=True, sort_keys=True))
                    return EXIT_EXCEPTION
                _write_ndjson(out_path, records)
                count = len(records)
                total += count
                print(f"OK date={date} count={count} out={out_path}")
            except Exception as exc:
                print(f"ORDER_HISTORY_FAIL date={date} error={exc}")
                return EXIT_EXCEPTION

        print(f"OK orders_written={total} dates={len(dates)}")
        return EXIT_OK
    finally:
        fubon_provider.close(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
