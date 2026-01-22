import argparse
import json
import logging
import os
import re
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


TIME_ONLY_RE = re.compile(r"^\d{2}:\d{2}:\d{2}(\.\d+)?$")


def _parse_args():
    parser = argparse.ArgumentParser(description="Replay trades NDJSON into 1m bars.")
    parser.add_argument("--input", required=True, help="NDJSON input path.")
    parser.add_argument("--out", required=True, help="Output directory.")
    parser.add_argument("--tz", default="Asia/Taipei", help="Timezone for bars.")
    parser.add_argument("--bar", default="1m", help="Bar size (only 1m).")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        return pa, pq
    except Exception as exc:
        raise SystemExit(f"ERROR: pyarrow is required: {exc}")


def _resolve_tz(name):
    if ZoneInfo is None:
        raise SystemExit("ERROR: zoneinfo not available in this Python.")
    try:
        return ZoneInfo(name)
    except Exception as exc:
        raise SystemExit(f"ERROR: invalid tz '{name}': {exc}")


def _normalize_key_part(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value).strip()


def _parse_ingest_ts(value, tz):
    if not value:
        return None
    if isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.astimezone(tz)
    return None


def _from_epoch(value, tz):
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num > 1e14:
        seconds = num / 1e6
    elif num > 1e12:
        seconds = num / 1e3
    elif num > 1e10:
        seconds = num / 1e3
    elif num > 1e9:
        seconds = num
    else:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(tz)


def _parse_time_only(value):
    if "." in value:
        return datetime.strptime(value, "%H:%M:%S.%f").time()
    return datetime.strptime(value, "%H:%M:%S").time()


def _parse_event_time(value, ingest_ts, tz):
    ingest_dt = _parse_ingest_ts(ingest_ts, tz)
    if value is None:
        return ingest_dt
    if isinstance(value, (int, float)):
        dt = _from_epoch(value, tz)
        if dt:
            return dt
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return ingest_dt
        if s.isdigit():
            dt = _from_epoch(int(s), tz)
            if dt:
                return dt
        if TIME_ONLY_RE.match(s):
            if ingest_dt:
                time_only = _parse_time_only(s)
                return datetime.combine(ingest_dt.date(), time_only, tzinfo=ingest_dt.tzinfo)
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        try:
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            return dt.astimezone(tz)
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
            "%Y%m%d %H:%M:%S",
            "%Y%m%d%H%M%S",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                dt = dt.replace(tzinfo=tz)
                return dt
            except ValueError:
                continue
    return ingest_dt


def _parse_number(value):
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except (TypeError, ValueError):
        return None


def _iter_records(path):
    with open(path, "r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                yield line_no, None
                continue
            yield line_no, record


def _make_dedup_key(trade):
    symbol = _normalize_key_part(trade.get("symbol"))
    time_key = _normalize_key_part(trade.get("time"))
    serial_key = _normalize_key_part(trade.get("serial"))
    if not symbol or not time_key or not serial_key:
        return None
    return f"{symbol}|{time_key}|{serial_key}"


def _validate_record(record):
    required = ["ingest_ts", "source", "event", "symbol", "dedup_key", "data"]
    for key in required:
        if key not in record:
            return None, None, "missing_field"
    if record.get("source") != "fubon_neo":
        return None, None, "invalid_source"
    if record.get("event") != "trade":
        return None, None, "non_trade_event"
    data = record.get("data")
    if not isinstance(data, dict):
        return None, None, "data_not_dict"
    for key in ("symbol", "time", "serial", "price", "size"):
        if key not in data:
            return None, None, "missing_trade_field"
    symbol = _normalize_key_part(record.get("symbol"))
    data_symbol = _normalize_key_part(data.get("symbol"))
    if not symbol or not data_symbol or symbol != data_symbol:
        return None, None, "symbol_mismatch"
    dedup_key = record.get("dedup_key")
    expected = _make_dedup_key(data)
    if not dedup_key or not expected or dedup_key != expected:
        return None, None, "dedup_key_mismatch"
    return data, dedup_key, None


def _write_parquet(rows, path, pa, pq):
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _write_summary(path, summary):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=True, indent=2)


def main():
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.bar != "1m":
        raise SystemExit("ERROR: only --bar 1m is supported.")

    tz = _resolve_tz(args.tz)
    pa, pq = _require_pyarrow()
    os.makedirs(args.out, exist_ok=True)

    seen = set()
    bars = {}
    stats = {
        "lines": 0,
        "invalid_json": 0,
        "invalid_record": 0,
        "deduped": 0,
        "trades_kept": 0,
    }
    min_ts = None
    max_ts = None

    for line_no, record in _iter_records(args.input):
        if record is None:
            stats["lines"] += 1
            stats["invalid_json"] += 1
            logging.warning("skip line %s: invalid json", line_no)
            continue
        stats["lines"] += 1
        trade, dedup_key, reason = _validate_record(record)
        if trade is None:
            stats["invalid_record"] += 1
            logging.warning("skip line %s: %s", line_no, reason)
            continue
        if dedup_key in seen:
            stats["deduped"] += 1
            continue
        seen.add(dedup_key)

        event_ts = _parse_event_time(trade.get("time"), record.get("ingest_ts"), tz)
        if event_ts is None:
            stats["invalid_record"] += 1
            logging.warning("skip line %s: invalid time", line_no)
            continue
        minute_ts = event_ts.replace(second=0, microsecond=0)

        price = _parse_number(trade.get("price"))
        size = _parse_number(trade.get("size"))
        if price is None or size is None:
            stats["invalid_record"] += 1
            logging.warning("skip line %s: invalid price/size", line_no)
            continue

        symbol = _normalize_key_part(trade.get("symbol"))
        bar_key = (symbol, minute_ts)
        bar = bars.get(bar_key)
        if bar is None:
            bars[bar_key] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
                "trades": 1,
                "first_ts": event_ts,
                "last_ts": event_ts,
            }
        else:
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["volume"] += size
            bar["trades"] += 1
            if event_ts < bar["first_ts"]:
                bar["open"] = price
                bar["first_ts"] = event_ts
            if event_ts > bar["last_ts"]:
                bar["close"] = price
                bar["last_ts"] = event_ts
        stats["trades_kept"] += 1
        min_ts = event_ts if min_ts is None or event_ts < min_ts else min_ts
        max_ts = event_ts if max_ts is None or event_ts > max_ts else max_ts

    rows = []
    for (symbol, minute_ts), bar in bars.items():
        rows.append(
            {
                "ts": minute_ts.isoformat(),
                "symbol": symbol,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
                "trades": bar["trades"],
            }
        )

    rows.sort(key=lambda r: (r["ts"], r["symbol"]))

    parquet_path = os.path.join(args.out, "bars_1m.parquet")
    _write_parquet(rows, parquet_path, pa, pq)

    summary = {
        "input": args.input,
        "bars": len(rows),
        "timezone": args.tz,
        "start_ts": min_ts.isoformat() if min_ts else None,
        "end_ts": max_ts.isoformat() if max_ts else None,
        **stats,
    }
    summary_path = os.path.join(args.out, "summary.json")
    _write_summary(summary_path, summary)


if __name__ == "__main__":
    main()
