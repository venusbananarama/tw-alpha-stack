import argparse
import json
import logging
import os


def _parse_args():
    parser = argparse.ArgumentParser(description="Generate a mock execution ledger from bars.")
    parser.add_argument("--bars", required=True, help="Input bars file (parquet).")
    parser.add_argument("--out", required=True, help="Output directory.")
    parser.add_argument("--side", default="buy", help="Side: buy or sell.")
    parser.add_argument("--qty", type=int, required=True, help="Quantity per bar.")
    parser.add_argument("--fee", type=float, default=0.0, help="Fee per trade.")
    parser.add_argument("--slippage", type=float, default=0.0, help="Slippage per trade.")
    parser.add_argument("--venue", default="Fubon", help="Venue label.")
    parser.add_argument("--remark", default="mock close", help="Remark text.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        return pa, pq
    except Exception as exc:
        raise SystemExit(f"ERROR: pyarrow is required: {exc}")


def _parse_number(value):
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_bars_parquet(path, pq):
    table = pq.read_table(path)
    return table.to_pylist()


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
    os.makedirs(args.out, exist_ok=True)

    side = args.side.lower().strip()
    if side not in {"buy", "sell"}:
        raise SystemExit("ERROR: --side must be buy or sell.")
    if args.qty <= 0:
        raise SystemExit("ERROR: --qty must be positive.")

    pa, pq = _require_pyarrow()
    bars = _read_bars_parquet(args.bars, pq)

    rows = []
    total_notional = 0.0
    total_qty = 0
    for bar in bars:
        ts = bar.get("ts")
        symbol = bar.get("symbol")
        price = _parse_number(bar.get("close"))
        if price is None:
            logging.warning("skip bar with invalid close: %s", bar)
            continue
        qty = args.qty
        notional = qty * price
        fee = args.fee
        slippage = args.slippage
        if side == "buy":
            net_cashflow = -(notional + fee + slippage)
        else:
            net_cashflow = notional - fee - slippage
        rows.append(
            {
                "ts": ts,
                "symbol": symbol,
                "side": side.upper(),
                "qty": qty,
                "price": price,
                "notional": notional,
                "fee": fee,
                "slippage": slippage,
                "net_cashflow": net_cashflow,
                "venue": args.venue,
                "remark": args.remark,
            }
        )
        total_notional += notional
        total_qty += qty

    parquet_path = os.path.join(args.out, "ledger.parquet")
    _write_parquet(rows, parquet_path, pa, pq)
    summary = {
        "bars": len(bars),
        "ledger_rows": len(rows),
        "side": side,
        "total_qty": total_qty,
        "total_notional": total_notional,
    }
    summary_path = os.path.join(args.out, "summary.json")
    _write_summary(summary_path, summary)


if __name__ == "__main__":
    main()
