# -*- coding: utf-8 -*-
import argparse, os, sys, time, logging
from _finmind_common import fetch_dataset

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="2330,2317")
    ap.add_argument("--interval", default="5s")  # polling interval
    ap.add_argument("--iterations", type=int, default=10)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    secs = int(args.interval.rstrip("s"))
    syms = [x.strip() for x in args.symbols.split(",") if x.strip()]

    for i in range(args.iterations):
        for sym in syms:
            try:
                # Example: minute price dataset
                df = fetch_dataset("TaiwanStockPriceMinute", "2025-01-01", "2025-12-31", data_id=sym)
                if not df.empty:
                    row = df.iloc[-1]
                    print(f"[{sym}] {row.to_dict()}")
            except Exception as ex:
                logging.warning("Intraday fetch failed %s → %s", sym, ex)
        time.sleep(secs)

if __name__ == "__main__":
    main()
