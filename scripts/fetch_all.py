from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from collections import defaultdict

from twalpha.data.symbols_tw import get_all_symbols
from twalpha.data.downloader_bulk import download_and_save as yahoo_download_all
from twalpha.data.downloader_twse import download_official

def _calc_symbol_start(sym: str, default_start: str, out_dir: str) -> str:
    f = Path(out_dir) / f"{sym.replace('.','_')}.csv"
    if not f.exists():
        return default_start
    try:
        last = pd.read_csv(f, usecols=["date"]).tail(1)["date"].iloc[0]
        return (pd.to_datetime(last) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return default_start

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["yahoo","twse"], default="yahoo", help="yahoo=快速批量 / twse=官方精準(目前占位)")
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--out", default="data")
    ap.add_argument("--no-tpex", action="store_true", help="只抓上市（跳過上櫃）")
    ap.add_argument("--update", action="store_true", help="增量更新：只補既有CSV的尾端")
    ap.add_argument("--batch-size", type=int, default=50, help="yfinance 批次大小")
    ap.add_argument("--pause", type=float, default=2.0, help="批次間延遲秒數")
    args = ap.parse_args()

    df = get_all_symbols(include_tpex=not args.no_tpex)
    uni_path = Path("configs/universe.tw_all.txt")
    uni_path.write_text("\n".join(df["symbol"].tolist()), encoding="utf-8")
    print(f"Universe saved: {uni_path} ({len(df)} symbols)")

    syms = df["symbol"].tolist()

    if args.mode == "yahoo":
        if args.update:
            buckets = defaultdict(list)
            for s in syms:
                start_s = _calc_symbol_start(s, args.start, args.out)
                buckets[start_s].append(s)
            for start_s, group_syms in buckets.items():
                yahoo_download_all(group_syms, out_dir=args.out, start=start_s,
                                   batch_size=args.batch_size, pause=args.pause)
        else:
            yahoo_download_all(syms, out_dir=args.out, start=args.start,
                               batch_size=args.batch_size, pause=args.pause)
    else:
        out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
        for i, sym in enumerate(syms, 1):
            start_s = _calc_symbol_start(sym, args.start, args.out) if args.update else args.start
            try:
                dfx = download_official(sym, start=start_s)
                if dfx.empty:
                    print(f"[skip] {sym} 無官方資料（占位下載器）")
                    continue
                out_path = out_dir / f"{sym.replace('.','_')}.csv"
                dfx.to_csv(out_path, index=False)
                print(f"[{i}/{len(syms)}] saved {sym} -> {out_path}")
            except Exception as e:
                print(f"[error] {sym}: {e}")

if __name__ == "__main__":
    main()
