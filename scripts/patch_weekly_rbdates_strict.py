#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
import re, sys

BLOCK = '''
    elif cfg.rebalance.upper() == "W":
        # Robust: last *available* trading day per weekly period
        rb_dates = df.groupby(df["date"].dt.to_period("W"))["date"].max().sort_values()
        try:
            rb_dates = rb_dates.astype("datetime64[ns]")
        except Exception:
            import pandas as pd
            rb_dates = pd.to_datetime(rb_dates)
        # Keep only those actually present in df["date"]
        import pandas as pd
        present = pd.Series(df["date"].unique())
        rb_dates = rb_dates[rb_dates.isin(present)]
'''

def patch_core(target: Path):
    s = target.read_text(encoding='utf-8')
    pat = r'elif\s+cfg\.rebalance\.upper\(\)\s*==\s*["\']W["\']:(?:.|\n)*?(\n\s*else:)'
    if not re.search(pat, s):
        print('[ERR] weekly branch not found'); sys.exit(2)
    s2 = re.sub(pat, BLOCK + r'\1', s)
    bak = target.with_suffix(f'.py.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    target.rename(bak)
    target.write_text(s2, encoding='utf-8')
    print(f'[OK] patched. Backup -> {bak.name}')

def main():
    if len(sys.argv)<2:
        print('Usage: patch_weekly_rbdates_strict.py backtest/core.py'); sys.exit(1)
    p = Path(sys.argv[1])
    if not p.exists():
        print(f'[ERR] not found: {p}'); sys.exit(2)
    patch_core(p)

if __name__=='__main__':
    main()
