#!/usr/bin/env python3
import argparse
from pathlib import Path
from collections import deque

def tail_file(p: Path, n: int = 120) -> str:
    try:
        with p.open('r', encoding='utf-8', errors='ignore') as f:
            dq = deque(f, maxlen=n)
        return ''.join(dq)
    except Exception as e:
        return f'[ERR] cannot read {p}: {e}\n'

def main():
    ap = argparse.ArgumentParser(description='Tail fail logs under outdir')
    ap.add_argument('--outdir', required=True, help='Grid outdir')
    ap.add_argument('--lines', type=int, default=200)
    args = ap.parse_args()
    base = Path(args.outdir)
    if not base.exists():
        print(f'[ERR] not found: {base}'); return
    found = False
    for sub in sorted(base.iterdir()):
        if not sub.is_dir(): continue
        failed = sub / 'run_failed.log'
        stderr = sub / 'run_stderr.log'
        if failed.exists():
            found = True
            print('='*90)
            print(f'[FAIL] {sub.name} -> run_failed.log')
            print(tail_file(failed, args.lines))
        elif stderr.exists() and stderr.stat().st_size>0:
            found = True
            print('='*90)
            print(f'[WARN] {sub.name} -> run_stderr.log')
            print(tail_file(stderr, args.lines))
    if not found:
        print('[OK] No fail logs detected.')
if __name__ == '__main__':
    main()
