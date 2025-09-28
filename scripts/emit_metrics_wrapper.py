# -*- coding: utf-8 -*-
"""emit_metrics_wrapper.py — fix6.2
- Forces unbuffered Python (-u) and PYTHONUNBUFFERED=1
- Streams child stdout/stderr to parent
Usage: python emit_metrics_wrapper.py <script.py> <args...>
"""
import os, sys, subprocess

def main():
    if len(sys.argv) < 2:
        print("usage: emit_metrics_wrapper.py <script.py> <args...>", file=sys.stderr)
        sys.exit(2)
    script = sys.argv[1]; args = sys.argv[2:]
    env = os.environ.copy(); env['PYTHONUNBUFFERED'] = '1'
    cmd = [sys.executable, '-u', script] + args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, bufsize=1)
    try:
        for line in proc.stdout: print(line, end='')
    finally:
        proc.wait()
    sys.exit(proc.returncode)

if __name__ == '__main__':
    main()
