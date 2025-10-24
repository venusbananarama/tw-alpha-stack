# -*- coding: utf-8 -*-
from pathlib import Path
import sys, os, subprocess
ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "scripts" / "wf_runner_core.py"
SAFE = ROOT / "scripts" / "wf_runner_safe.py"

def _run_safe():
    if not SAFE.exists():
        sys.stderr.write("[bridge] wf_runner_safe.py not found.\n")
        sys.exit(2)
    py = os.environ.get("PY") or str((ROOT/".venv"/"Scripts"/"python.exe"))
    if not Path(py).exists():
        py = sys.executable
    cmd = [py, str(SAFE), *sys.argv[1:]]
    raise SystemExit(subprocess.call(cmd))

def main():
    if CORE.exists():
        try:
            src = CORE.read_text(encoding="utf-8")
            code = compile(src, str(CORE), "exec")
            g = {"__name__":"__main__", "__file__":str(CORE)}
            exec(code, g, None)
            return
        except Exception as e:
            sys.stderr.write(f"[bridge] falling back to safe runner: {type(e).__name__}: {e}\n")
            _run_safe()
    else:
        _run_safe()

if __name__ == "__main__":
    main()
