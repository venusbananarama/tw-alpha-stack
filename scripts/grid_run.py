# Wrapper: forward to scripts/grid_run.py using current interpreter
import runpy, os, sys
here = os.path.dirname(os.path.abspath(__file__))
target = os.path.join(here, "scripts", "grid_run.py")
if not os.path.exists(target):
    raise SystemExit("scripts/grid_run.py not found. Place it there and re-run.")
runpy.run_path(target, run_name="__main__")
