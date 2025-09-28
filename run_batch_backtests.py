import os, runpy
root = os.path.dirname(__file__)
script = os.path.join(root, "scripts", "run_batch_backtests.py")
if not os.path.exists(script):
    raise SystemExit("Missing: scripts/run_batch_backtests.py")
runpy.run_path(script, run_name="__main__")
