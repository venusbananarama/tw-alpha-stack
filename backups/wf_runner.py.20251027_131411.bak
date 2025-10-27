import sys, runpy
from pathlib import Path

root = Path(__file__).resolve().parents[1]   # C:/AI/tw-alpha-stack
cands = [root / "scripts" / "wf_runner.py", Path("scripts") / "wf_runner.py"]
safe  = root / "scripts" / "wf_runner_safe.py"

def _try_run(p: Path) -> bool:
    try:
        sys.argv[0] = str(p)
        runpy.run_path(str(p), run_name="__main__")
        return True
    except (IndentationError, SyntaxError) as e:
        sys.stderr.write(f"[Bridge] {e.__class__.__name__} at {p}: {e}\n")
        return False

for p in cands:
    q = p if p.is_absolute() else (root / p)
    if q.exists() and _try_run(q):
        raise SystemExit(0)

# fallback
if safe.exists():
    sys.stderr.write("[Bridge] Falling back to wf_runner_safe.py\n")
    sys.argv[0] = str(safe)
    runpy.run_path(str(safe), run_name="__main__")
else:
    sys.stderr.write(f"[Bridge] SAFE runner missing: {safe}\n")
    sys.exit(3)
