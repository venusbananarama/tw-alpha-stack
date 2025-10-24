import sys, runpy
from pathlib import Path

# Bridge: locate real script from a candidate list, then run it as __main__
CANDIDATES = ['C:/AI/tw-alpha-stack/tools/scripts/finmind_backfill.py', 'C:/AI/tw-alpha-stack/scripts/finmind_backfill.py', 'scripts/finmind_backfill.py', 'tools/scripts/finmind_backfill.py']
def _norm(p): return str(p).replace("\\\\","/")

here = Path(__file__).resolve()
checked, target = [], None

# 1) absolute paths first
for s in CANDIDATES:
    p = Path(s)
    if p.is_absolute():
        if p.exists():
            target = p; break
        checked.append(_norm(p))

# 2) locate repo root (dir that contains "scripts")
if target is None:
    repo, cur = None, here.parent
    for _ in range(6):
        if (cur / "scripts").is_dir():
            repo = cur; break
        cur = cur.parent
    if repo is None:
        # fallback: try some higher parents
        for k in (3,2,1):
            if len(here.parents) > k:
                cand = here.parents[k]
                if (cand / "scripts").is_dir():
                    repo = cand; break
    if repo is None:
        sys.stderr.write("[Bridge] 無法定位 repo root from: %s\n" % _norm(here)); sys.exit(2)

    for s in CANDIDATES:
        p = Path(s)
        if not p.is_absolute():
            p = (repo / s)
        if p.exists():
            target = p; break
        checked.append(_norm(p))

if target is None:
    sys.stderr.write("[Bridge] 目標腳本不存在；已檢查：\n" + "".join("  - %s\n" % c for c in checked))
    sys.exit(3)

# 3) delegate
sys.argv = [str(target)] + sys.argv[1:]
runpy.run_path(str(target), run_name="__main__")
