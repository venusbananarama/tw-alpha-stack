# --- injected guard: run_path self-skip & safe str() ---
# --- injected guard: args fallback (v2) ---
import sys as _sys
try:
    args  # noqa: F821
except NameError:
    try:
        import argparse as _argparse
        _ap = _argparse.ArgumentParser(add_help=False)
        _ap.add_argument('--dir'); _ap.add_argument('--export'); _ap.add_argument('--root')
        _ap.add_argument('--config'); _ap.add_argument('--pattern')
        _ap.add_argument('--runs', nargs='*'); _ap.add_argument('--limit', type=int)
        _ap.add_argument('--workers', type=int); _ap.add_argument('--dryrun', action='store_true')
        _ap.add_argument('--verbose', action='store_true')
        args = _ap.parse_known_args(_sys.argv[1:])[0]
    except Exception:
        from types import SimpleNamespace as _NS
        args = _NS(dir='runs/wf_configs', export='reports', root='.', config=None, pattern=None, runs=None, limit=None, workers=None, dryrun=False, verbose=False)
globals()['args'] = args
# --- /injected guard: args fallback (v2) ---
# --- injected guard: results fallback + atexit exporter (v5) ---
from pathlib import Path as _P
import atexit as _atexit, json as _json, os as _os, time as _time

try:
    results  # noqa: F821
except NameError:
    results = None

def _ac_get_results():
    global results
    if results is None:
        results = {"runs": [], "meta": {"mode": "core-fallback", "note": "auto-filled due to missing results"}}
    return results

def _ac_export_path():
    # write ONLY to _runner_results.json to avoid clobbering gate_summary.json
    _default = _P.cwd() / "reports" / "_runner_results.json"
    _env = _os.getenv("AC_RUNNER_RESULTS", None)
    try:
        return _P(_env) if _env else _default
    except Exception:
        return _default

def _ac_export():
    try:
        _p = _ac_export_path()
        _p.parent.mkdir(parents=True, exist_ok=True)
        _payload = _ac_get_results()
        _meta = _payload.setdefault("meta", {})
        _meta.setdefault("exported_at", _time.strftime("%Y-%m-%dT%H:%M:%S"))
        if "mode" not in _meta:
            _meta["mode"] = "core-fallback"
        _p.write_text(_json.dumps(_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

_atexit.register(_ac_export)
# --- /injected guard ---
import runpy as _runpy
from pathlib import Path as _P
def _pstr(p):
    try:
        return str(p)
    except Exception:
        return p.as_posix() if hasattr(p,'as_posix') else repr(p)
_SELF = _P(__file__).resolve()
_orig_run_path = _runpy.run_path
def _guarded_run_path(p, *a, **kw):
    try:
        _pp = _P(p) if not hasattr(p,'resolve') else p
        if _pp.resolve() == _SELF:
            return None
        if not isinstance(p, (str, bytes)):
            p = _pstr(p)
    except Exception:
        try:
            p = _pstr(p)
        except Exception:
            pass
    return _orig_run_path(p, *a, **kw)
_runpy.run_path = _guarded_run_path
# --- /injected guard: run_path ---
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








