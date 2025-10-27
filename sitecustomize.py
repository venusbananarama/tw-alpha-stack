# --- injected by AC: builtins.args fallback (global) ---
import sys as _sys, builtins as _bi
try:
    getattr(_bi, 'args')
except Exception:
    try:
        import argparse as _ap
        _apb = _ap.ArgumentParser(add_help=False)
        _apb.add_argument('--dir')
        _apb.add_argument('--export')
        _apb.add_argument('--root')
        _apb.add_argument('--config')
        _apb.add_argument('--pattern')
        _apb.add_argument('--runs', nargs='*')
        _apb.add_argument('--limit', type=int)
        _apb.add_argument('--workers', type=int)
        _apb.add_argument('--dryrun', action='store_true')
        _apb.add_argument('--verbose', action='store_true')
        _ns = _apb.parse_known_args(_sys.argv[1:])[0]
    except Exception:
        from types import SimpleNamespace as _NS
        _ns = _NS(dir='runs/wf_configs', export='reports', root='.', config=None, pattern=None, runs=None, limit=None, workers=None, dryrun=False, verbose=False)
    setattr(_bi, 'args', _ns)
# --- /injected by AC ---
