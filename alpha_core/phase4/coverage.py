from __future__ import annotations

import hashlib
from typing import Dict, Set, Tuple


def compute_symbol_coverage(exec_symbols: Set[str], bronze_symbols: Set[str]) -> Tuple[float, Set[str]]:
    if not exec_symbols:
        return 0.0, set()
    missing = exec_symbols - bronze_symbols
    coverage = 1.0 - (len(missing) / max(len(exec_symbols), 1))
    return float(coverage), missing


def symbols_payload(symbols: Set[str], max_items: int = 20) -> Dict[str, object]:
    items = sorted(str(s) for s in symbols)
    joined = ",".join(items)
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest() if items else None
    return {
        "list": items[:max_items],
        "count": len(items),
        "hash": digest,
    }


_symbols_payload = symbols_payload
