from __future__ import annotations

from typing import Dict, Optional


_SCHEMA_ERRORS = {
    "exec_trades_missing_symbol_column",
    "exec_trades_missing_ts_filled",
}


def _classify_error(exec_error: Optional[str], bronze_error: Optional[str]) -> Optional[str]:
    if exec_error:
        if exec_error in _SCHEMA_ERRORS or exec_error.startswith("exec_trades_read_error"):
            return "schema_validation_failed"
        return "input_not_found"
    if bronze_error:
        return "input_not_found"
    return None


def build_preflight_gate(
    *,
    exec_trade_count: Optional[int],
    symbol_coverage: Optional[float],
    missing_symbols_count: Optional[int],
    min_trade_count: int,
    min_symbol_coverage: float,
    exec_error: Optional[str] = None,
    bronze_error: Optional[str] = None,
) -> Dict[str, object]:
    error_status = _classify_error(exec_error, bronze_error)
    metrics = {
        "exec_trade_count": exec_trade_count,
        "symbol_coverage": symbol_coverage,
        "missing_symbols_count": missing_symbols_count,
    }
    thresholds = {
        "min_trade_count": int(min_trade_count),
        "min_symbol_coverage": float(min_symbol_coverage),
    }
    if error_status:
        return {
            "status": error_status,
            "pass": False,
            "metrics": metrics,
            "thresholds": thresholds,
        }

    if exec_trade_count is None or symbol_coverage is None:
        status = "insufficient_data"
        passed = False
    elif exec_trade_count < min_trade_count or symbol_coverage < min_symbol_coverage:
        status = "insufficient_data"
        passed = False
    else:
        status = "pass"
        passed = True

    return {
        "status": status,
        "pass": passed,
        "metrics": metrics,
        "thresholds": thresholds,
    }
