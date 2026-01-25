from alpha_core.phase4.preflight_gate import build_preflight_gate


def test_preflight_gate_pass() -> None:
    gate = build_preflight_gate(
        exec_trade_count=10,
        symbol_coverage=0.8,
        missing_symbols_count=1,
        min_trade_count=5,
        min_symbol_coverage=0.6,
    )
    assert gate["status"] == "pass"
    assert gate["pass"] is True


def test_preflight_gate_insufficient() -> None:
    gate = build_preflight_gate(
        exec_trade_count=1,
        symbol_coverage=0.2,
        missing_symbols_count=5,
        min_trade_count=5,
        min_symbol_coverage=0.6,
    )
    assert gate["status"] == "insufficient_data"
    assert gate["pass"] is False


def test_preflight_gate_input_not_found() -> None:
    gate = build_preflight_gate(
        exec_trade_count=None,
        symbol_coverage=None,
        missing_symbols_count=None,
        min_trade_count=5,
        min_symbol_coverage=0.6,
        exec_error="exec_trades_not_found",
    )
    assert gate["status"] == "input_not_found"
    assert gate["pass"] is False


def test_preflight_gate_schema_validation_failed() -> None:
    gate = build_preflight_gate(
        exec_trade_count=1,
        symbol_coverage=0.2,
        missing_symbols_count=5,
        min_trade_count=5,
        min_symbol_coverage=0.6,
        exec_error="exec_trades_missing_symbol_column",
    )
    assert gate["status"] == "schema_validation_failed"
    assert gate["pass"] is False
