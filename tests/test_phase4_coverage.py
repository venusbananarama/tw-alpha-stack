from alpha_core.phase4.coverage import compute_symbol_coverage, symbols_payload


def test_coverage_empty_exec_symbols() -> None:
    coverage, missing = compute_symbol_coverage(set(), {"2330"})
    assert coverage == 0.0
    assert missing == set()


def test_coverage_partial_overlap() -> None:
    coverage, missing = compute_symbol_coverage({"2330", "2317"}, {"2330"})
    assert coverage == 0.5
    assert missing == {"2317"}


def test_symbols_payload_summary() -> None:
    payload = symbols_payload({"2330", "2317"})
    assert payload["count"] == 2
    assert payload["list"] == ["2317", "2330"]
    assert payload["hash"]
