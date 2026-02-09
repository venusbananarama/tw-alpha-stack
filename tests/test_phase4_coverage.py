from alpha_core.phase4.coverage import compute_symbol_coverage, symbols_payload
from alpha_core.phase2.corelib.factor_slo_lib import (
    FactorSloConfig,
    evaluate_factor_slo,
    extract_passed_factors,
)


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


def test_factor_slo_passed_list_uses_factor_payload_and_ignores_unknown() -> None:
    wf_summary = {
        "overall": {"windows": [6, 12, 24]},
        "factors_by_status": {
            "passed": ["quality_roeq", "vol_60d", "polluted_factor"],
        },
        "factors": {
            "quality_roeq": {"windows": [6, 12, 24], "pass": True},
            "vol_60d": {"windows": [6, 12, 24], "pass": True},
            "value_pe": {"windows": [6, 12, 24], "pass": False},
        },
    }
    slo = FactorSloConfig(
        source="rules_factors.yaml",
        profile="test",
        engine="classic",
        min_factors=1,
        min_per_window=1,
        required_factors=["value_pe"],
        per_window_min={},
        raw_gate_ready=None,
    )

    result = evaluate_factor_slo(wf_summary, slo)

    assert result.total_factors == 2
    assert result.per_window_counts == {6: 2, 12: 2, 24: 2}
    assert result.missing_required_factors == ["value_pe"]
    assert result.satisfied is False


def test_extract_passed_factors_list_fallback_windows_when_factor_map_missing() -> None:
    wf_summary = {
        "overall": {"windows": [6, 12, 24]},
        "factors_by_status": {"passed": ["ghost_factor"]},
        "factors": {},
    }

    passed = extract_passed_factors(wf_summary)

    assert passed == {"ghost_factor": {"windows": [6, 12, 24]}}
