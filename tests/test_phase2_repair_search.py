from __future__ import annotations

import csv
import json
from pathlib import Path

import alpha_core.phase2.repair as repair_mod
from alpha_core.phase2.repair.models import VARIANT_SORT_POLICY, RepairAttempt, VariantSpec
from alpha_core.phase2.repair.promote import promote_selected_variant
from alpha_core.phase2.repair.search import (
    build_candidates,
    early_stop_check,
    find_bottleneck_window_from_factor_summary,
    load_repair_profile,
    resolve_bottleneck_window,
    select_best,
)


def test_build_candidates_and_early_stop(tmp_path: Path) -> None:
    reason_root = tmp_path / "by_reason"
    reason_root.mkdir(parents=True, exist_ok=True)
    (reason_root / "rank_ic_min_threshold.yaml").write_text(
        "\n".join(
            [
                "reason: rank_ic_min_threshold",
                "variants:",
                "  - id: zzz_low_pr",
                "    priority: 10",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 1",
                "  - id: bbb_high_len2",
                "    priority: 50",
                "    transforms:",
                "      - name: rank",
                "        params:",
                "          pct: true",
                "      - name: zscore",
                "        params:",
                "          ddof: 0",
                "  - id: aaa_low_len1",
                "    priority: 10",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
                "  - id: aaa_same_len",
                "    priority: 50",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
                "  - id: bbb_same_len",
                "    priority: 50",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
            ]
        ),
        encoding="utf-8",
    )

    cands = build_candidates(
        factor_id="mom_6m",
        fail_reasons=["rank_ic_min_threshold"],
        reason_config_root=reason_root,
        max_attempts=5,
    )

    assert len(cands) == 5
    assert cands[0].variant_id.endswith("__aaa_same_len")
    assert cands[1].variant_id.endswith("__bbb_same_len")
    assert cands[2].variant_id.endswith("__bbb_high_len2")
    assert cands[3].variant_id.endswith("__aaa_low_len1")
    assert cands[4].variant_id.endswith("__zzz_low_pr")
    assert cands[0].to_dict().get("variant_priority") == 50

    should_stop = early_stop_check(
        metrics={"windows": {"6": {"rank_ic": 0.01, "coverage": 0.95}}},
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
        window=6,
    )
    assert should_stop is True


def test_select_best_prefers_passing_attempt() -> None:
    v1 = VariantSpec(variant_id="a", factor_id="f", reason="rank_ic_min_threshold", transforms=[])
    v2 = VariantSpec(variant_id="b", factor_id="f", reason="rank_ic_min_threshold", transforms=[{"name": "lag"}])

    a1 = RepairAttempt(
        attempt_id="attempt_001",
        variant=v1,
        bottleneck_window=6,
        early_stopped=False,
        passed=False,
        metrics={"windows": {"6": {"rank_ic": 0.01, "coverage": 0.95}}},
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )
    a2 = RepairAttempt(
        attempt_id="attempt_002",
        variant=v2,
        bottleneck_window=6,
        early_stopped=False,
        passed=True,
        metrics={"windows": {"6": {"rank_ic": 0.06, "coverage": 0.95}}},
        thresholds={"min_rank_ic": 0.03, "min_coverage": 0.9},
    )

    decision = select_best([a1, a2])
    assert decision.passed is True
    assert decision.selected_variant_id == "b"


def test_load_repair_profile_allowlist_semantics(tmp_path: Path) -> None:
    cfg = tmp_path / "repair_profile.yaml"
    cfg.write_text(
        "\n".join(
            [
                "auto_repair:",
                "  default:",
                "    enabled: true",
                "    reason_allowlist:",
                "      - rank_ic_min_threshold",
                "  profiles:",
                "    missing: {}",
                "    null_case:",
                "      reason_allowlist: null",
                "    deny_all:",
                "      reason_allowlist: []",
                "    allow_all:",
                "      reason_allowlist: ['*']",
                "    explicit:",
                "      reason_allowlist:",
                "        - coverage_min_threshold",
            ]
        ),
        encoding="utf-8",
    )

    missing = load_repair_profile(cfg, "missing")
    assert missing["allowlist_mode"] == "default"
    assert missing["resolved_reason_allowlist"] == ["rank_ic_min_threshold"]

    null_case = load_repair_profile(cfg, "null_case")
    assert null_case["allowlist_mode"] == "default"
    assert null_case["resolved_reason_allowlist"] == ["rank_ic_min_threshold"]

    deny_all = load_repair_profile(cfg, "deny_all")
    assert deny_all["allowlist_mode"] == "deny_all"
    assert deny_all["resolved_reason_allowlist"] == []
    assert deny_all["allow_all_reasons"] is False

    allow_all = load_repair_profile(cfg, "allow_all")
    assert allow_all["allowlist_mode"] == "allow_all"
    assert allow_all["allow_all_reasons"] is True
    assert allow_all["resolved_reason_allowlist"] == ["*"]

    explicit = load_repair_profile(cfg, "explicit")
    assert explicit["allowlist_mode"] == "explicit"
    assert explicit["allow_all_reasons"] is False
    assert explicit["resolved_reason_allowlist"] == ["coverage_min_threshold"]


def test_expand_variants_uses_per_factor_budget(tmp_path: Path) -> None:
    reason_root = tmp_path / "by_reason"
    reason_root.mkdir(parents=True, exist_ok=True)
    (reason_root / "rank_ic_min_threshold.yaml").write_text(
        "\n".join(
            [
                "reason: rank_ic_min_threshold",
                "variants:",
                "  - id: v1",
                "    priority: 10",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
                "  - id: v2",
                "    priority: 20",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 1",
                "  - id: v3",
                "    priority: 30",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 2",
            ]
        ),
        encoding="utf-8",
    )

    expanded = repair_mod._expand_variants(  # type: ignore[attr-defined]
        fail_reason_map={
            "mom_6m": ["rank_ic_min_threshold"],
            "value_pe": ["rank_ic_min_threshold"],
        },
        allowlist=["rank_ic_min_threshold"],
        allow_all_reasons=False,
        reason_config_root=reason_root,
        default_transforms=[],
        max_attempts_per_factor=2,
    )

    assert len(expanded) == 4
    by_factor = {}
    for item in expanded:
        by_factor[item.factor_id] = by_factor.get(item.factor_id, 0) + 1
    assert by_factor["mom_6m"] == 2
    assert by_factor["value_pe"] == 2


def test_bottleneck_window_from_factor_summary_then_fallback(tmp_path: Path) -> None:
    root = tmp_path
    factor_eval_dir = root / "reports" / "factor_eval"
    factor_eval_dir.mkdir(parents=True, exist_ok=True)
    summary_path = factor_eval_dir / "mom_6m_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "windows": {
                    "6": {"rank_ic": 0.03},
                    "12": {"rank_ic": 0.01},
                    "24": {"rank_ic": 0.02},
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    fail_csv = root / "reports" / "fail_results.csv"
    fail_csv.parent.mkdir(parents=True, exist_ok=True)
    with fail_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["factor_id", "window", "pass", "reason"])
        writer.writeheader()
        writer.writerow(
            {
                "factor_id": "mom_6m",
                "window": "24",
                "pass": "False",
                "reason": "rank_ic_min_threshold",
            }
        )

    from_summary = find_bottleneck_window_from_factor_summary(root, "mom_6m")
    assert from_summary == 12

    resolved = resolve_bottleneck_window(
        root=root,
        fail_results_path=fail_csv,
        factor_id="mom_6m",
        reason_key="rank_ic_min_threshold",
        fallback_windows=[6, 12, 24],
    )
    assert resolved == 12

    fallback_only = resolve_bottleneck_window(
        root=root,
        fail_results_path=fail_csv,
        factor_id="value_pe",
        reason_key="rank_ic_min_threshold",
        fallback_windows=[6, 12, 24],
    )
    assert fallback_only == 6


def test_promotion_mode_false_like_normalized_to_off() -> None:
    false_like = [False, 0, "0", "no", "off", "disabled", "none", "", "false"]
    for mode in false_like:
        out = promote_selected_variant(mode=mode, variant_id="mom_6m__v1")
        assert out.mode == "off"


def test_repair_plan_is_ssot_and_attempt_order_matches(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    as_of = "2026-02-07"
    run_id = "p2.test"

    repair_cfg = root / "configs" / "p2" / "repair"
    xform_cfg = root / "configs" / "p2" / "xforms" / "by_reason"
    repair_cfg.mkdir(parents=True, exist_ok=True)
    xform_cfg.mkdir(parents=True, exist_ok=True)

    (repair_cfg / "repair_profile.yaml").write_text(
        "\n".join(
            [
                "auto_repair:",
                "  default:",
                "    enabled: true",
                "    max_attempts_per_factor: 2",
                "    stop_fast: true",
                "    promotion_mode: 'off'",
                "    reason_allowlist:",
                "      - rank_ic_min_threshold",
                "  profiles:",
                "    test:",
                "      enabled: true",
                "      max_attempts_per_factor: 2",
                "      stop_fast: true",
                "      promotion_mode: 'off'",
                "      reason_allowlist:",
                "        - rank_ic_min_threshold",
            ]
        ),
        encoding="utf-8",
    )
    (root / "configs" / "p2" / "xforms" / "default.yaml").parent.mkdir(parents=True, exist_ok=True)
    (root / "configs" / "p2" / "xforms" / "default.yaml").write_text(
        "schema: phase2_xforms.v1\ntransforms: []\n",
        encoding="utf-8",
    )
    (xform_cfg / "rank_ic_min_threshold.yaml").write_text(
        "\n".join(
            [
                "reason: rank_ic_min_threshold",
                "variants:",
                "  - id: v_hi_1",
                "    priority: 90",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
                "  - id: v_hi_2",
                "    priority: 80",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 1",
                "  - id: v_lo",
                "    priority: 10",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 2",
            ]
        ),
        encoding="utf-8",
    )

    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    gate_summary_path = reports / "gate_summary.json"
    gate_summary_path.write_text(
        json.dumps(
            {
                "checks": [
                    {
                        "factor_id": "mom_6m",
                        "reasons": ["rank_ic_min_threshold"],
                        "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
                    },
                    {
                        "factor_id": "value_pe",
                        "reasons": ["rank_ic_min_threshold"],
                        "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    wf_summary_path = reports / "wf_summary.json"
    wf_summary_path.write_text("{}", encoding="utf-8")
    fail_results_path = reports / "fail_results.csv"
    with fail_results_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["factor_id", "window", "pass", "reason"])
        writer.writeheader()

    factor_eval_dir = reports / "factor_eval"
    factor_eval_dir.mkdir(parents=True, exist_ok=True)
    (factor_eval_dir / "mom_6m_summary.json").write_text(
        json.dumps({"windows": {"6": {"rank_ic": 0.02}, "12": {"rank_ic": 0.01}}}),
        encoding="utf-8",
    )
    (factor_eval_dir / "value_pe_summary.json").write_text(
        json.dumps({"windows": {"6": {"rank_ic": 0.03}, "12": {"rank_ic": 0.02}}}),
        encoding="utf-8",
    )

    class DummyEvalResult:
        def __init__(self) -> None:
            self.passed = False
            self.early_stopped = True
            self.windows = {"6": {"rank_ic": 0.0, "coverage": 0.0}}

    class DummyAdapter:
        def __init__(self, *, root: Path, as_of: str, windows):  # type: ignore[no-untyped-def]
            self.root = root
            self.as_of = as_of
            self.windows = windows

        def evaluate_variant(self, **kwargs):  # type: ignore[no-untyped-def]
            return DummyEvalResult()

        @staticmethod
        def summarize_metrics(result):  # type: ignore[no-untyped-def]
            return {
                "rank_ic_min": 0.0,
                "coverage_min": 0.0,
                "windows": result.windows,
                "passed": result.passed,
                "early_stopped": result.early_stopped,
            }

    monkeypatch.setattr(repair_mod, "EvalAdapter", DummyAdapter)

    out = repair_mod.run_auto_repair(
        root=root,
        as_of=as_of,
        profile="test",
        run_id=run_id,
        gate_summary_path=gate_summary_path,
        wf_summary_path=wf_summary_path,
        fail_results_path=fail_results_path,
        windows=[6, 12],
    )
    assert out is not None

    run_dir = root / "reports" / "p2_runs" / as_of / f"{run_id}.repair"
    plan = json.loads((run_dir / "repair_plan.json").read_text(encoding="utf-8"))
    assert plan["variant_sort_policy"] == VARIANT_SORT_POLICY

    seqs = [int(v["seq"]) for v in plan["variants"]]
    assert seqs == list(range(1, len(plan["variants"]) + 1))
    for variant in plan["variants"]:
        assert "seq" in variant
        assert "variant_id" in variant
        assert "factor_id" in variant
        assert "reason" in variant
        assert "variant_priority" in variant
        assert "transforms_len" in variant
        assert "transforms" in variant

    planned_variant_ids = [v["variant_id"] for v in plan["variants"]]

    attempt_paths = sorted((run_dir / "attempt_logs").glob("*/attempt_summary.json"))
    attempted_variant_ids = []
    attempt_ids = []
    for path in attempt_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        seq = int(payload["seq"])
        attempt_id = str(payload.get("attempt_id") or "")
        assert attempt_id == f"attempt_{seq:03d}"
        attempt_ids.append(attempt_id)
        attempted_variant_ids.append(payload["variant"]["variant_id"])

    assert attempted_variant_ids == planned_variant_ids
    assert attempt_ids == [f"attempt_{i:03d}" for i in range(1, len(attempt_paths) + 1)]

    holdout = json.loads((run_dir / "holdout_check.json").read_text(encoding="utf-8"))
    assert holdout["status"] == "skipped"
    assert holdout["reason"] == "no_selected_variant"

    gate_before = json.loads((run_dir / "gate_before.json").read_text(encoding="utf-8"))
    assert gate_before["schema"] == "p2_repair_gate_before.v1"
    assert gate_before["run_id"] == f"{run_id}.repair"
    assert gate_before["checks_count"] == 2

    gate_after = json.loads((run_dir / "gate_after.json").read_text(encoding="utf-8"))
    assert gate_after["schema"] == "p2_repair_gate_after.v1"
    assert gate_after["status"] == "skipped"
    assert gate_after["reason"] == "no_selected_variant"
    assert gate_after["attempt_id"] is None
    assert gate_after["metrics_summary"] == {}
    assert gate_after["passed"] is False

    final_result = json.loads((run_dir / "final_result.json").read_text(encoding="utf-8"))
    summary = final_result["summary"]
    assert summary["gate_after_present"] is True
    assert summary["gate_after_status"] == "skipped"
    assert summary["holdout_present"] is True
    assert summary["holdout_status"] == "skipped"
    assert summary["holdout_passed"] is False

    schema_report = json.loads((run_dir / "schema_report.json").read_text(encoding="utf-8"))
    assert schema_report["ok"] is True
    assert "data_stats" in schema_report


def test_empty_result_writes_schema_report_and_gate_after_metrics_summary(tmp_path: Path) -> None:
    root = tmp_path
    as_of = "2026-02-07"
    run_id = "p2.empty"

    repair_cfg = root / "configs" / "p2" / "repair"
    repair_cfg.mkdir(parents=True, exist_ok=True)
    (repair_cfg / "repair_profile.yaml").write_text(
        "\n".join(
            [
                "auto_repair:",
                "  default:",
                "    enabled: true",
                "  profiles:",
                "    test:",
                "      enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    (root / "configs" / "p2" / "xforms").mkdir(parents=True, exist_ok=True)
    (root / "configs" / "p2" / "xforms" / "default.yaml").write_text(
        "schema: phase2_xforms.v1\ntransforms: []\n",
        encoding="utf-8",
    )
    (root / "configs" / "p2" / "xforms" / "by_reason").mkdir(parents=True, exist_ok=True)

    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    gate_summary_path = reports / "gate_summary.json"
    gate_summary_path.write_text("{}", encoding="utf-8")
    wf_summary_path = reports / "wf_summary.json"
    wf_summary_path.write_text("{}", encoding="utf-8")
    fail_results_path = reports / "fail_results.csv"
    with fail_results_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["factor_id", "window", "pass", "reason"])
        writer.writeheader()

    out = repair_mod.run_auto_repair(
        root=root,
        as_of=as_of,
        profile="test",
        run_id=run_id,
        gate_summary_path=gate_summary_path,
        wf_summary_path=wf_summary_path,
        fail_results_path=fail_results_path,
        windows=[6, 12],
    )
    assert out is not None

    run_dir = root / "reports" / "p2_runs" / as_of / f"{run_id}.repair"
    assert (run_dir / "schema_report.json").is_file()
    assert (run_dir / "holdout_check.json").is_file()
    gate_after = json.loads((run_dir / "gate_after.json").read_text(encoding="utf-8"))
    assert gate_after["status"] == "skipped"
    assert gate_after["metrics_summary"] == {}
    schema_report = json.loads((run_dir / "schema_report.json").read_text(encoding="utf-8"))
    assert schema_report["ok"] is True


def test_repair_plan_deterministic_when_overwrite_same_run_dir(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    as_of = "2026-02-07"
    run_id = "p2.det"

    repair_cfg = root / "configs" / "p2" / "repair"
    xform_cfg = root / "configs" / "p2" / "xforms" / "by_reason"
    repair_cfg.mkdir(parents=True, exist_ok=True)
    xform_cfg.mkdir(parents=True, exist_ok=True)

    (repair_cfg / "repair_profile.yaml").write_text(
        "\n".join(
            [
                "auto_repair:",
                "  default:",
                "    enabled: true",
                "    max_attempts_per_factor: 3",
                "    stop_fast: true",
                "    promotion_mode: 'off'",
                "    reason_allowlist:",
                "      - rank_ic_min_threshold",
                "  profiles:",
                "    test:",
                "      enabled: true",
                "      max_attempts_per_factor: 3",
                "      stop_fast: true",
                "      promotion_mode: 'off'",
                "      reason_allowlist:",
                "        - rank_ic_min_threshold",
            ]
        ),
        encoding="utf-8",
    )
    (root / "configs" / "p2" / "xforms" / "default.yaml").parent.mkdir(parents=True, exist_ok=True)
    (root / "configs" / "p2" / "xforms" / "default.yaml").write_text(
        "schema: phase2_xforms.v1\ntransforms: []\n",
        encoding="utf-8",
    )
    (xform_cfg / "rank_ic_min_threshold.yaml").write_text(
        "\n".join(
            [
                "reason: rank_ic_min_threshold",
                "variants:",
                "  - id: v1",
                "    priority: 90",
                "    transforms:",
                "      - name: sign_flip",
                "        params: {}",
                "  - id: v2",
                "    priority: 50",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 1",
                "  - id: v3",
                "    priority: 20",
                "    transforms:",
                "      - name: lag",
                "        params:",
                "          periods: 2",
            ]
        ),
        encoding="utf-8",
    )

    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    gate_summary_path = reports / "gate_summary.json"
    gate_summary_path.write_text(
        json.dumps(
            {
                "checks": [
                    {
                        "factor_id": "mom_6m",
                        "reasons": ["rank_ic_min_threshold"],
                        "thresholds": {"min_rank_ic": 0.03, "min_coverage": 0.9},
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    wf_summary_path = reports / "wf_summary.json"
    wf_summary_path.write_text("{}", encoding="utf-8")
    fail_results_path = reports / "fail_results.csv"
    with fail_results_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["factor_id", "window", "pass", "reason"])
        writer.writeheader()

    factor_eval_dir = reports / "factor_eval"
    factor_eval_dir.mkdir(parents=True, exist_ok=True)
    (factor_eval_dir / "mom_6m_summary.json").write_text(
        json.dumps({"windows": {"6": {"rank_ic": 0.02}, "12": {"rank_ic": 0.01}}}),
        encoding="utf-8",
    )

    class DummyEvalResult:
        def __init__(self) -> None:
            self.passed = False
            self.early_stopped = True
            self.windows = {"6": {"rank_ic": 0.0, "coverage": 0.0}}

    class DummyAdapter:
        def __init__(self, *, root: Path, as_of: str, windows):  # type: ignore[no-untyped-def]
            self.root = root
            self.as_of = as_of
            self.windows = windows

        def evaluate_variant(self, **kwargs):  # type: ignore[no-untyped-def]
            return DummyEvalResult()

        @staticmethod
        def summarize_metrics(result):  # type: ignore[no-untyped-def]
            return {
                "rank_ic_min": 0.0,
                "coverage_min": 0.0,
                "windows": result.windows,
                "passed": result.passed,
                "early_stopped": result.early_stopped,
            }

    monkeypatch.setattr(repair_mod, "EvalAdapter", DummyAdapter)

    def _result_field(result, key: str):  # type: ignore[no-untyped-def]
        if isinstance(result, dict):
            return result.get(key)
        return getattr(result, key, None)

    def _snapshot(run_dir: Path):  # type: ignore[no-untyped-def]
        plan = json.loads((run_dir / "repair_plan.json").read_text(encoding="utf-8"))
        planned = [
            {
                "seq": int(v["seq"]),
                "variant_id": v["variant_id"],
                "factor_id": v["factor_id"],
                "reason": v["reason"],
                "variant_priority": v.get("variant_priority"),
                "transforms_len": v.get("transforms_len"),
                "transforms": list(v.get("transforms") or []),
            }
            for v in plan.get("variants", [])
        ]

        attempted = []
        attempted_pairs = []
        for path in sorted((run_dir / "attempt_logs").glob("*/attempt_summary.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            variant = payload.get("variant", {})
            seq = int(payload["seq"])
            attempt_id = str(payload.get("attempt_id") or "")
            assert attempt_id == f"attempt_{seq:03d}"
            attempted_pairs.append(
                {
                    "seq": seq,
                    "attempt_id": attempt_id,
                    "variant_id": variant["variant_id"],
                }
            )
            attempted.append(
                {
                    "seq": seq,
                    "variant_id": variant["variant_id"],
                    "factor_id": variant["factor_id"],
                    "reason": variant["reason"],
                    "variant_priority": variant.get("variant_priority"),
                    "transforms_len": variant.get("transforms_len"),
                    "transforms": list(variant.get("transforms") or []),
                }
            )
        return planned, attempted, attempted_pairs

    out1 = repair_mod.run_auto_repair(
        root=root,
        as_of=as_of,
        profile="test",
        run_id=run_id,
        gate_summary_path=gate_summary_path,
        wf_summary_path=wf_summary_path,
        fail_results_path=fail_results_path,
        windows=[6, 12],
    )
    assert out1 is not None

    run_dir = root / "reports" / "p2_runs" / as_of / f"{run_id}.repair"
    assert _result_field(out1, "run_dir") == str(run_dir)
    assert _result_field(out1, "run_id") == f"{run_id}.repair"
    planned_1, attempted_1, attempted_pairs_1 = _snapshot(run_dir)
    assert planned_1 == attempted_1

    out2 = repair_mod.run_auto_repair(
        root=root,
        as_of=as_of,
        profile="test",
        run_id=run_id,
        gate_summary_path=gate_summary_path,
        wf_summary_path=wf_summary_path,
        fail_results_path=fail_results_path,
        windows=[6, 12],
    )
    assert out2 is not None
    assert _result_field(out2, "run_dir") == str(run_dir)
    assert _result_field(out2, "run_id") == f"{run_id}.repair"

    planned_2, attempted_2, attempted_pairs_2 = _snapshot(run_dir)
    assert planned_2 == attempted_2
    assert planned_1 == planned_2
    assert attempted_1 == attempted_2
    assert attempted_pairs_1 == attempted_pairs_2
    assert [row["variant_id"] for row in attempted_pairs_1] == [row["variant_id"] for row in planned_1]
    assert _result_field(out1, "run_id") == _result_field(out2, "run_id") == f"{run_id}.repair"
    assert _result_field(out1, "run_dir") == _result_field(out2, "run_dir") == str(run_dir)
