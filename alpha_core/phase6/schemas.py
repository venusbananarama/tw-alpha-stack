from __future__ import annotations

from typing import Dict, List, TypedDict

P6_SUMMARY_SCHEMA_VERSION = "phase6.summary.v1"
P6_RISK_SCHEMA_VERSION = "phase6.risk.v1"
P6_MANIFEST_SCHEMA_VERSION = "phase6.manifest.v1"


class ArtifactNames:
    P6_RUN_LOG = "p6_run.log"
    P6_SUMMARY_JSON = "p6_summary.json"
    RISK_METRICS_JSON = "risk_metrics.json"
    RISK_BREAKDOWN_CSV = "risk_breakdown.csv"
    P6_MANIFEST_JSON = "p6_manifest.json"
    APPROVED_TARGET_FMT = "approved_target_portfolio_{as_of}.csv"
    TARGETS_INPUT_CSV = "targets_input.csv"
    TARGETS_RISK_ADJUSTED_CSV = "targets_risk_adjusted.csv"
    RISK_BUDGET_JSON = "risk_budget.json"
    ADJUSTMENT_TRACE_JSON = "adjustment_trace.json"


class ResolvedPaths(TypedDict):
    root: str
    as_of: str
    out_dir: str
    target_csv: str
    prices_parquet: str
    calendar_csv: str
    prev_exec_dir: str | None
    prev_positions_csv: str | None
    prev_account_json: str | None
    lock_path: str
    rules_path: str | None
    benchmark_file: str | None


class GateResult(TypedDict):
    status: str
    observed: object
    threshold: object
    detail: Dict[str, object]


class RiskMetrics(TypedDict):
    schema_version: str
    nav_estimate: float | None
    gross_exposure: float | None
    net_exposure: float | None
    cash_available: float | None
    cash_usage_ratio: float | None
    max_single_name_ratio: float | None
    topk_concentration_ratio: float | None
    turnover_ratio: float | None
    tracking_error: Dict[str, float] | None
    information_ratio: Dict[str, float] | None
    active_return: Dict[str, float] | None
    tracking_error_obs: Dict[str, int] | None
    benchmark_last_date: str | None
    benchmark_obs: int | None


class P6Summary(TypedDict):
    schema_version: str
    as_of: str
    pricing_asof: str
    status: str
    reason_code: str
    exit_code: int
    gate_results: Dict[str, GateResult]
    resolved_paths: ResolvedPaths
    hashes: Dict[str, str]
    created_at: str
    approved_target_path: str | None


class P6Manifest(TypedDict):
    schema_version: str
    inputs: Dict[str, object]
    hashes: Dict[str, str]
    resolved_paths: ResolvedPaths
    versions: Dict[str, str]
