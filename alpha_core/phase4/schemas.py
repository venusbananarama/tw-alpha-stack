from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


REF_PRICE_MODE_LAST = "last_trade_before"
REF_PRICE_MODE_VWAP = "vwap_window"


@dataclass(frozen=True)
class TableSchema:
    name: str
    required_cols: Tuple[str, ...]
    optional_cols: Tuple[str, ...] = ()

    def all_cols(self) -> Tuple[str, ...]:
        return self.required_cols + self.optional_cols


MARKET_TRADES = TableSchema(
    name="market_trades",
    required_cols=("ts", "symbol", "price", "qty"),
    optional_cols=("side", "source_file", "source_line"),
)

EXEC_TRADES = TableSchema(
    name="exec_trades",
    required_cols=("ts", "symbol", "side", "qty", "price", "trade_id", "run_id", "as_of"),
    optional_cols=(),
)

ALIGNED = TableSchema(
    name="aligned",
    required_cols=(
        "exec_ts",
        "symbol",
        "side",
        "qty",
        "price",
        "trade_id",
        "run_id",
        "as_of",
        "ref_price",
        "ref_ts",
        "ref_mode",
        "window_sec",
    ),
    optional_cols=("mkt_window_qty", "mkt_window_notional", "missing_ref"),
)

REPLAY_STATS = TableSchema(
    name="replay_stats",
    required_cols=(
        "as_of",
        "symbol",
        "n_exec_trades",
        "coverage_rate",
        "missing_ref_trades",
        "slippage_bps_p50",
        "slippage_bps_p95",
        "ref_price_mode",
        "window_sec",
        "created_at",
        "run_id",
    ),
    optional_cols=("status",),
)

DRIFT_METRICS = TableSchema(
    name="drift_metrics",
    required_cols=("month", "drift_value_pct", "drift_median_pct", "status", "n_days"),
    optional_cols=(),
)

P4_SUMMARY_REQUIRED_KEYS = (
    "status",
    "reason_code",
    "exit_code",
    "inputs",
    "thresholds",
    "artifacts",
)

P4_SUMMARY_INPUT_KEYS = (
    "as_of",
    "exec_run_id",
    "resolved_exec_trades_path",
    "bronze_dt_path",
    "exec_trade_count",
    "exec_symbols",
    "bronze_symbols",
    "missing_symbols",
    "symbol_coverage",
)

P4_SUMMARY_THRESHOLD_KEYS = (
    "min_symbol_coverage",
    "min_trade_count",
    "on_insufficient_data",
)

P4_SUMMARY = TableSchema(
    name="p4_summary",
    required_cols=P4_SUMMARY_REQUIRED_KEYS,
    optional_cols=("gates", "resolved_paths", "coverage"),
)
