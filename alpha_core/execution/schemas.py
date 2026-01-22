# alpha_core/execution/schemas.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SCHEMA_VERSION: str = "exec_logs.v1.1"  # v1.1 adds: time_in_force, buying_power, run_id PK for positions/account


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(str, Enum):
    # TWSE practical minimum set
    ROD = "ROD"  # Rest of Day (default)
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill


class OrderStatus(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class SnapshotSource(str, Enum):
    BROKER = "BROKER"
    DERIVED = "DERIVED"


class ExecMode(str, Enum):
    MOCK = "MOCK"
    PAPER = "PAPER"
    LIVE = "LIVE"


def enum_values(e: type[Enum]) -> List[str]:
    return [m.value for m in e]  # type: ignore[attr-defined]


@dataclass(frozen=True)
class ForeignKeySpec:
    from_cols: Tuple[str, ...]
    to_table: str
    to_cols: Tuple[str, ...]


@dataclass(frozen=True)
class TableSchema:
    name: str
    required_cols: Tuple[str, ...]
    optional_cols: Tuple[str, ...]
    primary_key: Tuple[str, ...]
    foreign_keys: Tuple[ForeignKeySpec, ...] = ()

    def all_cols(self) -> Tuple[str, ...]:
        return self.required_cols + self.optional_cols


# -------------------------
# Orders
# -------------------------
ORDERS = TableSchema(
    name="orders",
    required_cols=(
        "run_id",
        "as_of",
        "ts_created",
        "cl_order_id",     # PK, deterministic
        "symbol",
        "side",
        "order_type",
        "time_in_force",   # NEW in v1.1
        "qty",
        "filled_qty",
        "status",
        "strategy_id",     # traceability
    ),
    optional_cols=(
        "broker_order_id",  # nullable until ACK
        "limit_price",      # nullable unless LIMIT
        "reject_reason",    # required when status=REJECTED
        "ts_submitted",
        "ts_last_update",
    ),
    primary_key=("cl_order_id",),
)

# -------------------------
# Trades (fills)
# -------------------------
TRADES = TableSchema(
    name="trades",
    required_cols=(
        "run_id",
        "as_of",
        "trade_id",        # PK
        "cl_order_id",     # FK -> orders.cl_order_id
        "ts_filled",
        "symbol",
        "side",
        "price",
        "qty",
        "commission",      # cost granularity
        "tax",             # cost granularity
    ),
    optional_cols=(
        "broker_trade_id",  # optional but very useful for live reconciliation
    ),
    primary_key=("trade_id",),
    foreign_keys=(
        ForeignKeySpec(from_cols=("cl_order_id",), to_table="orders", to_cols=("cl_order_id",)),
    ),
)

# -------------------------
# Positions snapshot (run-scoped, immutable history)
# PK: (run_id, symbol)  locked per architect decision
# -------------------------
POSITIONS = TableSchema(
    name="positions",
    required_cols=(
        "run_id",
        "as_of",
        "symbol",
        "qty",
        "source",  # BROKER / DERIVED
    ),
    optional_cols=(
        "avg_cost",
        "market_value",
        "ts_snapshot",
    ),
    primary_key=("run_id", "symbol"),
)

# -------------------------
# Account snapshot (run-scoped)
# PK: (run_id, currency)  supports multi-currency; also immutable per run
# -------------------------
ACCOUNT_SNAPSHOT = TableSchema(
    name="account_snapshot",
    required_cols=(
        "run_id",
        "as_of",
        "ts_snapshot",
        "currency",
        "cash",
        "buying_power",  # NEW in v1.1
        "equity",
        "nav",
        "source",        # BROKER / DERIVED
    ),
    optional_cols=(
        "margin",
        "reserved_cash",
    ),
    primary_key=("run_id", "currency"),
)

# -------------------------
# Exec summary (JSON-like)
# -------------------------
EXEC_SUMMARY_REQUIRED_KEYS: Tuple[str, ...] = (
    "run_id",
    "as_of",
    "mode",
    "schema_version",
    "job_success",
    "started_at",
    "finished_at",
    "orders_count",
    "trades_count",
    "fill_rate",
    "reject_rate",
    "artefacts_manifest",
)

# --- exec summary optional keys (extend-only, backward compatible) ---
EXEC_SUMMARY_OPTIONAL_KEYS: Tuple[str, ...] = (
    "error_code",
    "error_message",
    "policy_hash",
    "config_hash",
    "commit_sha",
    # Step-1 MockExec metrics (optional, validated only by presence allowance)
    "total_commission",
    "total_tax",
    "qty_total",
    "filled_qty_total",
    "mock_fill_rate",
    "mock_commission_bps",
    "mock_tax_bps",
    "mock_price",
)

ALL_TABLE_SCHEMAS: Dict[str, TableSchema] = {
    ORDERS.name: ORDERS,
    TRADES.name: TRADES,
    POSITIONS.name: POSITIONS,
    ACCOUNT_SNAPSHOT.name: ACCOUNT_SNAPSHOT,
}


def get_table_schema(name: str) -> TableSchema:
    try:
        return ALL_TABLE_SCHEMAS[name]
    except KeyError as e:
        raise KeyError(f"Unknown table schema: {name}. Known={list(ALL_TABLE_SCHEMAS.keys())}") from e


def is_exec_summary_required_key(k: str) -> bool:
    return k in EXEC_SUMMARY_REQUIRED_KEYS


def is_exec_summary_allowed_key(k: str) -> bool:
    return (k in EXEC_SUMMARY_REQUIRED_KEYS) or (k in EXEC_SUMMARY_OPTIONAL_KEYS)
