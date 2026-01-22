# alpha_core/execution/broker_adapter.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Mapping, Optional, Sequence

import pandas as pd

from .schemas import (
    ExecMode,
    OrderStatus,
    SnapshotSource,
)


@dataclass(frozen=True)
class OrderAck:
    """
    Broker ACK / status update for an order.

    Contract notes:
    - cl_order_id is always required (our deterministic ID / PK).
    - broker_order_id may be None before broker assigns it, but should be returned once known.
    - status must map into OrderStatus enum.
    - reject_reason required when status=REJECTED.
    """
    cl_order_id: str
    ts_event: datetime
    status: OrderStatus
    broker_order_id: Optional[str] = None
    reject_reason: Optional[str] = None


@dataclass(frozen=True)
class SendOrdersResult:
    acks: List[OrderAck]


@dataclass(frozen=True)
class FillEvent:
    """
    A fill/trade event mapped to Phase-3 trades schema.

    commission/tax must be split (TWSE practical requirement).
    """
    trade_id: str
    cl_order_id: str
    ts_filled: datetime
    symbol: str
    side: str  # "BUY"/"SELL" (kept as str to avoid tight coupling; validator enforces)
    price: float
    qty: int
    commission: float
    tax: float
    broker_trade_id: Optional[str] = None


@dataclass(frozen=True)
class PollFillsResult:
    fills: List[FillEvent]
    cursor: Optional[str] = None  # broker-specific cursor/token for incremental polling


@dataclass(frozen=True)
class PositionRecord:
    run_id: str
    as_of: str
    symbol: str
    qty: int
    source: SnapshotSource
    ts_snapshot: Optional[datetime] = None
    avg_cost: Optional[float] = None
    market_value: Optional[float] = None


@dataclass(frozen=True)
class AccountRecord:
    run_id: str
    as_of: str
    ts_snapshot: datetime
    currency: str
    cash: float
    buying_power: float
    equity: float
    nav: float
    source: SnapshotSource
    margin: Optional[float] = None
    reserved_cash: Optional[float] = None


class BrokerAdapter(ABC):
    """
    Execution adapter boundary.

    This interface MUST be sufficient to populate:
    - orders: broker_order_id, status transitions, reject_reason
    - trades: commission, tax
    - positions/account_snapshot: run-scoped snapshots (keyed by run_id)
    """

    def __init__(self, *, mode: ExecMode) -> None:
        self._mode = mode

    @property
    def mode(self) -> ExecMode:
        return self._mode

    @abstractmethod
    def connect(self) -> None:
        """Establish session. Must be non-interactive in production runs."""
        raise NotImplementedError

    @abstractmethod
    def send_orders(self, orders_df: pd.DataFrame) -> SendOrdersResult:
        """
        Send orders to broker.

        Input: orders_df in Phase-3 orders schema shape.
        Output: acknowledgements including broker_order_id mapping and status/reject_reason.

        Idempotency expectation:
        - If the same cl_order_id is resent (after reconnect), implementation should be able
          to detect duplicates and return consistent broker state (or reject with explicit reason).
        """
        raise NotImplementedError

    @abstractmethod
    def poll_fills(self, cursor: Optional[str] = None) -> PollFillsResult:
        """
        Poll fills since a cursor. Must be safe to call repeatedly.
        Returns fills mapped into Phase-3 trades schema shape.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_positions(self, *, run_id: str, as_of: str) -> List[PositionRecord]:
        """Fetch current positions snapshot mapped into Phase-3 positions schema."""
        raise NotImplementedError

    @abstractmethod
    def fetch_account(self, *, run_id: str, as_of: str) -> List[AccountRecord]:
        """
        Fetch account snapshot(s). Multi-currency is allowed; hence list.
        Must provide buying_power (>=0) as part of the contract.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close session cleanly."""
        raise NotImplementedError
