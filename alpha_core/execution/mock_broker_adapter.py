# alpha_core/execution/mock_broker_adapter.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from .broker_adapter import (
    AccountRecord,
    BrokerAdapter,
    FillEvent,
    OrderAck,
    PollFillsResult,
    PositionRecord,
    SendOrdersResult,
)
from .schemas import ExecMode, OrderStatus


class MockBrokerAdapter(BrokerAdapter):
    def __init__(
        self,
        *,
        mode: ExecMode,
        ts_base: datetime,
        fill_rate: float,
        commission_bps: float,
        tax_bps: float,
        mock_price: float,
    ) -> None:
        super().__init__(mode=mode)
        self._ts_base = ts_base
        self._fill_rate = float(fill_rate)
        self._commission_bps = float(commission_bps)
        self._tax_bps = float(tax_bps)
        self._mock_price = float(mock_price)

        self._orders: Dict[str, Dict[str, Any]] = {}
        self._acks: Dict[str, OrderAck] = {}
        self._fills_emitted = False

    def connect(self) -> None:
        return None

    def send_orders(self, orders_df: pd.DataFrame) -> SendOrdersResult:
        if orders_df is None or orders_df.empty:
            return SendOrdersResult(acks=[])

        df = orders_df.sort_values(["cl_order_id"], kind="mergesort").reset_index(drop=True)
        acks: List[OrderAck] = []

        for i, row in df.iterrows():
            clid = str(row["cl_order_id"])
            if clid not in self._orders:
                self._orders[clid] = {
                    "run_id": row.get("run_id"),
                    "as_of": row.get("as_of"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "qty": int(row.get("qty", 0)),
                    "order_type": row.get("order_type"),
                    "limit_price": row.get("limit_price"),
                }

            if clid not in self._acks:
                ts_event = self._ts_base + timedelta(seconds=1 + i)
                self._acks[clid] = OrderAck(
                    cl_order_id=clid,
                    ts_event=ts_event,
                    status=OrderStatus.SUBMITTED,
                    broker_order_id=f"mock_{clid}",
                    reject_reason=None,
                )

            acks.append(self._acks[clid])

        return SendOrdersResult(acks=acks)

    def poll_fills(self, cursor: Optional[str] = None) -> PollFillsResult:
        if cursor == "done" or self._fills_emitted:
            return PollFillsResult(fills=[], cursor="done")

        fills: List[FillEvent] = []
        fr = max(0.0, min(1.0, float(self._fill_rate)))

        for i, clid in enumerate(sorted(self._orders.keys())):
            row = self._orders[clid]
            qty = int(row.get("qty", 0))
            if qty <= 0:
                continue

            fill_qty = int(round(qty * fr))
            if fill_qty <= 0:
                continue

            otype = str(row.get("order_type") or "LIMIT")
            price = float(self._mock_price)
            if otype == "LIMIT":
                lp = row.get("limit_price")
                lp_val = float(lp) if lp is not None else 0.0
                if lp_val > 0:
                    price = lp_val

            ts_filled = self._ts_base + timedelta(seconds=2 + i)
            gross = price * float(fill_qty)
            commission = round(gross * (self._commission_bps / 10000.0), 2)
            tax = 0.0
            if str(row.get("side", "")).upper() == "SELL":
                tax = round(gross * (self._tax_bps / 10000.0), 2)

            fills.append(
                FillEvent(
                    trade_id=f"trd_{clid}_001",
                    cl_order_id=clid,
                    ts_filled=ts_filled,
                    symbol=str(row.get("symbol")),
                    side=str(row.get("side")),
                    price=float(price),
                    qty=int(fill_qty),
                    commission=float(commission),
                    tax=float(tax),
                    broker_trade_id=None,
                )
            )

        self._fills_emitted = True
        return PollFillsResult(fills=fills, cursor="done")

    def fetch_positions(self, *, run_id: str, as_of: str) -> List[PositionRecord]:
        return []

    def fetch_account(self, *, run_id: str, as_of: str) -> List[AccountRecord]:
        return []

    def close(self) -> None:
        return None
