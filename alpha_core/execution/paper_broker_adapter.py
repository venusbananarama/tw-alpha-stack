# alpha_core/execution/paper_broker_adapter.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from .broker_adapter import (
    AccountRecord,
    BrokerAdapter,
    OrderAck,
    PollFillsResult,
    PositionRecord,
    SendOrdersResult,
)
from .schemas import ExecMode, OrderStatus


class PaperBrokerAdapter(BrokerAdapter):
    def __init__(self, *, mode: ExecMode, ts_base: datetime) -> None:
        super().__init__(mode=mode)
        self._ts_base = ts_base
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
                }

            if clid not in self._acks:
                ts_event = self._ts_base + timedelta(seconds=1 + i)
                self._acks[clid] = OrderAck(
                    cl_order_id=clid,
                    ts_event=ts_event,
                    status=OrderStatus.SUBMITTED,
                    broker_order_id=f"paper_{clid}",
                    reject_reason=None,
                )

            acks.append(self._acks[clid])

        return SendOrdersResult(acks=acks)

    def poll_fills(self, cursor: Optional[str] = None) -> PollFillsResult:
        if cursor == "done" or self._fills_emitted:
            return PollFillsResult(fills=[], cursor="done")

        self._fills_emitted = True
        return PollFillsResult(fills=[], cursor="done")

    def fetch_positions(self, *, run_id: str, as_of: str) -> List[PositionRecord]:
        return []

    def fetch_account(self, *, run_id: str, as_of: str) -> List[AccountRecord]:
        return []

    def close(self) -> None:
        return None
