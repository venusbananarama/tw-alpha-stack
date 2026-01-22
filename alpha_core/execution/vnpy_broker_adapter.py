# alpha_core/execution/vnpy_broker_adapter.py
from __future__ import annotations

import importlib
from typing import List, Optional

import pandas as pd

from .broker_adapter import (
    AccountRecord,
    BrokerAdapter,
    PollFillsResult,
    PositionRecord,
    SendOrdersResult,
)
from .schemas import ExecMode


class VnpyBrokerAdapter(BrokerAdapter):
    def __init__(self, *, mode: ExecMode) -> None:
        super().__init__(mode=mode)
        self._closed = False

    def connect(self) -> None:
        missing = self._check_imports()
        if missing:
            msg = "VNPY_IMPORT_FAILED: missing modules: " + ", ".join(missing)
            raise RuntimeError(msg)

    def send_orders(self, orders_df: pd.DataFrame) -> SendOrdersResult:
        raise RuntimeError("VNPY_NOT_READY")

    def poll_fills(self, cursor: Optional[str] = None) -> PollFillsResult:
        raise RuntimeError("VNPY_NOT_READY")

    def fetch_positions(self, *, run_id: str, as_of: str) -> List[PositionRecord]:
        raise RuntimeError("VNPY_NOT_READY")

    def fetch_account(self, *, run_id: str, as_of: str) -> List[AccountRecord]:
        raise RuntimeError("VNPY_NOT_READY")

    def close(self) -> None:
        if self._closed:
            return None
        self._closed = True
        return None

    @staticmethod
    def _check_imports() -> List[str]:
        required = [
            "vnpy",
            "vnpy.trader",
            "vnpy.trader.engine",
            "vnpy.trader.gateway",
        ]
        missing: List[str] = []
        for name in required:
            try:
                importlib.import_module(name)
            except Exception:
                missing.append(name)
        return missing
