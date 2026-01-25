from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class ExitCode(IntEnum):
    OK = 0
    INPUT_NOT_FOUND = 2
    NOT_TRADING_DAY = 41
    OUTDIR_NOT_EMPTY = 44
    SCHEMA_VALIDATION_FAILED = 47
    LOCKED = 48
    GATE_FAILED = 60


REASON_OK = "OK"
REASON_NOT_TRADING_DAY = "NOT_TRADING_DAY"
REASON_INCOMPLETE_INTRADAY_SKIPPED = "INCOMPLETE_INTRADAY_SKIPPED"
REASON_INPUT_NOT_FOUND = "INPUT_NOT_FOUND"
REASON_OUTDIR_NOT_EMPTY = "OUTDIR_NOT_EMPTY"
REASON_LOCKED = "LOCKED"
REASON_SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"
REASON_GATE_FAILED = "GATE_FAILED"
REASON_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
REASON_INSUFFICIENT_MARKET_COVERAGE = "INSUFFICIENT_MARKET_COVERAGE"
REASON_RUNTIME_ERROR = "RUNTIME_ERROR"


@dataclass
class Phase4Error(Exception):
    message: str
    reason_code: str
    exit_code: int

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.reason_code}: {self.message}"


class NotTradingDayError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_NOT_TRADING_DAY, ExitCode.NOT_TRADING_DAY)


class IncompleteDayError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_INCOMPLETE_INTRADAY_SKIPPED, ExitCode.OK)


class InputNotFoundError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_INPUT_NOT_FOUND, ExitCode.INPUT_NOT_FOUND)


class OutDirNotEmptyError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_OUTDIR_NOT_EMPTY, ExitCode.OUTDIR_NOT_EMPTY)


class LockedError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_LOCKED, ExitCode.LOCKED)


class SchemaValidationError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_SCHEMA_VALIDATION_FAILED, ExitCode.SCHEMA_VALIDATION_FAILED)


class GateFailedError(Phase4Error):
    def __init__(self, message: str) -> None:
        super().__init__(message, REASON_GATE_FAILED, ExitCode.GATE_FAILED)
