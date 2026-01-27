from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional


class ExitCode(IntEnum):
    OK = 0
    INPUT_NOT_FOUND = 40
    NOT_TRADING_DAY = 41
    INSUFFICIENT_DATA = 42
    OUTDIR_NOT_EMPTY = 44
    LOCKED = 48
    SCHEMA_INVALID = 60
    INFEASIBLE = 70
    RUNTIME_ERROR = 99


REASON_OK = "OK"
REASON_INPUT_NOT_FOUND = "INPUT_NOT_FOUND"
REASON_NOT_TRADING_DAY = "NOT_TRADING_DAY"
REASON_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
REASON_LOCKED = "LOCKED"
REASON_OUTDIR_NOT_EMPTY = "OUTDIR_NOT_EMPTY"
REASON_SCHEMA_INVALID = "SCHEMA_INVALID"
REASON_INFEASIBLE = "INFEASIBLE"
REASON_RUNTIME_ERROR = "RUNTIME_ERROR"


@dataclass
class Phase5Error(Exception):
    message: str
    reason_code: str
    exit_code: int
    details: Optional[Dict[str, object]] = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.reason_code}: {self.message}"


class InputNotFoundError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_INPUT_NOT_FOUND, ExitCode.INPUT_NOT_FOUND, details)


class NotTradingDayError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_NOT_TRADING_DAY, ExitCode.NOT_TRADING_DAY, details)


class InsufficientDataError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_INSUFFICIENT_DATA, ExitCode.INSUFFICIENT_DATA, details)


class OutDirNotEmptyError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_OUTDIR_NOT_EMPTY, ExitCode.OUTDIR_NOT_EMPTY, details)


class LockedError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_LOCKED, ExitCode.LOCKED, details)


class SchemaInvalidError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_SCHEMA_INVALID, ExitCode.SCHEMA_INVALID, details)


class InfeasibleError(Phase5Error):
    def __init__(self, message: str, details: Optional[Dict[str, object]] = None) -> None:
        super().__init__(message, REASON_INFEASIBLE, ExitCode.INFEASIBLE, details)
