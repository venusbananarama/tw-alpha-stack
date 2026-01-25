from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from alpha_core.execution.validator import ValidationResult, validate_trades

from .errors import InputNotFoundError, SchemaValidationError


def validate_exec_trades_schema(df: pd.DataFrame) -> None:
    vr: ValidationResult = validate_trades(df)
    if not vr.ok:
        messages = "; ".join(f"{e.code}:{e.message}" for e in vr.errors[:10])
        raise SchemaValidationError(f"exec trades schema invalid: {messages}")


def resolve_exec_trades_path(
    exec_root: Path,
    exec_run_id: str,
    explicit_path: Optional[Path] = None,
) -> Path:
    if explicit_path is not None:
        path = explicit_path if explicit_path.is_absolute() else (exec_root / explicit_path)
        if path.exists():
            return path
        raise InputNotFoundError(f"exec trades not found: {path}")

    base = exec_root / exec_run_id
    candidates: Iterable[Path] = (
        base / "trades.csv",
        base / "exec_run" / "trades.csv",
        base / "reconcile" / "trades.csv",
        base / "fubon_snapshot" / "trades.csv",
    )
    for path in candidates:
        if path.exists():
            return path
    raise InputNotFoundError(f"exec trades not found under: {base}")


def load_exec_trades(
    exec_root: Path,
    exec_run_id: Optional[str] = None,
    explicit_path: Optional[Path] = None,
) -> pd.DataFrame:
    if explicit_path is not None:
        trades_path = explicit_path if explicit_path.is_absolute() else (exec_root / explicit_path)
        if not trades_path.exists():
            raise InputNotFoundError(f"exec trades not found: {trades_path}")
    elif exec_run_id is not None:
        trades_path = resolve_exec_trades_path(exec_root, exec_run_id, None)
    else:
        raise InputNotFoundError(f"exec trades not found under: {exec_root}")
    df = pd.read_csv(trades_path)
    validate_exec_trades_schema(df)

    if "ts_filled" not in df.columns:
        raise SchemaValidationError("exec trades missing ts_filled")

    out = pd.DataFrame()
    out["exec_ts"] = pd.to_datetime(df["ts_filled"], errors="coerce")
    if out["exec_ts"].isna().any():
        raise SchemaValidationError("exec trades contain invalid ts_filled")

    out["symbol"] = df["symbol"].astype(str).str.strip()
    out["side"] = df["side"].astype(str).str.strip().str.upper()
    out["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    out["price"] = pd.to_numeric(df["price"], errors="coerce")
    out["trade_id"] = df["trade_id"].astype(str)
    out["run_id"] = df["run_id"].astype(str)
    out["as_of"] = df["as_of"].astype(str)

    out = out.sort_values(["exec_ts", "trade_id"], kind="mergesort").reset_index(drop=True)
    out.attrs["resolved_exec_trades_path"] = str(trades_path)
    return out
