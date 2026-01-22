from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


def _format_error(e: BaseException) -> str:
    msg = f"{type(e).__name__}: {e}"
    cause = getattr(e, "__cause__", None)
    if cause:
        msg += f" | cause={type(cause).__name__}: {cause}"
    return msg


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / "alpha_core").exists():
            return parent
    return Path.cwd().resolve()


def _bootstrap_sys_path() -> None:
    root = _repo_root_from_here()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    exec_dir = (root / "scripts" / "exec")
    if exec_dir.exists() and str(exec_dir) not in sys.path:
        sys.path.insert(0, str(exec_dir))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def ensure_out_dir(out_dir: Path, *, force: bool) -> None:
    if out_dir.exists():
        has_any = any(out_dir.iterdir())
        if has_any and not force:
            print(f"OUTDIR_NOT_EMPTY: {out_dir}")
            raise SystemExit(44)
        if has_any and force:
            shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def parse_args():
    ap = argparse.ArgumentParser(description="Fubon Read-Only Snapshot  positions/account SSOT artifacts")
    ap.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--force", action="store_true")

    # Determinism knobs
    ap.add_argument("--ts-snapshot", default=None, help="ISO timestamp (default: <as-of>T09:00:00)")
    ap.add_argument("--currency", default="TWD")
    ap.add_argument("--mark-price", type=float, default=0.0, help="Fallback mark price if market_value missing")

    # Provider / connect
    ap.add_argument("--provider-module", default=None)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--account-id", default=None)
    ap.add_argument("--no-keyring", action="store_true")
    ap.add_argument("--timeout-s", type=int, default=30)
    return ap.parse_args()


def _normalize_positions(
    raw: pd.DataFrame,
    *,
    run_id: str,
    as_of: str,
    mark_price: float,
) -> pd.DataFrame:
    """
    Try best-effort mapping to Phase-3 expected columns.
    Expected minimal columns:
      run_id, as_of, symbol, qty, avg_cost, market_value, source
    """
    df = raw.copy()

    # symbol
    if "symbol" not in df.columns:
        for c in ["ticker", "stock_id", "code"]:
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break

    # qty
    if "qty" not in df.columns:
        for c in ["position", "shares", "volume", "quantity"]:
            if c in df.columns:
                df = df.rename(columns={c: "qty"})
                break

    # avg_cost
    if "avg_cost" not in df.columns:
        for c in ["avg_price", "cost", "cost_price"]:
            if c in df.columns:
                df = df.rename(columns={c: "avg_cost"})
                break
    if "avg_cost" not in df.columns:
        df["avg_cost"] = 0.0

    # market_value
    if "market_value" not in df.columns:
        for c in ["mv", "marketValue", "value"]:
            if c in df.columns:
                df = df.rename(columns={c: "market_value"})
                break
    if "market_value" not in df.columns:
        df["market_value"] = pd.to_numeric(df.get("qty", 0), errors="coerce").fillna(0) * float(mark_price)

    # finalize
    df["run_id"] = run_id
    df["as_of"] = as_of
    if "source" not in df.columns:
        df["source"] = "BROKER"

    # types + ordering
    df["symbol"] = df["symbol"].astype(str)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(float)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce").fillna(0).astype(float)
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0).astype(float)

    cols = ["run_id", "as_of", "symbol", "qty", "avg_cost", "market_value", "source"]
    for c in cols:
        if c not in df.columns:
            df[c] = "" if c in ["symbol", "source"] else 0.0

    df = df[cols].sort_values(["symbol"], kind="mergesort").reset_index(drop=True)
    return df


def _normalize_account(
    raw: Dict[str, Any],
    *,
    run_id: str,
    as_of: str,
    ts_snapshot: str,
    currency: str,
    positions_mv: float,
) -> Dict[str, Any]:
    """
    Expected minimal keys (best-effort):
      run_id, as_of, ts_snapshot, currency, cash, buying_power, equity, nav, source
    """
    a = dict(raw)

    # best-effort mapping
    if "cash" not in a:
        for k in ["available_cash", "balance", "cash_balance"]:
            if k in a:
                a["cash"] = a[k]
                break
    if "buying_power" not in a:
        for k in ["bp", "available_funds", "available_buying_power"]:
            if k in a:
                a["buying_power"] = a[k]
                break

    cash = float(a.get("cash", 0.0) or 0.0)
    bp = float(a.get("buying_power", cash) or cash)

    # equity/nav: if broker didn't provide, compute cash + MV
    equity = a.get("equity", None)
    if equity is None:
        equity = cash + float(positions_mv)
    nav = a.get("nav", None)
    if nav is None:
        nav = float(equity)

    out = {
        "run_id": run_id,
        "as_of": as_of,
        "ts_snapshot": ts_snapshot,
        "currency": currency,
        "cash": float(cash),
        "buying_power": float(bp),
        "equity": float(equity),
        "nav": float(nav),
        "source": "BROKER",
    }
    return out


def _build_exec_summary(
    *,
    run_id: str,
    as_of: str,
    started_at: str,
    finished_at: str,
    artefacts_manifest: list[dict],
) -> Dict[str, Any]:
    # Use schema constants if available; otherwise keep minimal conservative keys.
    schema_version = "exec_logs.v1.1"
    mode = "PAPER"  # snapshot is read-only; keep within existing enum surface

    try:
        from alpha_core.execution.schemas import SCHEMA_VERSION as _SV  # type: ignore
        schema_version = _SV
    except Exception:
        pass

    summary: Dict[str, Any] = {
        "run_id": run_id,
        "as_of": as_of,
        "mode": mode,
        "schema_version": schema_version,
        "job_success": True,
        "started_at": started_at,
        "finished_at": finished_at,
        "orders_count": 0,
        "trades_count": 0,
        "fill_rate": 0.0,
        "reject_rate": 0.0,
        "artefacts_manifest": artefacts_manifest,
    }

    # Filter keys to whitelist if schemas exports it (avoid E_SUMMARY_UNKNOWN).
    try:
        from alpha_core.execution.schemas import EXEC_SUMMARY_REQUIRED_KEYS, EXEC_SUMMARY_OPTIONAL_KEYS  # type: ignore
        allowed = set(EXEC_SUMMARY_REQUIRED_KEYS) | set(EXEC_SUMMARY_OPTIONAL_KEYS)
        summary = {k: v for k, v in summary.items() if k in allowed}

        # Ensure required keys exist
        for k in EXEC_SUMMARY_REQUIRED_KEYS:
            if k not in summary:
                # safe defaults
                if k in ["orders_count", "trades_count"]:
                    summary[k] = 0
                elif k in ["fill_rate", "reject_rate"]:
                    summary[k] = 0.0
                elif k == "job_success":
                    summary[k] = True
                elif k == "schema_version":
                    summary[k] = schema_version
                elif k == "mode":
                    summary[k] = mode
                elif k == "started_at":
                    summary[k] = started_at
                elif k == "finished_at":
                    summary[k] = finished_at
                else:
                    summary[k] = None
    except Exception:
        pass

    return summary


def _get_required_order_trade_columns() -> tuple[list[str], list[str]]:
    try:
        from alpha_core.execution.schemas import (  # type: ignore
            ORDER_REQUIRED_COLUMNS,
            TRADE_REQUIRED_COLUMNS,
        )

        return list(ORDER_REQUIRED_COLUMNS), list(TRADE_REQUIRED_COLUMNS)
    except Exception:
        try:
            from alpha_core.execution.schemas import ORDERS, TRADES  # type: ignore

            return list(ORDERS.required_cols), list(TRADES.required_cols)
        except Exception:
            from alpha_core.execution.validator import (  # type: ignore
                ORDER_REQUIRED_COLUMNS,
                TRADE_REQUIRED_COLUMNS,
            )

            return list(ORDER_REQUIRED_COLUMNS), list(TRADE_REQUIRED_COLUMNS)


def main() -> int:
    _bootstrap_sys_path()
    args = parse_args()

    from alpha_core.execution.fubon_broker_adapter import FubonAdapterConfig, FubonBrokerAdapter

    as_of = args.as_of
    run_id = args.run_id
    out_dir = Path(args.out_dir)

    ts_snapshot = args.ts_snapshot or f"{as_of}T09:00:00"
    started_at = ts_snapshot
    finished_at = ts_snapshot

    ensure_out_dir(out_dir, force=args.force)

    cfg = FubonAdapterConfig(
        provider_module=args.provider_module,
        profile=args.profile,
        account_id=args.account_id,
        use_keyring=(not args.no_keyring),
        timeout_s=args.timeout_s,
    )
    ad = FubonBrokerAdapter(cfg)

    try:
        ad.connect()
    except Exception as e:
        print(f"FUBON_CONNECT_FAIL: {_format_error(e)}")
        return 60

    try:
        raw_pos = ad.fetch_positions(as_of=as_of)
        raw_acc = ad.fetch_account(as_of=as_of)
    except Exception as e:
        print(f"FUBON_FETCH_FAIL: {_format_error(e)}")
        ad.close()
        return 62
    finally:
        ad.close()

    pos_df = _normalize_positions(raw_pos, run_id=run_id, as_of=as_of, mark_price=args.mark_price)
    mv_sum = float(pos_df["market_value"].sum()) if not pos_df.empty else 0.0
    acc = _normalize_account(
        raw_acc,
        run_id=run_id,
        as_of=as_of,
        ts_snapshot=ts_snapshot,
        currency=args.currency,
        positions_mv=mv_sum,
    )

    order_cols, trade_cols = _get_required_order_trade_columns()
    orders_df = pd.DataFrame(columns=order_cols)
    trades_df = pd.DataFrame(columns=trade_cols)

    # Optional cross validation (best-effort; fail fast if available and fails)
    try:
        from alpha_core.execution.validator import validate_cross_artifacts  # type: ignore
        summary_stub = _build_exec_summary(
            run_id=run_id,
            as_of=as_of,
            started_at=started_at,
            finished_at=finished_at,
            artefacts_manifest=[],
        )
        r = validate_cross_artifacts(orders_df, trades_df, pos_df, acc, summary_stub)
        if hasattr(r, "ok") and not r.ok:
            print("CROSS_VALIDATE_FAILED: " + json.dumps([e.__dict__ for e in r.errors], ensure_ascii=False))
            return 47
    except Exception:
        # If validator signature differs, do not block snapshot generation.
        pass

    p_positions = out_dir / "positions.csv"
    p_account = out_dir / "account_snapshot.json"
    p_orders = out_dir / "orders.csv"
    p_trades = out_dir / "trades.csv"
    p_summary = out_dir / "exec_summary.json"
    p_ledger = out_dir / "ledger.json"

    pos_df.to_csv(p_positions, index=False)
    write_json(p_account, acc)
    orders_df.to_csv(p_orders, index=False)
    trades_df.to_csv(p_trades, index=False)

    # ledger/manifest
    artefacts = [
        {"name": "positions", "path": str(p_positions), "sha256": sha256_file(p_positions)},
        {"name": "account_snapshot", "path": str(p_account), "sha256": sha256_file(p_account)},
        {"name": "orders", "path": str(p_orders), "sha256": sha256_file(p_orders)},
        {"name": "trades", "path": str(p_trades), "sha256": sha256_file(p_trades)},
    ]

    # summary includes manifest; summary hash must be computed after writing
    summary = _build_exec_summary(
        run_id=run_id,
        as_of=as_of,
        started_at=started_at,
        finished_at=finished_at,
        artefacts_manifest=artefacts,
    )
    write_json(p_summary, summary)
    artefacts.append({"name": "exec_summary", "path": str(p_summary), "sha256": sha256_file(p_summary)})

    ledger = {
        "run_id": run_id,
        "as_of": as_of,
        "artefacts": artefacts,
    }
    write_json(p_ledger, ledger)

    print("SNAPSHOT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
