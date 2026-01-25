from pathlib import Path

from alpha_core.phase4.exec_loader import resolve_exec_trades_path


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")


def test_resolve_exec_trades_path_priority(tmp_path: Path) -> None:
    exec_root = tmp_path / "reports" / "exec"
    run_id = "r1"
    base = exec_root / run_id

    path_exec_run = base / "exec_run" / "trades.csv"
    _touch(path_exec_run)
    assert resolve_exec_trades_path(exec_root, run_id) == path_exec_run

    path_root = base / "trades.csv"
    _touch(path_root)
    assert resolve_exec_trades_path(exec_root, run_id) == path_root

    path_reconcile = base / "reconcile" / "trades.csv"
    _touch(path_reconcile)
    assert resolve_exec_trades_path(exec_root, run_id, explicit_path=path_reconcile) == path_reconcile
