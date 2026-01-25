from pathlib import Path

from alpha_core.phase4.runner import resolve_exec_trades_path


def test_resolve_exec_trades_path_prefers_root_then_exec_run(tmp_path: Path) -> None:
    repo = tmp_path
    run_id = "R1"

    base = repo / "reports" / "exec" / run_id
    (base / "exec_run").mkdir(parents=True, exist_ok=True)

    p_exec_run = base / "exec_run" / "trades.csv"
    p_exec_run.write_text("ts_filled,symbol\n2026-01-01 09:00:00,2330\n", encoding="utf-8")

    resolved = resolve_exec_trades_path(repo, run_id)
    assert resolved == p_exec_run

    p_root = base / "trades.csv"
    p_root.write_text("ts_filled,symbol\n2026-01-01 09:00:00,2317\n", encoding="utf-8")

    resolved2 = resolve_exec_trades_path(repo, run_id)
    assert resolved2 == p_root
