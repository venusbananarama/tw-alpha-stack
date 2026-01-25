from alpha_core.phase4.runner import build_parser, run


def test_runner_early_exit_writes_summary_and_ledger(tmp_path) -> None:
    as_of = "2026-01-19"
    run_id = f"p4_{as_of}"
    calendar_dir = tmp_path / "datahub" / "ref"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    (calendar_dir / "trading_days.csv").write_text("date\n2026-01-19\n", encoding="utf-8")

    parser = build_parser()
    args = parser.parse_args(
        [
            "--as-of",
            as_of,
            "--exec-run-id",
            "R20260119_fix",
            "--mode",
            "replay",
        ]
    )
    _ = run(args, repo_root=tmp_path)

    summary_path = tmp_path / "metrics" / "p4_summaries" / f"p4_summary.{as_of}.{run_id}.json"
    ledger_path = tmp_path / "metrics" / "p4_ledger.jsonl"
    assert summary_path.exists()
    assert ledger_path.exists()
    lines = ledger_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 1
