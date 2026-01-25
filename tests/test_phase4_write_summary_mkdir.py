from alpha_core.phase4.reporting import write_summary_atomic


def test_write_summary_atomic_creates_parent(tmp_path) -> None:
    target = tmp_path / "metrics" / "p4_summaries" / "p4_summary.json"
    write_summary_atomic({"status": "OK"}, target)
    assert target.exists()
