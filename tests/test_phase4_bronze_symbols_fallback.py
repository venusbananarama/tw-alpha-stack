import json

from alpha_core.phase4 import bronze_loader as bl


def test_list_bronze_symbols_fallback_reads_content(tmp_path) -> None:
    bronze_root = tmp_path / "datahub" / "bronze" / "fubon" / "trades"
    day_dir = bronze_root / "dt=2026-01-19"
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / "trades_unknown.ndjson"
    path.write_text(json.dumps({"symbol": "2330"}) + "\n", encoding="utf-8")

    symbols = bl.list_bronze_symbols(bronze_root, "2026-01-19")
    assert symbols == {"2330"}


def test_list_bronze_symbols_heuristic_skips_fallback(tmp_path, monkeypatch) -> None:
    bronze_root = tmp_path / "datahub" / "bronze" / "fubon" / "trades"
    day_dir = bronze_root / "dt=2026-01-20"
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / "symbol=2330.ndjson"
    path.write_text("", encoding="utf-8")

    def _boom(_files):
        raise AssertionError("fallback should not be called")

    monkeypatch.setattr(bl, "_fallback_symbols_from_files", _boom)

    symbols = bl.list_bronze_symbols(bronze_root, "2026-01-20")
    assert symbols == {"2330"}
