from __future__ import annotations

import json
from pathlib import Path

from alpha_core.phase2.repair.recorder import RunDirRecorder


def test_recorder_creates_run_dir_and_manifest(tmp_path: Path) -> None:
    root = tmp_path
    gate_path = root / "reports" / "gate_summary.json"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text('{"ok": false}', encoding="utf-8")

    recorder = RunDirRecorder(root=root, as_of="2026-02-06", run_id="p2.test.repair")
    recorder.start_run(tags={"profile": "test"})
    recorder.log_params({"enabled": True})
    recorder.log_metrics({"attempted": 0})

    manifest_path = recorder.write_manifest(
        resolved_paths={"gate_summary": gate_path},
        versions={"repair_schema": "p2_repair.v1"},
    )
    final_path = recorder.finalize(status="ok", summary={"attempted": 0, "passed": False})

    assert recorder.run_dir.exists()
    assert (recorder.run_dir / "tags.json").exists()
    assert (recorder.run_dir / "params.json").exists()
    assert (recorder.run_dir / "metrics.json").exists()
    assert manifest_path.exists()
    assert final_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["resolved_paths"]["gate_summary"] == str(gate_path.resolve())
    assert "gate_summary" in manifest["hashes"]
