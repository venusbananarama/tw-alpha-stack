import subprocess
import sys
from pathlib import Path


def test_p4_daily_help_has_print_resolved_paths() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "p4_daily_routine.py"
    result = subprocess.run(
        [sys.executable, str(script), "-h"],
        capture_output=True,
        text=True,
        check=False,
    )
    output = (result.stdout or "") + (result.stderr or "")
    assert "--print-resolved-paths" in output
