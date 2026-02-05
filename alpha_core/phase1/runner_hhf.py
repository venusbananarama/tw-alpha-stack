from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import os
import subprocess
import sys
import time


@dataclass
class RunnerResult:
    ok: bool
    exit_code: int
    duration_ms: int
    stdout_tail: List[str]
    stderr_tail: List[str]
    rate_limited: bool


def _tail_lines(text: str, max_lines: int = 20) -> List[str]:
    lines = [ln.rstrip() for ln in text.splitlines()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def _detect_rate_limit(text: str) -> bool:
    lowered = text.lower()
    return "429" in lowered or "rate limit" in lowered or "quota" in lowered


def _resolve_python(repo_root: Path) -> str:
    if sys.executable:
        return sys.executable
    return "python"


def _resolve_calls_per_hour(env: Dict[str, str], default: int = 6000) -> int:
    raw = (env.get("FINMIND_CALLS_PER_HOUR") or "").strip()
    if raw:
        try:
            val = int(float(raw))
            if val > 0:
                return val
        except ValueError:
            pass
    raw_qps = (env.get("FINMIND_QPS") or "").strip()
    if raw_qps:
        try:
            qps = float(raw_qps)
            if qps > 0:
                return int(round(qps * 3600.0))
        except ValueError:
            pass
    raw_rpm = (env.get("FINMIND_RPM") or "").strip()
    if raw_rpm:
        try:
            rpm = float(raw_rpm)
            if rpm > 0:
                return int(round(rpm * 60.0))
        except ValueError:
            pass
    return default


def run_hhf_day(
    repo_root: Path,
    datahub_root: Path,
    dataset: str,
    day: date,
    env: Dict[str, str],
    universe_ids: Optional[List[str]] = None,
) -> RunnerResult:
    script = repo_root / "scripts" / "p1_finmind_hhf_ingest.py"
    log_dir = repo_root / "logs" / "phase1"
    calls_per_hour = _resolve_calls_per_hour(env)

    args = [
        _resolve_python(repo_root),
        str(script),
        "--dataset",
        dataset,
        "--day",
        day.isoformat(),
        "--repo-root",
        str(repo_root),
        "--datahub-root",
        str(datahub_root),
        "--calls-per-hour",
        str(calls_per_hour),
        "--log-dir",
        str(log_dir),
    ]
    if universe_ids is not None:
        args.extend(["--symbols", ",".join(universe_ids)])

    merged_env = os.environ.copy()
    merged_env.update(env)

    t0 = time.time()
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        env=merged_env,
        cwd=str(repo_root),
    )
    duration_ms = int((time.time() - t0) * 1000)
    stdout_tail = _tail_lines(proc.stdout or "")
    stderr_tail = _tail_lines(proc.stderr or "")
    rate_limited = _detect_rate_limit(proc.stdout or "") or _detect_rate_limit(proc.stderr or "")
    ok = proc.returncode == 0
    return RunnerResult(
        ok=ok,
        exit_code=proc.returncode,
        duration_ms=duration_ms,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
        rate_limited=rate_limited,
    )
