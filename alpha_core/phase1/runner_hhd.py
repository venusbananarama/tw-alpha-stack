from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import os
import subprocess
import sys
import time


@dataclass
class BatchResult:
    ok: bool
    exit_code: int
    duration_ms: int
    stdout_tail: List[str]
    stderr_tail: List[str]
    rate_limited: bool


@dataclass
class DayResult:
    ok: bool
    exit_code: int
    duration_ms: int
    stdout_tail: List[str]
    stderr_tail: List[str]
    rate_limited: bool
    batches: int


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


def _split_batches(ids: List[str], batch_size: int) -> List[List[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    out: List[List[str]] = []
    for i in range(0, len(ids), batch_size):
        out.append(ids[i : i + batch_size])
    return out


def load_universe_ids(path: Path) -> List[str]:
    if not path.is_file():
        raise FileNotFoundError(f"universe file not found: {path}")
    ids: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        ids.append(line)
    if not ids:
        raise ValueError(f"universe file is empty: {path}")
    return sorted(set(ids))


def _run_batch(
    repo_root: Path,
    datahub_root: Path,
    dataset: str,
    day: date,
    ids: List[str],
    env: Dict[str, str],
    config_path: Optional[Path],
    batch_size: int,
) -> BatchResult:
    script = repo_root / "scripts" / "p1_finmind_dateid_ingest.py"
    log_dir = repo_root / "logs" / "phase1"
    calls_per_hour = _resolve_calls_per_hour(env)
    args = [
        _resolve_python(repo_root),
        str(script),
        "--dataset",
        dataset,
        "--day",
        day.isoformat(),
        "--ids",
        ",".join(ids),
        "--repo-root",
        str(repo_root),
        "--datahub-root",
        str(datahub_root),
        "--calls-per-hour",
        str(calls_per_hour),
        "--batch-size",
        str(batch_size),
        "--log-dir",
        str(log_dir),
    ]
    if config_path and config_path.is_file():
        args.extend(["--config", str(config_path)])

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
    return BatchResult(
        ok=ok,
        exit_code=proc.returncode,
        duration_ms=duration_ms,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
        rate_limited=rate_limited,
    )


def run_hhd_day(
    repo_root: Path,
    datahub_root: Path,
    dataset: str,
    day: date,
    universe_ids: List[str],
    batch_size: int,
    env: Dict[str, str],
    config_path: Optional[Path],
) -> DayResult:
    if dataset == "gov_bank":
        batches = [["ALL"]]
    else:
        batches = _split_batches(universe_ids, batch_size)

    all_stdout: List[str] = []
    all_stderr: List[str] = []
    rate_limited = False
    total_duration = 0

    for batch in batches:
        res = _run_batch(
            repo_root=repo_root,
            datahub_root=datahub_root,
            dataset=dataset,
            day=day,
            ids=batch,
            env=env,
            config_path=config_path,
            batch_size=batch_size,
        )
        total_duration += res.duration_ms
        all_stdout.extend(res.stdout_tail)
        all_stderr.extend(res.stderr_tail)
        if res.rate_limited:
            rate_limited = True
        if not res.ok:
            return DayResult(
                ok=False,
                exit_code=res.exit_code,
                duration_ms=total_duration,
                stdout_tail=all_stdout[-20:],
                stderr_tail=all_stderr[-20:],
                rate_limited=rate_limited,
                batches=len(batches),
            )

    return DayResult(
        ok=True,
        exit_code=0,
        duration_ms=total_duration,
        stdout_tail=all_stdout[-20:],
        stderr_tail=all_stderr[-20:],
        rate_limited=rate_limited,
        batches=len(batches),
    )
