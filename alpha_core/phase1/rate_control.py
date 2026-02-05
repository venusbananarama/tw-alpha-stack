from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import json
import logging
import os
import socket
import time

from . import paths


_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateConfig:
    qps: Optional[float]
    rpm: Optional[int]
    calls_per_hour: Optional[int]


def resolve_rate(
    calls_per_hour: Optional[int],
    qps: Optional[float],
    rpm: Optional[int],
) -> RateConfig:
    if qps is not None and qps > 0:
        return RateConfig(qps=float(qps), rpm=int(rpm) if rpm else None, calls_per_hour=calls_per_hour)
    if rpm is not None and rpm > 0:
        qps_val = float(rpm) / 60.0
        return RateConfig(qps=qps_val, rpm=int(rpm), calls_per_hour=calls_per_hour)
    if calls_per_hour is None or calls_per_hour <= 0:
        calls_per_hour = 600
    qps_val = float(calls_per_hour) / 3600.0
    rpm_val = max(1, int(round(float(calls_per_hour) / 60.0)))
    return RateConfig(qps=qps_val, rpm=rpm_val, calls_per_hour=calls_per_hour)


class RateController:
    def __init__(self, base_qps: float, min_qps: float = 0.1, max_qps: Optional[float] = None) -> None:
        self.base_qps = max(min_qps, base_qps)
        self.min_qps = min_qps
        self.max_qps = max_qps or self.base_qps
        self.current_qps = self.base_qps
        self.success_streak = 0

    def record(self, status_code: Optional[int]) -> None:
        if status_code == 429:
            self.current_qps = max(self.min_qps, self.current_qps * 0.7)
            self.success_streak = 0
            return

        if status_code is None:
            return

        if 200 <= status_code < 300:
            self.success_streak += 1
            if self.success_streak >= 20:
                self.current_qps = min(self.max_qps, self.current_qps * 1.05)
                self.success_streak = 0

    def env_qps(self) -> str:
        return f"{self.current_qps:.6f}"

    def derived_rpm(self) -> int:
        return max(1, int(round(self.current_qps * 60.0)))


@dataclass
class BucketStatsSnapshot:
    acquire_count: int
    wait_ms_total: float
    wait_ms_max: float
    lease_refills: int
    timeouts: int


class SharedTokenBucket:
    def __init__(
        self,
        *,
        state_path: Path,
        lock_path: Path,
        rpm: int,
        burst: int,
        lock_ttl_sec: int,
        lease_size: int = 1,
        max_wait_sec: Optional[float] = None,
    ) -> None:
        if rpm <= 0:
            raise ValueError(f"rpm must be > 0, got {rpm}")
        if burst <= 0:
            raise ValueError(f"burst must be > 0, got {burst}")
        if lock_ttl_sec <= 0:
            raise ValueError(f"lock_ttl_sec must be > 0, got {lock_ttl_sec}")
        if lease_size <= 0:
            raise ValueError(f"lease_size must be > 0, got {lease_size}")
        self.state_path = state_path
        self.lock_path = lock_path
        self.rpm = int(rpm)
        self.burst = int(burst)
        self.lock_ttl_sec = int(lock_ttl_sec)
        self.lease_size = int(lease_size)
        self.max_wait_sec = max_wait_sec
        self.refill_rate = float(self.rpm) / 60.0
        self._lease_remaining = 0
        self._acquire_count = 0
        self._wait_ms_total = 0.0
        self._wait_ms_max = 0.0
        self._lease_refills = 0
        self._timeouts = 0

    def acquire(self) -> None:
        self._acquire_count += 1
        if self._lease_remaining > 0:
            self._lease_remaining -= 1
            return
        deadline = time.time() + self.max_wait_sec if self.max_wait_sec else None
        while True:
            self._acquire_lock(deadline)
            sleep_sec = 0.0
            try:
                now = time.time()
                state = self._load_state(now)
                tokens = self._refill_tokens(state, now)
                if tokens >= 1.0:
                    lease = min(self.lease_size, int(tokens))
                    lease = max(1, lease)
                    state["tokens"] = tokens - float(lease)
                    state["last_ts"] = now
                    self._write_state(state)
                    self._lease_remaining = lease - 1
                    self._lease_refills += 1
                    return
                state["tokens"] = tokens
                state["last_ts"] = now
                self._write_state(state)
                if self.refill_rate > 0:
                    sleep_sec = max((1.0 - tokens) / self.refill_rate, 0.01)
                else:
                    sleep_sec = 0.5
            finally:
                self._release_lock()
            if deadline is not None and time.time() >= deadline:
                self._timeouts += 1
                raise TimeoutError("shared bucket acquire timeout")
            if sleep_sec > 0:
                wait_ms = sleep_sec * 1000.0
                self._wait_ms_total += wait_ms
                if wait_ms > self._wait_ms_max:
                    self._wait_ms_max = wait_ms
                if sleep_sec >= 2.0:
                    _logger.warning("shared bucket wait %.2fs", sleep_sec)
                time.sleep(sleep_sec)

    def _load_state(self, now: float) -> dict:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            return {"tokens": float(self.burst), "last_ts": now}
        try:
            raw = self.state_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            tokens = float(payload.get("tokens", self.burst))
            last_ts = float(payload.get("last_ts", now))
            return {"tokens": tokens, "last_ts": last_ts}
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            _logger.warning("shared bucket state reset: %r", exc)
            return {"tokens": float(min(1, self.burst)), "last_ts": now}

    def _refill_tokens(self, state: dict, now: float) -> float:
        tokens = float(state.get("tokens", 0.0))
        last_ts = float(state.get("last_ts", now))
        elapsed = max(0.0, now - last_ts)
        tokens = min(float(self.burst), tokens + elapsed * self.refill_rate)
        return tokens

    def _write_state(self, state: dict) -> None:
        writer = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "ts": int(time.time()),
        }
        payload = {
            "version": 1,
            "tokens": float(state.get("tokens", 0.0)),
            "last_ts": float(state.get("last_ts", time.time())),
            "rpm": int(self.rpm),
            "burst": int(self.burst),
            "last_writer": writer,
        }
        atomic_write_json(self.state_path, payload)

    def _acquire_lock(self, deadline: Optional[float]) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                with self.lock_path.open("x", encoding="utf-8") as lf:
                    lf.write(f"pid={os.getpid()}\n")
                    lf.write(f"ts={int(time.time())}\n")
                return
            except FileExistsError:
                if self._lock_is_stale():
                    self._break_stale_lock()
                else:
                    if deadline is not None and time.time() >= deadline:
                        raise TimeoutError("shared bucket lock timeout")
                    time.sleep(0.05)

    def _lock_is_stale(self) -> bool:
        try:
            age = time.time() - self.lock_path.stat().st_mtime
        except FileNotFoundError:
            return False
        return age > float(self.lock_ttl_sec)

    def _break_stale_lock(self) -> None:
        ts = int(time.time())
        stale = self.lock_path.with_name(f"{self.lock_path.name}.stale.{ts}")
        try:
            self.lock_path.rename(stale)
        except FileNotFoundError:
            return

    def _release_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            return

    def stats_snapshot(self) -> BucketStatsSnapshot:
        return BucketStatsSnapshot(
            acquire_count=self._acquire_count,
            wait_ms_total=self._wait_ms_total,
            wait_ms_max=self._wait_ms_max,
            lease_refills=self._lease_refills,
            timeouts=self._timeouts,
        )


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception as exc:
        raise RuntimeError(f"shared bucket state write failed: {exc}") from exc
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def load_bucket_from_env(repo_root: Path) -> Optional[SharedTokenBucket]:
    raw = (os.environ.get("FINMIND_SHARED_BUCKET") or "").strip().lower()
    if raw in ("", "0", "false", "no", "off"):
        return None

    state_path_raw = (os.environ.get("FINMIND_BUCKET_STATE_PATH") or "").strip()
    rpm_raw = (os.environ.get("FINMIND_BUCKET_RPM") or "").strip()
    burst_raw = (os.environ.get("FINMIND_BUCKET_BURST") or "").strip()
    ttl_raw = (os.environ.get("FINMIND_BUCKET_LOCK_TTL_SEC") or "").strip()
    lease_raw = (os.environ.get("FINMIND_BUCKET_LEASE_SIZE") or "").strip()
    max_wait_raw = (os.environ.get("FINMIND_BUCKET_MAX_WAIT_SEC") or "").strip()

    if not state_path_raw or not rpm_raw or not burst_raw or not ttl_raw:
        raise ValueError("shared bucket env missing: FINMIND_BUCKET_STATE_PATH/RPM/BURST/LOCK_TTL_SEC")

    try:
        rpm = int(float(rpm_raw))
    except ValueError as exc:
        raise ValueError(f"invalid FINMIND_BUCKET_RPM: {rpm_raw!r}") from exc
    try:
        burst = int(float(burst_raw))
    except ValueError as exc:
        raise ValueError(f"invalid FINMIND_BUCKET_BURST: {burst_raw!r}") from exc
    try:
        lock_ttl_sec = int(float(ttl_raw))
    except ValueError as exc:
        raise ValueError(f"invalid FINMIND_BUCKET_LOCK_TTL_SEC: {ttl_raw!r}") from exc
    if lease_raw:
        try:
            lease_size = int(float(lease_raw))
        except ValueError as exc:
            raise ValueError(f"invalid FINMIND_BUCKET_LEASE_SIZE: {lease_raw!r}") from exc
    else:
        lease_size = 1
    if max_wait_raw:
        try:
            max_wait_sec = float(max_wait_raw)
        except ValueError as exc:
            raise ValueError(f"invalid FINMIND_BUCKET_MAX_WAIT_SEC: {max_wait_raw!r}") from exc
    else:
        max_wait_sec = None

    state_path = Path(state_path_raw)
    if not state_path.is_absolute():
        state_path = repo_root / state_path
    lock_path = paths.finmind_bucket_lock_path(repo_root)
    return SharedTokenBucket(
        state_path=state_path,
        lock_path=lock_path,
        rpm=rpm,
        burst=burst,
        lock_ttl_sec=lock_ttl_sec,
        lease_size=lease_size,
        max_wait_sec=max_wait_sec,
    )
