from __future__ import annotations

import errno
import json
import logging
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

_LOG = logging.getLogger(__name__)
_LOCK_VERSION = 1


class LockActiveError(RuntimeError):
    def __init__(self, path: Path, info: Optional[Dict[str, Any]] = None) -> None:
        message = f"lock active: {path}"
        if info:
            pid = info.get("pid")
            host = info.get("hostname")
            created_at = info.get("created_at_utc")
            message = f"{message} pid={pid} host={host} created_at_utc={created_at}"
        super().__init__(message)
        self.path = path
        self.info = info or {}


def _utcnow() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def _default_command() -> str:
    try:
        parts = [str(item) for item in sys.argv if item is not None]
    except Exception:
        return ""
    return " ".join(parts).strip()


def _safe_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _parse_created_at(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1]
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _read_lock_payload(lock_path: Path) -> Optional[Dict[str, Any]]:
    try:
        text = lock_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    try:
        obj = json.loads(text)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def _age_minutes_from_dt(dt: datetime, now: datetime) -> float:
    return (now - dt).total_seconds() / 60.0


def _mtime_age_minutes(lock_path: Path, now: datetime) -> Optional[float]:
    try:
        ts = lock_path.stat().st_mtime
    except Exception:
        return None
    return _age_minutes_from_dt(datetime.utcfromtimestamp(ts), now)


def _is_same_host(hostname: object) -> bool:
    if not isinstance(hostname, str):
        return False
    try:
        return hostname.strip().lower() == socket.gethostname().strip().lower()
    except Exception:
        return False


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _is_pid_alive_windows(pid)
    return _is_pid_alive_posix(pid)


def _is_pid_alive_windows(pid: int) -> bool:
    return True


def _is_pid_alive_posix(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _assess_lock(lock_path: Path, ttl_minutes: int) -> Dict[str, Any]:
    now = _utcnow()
    info: Dict[str, Any] = {
        "path": str(lock_path),
        "ttl_minutes": ttl_minutes,
        "checked_at_utc": now.isoformat(),
    }
    payload = _read_lock_payload(lock_path)
    info["source"] = "json" if payload else "legacy"
    info["lock_payload"] = payload
    if payload:
        info["pid"] = _safe_int(payload.get("pid"))
        info["hostname"] = payload.get("hostname")
        info["created_at_utc"] = payload.get("created_at_utc")
        info["command"] = payload.get("command")
    else:
        info["pid"] = None
        info["hostname"] = None
        info["created_at_utc"] = None
        info["command"] = None
    created_dt = _parse_created_at(info.get("created_at_utc"))
    age_minutes = _age_minutes_from_dt(created_dt, now) if created_dt else None
    if age_minutes is None:
        age_minutes = _mtime_age_minutes(lock_path, now)
    info["age_minutes"] = age_minutes

    stale = False
    reason = None
    if ttl_minutes <= 0:
        stale = True
        reason = "ttl_expired"
    else:
        if _is_same_host(info.get("hostname")):
            pid = info.get("pid")
            if isinstance(pid, int) and not _is_pid_alive(pid):
                stale = True
                reason = "pid_dead"
        if not stale and age_minutes is not None and age_minutes >= ttl_minutes:
            stale = True
            reason = "ttl_expired" if payload else "legacy_ttl_expired"
    info["stale"] = stale
    info["reason"] = reason
    return info


def break_stale_lock(lock_path: Path | str, ttl_minutes: int) -> Optional[Dict[str, Any]]:
    path = Path(lock_path)
    if not path.exists():
        return None
    info = _assess_lock(path, ttl_minutes)
    if not info.get("stale"):
        return None
    try:
        path.unlink()
        info["removed"] = True
    except FileNotFoundError:
        info["removed"] = True
    except Exception as exc:
        info["removed"] = False
        info["error"] = f"{type(exc).__name__}: {exc}"
    return info


class FileLock:
    def __init__(
        self,
        lock_path: Path | str,
        *,
        ttl_minutes: int,
        auto_break_stale: bool = False,
        force_break: bool = False,
        command: Optional[str] = None,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.ttl_minutes = int(ttl_minutes)
        self.auto_break_stale = bool(auto_break_stale)
        self.force_break = bool(force_break)
        self.command = command if command is not None else _default_command()
        self._acquired = False
        self.info: Dict[str, Any] = {}

    def acquire(self) -> None:
        if self._acquired:
            return
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(2):
            payload = self._build_payload()
            try:
                self._write_payload(payload)
                self._acquired = True
                self.info = payload
                return
            except FileExistsError:
                if self.force_break:
                    self._force_break_existing()
                    continue
                if not self.auto_break_stale:
                    self._raise_active()
                stale_info = break_stale_lock(self.lock_path, self.ttl_minutes)
                if stale_info and stale_info.get("removed"):
                    _LOG.info(
                        "stale lock removed path=%s reason=%s pid=%s host=%s created_at_utc=%s",
                        self.lock_path,
                        stale_info.get("reason"),
                        stale_info.get("pid"),
                        stale_info.get("hostname"),
                        stale_info.get("created_at_utc"),
                    )
                    continue
                if not self.lock_path.exists():
                    continue
                if stale_info and not stale_info.get("removed"):
                    self._raise_active(extra=stale_info)
                self._raise_active()
            except Exception:
                if self.lock_path.exists():
                    try:
                        self.lock_path.unlink()
                    except Exception:
                        pass
                raise
        self._raise_active()

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            self.lock_path.unlink(missing_ok=True)
        except Exception:
            return
        finally:
            self._acquired = False

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def _build_payload(self) -> Dict[str, Any]:
        created_at = _utcnow().isoformat()
        pid = os.getpid()
        hostname = socket.gethostname()
        lock_id = f"{hostname}:{pid}:{created_at}"
        return {
            "version": _LOCK_VERSION,
            "lock_id": lock_id,
            "pid": pid,
            "hostname": hostname,
            "created_at_utc": created_at,
            "command": self.command,
            "ttl_minutes": self.ttl_minutes,
        }

    def _write_payload(self, payload: Dict[str, Any]) -> None:
        text = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
        with self.lock_path.open("x", encoding="utf-8") as f:
            f.write(text)
            f.write("\n")

    def _raise_active(self, extra: Optional[Dict[str, Any]] = None) -> None:
        info = _read_lock_payload(self.lock_path) or {}
        if extra:
            info["stale_error"] = extra.get("error")
        raise LockActiveError(self.lock_path, info)

    def _force_break_existing(self) -> None:
        info = _read_lock_payload(self.lock_path) or {}
        try:
            self.lock_path.unlink()
            _LOG.info(
                "lock removed by force path=%s pid=%s host=%s created_at_utc=%s",
                self.lock_path,
                info.get("pid"),
                info.get("hostname"),
                info.get("created_at_utc"),
            )
        except FileNotFoundError:
            return
        except Exception:
            self._raise_active()
