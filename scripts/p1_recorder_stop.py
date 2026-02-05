from __future__ import annotations

import argparse
import ctypes
import json
import os
import signal
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def _resolve_path(value: str) -> tuple[str, Path]:
    path_str = value.strip()
    path_obj = Path(path_str)
    if path_obj.is_absolute():
        return path_str, path_obj
    return path_str, _REPO_ROOT / path_obj


def _safe_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _read_lock_payload(path: Path) -> tuple[dict | None, str | None, str | None]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}", None
    try:
        payload = json.loads(text)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}", text
    if not isinstance(payload, dict):
        return None, "invalid_lock_payload", text
    return payload, None, None


def _is_pid_alive(pid: int | None) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    if os.name == "nt":
        return _is_pid_alive_windows(pid)
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _is_pid_alive_windows(pid: int) -> bool:
    process_query = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(process_query, 0, pid)
    if not handle:
        return False
    ctypes.windll.kernel32.CloseHandle(handle)
    return True


def _terminate_pid(pid: int | None) -> dict:
    if not isinstance(pid, int) or pid <= 0:
        return {"attempted": False, "success": False, "error": "pid_missing"}
    if os.name == "nt":
        return _terminate_pid_windows(pid)
    try:
        os.kill(pid, signal.SIGTERM)
        return {"attempted": True, "success": True, "error": None}
    except Exception as exc:
        return {"attempted": True, "success": False, "error": f"{type(exc).__name__}: {exc}"}


def _terminate_pid_windows(pid: int) -> dict:
    process_terminate = 0x0001
    handle = ctypes.windll.kernel32.OpenProcess(process_terminate, 0, pid)
    if not handle:
        return {"attempted": True, "success": False, "error": "open_process_failed"}
    try:
        result = ctypes.windll.kernel32.TerminateProcess(handle, 1)
        if result == 0:
            return {"attempted": True, "success": False, "error": "terminate_failed"}
        return {"attempted": True, "success": True, "error": None}
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


def _write_stop_token(path: Path) -> tuple[bool, dict | None, str | None]:
    payload = {
        "requested_at_utc": _now_iso(),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.write("\n")
        return True, payload, None
    except Exception as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


def _remove_lock(path: Path) -> tuple[bool, str | None]:
    try:
        if not path.exists():
            return True, None
        path.unlink()
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _write_report(report_path: Path, report: dict) -> tuple[bool, str | None]:
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.write("\n")
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stop P1 recorder with audit report.")
    parser.add_argument("--lock-path", required=True, help="Recorder lock path.")
    parser.add_argument("--stop-token-path", required=True, help="Stop token path.")
    parser.add_argument("--grace-seconds", type=int, default=60, help="Grace seconds.")
    parser.add_argument("--report-dir", default="reports/phase1/audit", help="Report directory.")
    args = parser.parse_args(argv)

    lock_path, lock_abs = _resolve_path(args.lock_path)
    stop_token_path, stop_abs = _resolve_path(args.stop_token_path)
    report_dir, report_dir_abs = _resolve_path(args.report_dir)

    report: dict = {
        "lock_path": lock_path,
        "stop_token_path": stop_token_path,
        "report_dir": report_dir,
        "grace_seconds": int(args.grace_seconds),
        "requested_at_utc": _now_iso(),
        "stop_token_written": False,
        "stop_token_error": None,
        "stop_token_removed": None,
        "stop_token_remove_error": None,
        "lock_payload": None,
        "lock_raw": None,
        "lock_error": None,
        "pid": None,
        "pid_alive_after_grace": None,
        "pid_alive_after_terminate": None,
        "hard_kill": False,
        "hard_kill_success": None,
        "hard_kill_error": None,
        "lock_removed": None,
        "lock_remove_error": None,
        "finished_at_utc": None,
        "report_path": None,
    }

    report_path = None
    try:
        written, payload, err = _write_stop_token(stop_abs)
        report["stop_token_written"] = written
        report["stop_token_error"] = err
        report["stop_token_payload"] = payload

        lock_payload, lock_error, lock_raw = _read_lock_payload(lock_abs)
        report["lock_payload"] = lock_payload
        report["lock_error"] = lock_error
        report["lock_raw"] = lock_raw
        pid = _safe_int(lock_payload.get("pid") if isinstance(lock_payload, dict) else None)
        report["pid"] = pid

        time.sleep(max(0, int(args.grace_seconds)))

        alive_after_grace = _is_pid_alive(pid)
        report["pid_alive_after_grace"] = alive_after_grace
        if alive_after_grace:
            result = _terminate_pid(pid)
            report["hard_kill"] = bool(result.get("attempted"))
            report["hard_kill_success"] = result.get("success")
            report["hard_kill_error"] = result.get("error")
            time.sleep(2)
            report["pid_alive_after_terminate"] = _is_pid_alive(pid)
        else:
            report["hard_kill"] = False

        removed, remove_error = _remove_lock(lock_abs)
        report["lock_removed"] = removed
        report["lock_remove_error"] = remove_error

        removed2, remove_error2 = _remove_lock(stop_abs)
        report["stop_token_removed"] = removed2
        report["stop_token_remove_error"] = remove_error2
    finally:
        report["finished_at_utc"] = _now_iso()
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base = Path(lock_path).stem or "recorder"
        report_path = report_dir_abs / f"{base}_stop_{stamp}.json"
        report["report_path"] = str(report_path)
        _write_report(report_path, report)

    print(f"stop_report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
