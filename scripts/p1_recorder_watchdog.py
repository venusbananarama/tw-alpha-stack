from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def is_pid_alive_windows(pid: int) -> bool:
    import ctypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
    if not h:
        return False
    ctypes.windll.kernel32.CloseHandle(h)
    return True


def is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return is_pid_alive_windows(pid)
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_audit(audit_dir: Path, payload: dict) -> None:
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = audit_dir / f"recorder_watchdog_{stamp}.json"
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lock-path", required=True)
    ap.add_argument("--stop-token-path", required=True)
    ap.add_argument("--start-task-fullname", required=True, help=r'Like "\AlphaCity\Phase1_MarketData\Fubon\_P1_REC_...START_0830"')
    ap.add_argument("--audit-dir", default="reports/phase1/audit")
    args = ap.parse_args(argv)

    repo = Path(__file__).resolve().parents[1]
    lock = Path(args.lock_path)
    stop_token = Path(args.stop_token_path)
    audit_dir = Path(args.audit_dir)
    if not audit_dir.is_absolute():
        audit_dir = repo / audit_dir

    # 若 stop token 存在：代表正在停機或已停機  watchdog 不介入
    if stop_token.exists():
        return 0

    if not lock.exists():
        return 0

    lock_payload = read_json(lock) or {}
    pid = int(lock_payload.get("pid") or 0)
    alive = is_pid_alive(pid)

    if alive:
        return 0

    # pid 不在了：把 lock 搬成 stale，然後觸發 start task 重新拉起
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    stale = lock.with_name(lock.name + f".stale.watchdog.{ts}")
    try:
        lock.rename(stale)
    except Exception:
        # 退一步：刪不掉就不啟動，避免誤判造成重入
        write_audit(
            audit_dir,
            {
                "at_utc": now_iso(),
                "hostname": socket.gethostname(),
                "action": "lock_rename_failed_no_restart",
                "lock": str(lock),
                "pid": pid,
                "lock_payload": lock_payload,
            },
        )
        return 2

    # 觸發 start task
    cmd = ["schtasks", "/run", "/tn", args.start_task_fullname]
    rc = subprocess.call(cmd)

    write_audit(
        audit_dir,
        {
            "at_utc": now_iso(),
            "hostname": socket.gethostname(),
            "action": "restart_triggered",
            "lock_stale": str(stale),
            "pid": pid,
            "schtasks_rc": rc,
            "start_task": args.start_task_fullname,
        },
    )
    return 0 if rc == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
