from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import hashlib
import json
import os
import time

from . import paths


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def stable_hash_sid(sid: str) -> int:
    return int(hashlib.md5(sid.encode("utf-8")).hexdigest(), 16)


def stable_hash_universe(sids: Iterable[str]) -> str:
    items = sorted({s.strip() for s in sids if s and s.strip()})
    payload = "\n".join(items)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def resolve_policy(policy: str, run_type: str) -> str:
    raw = (policy or "auto").strip().lower()
    if raw == "auto":
        return "full" if (run_type or "").strip().lower() == "backfill" else "sharded"
    if raw in ("full", "sharded", "ttl"):
        return raw
    raise ValueError(f"invalid dividend policy: {policy!r}")


@dataclass(frozen=True)
class DividendPlan:
    todo: List[str]
    meta: Dict[str, object]


class DividendTodoPlanner:
    def __init__(self, trading_days: List[date]) -> None:
        if not trading_days:
            raise ValueError("trading_days is required")
        self.trading_days = sorted(set(trading_days))
        self.index_map = {d: i for i, d in enumerate(self.trading_days)}

    def trading_index(self, day: date) -> int:
        if day not in self.index_map:
            raise ValueError(f"day not in trading calendar: {day.isoformat()}")
        return self.index_map[day]

    def build_plan(
        self,
        *,
        day: date,
        universe: Iterable[str],
        policy: str,
        shard_count: int,
        max_staleness_trading_days: int,
        state: Dict[str, object],
    ) -> DividendPlan:
        if shard_count <= 0:
            raise ValueError("shard_count must be > 0")
        if max_staleness_trading_days <= 0:
            max_staleness_trading_days = shard_count

        trading_index = self.trading_index(day)
        shard_index = trading_index % shard_count

        universe_items = sorted({s.strip() for s in universe if s and s.strip()})
        universe_hash = stable_hash_universe(universe_items)

        last_checked = state.get("sid_last_checked_trading_index", {})
        if not isinstance(last_checked, dict):
            last_checked = {}

        shard_set: List[str] = []
        overdue_set: List[str] = []

        for sid in universe_items:
            if stable_hash_sid(sid) % shard_count == shard_index:
                shard_set.append(sid)
            last_idx = last_checked.get(sid)
            try:
                last_idx_val = int(last_idx)
            except Exception:
                last_idx_val = None
            if last_idx_val is None or (trading_index - last_idx_val) >= max_staleness_trading_days:
                overdue_set.append(sid)

        if policy == "full":
            todo = list(universe_items)
        elif policy == "ttl":
            todo = sorted(set(overdue_set))
        elif policy == "sharded":
            todo = sorted(set(shard_set) | set(overdue_set))
        else:
            raise ValueError(f"invalid policy: {policy!r}")

        meta = {
            "day": day.isoformat(),
            "policy": policy,
            "shard_count": shard_count,
            "shard_index": shard_index,
            "max_staleness_trading_days": max_staleness_trading_days,
            "todo_count": len(todo),
            "todo_shard_count": len(set(shard_set)),
            "todo_ttl_count": len(set(overdue_set)),
            "universe_hash": universe_hash,
            "trading_index": trading_index,
            "universe_size": len(universe_items),
        }
        return DividendPlan(todo=todo, meta=meta)


class DividendScanStateStore:
    def __init__(self, state_path: Path, lock_path: Path) -> None:
        self.state_path = state_path
        self.lock_path = lock_path

    def load(self) -> Tuple[Dict[str, object], bool]:
        if not self.state_path.exists():
            return self._default_state(), False
        try:
            raw = self.state_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except Exception:
            return self._default_state(), True
        if not isinstance(payload, dict):
            return self._default_state(), True
        payload.setdefault("version", 1)
        payload.setdefault("universe_hash", "")
        payload.setdefault("sid_last_checked_trading_index", {})
        payload.setdefault("sid_last_result", {})
        payload.setdefault("updated_at", _now_iso())
        if not isinstance(payload.get("sid_last_checked_trading_index"), dict):
            payload["sid_last_checked_trading_index"] = {}
        if not isinstance(payload.get("sid_last_result"), dict):
            payload["sid_last_result"] = {}
        return payload, False

    def merge_and_save(
        self,
        updates: Dict[str, str],
        *,
        trading_index: int,
        universe_hash: str,
        lock_ttl_sec: int,
        force_reset: bool = False,
    ) -> bool:
        self.acquire_lock(lock_ttl_sec, force_stale_break=True)
        try:
            state, reset = self.load()
            if force_reset or reset:
                state = self._default_state()
                reset = True
            checked = state.setdefault("sid_last_checked_trading_index", {})
            results = state.setdefault("sid_last_result", {})
            for sid, result in updates.items():
                checked[sid] = int(trading_index)
                results[sid] = str(result)
            state["universe_hash"] = universe_hash
            state["updated_at"] = _now_iso()
            state["version"] = 1
            self._save(state)
            return bool(reset or force_reset)
        finally:
            self.release_lock()

    def acquire_lock(self, ttl_sec: int, force_stale_break: bool) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + 30.0
        while True:
            try:
                with self.lock_path.open("x", encoding="utf-8") as lf:
                    lf.write(f"pid={os.getpid()}\n")
                    lf.write(f"ts={int(time.time())}\n")
                return
            except FileExistsError:
                if force_stale_break and self._lock_is_stale(ttl_sec):
                    self._break_stale_lock()
                    continue
                if time.time() >= deadline:
                    raise TimeoutError("dividend scan state lock timeout")
                time.sleep(0.05)

    def release_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            return

    def _lock_is_stale(self, ttl_sec: int) -> bool:
        try:
            age = time.time() - self.lock_path.stat().st_mtime
        except FileNotFoundError:
            return False
        return age > float(ttl_sec)

    def _break_stale_lock(self) -> None:
        ts = int(time.time())
        stale = self.lock_path.with_name(f"{self.lock_path.name}.stale.{ts}")
        try:
            self.lock_path.rename(stale)
        except FileNotFoundError:
            return

    def _default_state(self) -> Dict[str, object]:
        return {
            "version": 1,
            "universe_hash": "",
            "sid_last_checked_trading_index": {},
            "sid_last_result": {},
            "updated_at": _now_iso(),
        }

    def _save(self, state: Dict[str, object]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=True), encoding="utf-8")
        os.replace(tmp, self.state_path)


def write_evidence(path: Path, meta: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(meta)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    os.replace(tmp, path)


def read_evidence(path: Path) -> Optional[Dict[str, object]]:
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def evidence_satisfies(
    evidence: Optional[Dict[str, object]],
    *,
    required_policy: str,
    shard_count: Optional[int] = None,
    max_staleness_trading_days: Optional[int] = None,
) -> bool:
    if not evidence:
        return False
    if evidence.get("policy") != required_policy:
        return False
    if required_policy == "sharded":
        if shard_count is not None and int(evidence.get("shard_count", -1)) != int(shard_count):
            return False
        if (
            max_staleness_trading_days is not None
            and int(evidence.get("max_staleness_trading_days", -1)) != int(max_staleness_trading_days)
        ):
            return False
    if required_policy == "ttl":
        if (
            max_staleness_trading_days is not None
            and int(evidence.get("max_staleness_trading_days", -1)) != int(max_staleness_trading_days)
        ):
            return False
    return True


def evidence_path(repo_root: Path, day: date) -> Path:
    return paths.dividend_evidence_path(repo_root, day)
