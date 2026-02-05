from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

from . import dividend_scan, rate_control, paths, silver_writer

LogFn = Callable[[str, bool], None]

_SHARED_BUCKET: Optional[rate_control.SharedTokenBucket] = None
_SHARED_BUCKET_READY = False


class IngestError(RuntimeError):
    pass


class RateLimitError(IngestError):
    pass


class ConfigError(IngestError):
    pass


class ApiError(IngestError):
    pass


class SchemaError(IngestError):
    pass


class NetworkError(IngestError):
    pass


@dataclass
class IngestResult:
    dataset: str
    day: str
    rows_written: int
    output_paths: List[Path]
    skipped: bool
    duration_sec: float


@dataclass(frozen=True)
class DateIdDatasetSpec:
    key: str
    finmind_name: str
    output_root: Path
    min_date: dt.date
    enabled: bool
    frequency: Optional[str] = None
    required_columns: Optional[List[str]] = None


HHF_DATASET_ALIASES: Dict[str, str] = {
    "prices": "TaiwanStockPrice",
    "price": "TaiwanStockPrice",
    "taiwanstockprice": "TaiwanStockPrice",
    "chip": "TaiwanStockInstitutionalInvestorsBuySell",
    "institutional": "TaiwanStockInstitutionalInvestorsBuySell",
    "per": "TaiwanStockPER",
    "taiwanstockper": "TaiwanStockPER",
    "dividend": "TaiwanStockDividend",
    "taiwanstockdividend": "TaiwanStockDividend",
}

DATEID_EXPECTED_KEYS = {
    "finstmt",
    "bs",
    "cfs",
    "shareholding",
    "inst_total",
    "gov_bank",
}

DATEID_TRADING_DATASETS = {"shareholding", "inst_total", "gov_bank"}

DATEID_ALLOW_SYNTHETIC_STOCK_ID = {"inst_total", "gov_bank"}


def _log(log: Optional[LogFn], message: str, *, err: bool = False) -> None:
    if log:
        log(message, err)


def _shared_bucket() -> Optional[rate_control.SharedTokenBucket]:
    global _SHARED_BUCKET
    global _SHARED_BUCKET_READY
    if not _SHARED_BUCKET_READY:
        _SHARED_BUCKET = rate_control.load_bucket_from_env(paths.repo_root())
        _SHARED_BUCKET_READY = True
    return _SHARED_BUCKET


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f"invalid date {value!r}") from exc


def _get_env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _base_url() -> str:
    return os.environ.get("FINMIND_BASE_URL", "https://api.finmindtrade.com/api/v4/data")


def _require_token(env_name: str = "FINMIND_TOKEN") -> str:
    token = (os.environ.get(env_name) or "").strip()
    if not token:
        raise ConfigError(f"{env_name} is not set")
    return token


def _resolve_bearer_token(env_name: str) -> str:
    token = (os.environ.get(env_name) or "").strip()
    if token:
        return token
    return _require_token("FINMIND_TOKEN")


def _resolve_hhf_dataset(name: str) -> Tuple[str, str]:
    key = (name or "").strip()
    if not key:
        raise ConfigError("dataset is required")
    lower = key.lower()
    finmind = HHF_DATASET_ALIASES.get(lower, key)
    kind = _dataset_to_kind(finmind)
    return finmind, kind


def _dataset_to_kind(ds: str) -> str:
    lower = ds.lower()
    if "price" in lower:
        return "prices"
    if "buysell" in lower or "institutional" in lower:
        return "chip"
    if ("per" in lower and "taiwanstockper" in lower) or lower.endswith("per"):
        return "per"
    if "dividend" in lower:
        return "dividend"
    return "prices"


def _resolve_calls_cap(calls_per_hour: Optional[int]) -> Optional[float]:
    if calls_per_hour is None:
        return None
    if calls_per_hour <= 0:
        return None
    return float(calls_per_hour) / 3600.0


def _resolve_hhf_qps(kind: str, calls_per_hour: Optional[int]) -> float:
    base_raw = os.environ.get("FINMIND_QPS", "1.5")
    kind_key = {
        "prices": "PRICES",
        "chip": "CHIP",
        "per": "PER",
        "dividend": "DIVIDEND",
    }.get(kind)
    raw = os.environ.get(f"FINMIND_QPS_{kind_key}") if kind_key else None
    if not raw:
        raw = base_raw
    try:
        qps = float(raw)
    except ValueError:
        qps = 1.5
    if qps <= 0:
        qps = 1.5
    cap = _resolve_calls_cap(calls_per_hour)
    if cap is not None:
        qps = min(qps, cap)
    return qps


def _resolve_dateid_qps(calls_per_hour: Optional[int]) -> float:
    qps = _get_env_float("FINMIND_QPS", 1.5)
    if qps <= 0:
        qps = 1.5
    cap = _resolve_calls_cap(calls_per_hour)
    if cap is not None:
        qps = min(qps, cap)
    return qps


class AdaptiveRateLimiter:
    def __init__(self, qps: float, min_qps: float = 0.05) -> None:
        self.qps = max(min_qps, qps)
        self.min_qps = min_qps
        self.last_ts = 0.0

    def wait(self) -> None:
        if self.qps <= 0:
            return
        now = time.monotonic()
        if self.last_ts > 0:
            min_interval = 1.0 / self.qps
            elapsed = now - self.last_ts
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self.last_ts = time.monotonic()

    def backoff(self) -> None:
        self.qps = max(self.min_qps, self.qps * 0.7)


def _retry_delays() -> List[int]:
    return [3, 5, 8, 13, 21, 34]


def _http_get_json(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout_sec: float = 30.0,
) -> Dict[str, object]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        bucket = _shared_bucket()
        if bucket is not None:
            bucket.acquire()
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload_bytes = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            raise RateLimitError(f"429 rate limit: {exc}") from exc
        if exc.code >= 500:
            raise NetworkError(f"http {exc.code}: {exc}") from exc
        raise ApiError(f"http {exc.code}: {exc}") from exc
    except urllib.error.URLError as exc:
        raise NetworkError(f"url error: {exc}") from exc

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise SchemaError(f"invalid json: {exc}") from exc

    status = payload.get("status")
    msg = str(payload.get("msg", "") or "")
    if status not in (200, "200", None):
        msg_lower = msg.lower()
        if "too many" in msg_lower or "rate limit" in msg_lower:
            raise RateLimitError(f"429 rate limit: status={status} msg={msg}")
        raise ApiError(f"finmind error: status={status} msg={msg}")

    return payload


def _extract_data(payload: Dict[str, object]) -> List[Dict[str, object]]:
    data = payload.get("data")
    if not data:
        return []
    if not isinstance(data, list):
        raise SchemaError(f"unexpected data type: {type(data)}")
    rows: List[Dict[str, object]] = []
    for row in data:
        if not isinstance(row, dict):
            raise SchemaError(f"unexpected row type: {type(row)}")
        rows.append(dict(row))
    return rows


def _fetch_with_retry(
    fetch_fn: Callable[[], List[Dict[str, object]]],
    rate: AdaptiveRateLimiter,
    log: Optional[LogFn],
    *,
    retries: int,
) -> List[Dict[str, object]]:
    delays = _retry_delays()
    attempt = 0
    while True:
        try:
            rate.wait()
            return fetch_fn()
        except RateLimitError as exc:
            rate.backoff()
            attempt += 1
            if attempt > retries:
                raise
            delay = delays[min(attempt - 1, len(delays) - 1)]
            _log(log, f"[retry] 429 rate limit attempt={attempt}/{retries} sleep={delay}s", err=True)
            time.sleep(delay)
        except (NetworkError, ApiError, SchemaError) as exc:
            attempt += 1
            if attempt > retries:
                raise
            delay = delays[min(attempt - 1, len(delays) - 1)]
            _log(log, f"[retry] error={exc!r} attempt={attempt}/{retries} sleep={delay}s", err=True)
            time.sleep(delay)


def _normalize_hhf_frame(df: pd.DataFrame, end_exclusive: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        dt_series = pd.to_datetime(out["date"], errors="coerce")
        mask = dt_series < pd.to_datetime(end_exclusive)
        out = out.loc[mask].copy()
        out["date"] = dt_series.loc[mask].dt.strftime("%Y-%m-%d")
    if "stock_id" in out.columns:
        out["stock_id"] = out["stock_id"].astype(str)
        out["symbol"] = out["stock_id"].astype(str).str.replace(".TW", "", regex=False)
    return out


def _normalize_dateid_frame(
    df: pd.DataFrame,
    spec: DateIdDatasetSpec,
    day: dt.date,
    request_stock_id: Optional[str],
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "stock_id" not in out.columns:
        if "data_id" in out.columns:
            out = out.rename(columns={"data_id": "stock_id"})
        elif spec.key in DATEID_ALLOW_SYNTHETIC_STOCK_ID and request_stock_id:
            out["stock_id"] = request_stock_id
        else:
            raise SchemaError(
                f"dataset {spec.key} missing stock_id (or data_id) column"
            )

    if "date" not in out.columns:
        out["date"] = day.isoformat()
    else:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)

    if spec.required_columns:
        missing = [c for c in spec.required_columns if c not in out.columns]
        if missing:
            raise SchemaError(
                f"dataset {spec.key} missing required columns: {', '.join(missing)}"
            )

    out = out.drop_duplicates()
    return out


def _load_yaml_dict(path: Path) -> Mapping[str, object]:
    if not path.exists():
        raise ConfigError(f"config not found: {path}")
    if yaml is None:
        raise ConfigError("PyYAML is required to read dataset config")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        raise ConfigError(f"failed to parse yaml {path}: {exc}") from exc
    if not isinstance(data, Mapping):
        raise ConfigError(f"yaml root of {path} must be a mapping")
    return data


def load_dateid_specs(config_path: Optional[Path]) -> Dict[str, DateIdDatasetSpec]:
    if config_path is None:
        candidates = [
            Path("config") / "dateid_datasets.yaml",
            Path("configs") / "dateid_datasets.yaml",
        ]
        for p in candidates:
            if p.exists():
                config_path = p
                break
        if config_path is None:
            tried = ", ".join(str(p) for p in candidates)
            raise ConfigError(f"dateid_datasets.yaml not found. tried: {tried}")

    data = _load_yaml_dict(config_path)
    node = data.get("dateid_datasets")
    if not isinstance(node, Mapping):
        raise ConfigError(f"config {config_path} missing dateid_datasets mapping")

    specs: Dict[str, DateIdDatasetSpec] = {}
    for key, cfg in node.items():
        if not isinstance(cfg, Mapping):
            raise ConfigError(f"dataset entry {key!r} must be mapping")
        dataset_key = str(cfg.get("key") or key).strip()
        if not dataset_key:
            raise ConfigError(f"dataset entry {key!r} missing key")

        finmind_name = str(cfg.get("finmind_name") or "").strip()
        if not finmind_name:
            raise ConfigError(f"dataset {dataset_key!r} missing finmind_name")

        output_root_raw = cfg.get("output_root")
        if not isinstance(output_root_raw, str) or not output_root_raw:
            raise ConfigError(f"dataset {dataset_key!r} missing output_root")
        output_root = Path(output_root_raw)

        min_date_raw = str(cfg.get("min_date") or "").strip()
        if not min_date_raw:
            raise ConfigError(f"dataset {dataset_key!r} missing min_date")
        try:
            min_date = dt.date.fromisoformat(min_date_raw)
        except ValueError as exc:
            raise ConfigError(
                f"dataset {dataset_key!r} invalid min_date {min_date_raw!r}"
            ) from exc

        enabled = bool(cfg.get("enabled", True))
        frequency = cfg.get("frequency")
        if frequency is not None:
            frequency = str(frequency)

        req_cols_cfg = cfg.get("required_columns")
        if req_cols_cfg is None:
            required_columns: Optional[List[str]] = None
        elif isinstance(req_cols_cfg, list):
            required_columns = [str(c) for c in req_cols_cfg]
        else:
            raise ConfigError(
                f"dataset {dataset_key!r} invalid required_columns"
            )

        specs[dataset_key] = DateIdDatasetSpec(
            key=dataset_key,
            finmind_name=finmind_name,
            output_root=output_root,
            min_date=min_date,
            enabled=enabled,
            frequency=frequency,
            required_columns=required_columns,
        )

    missing = sorted(DATEID_EXPECTED_KEYS - set(specs.keys()))
    extra = sorted(set(specs.keys()) - DATEID_EXPECTED_KEYS)
    if missing:
        raise ConfigError(f"config missing dataset(s): {', '.join(missing)}")
    if extra:
        raise ConfigError(f"config has unknown dataset(s): {', '.join(extra)}")

    return specs


def get_dateid_spec(dataset_key: str, specs: Mapping[str, DateIdDatasetSpec]) -> DateIdDatasetSpec:
    try:
        spec = specs[dataset_key]
    except KeyError as exc:
        raise ConfigError(f"unknown dataset key {dataset_key!r}") from exc
    if not spec.enabled:
        raise ConfigError(f"dataset {dataset_key!r} disabled by config")
    return spec


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
    except Exception:
        return None
    if df.empty:
        return None
    return df


def load_trading_days(path: Path) -> List[dt.date]:
    df = _safe_read_csv(path)
    if df is None:
        return []
    col = "date" if "date" in df.columns else df.columns[0]
    try:
        s = pd.to_datetime(df[col], errors="coerce").dt.date
    except Exception:
        return []
    dates = [d for d in s.tolist() if isinstance(d, dt.date)]
    return sorted(set(dates))


def resolve_trading_calendar_path(repo_root: Path, datahub_root: Path) -> Optional[Path]:
    candidates = [
        datahub_root / "ref" / "trading_days.csv",
        repo_root / "datahub" / "ref" / "trading_days.csv",
        repo_root / "cal" / "trading_days.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def last_trading_day_before(end_day: dt.date, trading_days: List[dt.date]) -> dt.date:
    if not trading_days:
        return end_day - dt.timedelta(days=1)
    candidates = [d for d in trading_days if d < end_day]
    if not candidates:
        return end_day - dt.timedelta(days=1)
    return max(candidates)


def has_trading_day_between(
    start: dt.date,
    end: dt.date,
    trading_days: List[dt.date],
) -> bool:
    if not trading_days:
        return True
    for d in trading_days:
        if d < start:
            continue
        if d >= end:
            break
        return True
    return False


def is_trading_day(date: dt.date, trading_days: Optional[pd.DatetimeIndex]) -> bool:
    if trading_days is not None:
        return pd.Timestamp(date).normalize() in trading_days
    return date.weekday() < 5


_TRADING_DAYS_CACHE: Optional[pd.DatetimeIndex] = None
_TRADING_DAYS_ROOT: Optional[Path] = None


def load_trading_days_for_dateid(datahub_root: Path) -> Optional[pd.DatetimeIndex]:
    global _TRADING_DAYS_CACHE, _TRADING_DAYS_ROOT
    if _TRADING_DAYS_CACHE is not None and _TRADING_DAYS_ROOT == datahub_root:
        return _TRADING_DAYS_CACHE

    path = datahub_root / "ref" / "trading_days.csv"
    if not path.exists():
        _TRADING_DAYS_CACHE = None
        _TRADING_DAYS_ROOT = datahub_root
        return None

    try:
        df = pd.read_csv(path)
    except Exception:
        _TRADING_DAYS_CACHE = None
        _TRADING_DAYS_ROOT = datahub_root
        return None
    if df.empty:
        _TRADING_DAYS_CACHE = None
        _TRADING_DAYS_ROOT = datahub_root
        return None

    col = "date" if "date" in df.columns else df.columns[0]
    try:
        s = pd.to_datetime(df[col], errors="coerce")
    except Exception:
        _TRADING_DAYS_CACHE = None
        _TRADING_DAYS_ROOT = datahub_root
        return None

    s = s.dropna()
    if s.empty:
        _TRADING_DAYS_CACHE = None
        _TRADING_DAYS_ROOT = datahub_root
        return None

    idx = pd.DatetimeIndex(s.dt.normalize().unique()).sort_values()
    _TRADING_DAYS_CACHE = idx
    _TRADING_DAYS_ROOT = datahub_root
    return idx


def load_pool(root: Path) -> List[str]:
    candidates = [
        root / "investable_universe.txt",
        root / "configs" / "investable_universe.txt",
        root / "universe.tw_all.txt",
    ]
    for p in candidates:
        if p.exists():
            syms: List[str] = []
            with p.open("r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    x = line.strip().replace(".TW", "")
                    if x and len(x) == 4 and x.isdigit():
                        syms.append(x)
            return sorted(set(syms))
    return []


def build_day_index(datahub_root: Path, kind: str, day: dt.date) -> Tuple[set, int]:
    base = datahub_root / "silver" / "alpha" / kind
    ym = f"{day.year:04d}{day.month:02d}"
    patterns = [base / f"yyyymm={ym}"]
    if day.day == 1:
        prev = (day.replace(day=1) - dt.timedelta(days=1))
        patterns.append(base / f"yyyymm={prev.year:04d}{prev.month:02d}")

    files: List[Path] = []
    for p in patterns:
        if p.exists():
            files.extend(p.rglob("*.parquet"))

    seen: set = set()
    for fpath in files:
        try:
            df = pd.read_parquet(fpath, columns=["date", "stock_id"])
        except Exception:
            continue
        if df.empty:
            continue
        s = df["date"].astype(str) == day.isoformat()
        if not s.any():
            continue
        sy = df.loc[s, "stock_id"].astype(str).str.replace(".TW", "", regex=False).tolist()
        seen.update(sy)
    return seen, len(files)


def ingest_hhf(
    *,
    dataset: str,
    day: dt.date,
    repo_root: Path,
    datahub_root: Path,
    calls_per_hour: Optional[int],
    symbols: Optional[List[str]] = None,
    force: bool = False,
    log: Optional[LogFn] = None,
) -> IngestResult:
    t0 = time.monotonic()
    finmind_name, kind = _resolve_hhf_dataset(dataset)
    start = day.isoformat()
    end = (day + dt.timedelta(days=1)).isoformat()

    trading_days: List[dt.date] = []
    cal_path = resolve_trading_calendar_path(repo_root, datahub_root)
    if cal_path:
        trading_days = load_trading_days(cal_path)
        _log(log, f"[cal] loaded {len(trading_days)} days from {cal_path}")
    else:
        _log(log, "[cal] trading_days.csv not found; calendar gating disabled", err=True)

    if trading_days and not has_trading_day_between(day, day + dt.timedelta(days=1), trading_days):
        return IngestResult(
            dataset=dataset,
            day=start,
            rows_written=0,
            output_paths=[],
            skipped=True,
            duration_sec=time.monotonic() - t0,
        )

    if symbols:
        syms = symbols
    else:
        syms = load_pool(repo_root)

    if not syms:
        _log(log, "[pool] empty universe; nothing to ingest", err=True)
        return IngestResult(
            dataset=dataset,
            day=start,
            rows_written=0,
            output_paths=[],
            skipped=True,
            duration_sec=time.monotonic() - t0,
        )

    plan_meta: Optional[Dict[str, object]] = None
    scan_store: Optional[dividend_scan.DividendScanStateStore] = None
    scan_state_reset = False
    scan_updates: Dict[str, str] = {}
    scan_lock_ttl = 120

    if kind == "dividend":
        policy_raw = (os.environ.get("P1_DIVIDEND_SCAN_POLICY") or "auto").strip()
        run_type = (os.environ.get("P1_RUN_TYPE") or "").strip()
        policy_effective = dividend_scan.resolve_policy(policy_raw, run_type)
        shard_count = _get_env_int("P1_DIVIDEND_SHARD_COUNT", 5)
        max_staleness = _get_env_int("P1_DIVIDEND_MAX_STALENESS_TRADING_DAYS", shard_count)
        scan_lock_ttl = _get_env_int("P1_DIVIDEND_SCAN_LOCK_TTL_SEC", 120)
        state_path_raw = (os.environ.get("P1_DIVIDEND_SCAN_STATE_PATH") or "").strip()
        state_path = Path(state_path_raw) if state_path_raw else paths.dividend_scan_state_path(repo_root)
        if not state_path.is_absolute():
            state_path = repo_root / state_path
        scan_store = dividend_scan.DividendScanStateStore(
            state_path=state_path,
            lock_path=paths.dividend_scan_lock_path(repo_root),
        )
        if not trading_days:
            raise ConfigError("dividend scan requires trading calendar")
        planner = dividend_scan.DividendTodoPlanner(trading_days)
        scan_state, scan_state_reset = scan_store.load()
        if force:
            policy_effective = "full"
        plan = planner.build_plan(
            day=day,
            universe=syms,
            policy=policy_effective,
            shard_count=shard_count,
            max_staleness_trading_days=max_staleness,
            state=scan_state,
        )
        todo = plan.todo
        plan_meta = plan.meta
        plan_meta["policy_requested"] = policy_raw
        plan_meta["policy_effective"] = policy_effective
        plan_meta["state_reset"] = bool(scan_state_reset)
        _log(
            log,
            f"[dividend-plan] policy={policy_effective} shard_count={shard_count} "
            f"shard_index={plan_meta.get('shard_index')} ttl={max_staleness} todo={len(todo)}",
        )
    elif not force:
        anchor = last_trading_day_before(day + dt.timedelta(days=1), trading_days)
        covered, fcnt = build_day_index(datahub_root, kind, anchor)
        todo = [s for s in syms if s not in covered]
        _log(
            log,
            f"[index] anchor={anchor.isoformat()} files={fcnt} covered={len(covered)} todo={len(todo)}",
        )
    else:
        todo = list(syms)

    if not todo:
        if kind == "dividend" and plan_meta is not None:
            state_reset = False
            if scan_store is not None:
                state_reset = scan_store.merge_and_save(
                    {},
                    trading_index=int(plan_meta.get("trading_index", 0)),
                    universe_hash=str(plan_meta.get("universe_hash", "")),
                    lock_ttl_sec=scan_lock_ttl,
                    force_reset=scan_state_reset,
                )
            evidence = dict(plan_meta)
            evidence["state_updated"] = False
            evidence["state_reset"] = bool(state_reset)
            dividend_scan.write_evidence(
                dividend_scan.evidence_path(repo_root, day),
                evidence,
            )
        return IngestResult(
            dataset=dataset,
            day=start,
            rows_written=0,
            output_paths=[],
            skipped=True,
            duration_sec=time.monotonic() - t0,
        )

    token = _require_token("FINMIND_TOKEN")
    base_url = _base_url()
    qps = _resolve_hhf_qps(kind, calls_per_hour)
    rate = AdaptiveRateLimiter(qps)
    retries = _get_env_int("FINMIND_MAX_RETRIES", 5)
    timeout_sec = _get_env_float("FINMIND_TIMEOUT_SEC", 30.0)

    errors: List[str] = []
    total_added = 0
    out_paths: List[Path] = []

    for sid in todo:
        params = {
            "dataset": finmind_name,
            "data_id": sid,
            "start_date": start,
            "end_date": end,
            "token": token,
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"

        def _do_fetch() -> List[Dict[str, object]]:
            payload = _http_get_json(url, timeout_sec=timeout_sec)
            return _extract_data(payload)

        try:
            rows = _fetch_with_retry(_do_fetch, rate, log, retries=retries)
            df = pd.DataFrame(rows)
            df = _normalize_hhf_frame(df, end_exclusive=end)
            if df.empty:
                if kind == "dividend":
                    scan_updates[sid] = "empty"
                continue
            out_path = silver_writer.compute_hhf_path(kind, start, datahub_root)
            if "stock_id" in df.columns:
                dedupe_keys = ["date", "stock_id"]
                sort_keys = ["date", "stock_id"]
            elif "symbol" in df.columns:
                dedupe_keys = ["date", "symbol"]
                sort_keys = ["date", "symbol"]
            else:
                dedupe_keys = ["date"] if "date" in df.columns else None
                sort_keys = ["date"] if "date" in df.columns else None
            stats = silver_writer.write_parquet_atomic(
                df,
                out_path,
                dedupe_keys=dedupe_keys,
                sort_keys=sort_keys,
            )
            silver_writer.validate_minimum_output(out_path, stats.rows_added)
            total_added += stats.rows_added
            out_paths.append(out_path)
            if kind == "dividend":
                scan_updates[sid] = "data"
            _log(
                log,
                f"[write] dataset={kind} sid={sid} rows_in={stats.rows_in} rows_added={stats.rows_added} path={out_path}",
            )
        except RateLimitError as exc:
            errors.append(f"{sid} 429 rate limit: {exc}")
            if kind == "dividend":
                scan_updates[sid] = "error"
        except Exception as exc:
            errors.append(f"{sid} error: {exc}")
            if kind == "dividend":
                scan_updates[sid] = "error"

    if kind == "dividend" and plan_meta is not None and scan_store is not None:
        state_reset = scan_store.merge_and_save(
            scan_updates,
            trading_index=int(plan_meta.get("trading_index", 0)),
            universe_hash=str(plan_meta.get("universe_hash", "")),
            lock_ttl_sec=scan_lock_ttl,
            force_reset=scan_state_reset,
        )
        plan_meta["state_updated"] = bool(scan_updates)
        plan_meta["state_reset"] = bool(plan_meta.get("state_reset") or state_reset)

    if errors:
        raise IngestError(f"dataset={dataset} day={start} errors={len(errors)} detail={errors[:3]}")

    if kind == "dividend" and plan_meta is not None:
        dividend_scan.write_evidence(
            dividend_scan.evidence_path(repo_root, day),
            dict(plan_meta),
        )

    return IngestResult(
        dataset=dataset,
        day=start,
        rows_written=total_added,
        output_paths=sorted(set(out_paths)),
        skipped=False,
        duration_sec=time.monotonic() - t0,
    )


def ingest_dateid(
    *,
    dataset: str,
    day: dt.date,
    ids: List[str],
    repo_root: Path,
    datahub_root: Path,
    calls_per_hour: Optional[int],
    config_path: Optional[Path],
    gov_bank_bearer_env: str,
    log: Optional[LogFn] = None,
) -> IngestResult:
    t0 = time.monotonic()
    specs = load_dateid_specs(config_path)
    spec = get_dateid_spec(dataset, specs)

    if day < spec.min_date:
        raise ConfigError(
            f"dataset {spec.key} min_date={spec.min_date.isoformat()} day={day.isoformat()}"
        )

    if spec.key in DATEID_TRADING_DATASETS:
        trading_days = load_trading_days_for_dateid(datahub_root)
        if not is_trading_day(day, trading_days):
            _log(log, f"[calendar] {spec.key} {day.isoformat()} skip non-trading day")
            return IngestResult(
                dataset=dataset,
                day=day.isoformat(),
                rows_written=0,
                output_paths=[],
                skipped=True,
                duration_sec=time.monotonic() - t0,
            )

    base_url = _base_url()
    timeout_sec = _get_env_float("FINMIND_TIMEOUT_SEC", 60.0)
    retries = _get_env_int("FINMIND_MAX_RETRIES", 5)
    qps = _resolve_dateid_qps(calls_per_hour)
    rate = AdaptiveRateLimiter(qps)

    rows: List[Dict[str, object]] = []
    errors: List[str] = []

    if spec.key == "gov_bank":
        bearer = _resolve_bearer_token(gov_bank_bearer_env)
        params = {"dataset": spec.finmind_name, "start_date": day.isoformat()}
        url = f"{base_url}?{urllib.parse.urlencode(params)}"

        def _do_fetch() -> List[Dict[str, object]]:
            payload = _http_get_json(
                url,
                headers={"Authorization": f"Bearer {bearer}"},
                timeout_sec=timeout_sec,
            )
            return _extract_data(payload)

        try:
            rows = _fetch_with_retry(_do_fetch, rate, log, retries=retries)
        except RateLimitError as exc:
            errors.append(f"gov_bank 429 rate limit: {exc}")
        except Exception as exc:
            errors.append(f"gov_bank error: {exc}")
    else:
        token = _require_token("FINMIND_TOKEN")
        frames: List[pd.DataFrame] = []
        for sid in ids:
            stock_id = str(sid).strip()
            if not stock_id:
                continue
            params = {
                "dataset": spec.finmind_name,
                "data_id": stock_id,
                "start_date": day.isoformat(),
                "end_date": day.isoformat(),
                "token": token,
            }
            url = f"{base_url}?{urllib.parse.urlencode(params)}"

            def _do_fetch_id() -> List[Dict[str, object]]:
                try:
                    payload = _http_get_json(url, timeout_sec=timeout_sec)
                except ApiError as exc:
                    if "http 400" in str(exc):
                        return []
                    raise
                return _extract_data(payload)

            try:
                rows_part = _fetch_with_retry(_do_fetch_id, rate, log, retries=retries)
                if not rows_part:
                    continue
                df_part = pd.DataFrame(rows_part)
                df_part = _normalize_dateid_frame(df_part, spec, day, stock_id)
                if df_part.empty:
                    continue
                frames.append(df_part)
            except ApiError as exc:
                msg = str(exc)
                if "http 400" in msg:
                    continue
                errors.append(f"{stock_id} error: {exc}")
            except RateLimitError as exc:
                errors.append(f"{stock_id} 429 rate limit: {exc}")
            except Exception as exc:
                errors.append(f"{stock_id} error: {exc}")

    if errors:
        raise IngestError(f"dataset={dataset} day={day.isoformat()} errors={len(errors)} detail={errors[:3]}")

    if spec.key != "gov_bank":
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        df = pd.DataFrame(rows)
        df = _normalize_dateid_frame(df, spec, day, None)
    if df.empty:
        return IngestResult(
            dataset=dataset,
            day=day.isoformat(),
            rows_written=0,
            output_paths=[],
            skipped=False,
            duration_sec=time.monotonic() - t0,
        )

    out_path = silver_writer.compute_dateid_path(spec.key, day.isoformat(), datahub_root, spec.output_root)
    stats = silver_writer.write_parquet_atomic(
        df,
        out_path,
        dedupe_keys=None,
        sort_keys=["date", "stock_id"],
    )
    silver_writer.validate_minimum_output(out_path, stats.rows_added)
    _log(
        log,
        f"[write] dataset={spec.key} rows_in={stats.rows_in} rows_added={stats.rows_added} path={out_path}",
    )

    return IngestResult(
        dataset=dataset,
        day=day.isoformat(),
        rows_written=stats.rows_added,
        output_paths=[out_path],
        skipped=False,
        duration_sec=time.monotonic() - t0,
    )
