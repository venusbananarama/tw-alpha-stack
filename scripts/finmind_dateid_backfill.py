#!/usr/bin/env python
"""
finmind_dateid_backfill.py

DateID ingestion engine for Phase-1.

Responsibilities:
- Load dataset specifications from config/dateid_datasets.yaml
- For a single (dataset, date, ids batch), fetch data from FinMind v4 API
- Normalize schema and append/write Parquet under datahub/silver/alpha/<dataset>/yyyymm=YYYYMM/
- Exit with non-zero status on any hard error, leaving higher-level retry logic to callers.

This script is intended to be called by PowerShell FullMarket-DateID.ps1.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Any

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DateIdConfigError(Exception):
    """Configuration or dataset spec problem."""


class DateIdApiError(Exception):
    """FinMind API or network problem."""


class DateIdSchemaError(Exception):
    """Returned data does not match expected schema."""


# ---------------------------------------------------------------------------
# Dataset spec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DateIdDatasetSpec:
    key: str
    finmind_name: str
    output_root: Path
    min_date: dt.date
    enabled: bool
    frequency: Optional[str] = None
    required_columns: Optional[List[str]] = None


EXPECTED_KEYS = {
    "finstmt",
    "bs",
    "cfs",
    "shareholding",
    "inst_total",
    "gov_bank",
}

# 只對這三個 dataset 依交易日決定是否打 API
TRADING_CALENDAR_DATASETS = {
    "shareholding",
    "inst_total",
    "gov_bank",
}

# 部分 dateID dataset 的 FinMind 回應可能沒有 stock_id / data_id 欄位，
# 但我們是「每支股票單獨打 API」，知道 request 的 stock_id。
# 對這些 dataset，若回應缺少 id 欄位，就用 request 的 stock_id 合成一欄。
DATASETS_ALLOW_SYNTHETIC_STOCK_ID = {
    "inst_total",
    "gov_bank",
}

# trading_days cache（避免每批都重讀 CSV）
_TRADING_DAYS_CACHE: Optional[pd.DatetimeIndex] = None
_TRADING_DAYS_ROOT: Optional[Path] = None


def _load_yaml_dict(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        raise DateIdConfigError(f"Config file not found: {path}")
    if yaml is None:
        raise DateIdConfigError(
            "PyYAML is not available but is required to read dataset config."
        )
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        raise DateIdConfigError(f"Failed to parse YAML config {path}: {exc}") from exc

    if not isinstance(data, Mapping):
        raise DateIdConfigError(f"YAML root of {path} must be a mapping.")
    return data


def load_dataset_specs(config_path: Optional[Path]) -> Dict[str, DateIdDatasetSpec]:
    """
    Load dataset specs from YAML file.

    Default search order when config_path is None:
      1) config/dateid_datasets.yaml
      2) configs/dateid_datasets.yaml

    Expected structure:

      dateid_datasets:
        finstmt:
          key: finstmt
          finmind_name: TaiwanStockFinancialStatements
          output_root: silver/alpha/finstmt
          min_date: 2004-01-01
          enabled: true
          ...
    """
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
            raise DateIdConfigError(
                f"Config dateid_datasets.yaml not found. Tried: {tried}"
            )

    data = _load_yaml_dict(config_path)

    node = data.get("dateid_datasets")
    if not isinstance(node, Mapping):
        raise DateIdConfigError(
            f"Config {config_path} must contain mapping 'dateid_datasets'."
        )

    specs: Dict[str, DateIdDatasetSpec] = {}

    for key, cfg in node.items():
        if not isinstance(cfg, Mapping):
            raise DateIdConfigError(
                f"Dataset entry {key!r} in {config_path} must be a mapping."
            )

        dataset_key = str(cfg.get("key") or key).strip()
        if not dataset_key:
            raise DateIdConfigError(
                f"Dataset entry {key!r} in {config_path} missing 'key'."
            )

        finmind_name = str(cfg.get("finmind_name") or "").strip()
        if not finmind_name:
            raise DateIdConfigError(
                f"Dataset {dataset_key!r} missing 'finmind_name' in {config_path}."
            )

        output_root_raw = cfg.get("output_root")
        if not isinstance(output_root_raw, str) or not output_root_raw:
            raise DateIdConfigError(
                f"Dataset {dataset_key!r} missing 'output_root' in {config_path}."
            )
        output_root = Path(output_root_raw)

        min_date_raw = str(cfg.get("min_date") or "").strip()
        if not min_date_raw:
            raise DateIdConfigError(
                f"Dataset {dataset_key!r} missing 'min_date' in {config_path}."
            )
        try:
            min_date = dt.date.fromisoformat(min_date_raw)
        except ValueError as exc:
            raise DateIdConfigError(
                f"Dataset {dataset_key!r} has invalid min_date "
                f"{min_date_raw!r} in {config_path}."
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
            raise DateIdConfigError(
                f"Dataset {dataset_key!r} has invalid 'required_columns' "
                "(must be list of strings)."
            )

        spec = DateIdDatasetSpec(
            key=dataset_key,
            finmind_name=finmind_name,
            output_root=output_root,
            min_date=min_date,
            enabled=enabled,
            frequency=frequency,
            required_columns=required_columns,
        )
        specs[dataset_key] = spec

    # Validate key set matches expectation.
    missing = sorted(EXPECTED_KEYS - set(specs.keys()))
    extra = sorted(set(specs.keys()) - EXPECTED_KEYS)
    if missing:
        raise DateIdConfigError(
            f"Config {config_path} is missing dataset(s): {', '.join(missing)}"
        )
    if extra:
        raise DateIdConfigError(
            f"Config {config_path} has unknown dataset(s): {', '.join(extra)}"
        )

    return specs


def get_dataset_spec(
    dataset_key: str, specs: Mapping[str, DateIdDatasetSpec]
) -> DateIdDatasetSpec:
    try:
        spec = specs[dataset_key]
    except KeyError as exc:
        raise DateIdConfigError(f"Unknown dataset key {dataset_key!r}.") from exc
    if not spec.enabled:
        raise DateIdConfigError(f"Dataset {dataset_key!r} is disabled by configuration.")
    return spec


# ---------------------------------------------------------------------------
# Trading calendar helpers (for dateID)
# ---------------------------------------------------------------------------


def load_trading_days_for_dateid(datahub_root: Path) -> Optional[pd.DatetimeIndex]:
    """
    從 <datahub_root>/ref/trading_days.csv 讀取交易日，結果 cache 在模組層。

    回傳 DatetimeIndex（normalised 到日期），若檔案不存在或內容不合法，回傳 None。
    """
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

    col = "date"
    if col not in df.columns:
        col = df.columns[0]

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


def is_trading_day(date: dt.date, trading_days: Optional[pd.DatetimeIndex]) -> bool:
    """
    判斷指定 date 是否為交易日。

    - 若 trading_days 不為 None：以行事曆為準
    - 若 trading_days 為 None：fallback → 只認週一～週五為交易日（六、日一律視為非交易日）
    """
    if trading_days is not None:
        ts = pd.Timestamp(date)
        return ts.normalize() in trading_days

    # fallback：沒有行事曆檔時，至少保證週末不會打 API
    return date.weekday() < 5  # 0=Mon ... 6=Sun


def should_use_trading_calendar(spec: DateIdDatasetSpec) -> bool:
    """
    僅對 shareholding / inst_total / gov_bank 這三個 dataset 啟用交易日 gating。
    """
    return spec.key in TRADING_CALENDAR_DATASETS


# ---------------------------------------------------------------------------
# FinMind client helpers
# ---------------------------------------------------------------------------


def _get_finmind_base_url() -> str:
    base = os.environ.get(
        "FINMIND_BASE_URL", "https://api.finmindtrade.com/api/v4/data"
    )
    return base.rstrip("/")


def _get_finmind_token() -> str:
    token = (os.environ.get("FINMIND_TOKEN") or "").strip()
    if not token:
        raise DateIdConfigError("Environment variable FINMIND_TOKEN is not set.")
    return token


def _get_qps() -> float:
    """
    讀取 FINMIND_QPS，提供 dateID engine 使用的實際 QPS。

    - 預設 1.5 QPS
    - 非法或 <=0 都 fallback 回 1.5
    """
    raw = os.environ.get("FINMIND_QPS", "1.5")
    try:
        qps = float(raw)
    except Exception:
        qps = 1.5
    if qps <= 0:
        qps = 1.5
    return qps


def fetch_gov_bank_merged(
    spec: DateIdDatasetSpec,
    date: dt.date,
    ids: Iterable[str],
) -> pd.DataFrame:
    """
    Fetch gov_bank records using FinMind merged daily API.

    This endpoint uses dataset + start_date only and requires Bearer token
    in Authorization header. The ids parameter is ignored.
    """
    base_url = _get_finmind_base_url()
    token = _get_finmind_token()
    date_str = date.isoformat()

    params = {
        "dataset": spec.finmind_name,
        "start_date": date_str,
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload_bytes = resp.read()
    except urllib.error.HTTPError as exc:
        raise DateIdApiError(
            f"HTTPError for dataset={spec.key} date={date_str}: {exc}"
        ) from exc
    except urllib.error.URLError as exc:
        raise DateIdApiError(
            f"URLError for dataset={spec.key} date={date_str}: {exc}"
        ) from exc

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise DateIdApiError(
            f"Invalid JSON response for dataset={spec.key} date={date_str}: {exc}"
        ) from exc

    status = payload.get("status")
    msg = payload.get("msg")
    if status not in (200, "200"):
        raise DateIdApiError(
            f"FinMind error for dataset={spec.key} date={date_str}: "
            f"status={status} msg={msg}"
        )

    data = payload.get("data")
    if not data:
        return pd.DataFrame()

    if not isinstance(data, list):
        raise DateIdApiError(
            f"Unexpected 'data' format from FinMind for dataset={spec.key}: {type(data)}"
        )

    rows: List[Mapping[str, Any]] = []
    for row in data:
        if not isinstance(row, Mapping):
            raise DateIdApiError(
                f"Unexpected row type from FinMind for dataset={spec.key}: {type(row)}"
            )
        rows.append(dict(row))

    return pd.DataFrame(rows)


def fetch_dateid_raw(
    spec: DateIdDatasetSpec,
    date: dt.date,
    ids: Iterable[str],
) -> pd.DataFrame:
    """
    Fetch raw records from FinMind for given dataset/date/ids.

    This uses the generic v4 'data_id' + 'start_date'/'end_date' pattern
    for dateID datasets, except gov_bank uses merged daily endpoint.
    """
    if spec.key == "gov_bank":
        return fetch_gov_bank_merged(spec, date, ids)

    base_url = _get_finmind_base_url()
    token = _get_finmind_token()
    date_str = date.isoformat()

    qps = _get_qps()
    sleep_sec = 1.0 / qps if qps > 0 else 0.0

    rows: List[Mapping[str, Any]] = []

    for stock_id in ids:
        stock_id_str = str(stock_id).strip()
        if not stock_id_str:
            continue

        params = {
            "dataset": spec.finmind_name,
            "data_id": stock_id_str,
            "start_date": date_str,
            "end_date": date_str,
            "token": token,
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"

        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                payload_bytes = resp.read()
        except urllib.error.HTTPError as exc:
            # 400/Bad Request 在某些日期可能代表「該日沒有資料」，
            # 對 dateID 來說可以視為空集合直接略過。
            if exc.code == 400:
                # 保持 QPS 節奏
                if sleep_sec > 0.0:
                    time.sleep(sleep_sec)
                continue
            raise DateIdApiError(
                f"URLError for dataset={spec.key} stock_id={stock_id_str} "
                f"date={date_str}: {exc}"
            ) from exc
        except urllib.error.URLError as exc:
            raise DateIdApiError(
                f"URLError for dataset={spec.key} stock_id={stock_id_str} "
                f"date={date_str}: {exc}"
            ) from exc

        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:
            raise DateIdApiError(
                f"Invalid JSON response for dataset={spec.key} "
                f"stock_id={stock_id_str} date={date_str}: {exc}"
            ) from exc

        # QPS gating：每打一支股票 sleep 一下，實際 QPS 對齊 PowerShell 端設定
        if sleep_sec > 0.0:
            time.sleep(sleep_sec)

        # FinMind usually returns {"msg":"success","status":200,"data":[...]}
        status = payload.get("status")
        msg = payload.get("msg")
        if status not in (200, "200", None) and payload.get("data") is None:
            raise DateIdApiError(
                f"FinMind error for dataset={spec.key} stock_id={stock_id_str} "
                f"date={date_str}: status={status} msg={msg}"
            )

        data = payload.get("data")
        if not data:
            # No data for this id/date is not considered an error; just skip.
            continue

        if not isinstance(data, list):
            raise DateIdApiError(
                f"Unexpected 'data' format from FinMind for dataset={spec.key} "
                f"stock_id={stock_id_str}: {type(data)}"
            )

        for row in data:
            if not isinstance(row, Mapping):
                raise DateIdApiError(
                    f"Unexpected row type from FinMind for dataset={spec.key} "
                    f"stock_id={stock_id_str}: {type(row)}"
                )
            rec: Dict[str, Any] = dict(row)

            # 若回傳沒有 stock_id / data_id，且屬於允許合成 id 的 dataset，
            # 用 request 的 stock_id_str 補上一欄，避免後面 schema 檢查噴錯。
            if (
                "stock_id" not in rec
                and "data_id" not in rec
                and spec.key in DATASETS_ALLOW_SYNTHETIC_STOCK_ID
            ):
                rec["stock_id"] = stock_id_str

            rows.append(rec)

    if not rows:
        # Return empty DataFrame
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


def normalize_dateid_frame(
    df: pd.DataFrame,
    spec: DateIdDatasetSpec,
    date: dt.date,
) -> pd.DataFrame:
    """
    Normalize FinMind raw DataFrame to a consistent schema.

    - Ensure 'date' column exists and is ISO string.
    - Ensure 'stock_id' column exists (rename 'data_id' if needed).
    - Optionally validate required_columns.
    """
    if df.empty:
        return df

    # Normalize stock_id column.
    if "stock_id" not in df.columns:
        if "data_id" in df.columns:
            df = df.rename(columns={"data_id": "stock_id"})
        else:
            raise DateIdSchemaError(
                f"Dataset {spec.key!r} is missing 'stock_id' (or 'data_id') "
                "column in FinMind response."
            )

    # Attach/normalize date column.
    if "date" not in df.columns:
        df["date"] = date.isoformat()
    else:
        try:
            df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
        except Exception as exc:
            raise DateIdSchemaError(
                f"Failed to normalize 'date' column for dataset {spec.key!r}: {exc}"
            ) from exc

    # Basic required_columns validation.
    if spec.required_columns:
        missing = [c for c in spec.required_columns if c not in df.columns]
        if missing:
            raise DateIdSchemaError(
                f"Dataset {spec.key!r} missing required column(s): "
                f"{', '.join(missing)}"
            )

    # Drop obvious duplicates (same row multiple times).
    df = df.drop_duplicates()

    return df


def make_output_path(
    spec: DateIdDatasetSpec,
    date: dt.date,
    datahub_root: Path,
) -> Path:
    """
    Compute Parquet output path for given dataset/date under datahub root.

    Layout:
      <datahub_root>/<output_root>/yyyymm=YYYYMM/<key>_YYYY-MM-DD.parquet
    """
    yyyymm = date.strftime("%Y%m")
    date_str = date.isoformat()

    base_root = datahub_root / spec.output_root  # e.g. silver/alpha/finstmt
    partition_dir = base_root / f"yyyymm={yyyymm}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_path = partition_dir / f"{spec.key}_{date_str}.parquet"
    return file_path


def write_dateid_parquet(
    df: pd.DataFrame,
    spec: DateIdDatasetSpec,
    date: dt.date,
    datahub_root: Path,
) -> int:
    """
    Append/overwrite Parquet for given dataset/date.

    Strategy:
    - If file exists: load existing, append, drop duplicates, overwrite.
    - Else: write new file.

    Returns number of rows written.
    """
    if df.empty:
        # 沒有資料代表那天該批沒有紀錄，本身不算錯誤
        return 0

    file_path = make_output_path(spec, date, datahub_root)

    if file_path.exists():
        try:
            existing = pd.read_parquet(file_path)
        except Exception as exc:
            raise DateIdSchemaError(
                f"Failed to read existing Parquet file {file_path}: {exc}"
            ) from exc
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates()
    else:
        combined = df

    try:
        combined.to_parquet(file_path, index=False)
    except Exception as exc:
        raise DateIdSchemaError(
            f"Failed to write Parquet file {file_path}: {exc}"
        ) from exc

    return int(len(combined))


def run_dateid_batch(
    dataset_key: str,
    date: dt.date,
    ids: Iterable[str],
    datahub_root: Path,
    config_path: Optional[Path] = None,
) -> int:
    """
    High-level pipeline:
      spec -> (trading-calendar gating) -> fetch_dateid_raw -> normalize_dateid_frame -> write_dateid_parquet

    Returns number of rows currently stored in the date's Parquet file (after de-dup).
    """
    specs = load_dataset_specs(config_path)
    spec = get_dataset_spec(dataset_key, specs)

    # Prevent obviously 錯誤的早期日期
    if date < spec.min_date:
        raise DateIdConfigError(
            f"Requested date {date.isoformat()} earlier than min_date "
            f"{spec.min_date.isoformat()} for dataset {spec.key!r}."
        )

    # 只跑交易日：shareholding / inst_total / gov_bank 會參考交易日行事曆
    if should_use_trading_calendar(spec):
        trading_days = load_trading_days_for_dateid(datahub_root)
        if not is_trading_day(date, trading_days):
            # 非交易日視為「該天沒有資料」，直接成功返回 0，不打 API。
            print(
                f"[calendar] {spec.key} {date.isoformat()} 非交易日，跳過 API",
                file=sys.stderr,
            )
            return 0

    df_raw = fetch_dateid_raw(spec, date, ids)
    if df_raw.empty:
        # 沒有資料代表那天該批沒有紀錄，本身不算錯誤
        return 0

    df_norm = normalize_dateid_frame(df_raw, spec, date)
    rows_written = write_dateid_parquet(df_norm, spec, date, datahub_root)
    return rows_written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FinMind dateID backfill engine (single dataset/date/batch).",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset key (finstmt|bs|cfs|shareholding|inst_total|gov_bank).",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="As-of date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--ids",
        required=True,
        help="Comma-separated list of stock IDs for this batch.",
    )
    parser.add_argument(
        "--datahub-root",
        required=True,
        help="Root directory for datahub (e.g. C:/AI/tw-alpha-stack/datahub).",
    )
    parser.add_argument(
        "--config",
        help=(
            "Path to dateid_datasets.yaml (optional, defaults to "
            "config/ or configs/ under repo root)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (currently informational only).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Plan-only mode: validate config and print actions without calling "
            "FinMind or writing files."
        ),
    )
    return parser.parse_args(argv)


def cli_run(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    dataset_key = args.dataset.strip()
    try:
        date = dt.date.fromisoformat(args.date.strip())
    except ValueError as exc:
        raise DateIdConfigError(
            f"Invalid --date {args.date!r}, expected YYYY-MM-DD."
        ) from exc

    ids = [s for s in (x.strip() for x in args.ids.split(",")) if s]
    if not ids:
        raise DateIdConfigError("No valid IDs supplied via --ids.")

    datahub_root = Path(args.datahub_root).resolve()
    config_path = Path(args.config).resolve() if args.config else None

    # Dry-run：只檢查 config 與輸出計畫，不打 API、不寫檔
    if args.dry_run:
        specs = load_dataset_specs(config_path)
        spec = get_dataset_spec(dataset_key, specs)
        print(
            json.dumps(
                {
                    "mode": "dry-run",
                    "dataset": spec.key,
                    "finmind_name": spec.finmind_name,
                    "date": date.isoformat(),
                    "ids_count": len(ids),
                    "datahub_root": str(datahub_root),
                    "output_root": str(datahub_root / spec.output_root),
                    "config_path": str(config_path) if config_path else None,
                },
                ensure_ascii=False,
            )
        )
        return 0

    rows = run_dateid_batch(
        dataset_key,
        date,
        ids,
        datahub_root,
        config_path=config_path,
    )

    print(
        json.dumps(
            {
                "mode": "run",
                "dataset": dataset_key,
                "date": date.isoformat(),
                "ids_count": len(ids),
                "rows_after_write": rows,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> None:
    try:
        exit_code = cli_run()
    except DateIdConfigError as exc:
        print(f"[CONFIG] {exc}", file=sys.stderr)
        exit_code = 2
    except DateIdSchemaError as exc:
        print(f"[SCHEMA] {exc}", file=sys.stderr)
        exit_code = 4
    except DateIdApiError as exc:
        # Exit code 3 reserved for API/network errors.
        print(f"[API] {exc}", file=sys.stderr)
        exit_code = 3
    except Exception as exc:
        # Unexpected error; keep stacktrace for debugging.
        print(f"[FATAL] {exc}", file=sys.stderr)
        raise
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
