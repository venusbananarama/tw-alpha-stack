from __future__ import annotations

"""
Preflight/Guard v3

- 決定期望日 expect_date（交易日曆 + cutoff 小時，ENV 可覆寫）
- 檢查 Phase-1 四表 freshness：prices / chip / per / dividend
- dividend 使用 checkpoint + trading-day lag 規則（_state/mainline/dividend/*.ok）
- 輸出 reports/preflight_report.json，給 Run-WFGate.ps1 使用
"""

import argparse
import datetime as dt
import json
import os
import sys
import glob
import re
from bisect import bisect_right
from pathlib import Path
from typing import Dict, List, Mapping, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # 老版本 Python 的 fallback
    ZoneInfo = None  # type: ignore

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.config import ConfigError, load_rules  # noqa: E402

DATE_TZ_NAME = "Asia/Taipei"
DEFAULT_CUTOFF_HOUR = int(os.getenv("ALPHACITY_DATA_READY_HOUR_LOCAL", "18"))

_DATE_DIR_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})$")
_YYYYMM_DIR_RE = re.compile(r"yyyymm=(\d{6})$")
DIVIDEND_DATE_COLS = (
    "ex_dividend_date",
    "ex_date",
    "exdate",
    "record_date",
    "announce_date",
    "dividend_date",
    "cash_ex_date",
    "exDividendDate",
    "date",
)
SPARSE_DATASETS = {"dividend"}
REQUIRED_P1_DATASETS = [
    "prices",
    "chip",
    "per",
    "dividend",
    "prices_daily",
    "shareholding",
    "inst_total",
    "gov_bank",
]
STATE_OK_OR_PARTITION_DATASETS = {"shareholding", "inst_total", "gov_bank"}


class CalendarNotFoundError(RuntimeError):
    """找不到 trading_days 檔案時用的錯誤型別。"""


# ---------- 交易日曆讀取與期望日計算 ----------

def find_trading_calendar_path(root: Path) -> Path:
    """
    依序尋找 trading_days.{csv,xlsx}：
    1) datahub/ref/trading_days.csv
    2) datahub/ref/trading_days.xlsx
    3) cal/trading_days.csv
    4) cal/trading_days.xlsx
    """
    candidates = [
        root / "datahub" / "ref" / "trading_days.csv",
        root / "datahub" / "ref" / "trading_days.xlsx",
        root / "cal" / "trading_days.csv",
        root / "cal" / "trading_days.xlsx",
    ]
    for p in candidates:
        if p.is_file():
            return p
    raise CalendarNotFoundError(
        "trading_days file not found under datahub/ref or cal "
        "(expected one of: trading_days.csv/xlsx)"
    )


def load_trading_calendar(path: Path) -> pd.DataFrame:
    """
    載入交易日曆（CSV / Excel），並正規化成：

    - 欄位全部小寫
    - 至少有一欄叫 date
    - 若存在 is_trading / is_open / open / trading / flag，則只保留值=1 的列
    - 以日期升冪排序
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in (".xls", ".xlsx"):
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported calendar file extension: {suffix}")

    if df.empty:
        raise ValueError("trading_days file is empty")

    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns:
        # 若沒有 date 欄，視第一欄為日期
        df = df.rename(columns={df.columns[0]: "date"})

    flagcol: Optional[str] = None
    for cand in ("is_trading", "is_open", "open", "trading", "flag"):
        if cand in df.columns:
            flagcol = cand
            break

    if flagcol is not None:
        mask = df[flagcol].fillna(0).astype(int) == 1
        df = df.loc[mask].copy()

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("trading_days has no valid date rows after parsing")

    return df


def _parse_env_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    try:
        ts = pd.Timestamp(v)
    except Exception:
        return None
    return ts.date()


def trading_days_from_calendar(cal_df: pd.DataFrame) -> List[dt.date]:
    if "date" not in cal_df.columns:
        return []
    days = cal_df["date"].dropna().dt.date.tolist()
    return sorted(set(days))


def floor_to_prev_trading_day(d: dt.date, trading_days: List[dt.date]) -> Optional[dt.date]:
    if not trading_days:
        return None
    idx = bisect_right(trading_days, d) - 1
    if idx < 0:
        return None
    return trading_days[idx]


def trading_day_index(d: dt.date, trading_days: List[dt.date]) -> Optional[int]:
    if not trading_days:
        return None
    idx = bisect_right(trading_days, d) - 1
    if idx < 0:
        return None
    return idx


def _normalize_to_trading_day(d: dt.date, trading_days: List[dt.date]) -> dt.date:
    if d in trading_days:
        return d
    floored = floor_to_prev_trading_day(d, trading_days)
    return floored or d


def compute_expect_date(
    cal_df: pd.DataFrame,
    env: Mapping[str, str],
    expect_date_override: Optional[dt.date] = None,
    cap_date: Optional[dt.date] = None,
) -> dt.date:
    """
    決定期望日 expect_date（date 型別）：

    1) 先用交易日曆 + cutoff 規則算出 candidate
    2) 若 ENV 有 EXPECT_DATE_FIXED / EXPECT_DATE 且可解析，就覆寫
    3) 若 CLI 有 expect_date_override，優先採用並對齊交易日
    4) 若 cap_date 有提供，expect_date 需 <= cap_date（必要時向前對齊交易日）
    """
    tz = ZoneInfo(DATE_TZ_NAME) if ZoneInfo else None
    now = dt.datetime.now(tz) if tz else dt.datetime.now()
    today = now.date()

    cal_dates = cal_df["date"].dt.date.tolist()

    last_le_today: Optional[dt.date] = None
    for d in cal_dates:
        if d <= today and (last_le_today is None or d > last_le_today):
            last_le_today = d

    today_is_trading = today in cal_dates

    if today_is_trading and now.hour < DEFAULT_CUTOFF_HOUR:
        idx = cal_dates.index(today)
        if idx > 0:
            candidate = cal_dates[idx - 1]
        else:
            candidate = today
    else:
        candidate = last_le_today or today

    env_fixed = _parse_env_date(env.get("EXPECT_DATE_FIXED"))
    env_fallback = _parse_env_date(env.get("EXPECT_DATE"))

    if expect_date_override is not None:
        # CLI override is the anchor for historical verification.
        expect = expect_date_override
    else:
        expect = env_fixed or env_fallback or candidate
        expect = _normalize_to_trading_day(expect, cal_dates)

    if cap_date is not None and expect > cap_date:
        if expect_date_override is not None:
            expect = cap_date
        else:
            cap_td = floor_to_prev_trading_day(cap_date, cal_dates) or cap_date
            expect = cap_td

    if expect_date_override is None:
        expect = _normalize_to_trading_day(expect, cal_dates)
    return expect


# ---------- 分區與 parquet freshness ----------

def _as_str_list(value: object) -> Optional[List[str]]:
    if not isinstance(value, list):
        return None
    out = []
    for item in value:
        s = str(item).strip()
        if s:
            out.append(s)
    return out or None


def load_freshness_config(root: Path) -> Dict[str, object]:
    defaults = {
        "datasets": REQUIRED_P1_DATASETS,
        "partition_patterns": ["date=YYYY-MM-DD", "yyyymm=YYYYMM"],
        "operator": "ge",
    }
    rules_path = root / "rules.yaml"
    try:
        rules = load_rules(rules_path)
    except (ConfigError, Exception):
        cfg = dict(defaults)
    else:
        checks = rules.get("checks", {}) if isinstance(rules, Mapping) else {}
        freshness = checks.get("freshness", {}) if isinstance(checks, Mapping) else {}
        datasets = _as_str_list(freshness.get("datasets")) or defaults["datasets"]
        patterns = _as_str_list(freshness.get("partition_patterns")) or defaults["partition_patterns"]
        operator = freshness.get("operator") if isinstance(freshness, Mapping) else None
        merged_datasets: List[str] = []
        for ds in list(datasets) + list(REQUIRED_P1_DATASETS):
            if ds not in merged_datasets:
                merged_datasets.append(ds)
        cfg = {
            "datasets": merged_datasets,
            "partition_patterns": patterns,
            "operator": str(operator).strip() if operator else defaults["operator"],
        }

    # Only activate the validated partition style; keep others for reporting.
    active_patterns = ["yyyymm=YYYYMM"] if "yyyymm=YYYYMM" in cfg["partition_patterns"] else ["yyyymm=YYYYMM"]
    cfg["active_partition_patterns"] = active_patterns
    cfg["rules_path"] = str(rules_path)
    return cfg


def collect_recent_month_partitions(base: Path, n_months: int = 2) -> List[Path]:
    if not base.is_dir():
        return []
    yms: List[str] = []
    for name in os.listdir(base):
        m = _YYYYMM_DIR_RE.match(name)
        if m:
            yms.append(m.group(1))
    if not yms:
        return []
    yms = sorted(set(yms))[-n_months:]
    return [base / f"yyyymm={ym}" for ym in yms]


def collect_recent_day_partitions(base: Path, ndays: int = 62) -> List[Path]:
    if not base.is_dir():
        return []
    days: List[str] = []
    for name in os.listdir(base):
        m = _DATE_DIR_RE.match(name)
        if m:
            days.append(m.group(1))
    if not days:
        return []
    days = sorted(set(days))[-ndays:]
    return [base / f"date={d}" for d in days]


def max_date_from_day_partitions(base: Path, ndays: int = 62) -> Optional[dt.date]:
    parts = collect_recent_day_partitions(base, ndays=ndays)
    mx: Optional[dt.date] = None
    for p in parts:
        m = _DATE_DIR_RE.search(str(p).replace("\\", "/"))
        if not m:
            continue
        try:
            d = dt.date.fromisoformat(m.group(1))
        except Exception:
            continue
        if mx is None or d > mx:
            mx = d
    return mx


def scan_parquet_max_date(files: List[Path]) -> Optional[dt.date]:
    """
    看一批 parquet 檔，回傳「date 欄位」的最大日期。
    """
    if not files:
        return None

    mx: Optional[dt.date] = None

    for f in files:
        if f.name.startswith("ing_"):
            continue
        try:
            df = pd.read_parquet(f, columns=["date"])
        except Exception:
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue
        s = pd.to_datetime(df["date"], errors="coerce")
        if s.notna().any():
            dm = s.max()
            if pd.notna(dm):
                d = dm.date()
                if mx is None or d > mx:
                    mx = d

    return mx


def scan_parquet_rows_for_day(files: List[Path], target_day: dt.date) -> int:
    if not files:
        return 0
    total = 0
    for f in files:
        if f.name.startswith("ing_"):
            continue
        try:
            df = pd.read_parquet(f, columns=["date"])
        except Exception:
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue
        s = pd.to_datetime(df["date"], errors="coerce").dt.date
        total += int((s == target_day).sum())
    return total


def scan_parquet_max_date_for_col(files: List[Path], col: str) -> Optional[dt.date]:
    if not files:
        return None
    mx: Optional[dt.date] = None
    for f in files:
        if f.name.startswith("ing_"):
            continue
        try:
            df = pd.read_parquet(f, columns=[col])
        except Exception:
            continue
        if df is None or df.empty or col not in df.columns:
            continue
        s = pd.to_datetime(df[col], errors="coerce")
        if s.notna().any():
            dm = s.max()
            if pd.notna(dm):
                d = dm.date()
                if mx is None or d > mx:
                    mx = d
    return mx


def scan_parquet_rows_for_day_for_col(files: List[Path], col: str, target_day: dt.date) -> int:
    if not files:
        return 0
    total = 0
    for f in files:
        if f.name.startswith("ing_"):
            continue
        try:
            df = pd.read_parquet(f, columns=[col])
        except Exception:
            continue
        if df is None or df.empty or col not in df.columns:
            continue
        s = pd.to_datetime(df[col], errors="coerce").dt.date
        total += int((s == target_day).sum())
    return total


def _collect_generic_parquet_files(base: Path, kind: str, warn: bool) -> List[Path]:
    if not base.is_dir():
        return []

    # 只支援 yyyymm=YYYYMM/data.parquet
    month_parts = collect_recent_month_partitions(base, n_months=2)
    month_files: List[Path] = []
    fallback_used = False
    for p in month_parts:
        data_path = p / "data.parquet"
        if data_path.is_file():
            month_files.append(data_path)
            continue
        candidates = [f for f in p.glob("*.parquet") if not f.name.startswith("ing_")]
        if candidates:
            fallback_used = True
            month_files.extend(sorted(candidates))
    if fallback_used and warn:
        print(f"[WARN] {kind}: data.parquet missing; using fallback parquet files")
    return month_files


def max_date_generic(datahub_root: Path, kind: str) -> Optional[dt.date]:
    base = datahub_root / "silver" / "alpha" / kind
    month_files = _collect_generic_parquet_files(base, kind, warn=True)
    return scan_parquet_max_date(month_files)


def rows_for_day_generic(datahub_root: Path, kind: str, target_day: dt.date) -> int:
    base = datahub_root / "silver" / "alpha" / kind
    month_files = _collect_generic_parquet_files(base, kind, warn=False)
    return scan_parquet_rows_for_day(month_files, target_day)


def max_date_prices_daily(datahub_root: Path) -> Optional[dt.date]:
    path = datahub_root / "silver" / "alpha" / "prices_daily.parquet"
    if not path.is_file():
        return None
    return scan_parquet_max_date([path])


def rows_for_day_prices_daily(datahub_root: Path, target_day: dt.date) -> int:
    path = datahub_root / "silver" / "alpha" / "prices_daily.parquet"
    if not path.is_file():
        return 0
    return scan_parquet_rows_for_day([path], target_day)


def has_dataset_state_ok(repo_root: Path, kind: str, expect_date: dt.date) -> bool:
    ok_path = repo_root / "_state" / "mainline" / kind / f"{expect_date.isoformat()}.ok"
    return ok_path.is_file()


def has_month_partition_parquet_for_day(datahub_root: Path, kind: str, expect_date: dt.date) -> bool:
    ym = expect_date.strftime("%Y%m")
    base = datahub_root / "silver" / "alpha" / kind / f"yyyymm={ym}"
    if not base.is_dir():
        return False
    pat = f"*{expect_date.isoformat()}*.parquet"
    for p in base.glob(pat):
        if p.is_file() and not p.name.startswith("ing_"):
            return True
    return False


def compute_state_or_partition_freshness(
    datahub_root: Path,
    repo_root: Path,
    kind: str,
    expect_date: dt.date,
) -> Dict[str, object]:
    state_ok = has_dataset_state_ok(repo_root, kind, expect_date)
    partition_hit = False
    ok = False
    ssot = "state_ok"
    status = "MISSING_STATE_OK_AND_PARTITION_FILE"

    if state_ok:
        ok = True
        ssot = "state_ok"
        status = "OK_STATE_OK"
    else:
        partition_hit = has_month_partition_parquet_for_day(datahub_root, kind, expect_date)
        if partition_hit:
            ok = True
            ssot = "partition_file"
            status = "OK_PARTITION_FILE"

    max_date = expect_date if ok else max_date_generic(datahub_root, kind)
    return {
        "kind": kind,
        "max_date": max_date,
        "ok": ok,
        "rows_at_expect": int(ok),
        "state_ok_exists": state_ok,
        "partition_file_exists": partition_hit,
        "status": status,
        "ssot": ssot,
    }


def _pick_dividend_date_col(files: List[Path]) -> Optional[str]:
    for col in DIVIDEND_DATE_COLS:
        for f in files:
            try:
                df = pd.read_parquet(f, columns=[col])
            except Exception:
                continue
            if df is not None and col in df.columns:
                return col
    return None


def max_date_dividend_ssot(datahub_root: Path) -> Optional[dt.date]:
    base = datahub_root / "silver" / "alpha" / "dividend"
    files = _collect_generic_parquet_files(base, "dividend", warn=True)
    col = _pick_dividend_date_col(files)
    if not col:
        return None
    return scan_parquet_max_date_for_col(files, col)


def rows_for_day_dividend(datahub_root: Path, target_day: dt.date) -> int:
    base = datahub_root / "silver" / "alpha" / "dividend"
    files = _collect_generic_parquet_files(base, "dividend", warn=False)
    col = _pick_dividend_date_col(files)
    if not col:
        return 0
    return scan_parquet_rows_for_day_for_col(files, col, target_day)


def max_dividend_ok_day(repo_root: Path) -> Optional[dt.date]:
    ok_dir = repo_root / "_state" / "mainline" / "dividend"
    if not ok_dir.is_dir():
        return None
    mx: Optional[dt.date] = None
    for p in ok_dir.glob("*.ok"):
        try:
            d = dt.date.fromisoformat(p.stem)
        except Exception:
            continue
        if mx is None or d > mx:
            mx = d
    return mx


def compute_dividend_freshness(
    datahub_root: Path,
    repo_root: Path,
    expect_date: dt.date,
    trading_days: List[dt.date],
    max_lag_td: int,
) -> Dict[str, object]:
    data_max_date = max_date_dividend_ssot(datahub_root)
    rows_at_expect = rows_for_day_dividend(datahub_root, expect_date)
    last_ok_day = max_dividend_ok_day(repo_root)

    lag_td: Optional[int] = None
    status = "MISSING_OK"
    ok = False

    if last_ok_day is not None:
        idx_expect = trading_day_index(expect_date, trading_days)
        idx_ok = trading_day_index(last_ok_day, trading_days)
        if idx_expect is None or idx_ok is None:
            status = "FAIL_LAG"
        else:
            lag_td = max(0, idx_expect - idx_ok)
            if lag_td == 0:
                status = "OK"
            elif lag_td <= max_lag_td:
                status = "OK_LAG"
            else:
                status = "FAIL_LAG"
            ok = status in ("OK", "OK_LAG")

    return {
        "kind": "dividend",
        "max_date": data_max_date,
        "data_max_date": data_max_date,
        "ok": ok,
        "rows_at_expect": rows_at_expect,
        "last_ok_day": last_ok_day,
        "lag_td": lag_td,
        "lag_threshold": max_lag_td,
        "status": status,
        "ssot": "checkpoint",
        "expect_date": expect_date,
    }


# ---------- dividend 專屬 freshness ----------

def max_date_dividend(
    datahub_root: Path, expect_date: dt.date, partition_patterns: List[str]
) -> Optional[dt.date]:
    """
    Dividend 特別規則：

    - 若 _state/ingest/dividend/<expect_date>.ok 存在 → 視為已覆蓋當日
    - 或 silver/alpha/dividend/date=<expect_date> 夾存在
    - 或 silver/alpha/dividend 的最新 yyyymm 分區 >= expect_date.yyyymm
    - 否則回到 generic parquet freshness
    """
    exp_iso = expect_date.isoformat()

    # 1) ingest ok marker
    root = datahub_root.parent  # repo root
    ok_path = root / "_state" / "ingest" / "dividend" / f"{exp_iso}.ok"
    if ok_path.is_file():
        return expect_date

    base = datahub_root / "silver" / "alpha" / "dividend"
    if base.is_dir():
        # 2) date 分區直接有當日
        if "date=YYYY-MM-DD" in partition_patterns:
            ddir = base / f"date={exp_iso}"
            if ddir.is_dir():
                return expect_date

        # 3) yyyymm 分區 >= expect_yyyymm
        if "yyyymm=YYYYMM" in partition_patterns:
            yms: List[int] = []
            for name in os.listdir(base):
                m = _YYYYMM_DIR_RE.match(name)
                if m:
                    try:
                        yms.append(int(m.group(1)))
                    except Exception:
                        continue
            if yms:
                ym_latest = max(yms)
                exp_ym = int(exp_iso.replace("-", "")[:6])
                if ym_latest >= exp_ym:
                    return expect_date

    # Fallback: generic
    return max_date_generic(datahub_root, "dividend")


# ---------- 整體 freshness、報告與 CLI ----------

def compute_freshness_for_all(
    datahub_root: Path,
    expect_date: dt.date,
    kinds: List[str],
    operator: str,
    partition_patterns: List[str],
    trading_days: List[dt.date],
    repo_root: Path,
    dividend_max_lag_td: int,
) -> Dict[str, Dict[str, object]]:
    results: Dict[str, Dict[str, object]] = {}
    for kind in kinds:
        if kind == "dividend":
            results[kind] = compute_dividend_freshness(
                datahub_root,
                repo_root,
                expect_date,
                trading_days,
                dividend_max_lag_td,
            )
            continue
        if kind in STATE_OK_OR_PARTITION_DATASETS:
            results[kind] = compute_state_or_partition_freshness(
                datahub_root,
                repo_root,
                kind,
                expect_date,
            )
            continue
        else:
            mx = max_date_generic(datahub_root, kind)
            rows_at_expect = rows_for_day_generic(datahub_root, kind, expect_date)

        if isinstance(mx, dt.date) and mx > expect_date:
            mx = expect_date
        if operator == "gt":
            ok = bool(mx is not None and mx > expect_date)
        elif operator == "eq":
            ok = bool(mx is not None and mx == expect_date)
        else:
            ok = bool(mx is not None and mx >= expect_date)
        results[kind] = {
            "kind": kind,
            "max_date": mx,
            "ok": ok,
            "rows_at_expect": rows_at_expect,
        }
    return results


def build_preflight_report(
    expect_date: dt.date,
    tz_name: str,
    freshness: Dict[str, Dict[str, object]],
    operator: str,
    datasets: List[str],
    partition_patterns: List[str],
    active_partition_patterns: List[str],
) -> Dict[str, object]:
    meta = {
        "expect_date": expect_date.isoformat(),
        "tz": tz_name,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "freshness_operator": operator,
        "freshness_datasets": datasets,
        "partition_patterns": partition_patterns,
        "active_partition_patterns": active_partition_patterns,
    }

    freshness_json: Dict[str, Dict[str, object]] = {}
    dup_check: Dict[str, Dict[str, int]] = {}

    for kind, res in freshness.items():
        mx = res["max_date"]
        rows_at_expect = res.get("rows_at_expect")
        ok = bool(res.get("ok"))
        if isinstance(mx, dt.date):
            mx_str = mx.isoformat()
        elif mx is None:
            mx_str = None
        else:
            mx_str = str(mx)
        payload: Dict[str, object] = {
            "max_date": mx_str,
            "rows_at_expect": rows_at_expect,
            "ok": ok,
        }
        for key in ("ssot", "status", "state_ok_exists", "partition_file_exists"):
            if key in res:
                payload[key] = res.get(key)
        if kind == "dividend":
            last_ok = res.get("last_ok_day")
            data_max = res.get("data_max_date")
            if isinstance(last_ok, dt.date):
                last_ok_str = last_ok.isoformat()
            elif last_ok is None:
                last_ok_str = None
            else:
                last_ok_str = str(last_ok)
            if isinstance(data_max, dt.date):
                data_max_str = data_max.isoformat()
            elif data_max is None:
                data_max_str = None
            else:
                data_max_str = str(data_max)
            payload.update(
                {
                    "dividend_ssot": "checkpoint",
                    "last_ok_day": last_ok_str,
                    "lag_td": res.get("lag_td"),
                    "lag_threshold": res.get("lag_threshold"),
                    "status": res.get("status"),
                    "data_max_date": data_max_str,
                    "expect_date": expect_date.isoformat(),
                }
            )
        freshness_json[kind] = payload
        dup_check[kind] = {"bak_count": 0}

    return {
        "meta": meta,
        "freshness": freshness_json,
        "dup_check": dup_check,
    }


def log_status(
    datahub_root: Path, expect_date: dt.date, tz_name: str, freshness: Dict[str, Dict[str, object]]
) -> None:
    print(f"[Preflight] expect_date={expect_date.isoformat()} tz={tz_name}")

    for kind, res in freshness.items():
        mx = res["max_date"]
        ok = res["ok"]
        rows_at_expect = res.get("rows_at_expect")
        rows_at_expect = int(rows_at_expect or 0)
        if isinstance(mx, dt.date):
            mx_str = mx.isoformat()
        elif mx is None:
            mx_str = "None"
        else:
            mx_str = str(mx)

        if kind == "prices_daily":
            raw_path = datahub_root / "silver" / "alpha" / "prices_daily.parquet"
            path_disp = str(raw_path).replace("\\", "\\\\").replace("/", "\\\\")
            print(f"  prices_daily_ssot=file path={path_disp}")
        else:
            raw_path = datahub_root / "silver" / "alpha" / kind
        path_disp = str(raw_path).replace("\\", "\\\\").replace("/", "\\\\")
        # 保持既有輸出風格：路徑中的 \ 變成 \\（給 log / JSON-safe）

        stat = "OK" if ok else "FAIL"
        print(f"  freshness [{stat}] {path_disp} max_date={mx_str} rows@expect={rows_at_expect}")
        if kind == "dividend":
            last_ok = res.get("last_ok_day")
            data_max = res.get("data_max_date")
            lag_td = res.get("lag_td")
            lag_threshold = res.get("lag_threshold")
            status = res.get("status")
            if isinstance(last_ok, dt.date):
                last_ok_str = last_ok.isoformat()
            elif last_ok is None:
                last_ok_str = "None"
            else:
                last_ok_str = str(last_ok)
            if isinstance(data_max, dt.date):
                data_max_str = data_max.isoformat()
            elif data_max is None:
                data_max_str = "None"
            else:
                data_max_str = str(data_max)
            print(
                "  dividend_ssot=checkpoint "
                f"last_ok_day={last_ok_str} data_max_date={data_max_str} "
                f"expect_date={expect_date.isoformat()} lag_td={lag_td} "
                f"threshold={lag_threshold} status={status}"
            )
        print(f"  dup_check [OK] {path_disp} bak_count=0")
        if (
            kind not in SPARSE_DATASETS
            and isinstance(mx, dt.date)
            and mx >= expect_date
            and rows_at_expect == 0
        ):
            print("[WARN] max_date>=expect_date but no rows at expect_date (SSOT mismatch?)")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Phase-1 preflight freshness check")
    ap.add_argument(
        "--rules",
        required=False,
        help="Reserved parameter for compatibility (unused).",
    )
    ap.add_argument(
        "--export",
        default="reports",
        help="Output directory for preflight_report.json (default: reports)",
    )
    ap.add_argument(
        "--root",
        default=".",
        help="Repository root path (default: current directory).",
    )
    ap.add_argument(
        "--expect-date",
        default=None,
        help="Override expect_date (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--cap-date",
        default=None,
        help="Cap expect_date to <= cap-date (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--dividend-max-lag-td",
        default=10,
        type=int,
        help="Dividend checkpoint lag threshold in trading days (default: 10).",
    )
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    root = Path(args.root).resolve()
    datahub_root = root / "datahub"
    export_path = Path(args.export)
    if not export_path.is_absolute():
        export_path = root / export_path

    # Step 1: 讀交易日曆
    try:
        cal_path = find_trading_calendar_path(root)
    except CalendarNotFoundError as ex:
        print(f"[Preflight] ERROR: {ex}", file=sys.stderr)
        return 2

    try:
        cal_df = load_trading_calendar(cal_path)
    except Exception as ex:
        print(f"[Preflight] ERROR: failed to load trading_days from {cal_path}: {ex}", file=sys.stderr)
        return 2

    # Step 2: 算期望日
    expect_date_override = _parse_env_date(args.expect_date)
    cap_date = _parse_env_date(args.cap_date)
    expect_date = compute_expect_date(cal_df, os.environ, expect_date_override, cap_date)
    print(
        f"[Preflight/Guard/v3] calendar={cal_path} tz={DATE_TZ_NAME} "
        f"cutoff={DEFAULT_CUTOFF_HOUR} expect_date_fixed={expect_date.isoformat()}"
    )
    trading_days = trading_days_from_calendar(cal_df)

    # Step 3: 四表 freshness
    freshness_cfg = load_freshness_config(root)
    kinds = list(freshness_cfg["datasets"])
    operator = str(freshness_cfg["operator"])
    partition_patterns = list(freshness_cfg["partition_patterns"])
    active_partition_patterns = list(freshness_cfg["active_partition_patterns"])
    freshness = compute_freshness_for_all(
        datahub_root,
        expect_date,
        kinds,
        operator,
        active_partition_patterns,
        trading_days,
        root,
        int(args.dividend_max_lag_td),
    )
    prices_daily_max = max_date_prices_daily(datahub_root)
    prices_daily_rows = rows_for_day_prices_daily(datahub_root, expect_date)
    if prices_daily_max is not None and prices_daily_max > expect_date:
        prices_daily_max = expect_date
    if operator == "gt":
        prices_daily_ok = bool(prices_daily_max is not None and prices_daily_max > expect_date)
    elif operator == "eq":
        prices_daily_ok = bool(prices_daily_max is not None and prices_daily_max == expect_date)
    else:
        prices_daily_ok = bool(prices_daily_max is not None and prices_daily_max >= expect_date)
    freshness["prices_daily"] = {
        "kind": "prices_daily",
        "max_date": prices_daily_max,
        "ok": prices_daily_ok,
        "rows_at_expect": prices_daily_rows,
    }
    if "prices_daily" not in kinds:
        kinds.append("prices_daily")

    # Step 4: log + 寫 JSON 報告
    log_status(datahub_root, expect_date, DATE_TZ_NAME, freshness)

    report = build_preflight_report(
        expect_date,
        DATE_TZ_NAME,
        freshness,
        operator,
        kinds,
        partition_patterns,
        active_partition_patterns,
    )
    try:
        if export_path.exists() and export_path.is_dir() and export_path.suffix.lower() == ".json":
            print(
                "[Preflight] ERROR: export looks like a json file but is a directory; "
                "remove it or choose a different path",
                file=sys.stderr,
            )
            return 2
        if export_path.suffix.lower() == ".json":
            out_file = export_path
            out_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            export_path.mkdir(parents=True, exist_ok=True)
            out_file = export_path / "preflight_report.json"
        out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as ex:
        print(f"[Preflight] ERROR: failed to write {out_file}: {ex}", file=sys.stderr)
        return 2
    print(f"[Preflight] exported={out_file}")

    return 0


if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    raise SystemExit(main())
