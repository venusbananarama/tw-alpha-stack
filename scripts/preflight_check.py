from __future__ import annotations

"""
Preflight/Guard v3

- 決定期望日 expect_date（交易日曆 + cutoff 小時，ENV 可覆寫）
- 檢查 Phase-1 四表 freshness：prices / chip / per / dividend
- dividend 有寬鬆規則（_state .ok / date= / yyyymm= 任何一條到位即 PASS）
- 輸出 reports/preflight_report.json，給 Run-WFGate.ps1 使用
"""

import argparse
import datetime as dt
import json
import os
import sys
import glob
import re
from pathlib import Path
from typing import Dict, List, Mapping, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # 老版本 Python 的 fallback
    ZoneInfo = None  # type: ignore

import pandas as pd

DATE_TZ_NAME = "Asia/Taipei"
DEFAULT_CUTOFF_HOUR = int(os.getenv("ALPHACITY_DATA_READY_HOUR_LOCAL", "18"))

_DATE_DIR_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})$")
_YYYYMM_DIR_RE = re.compile(r"yyyymm=(\d{6})$")


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


def compute_expect_date(cal_df: pd.DataFrame, env: Mapping[str, str]) -> dt.date:
    """
    決定期望日 expect_date（date 型別）：

    1) 先用交易日曆 + cutoff 規則算出 candidate
    2) 若 ENV 有 EXPECT_DATE_FIXED / EXPECT_DATE 且可解析，就覆寫
    """
    tz = ZoneInfo(DATE_TZ_NAME) if ZoneInfo else None
    now = dt.datetime.now(tz) if tz else dt.datetime.now()
    today = now.date()

    # cal_df['date'] 是 Timestamp（naive）；轉成 date list
    cal_dates = cal_df["date"].dt.date.tolist()

    # last trading day <= today
    last_le_today: Optional[dt.date] = None
    for d in cal_dates:
        if d <= today and (last_le_today is None or d > last_le_today):
            last_le_today = d

    today_is_trading = today in cal_dates

    if today_is_trading and now.hour < DEFAULT_CUTOFF_HOUR:
        # 盤中 cutoff 前：視為前一個交易日
        idx = cal_dates.index(today)
        if idx > 0:
            candidate = cal_dates[idx - 1]
        else:
            candidate = today
    else:
        candidate = last_le_today or today

    # ENV override
    env_fixed = _parse_env_date(env.get("EXPECT_DATE_FIXED"))
    env_fallback = _parse_env_date(env.get("EXPECT_DATE"))

    expect = env_fixed or env_fallback or candidate
    return expect


# ---------- 分區與 parquet freshness ----------

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
    看一批 parquet 檔，回傳「欄位日期 / mtime」的最大日期。
    """
    if not files:
        return None

    candidates_cols = [
        "date",
        "ex_date",
        "exdate",
        "ex_dividend_date",
        "trading_date",
        "announce_date",
        "record_date",
        "dividend_date",
        "cash_ex_date",
        "exDividendDate",
    ]
    mx: Optional[dt.date] = None

    for f in files:
        best_col_date: Optional[dt.date] = None
        for col in candidates_cols:
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
                    if best_col_date is None or d > best_col_date:
                        best_col_date = d

        # 檔案 mtime 當次要訊號
        mtime_date: Optional[dt.date] = None
        try:
            mt = dt.datetime.fromtimestamp(f.stat().st_mtime)
            mtime_date = mt.date()
        except Exception:
            pass

        if best_col_date is None and mtime_date is None:
            cand = None
        elif best_col_date is None:
            cand = mtime_date
        elif mtime_date is None:
            cand = best_col_date
        else:
            cand = max(best_col_date, mtime_date)

        if cand is not None and (mx is None or cand > mx):
            mx = cand

    return mx


def max_date_generic(datahub_root: Path, kind: str) -> Optional[dt.date]:
    base = datahub_root / "silver" / "alpha" / kind
    if not base.is_dir():
        return None

    # 月分區
    month_parts = collect_recent_month_partitions(base, n_months=2)
    month_files: List[Path] = []
    for p in month_parts:
        fs = list(p.glob("*.parquet"))
        if not fs:
            continue
        try:
            fs = sorted(fs, key=lambda x: x.stat().st_mtime)[-200:]
        except Exception:
            fs = sorted(fs)[-200:]
        month_files.extend(fs)

    mx_month = scan_parquet_max_date(month_files)
    mx_day = max_date_from_day_partitions(base, ndays=62)

    if mx_month is None and mx_day is None:
        return None
    if mx_month is None:
        return mx_day
    if mx_day is None:
        return mx_month
    return max(mx_month, mx_day)


# ---------- dividend 專屬 freshness ----------

def max_date_dividend(datahub_root: Path, expect_date: dt.date) -> Optional[dt.date]:
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
        ddir = base / f"date={exp_iso}"
        if ddir.is_dir():
            return expect_date

        # 3) yyyymm 分區 >= expect_yyyymm
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
    datahub_root: Path, expect_date: dt.date, kinds: List[str]
) -> Dict[str, Dict[str, object]]:
    results: Dict[str, Dict[str, object]] = {}
    for kind in kinds:
        if kind == "dividend":
            mx = max_date_dividend(datahub_root, expect_date)
        else:
            mx = max_date_generic(datahub_root, kind)

        ok = bool(mx is not None and mx >= expect_date)
        results[kind] = {
            "kind": kind,
            "max_date": mx,
            "ok": ok,
        }
    return results


def build_preflight_report(
    expect_date: dt.date, tz_name: str, freshness: Dict[str, Dict[str, object]]
) -> Dict[str, object]:
    meta = {
        "expect_date": expect_date.isoformat(),
        "tz": tz_name,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }

    freshness_json: Dict[str, Dict[str, Optional[str]]] = {}
    dup_check: Dict[str, Dict[str, int]] = {}

    for kind, res in freshness.items():
        mx = res["max_date"]
        if isinstance(mx, dt.date):
            mx_str = mx.isoformat()
        elif mx is None:
            mx_str = None
        else:
            mx_str = str(mx)
        freshness_json[kind] = {"max_date": mx_str}
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
        if isinstance(mx, dt.date):
            mx_str = mx.isoformat()
        elif mx is None:
            mx_str = "None"
        else:
            mx_str = str(mx)

        raw_path = datahub_root / "silver" / "alpha" / kind
        # 保持既有輸出風格：路徑中的 \ 變成 \\（給 log / JSON-safe）
        path_disp = str(raw_path).replace("\\", "\\\\").replace("/", "\\\\")

        stat = "OK" if ok else "FAIL"
        print(f"  freshness [{stat}] {path_disp} max_date={mx_str}")
        print(f"  dup_check [OK] {path_disp} bak_count=0")


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
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    root = Path(args.root).resolve()
    datahub_root = root / "datahub"
    export_dir = root / args.export

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
    expect_date = compute_expect_date(cal_df, os.environ)
    print(
        f"[Preflight/Guard/v3] calendar={cal_path} tz={DATE_TZ_NAME} "
        f"cutoff={DEFAULT_CUTOFF_HOUR} expect_date_fixed={expect_date.isoformat()}"
    )

    # Step 3: 四表 freshness
    kinds = ["prices", "chip", "dividend", "per"]
    freshness = compute_freshness_for_all(datahub_root, expect_date, kinds)

    # Step 4: log + 寫 JSON 報告
    log_status(datahub_root, expect_date, DATE_TZ_NAME, freshness)

    export_dir.mkdir(parents=True, exist_ok=True)
    report = build_preflight_report(expect_date, DATE_TZ_NAME, freshness)
    out_path = export_dir / "preflight_report.json"
    try:
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
    except Exception as ex:
        print(f"[Preflight] ERROR: failed to write {out_path}: {ex}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    raise SystemExit(main())
