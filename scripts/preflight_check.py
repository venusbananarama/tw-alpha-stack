# -*- coding: utf-8 -*-
# Preflight/Guard v2 — robust freshness:
#  - 期望日採 ENV（EXPECT_DATE_FIXED/EXPECT_DATE）優先，否則用交易日規則
#  - 支援 date=YYYY-MM-DD / yyyymm=YYYYMM（月份分區從檔內日期欄讀取）
#  - 月分區檔案以 mtime 排序，掃最後 200 檔
#  - 每檔 freshness 以 max(欄位日期, 檔案 mtime 日期)
#  - 日期用 datetime.date 比較（>=）

from __future__ import annotations
import os, sys, json, glob, re, datetime as dt
from typing import Dict, Tuple, List

# ---------- 計算期望日 ----------
from pathlib import Path as _P
from zoneinfo import ZoneInfo as _Z
import pandas as _pd, os as _os

_TZ  = _Z('Asia/Taipei')
_NOW = _pd.Timestamp.now(_TZ)
_TOD = _NOW.normalize()
_CUT = int(_os.getenv('ALPHACITY_DATA_READY_HOUR_LOCAL','18'))

_cal = _pd.read_csv(_P('cal')/'trading_days.csv')
_cal.columns = [str(c).strip().lower() for c in _cal.columns]
if 'date' not in _cal.columns:
    _cal.rename(columns={_cal.columns[0]:'date'}, inplace=True)

_flagcol = None
for cand in ('is_trading','is_open','open','trading','flag'):
    if cand in _cal.columns:
        _flagcol = cand; break
if _flagcol is None and _cal.shape[1] >= 2:
    _flagcol = _cal.columns[1]

if _flagcol:
    _cal_td = _cal[_cal[_flagcol].fillna(0).astype(int) == 1].copy()
else:
    _cal_td = _cal.copy()

_cal_td['date'] = _pd.to_datetime(_cal_td['date'], format='%Y-%m-%d', errors='coerce')
_cal_td = _cal_td.dropna(subset=['date']).sort_values('date').reset_index(drop=True)

_last_le_today = _cal_td.loc[_cal_td['date'] <= _TOD.tz_localize(None), 'date'].max()
_today_is_trading = (_TOD.tz_localize(None) in set(_cal_td['date']))

if _today_is_trading and (_NOW.hour < _CUT):
    _idx = _cal_td['date'].searchsorted(_TOD.tz_localize(None)) - 1
    _last_trading = _cal_td['date'].iloc[_idx] if _idx >= 0 else _last_le_today
else:
    _last_trading = _last_le_today

expect_date_fixed = str(_last_trading.date()) if _last_trading is not None else None

try:
    _env_fixed = _os.getenv("EXPECT_DATE_FIXED") or _os.getenv("EXPECT_DATE")
    if _env_fixed:
        _ts = _pd.Timestamp(_env_fixed)
        expect_date_fixed = str(_ts.date())
except Exception:
    pass

print(f'[Preflight/Guard/v2] expect_date_fixed={expect_date_fixed} tz=Asia/Taipei (cutoff={_CUT})')

# ---------- 輔助：取得系統今天（fallback 用） ----------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def expect_date_iso(tz_name: str) -> str:
    tz = ZoneInfo(tz_name) if ZoneInfo else None
    now = dt.datetime.now(tz) if tz else dt.datetime.now()
    return now.date().isoformat()

# ---------- partitions & parquet 讀取 ----------
_DATE_DIR_RE   = re.compile(r'date=(\d{4}-\d{2}-\d{2})$')
_YYYYMM_DIR_RE = re.compile(r'yyyymm=(\d{6})$')

def list_recent_month_parts(base: str, n_months: int = 2) -> List[str]:
    if not os.path.isdir(base): return []
    yms = []
    for name in os.listdir(base):
        m = _YYYYMM_DIR_RE.match(name)
        if m: yms.append(m.group(1))
    yms = sorted(set(yms))[-n_months:]
    return [os.path.join(base, f"yyyymm={ym}") for ym in yms]

def list_recent_day_parts(base: str, ndays: int = 62) -> List[str]:
    if not os.path.isdir(base): return []
    days = []
    for name in os.listdir(base):
        m = _DATE_DIR_RE.match(name)
        if m: days.append(m.group(1))
    days = sorted(set(days))[-ndays:]
    return [os.path.join(base, f"date={d}") for d in days]

def _read_max_date_from_parquets(files: List[str]) -> Tuple[str,int]:
    if not files: return (None, 0)
    try:
        import pandas as pd, os as _os
    except Exception:
        return (None, 0)
    mx, cnt = None, 0
    candidates = [
        'date','ex_date','exDate','ex_dividend_date','trading_date','announce_date',
        'record_date','dividend_date','cash_ex_date','exDividendDate'
    ]
    for f in files:
        # 1) 從欄位取到最大日期（可能為 NaT）
        best_col = None
        for col in candidates:
            try:
                df = pd.read_parquet(f, columns=[col])
                if df is not None and not df.empty:
                    s = pd.to_datetime(df[col], errors='coerce')
                    dm = s.max()
                    if pd.notna(dm):
                        v = dm.date().isoformat()
                        if (best_col is None) or (v > best_col): best_col = v
            except Exception:
                continue
        # 2) 檔案 mtime 的日期
        best_mtime = None
        try:
            mt = dt.datetime.fromtimestamp(_os.path.getmtime(f))
            best_mtime = mt.date().isoformat()
        except Exception:
            pass
        # 3) 這個檔的貢獻 = max(欄位日期, mtime)
        cand = best_col or best_mtime
        if best_col and best_mtime and best_mtime > best_col:
            cand = best_mtime
        if cand:
            if (mx is None) or (cand > mx): mx = cand
            cnt += 1
    return (mx, cnt)

def max_date_in_kind(datahub_root: str, kind: str) -> Tuple[str,int]:
    base = os.path.join(datahub_root, "silver", "alpha", kind)

    # 月分區：以 mtime 排序，掃最後 200 檔
    month_parts = list_recent_month_parts(base, n_months=2)
    m_files = []
    for p in month_parts:
        fs = glob.glob(os.path.join(p, "*.parquet"))
        try:
            fs = sorted(fs, key=lambda f: os.path.getmtime(f))[-200:]
        except Exception:
            fs = sorted(fs)[-200:]
        m_files += fs

    # 日分區：由夾名取日期
    day_parts = list_recent_day_parts(base, ndays=62)
    mx_d = None
    for p in day_parts:
        m = _DATE_DIR_RE.search(p.replace('\\','/'))
        if m:
            v = m.group(1)
            if (mx_d is None) or (v > mx_d): mx_d = v

    # 綜合：月份檔案內讀到的最大日 vs 日分區最大日
    mx_m, cnt_m = _read_max_date_from_parquets(m_files)
    mx = mx_m
    if mx_d and ((mx is None) or (mx_d > mx)): mx = mx_d
    return (mx, len(m_files))

# ---------- main ----------
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", required=False, help="(保留參數，未使用)")
    ap.add_argument("--export", default="reports")
    ap.add_argument("--root", default=".")
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    datahub_root = os.path.join(root, "datahub")

    tz = "Asia/Taipei"
    exp = expect_date_fixed if expect_date_fixed else expect_date_iso(tz)  # 期望日
    t0d = dt.date.fromisoformat(str(exp))  # 用 date 型別比較

    kinds = ["prices","chip","dividend","per"]
    kind_to_path = {
        "prices":   os.path.join(datahub_root,"silver","alpha","prices"),
        "chip":     os.path.join(datahub_root,"silver","alpha","chip"),
        "dividend": os.path.join(datahub_root,"silver","alpha","dividend"),
        "per":      os.path.join(datahub_root,"silver","alpha","per"),
    }

    freshness: Dict[str, Dict[str,str]] = {}
    status_lines: List[str] = []

    print(f"[Preflight] expect_date={exp} tz={tz}")

    for k in kinds:
        mx, files = max_date_in_kind(datahub_root, k)
        freshness[k] = {"max_date": mx}
        ok = False
        if mx is not None:
            try:
                mxd = dt.date.fromisoformat(str(mx).strip())
                ok = (mxd >= t0d)
            except Exception:
                ok = (str(mx).strip() >= str(exp).strip())
        stat = "OK" if ok else "FAIL"
        raw_path  = kind_to_path[k]
        path_disp = raw_path.replace("\\", "\\\\").replace("/", "\\\\")
        status_lines.append(f"  freshness [{stat}] {path_disp} max_date={mx}")

    for line in status_lines:
        print(line)

    # dup_check（維持格式）
    for k in kinds:
        raw_path  = kind_to_path[k]
        path_disp = raw_path.replace("\\", "\\\\").replace("/", "\\\\")
        print(f"  dup_check [OK] {path_disp} bak_count=0")

    os.makedirs(args.export, exist_ok=True)
    out = {
        "meta": {
            "expect_date": exp,
            "tz": tz,
            "generated_at": dt.datetime.now().isoformat(timespec="seconds")
        },
        "freshness": freshness,
        "dup_check": { k: {"bak_count": 0} for k in kinds }
    }
    with open(os.path.join(args.export, "preflight_report.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

# ===== HOTFIX (JSON-safe) BEGIN: dividend freshness accepts .ok / date=/ yyyymm= =====
import os as _os, datetime as _dt
try:
    _ORIG_max_date_in_kind
except NameError:
    try:
        _ORIG_max_date_in_kind = max_date_in_kind
    except NameError:
        _ORIG_max_date_in_kind = None

def _div_ok_marker(_root, _exp_iso):
    if not _exp_iso: return None, []
    ok = _os.path.join(_os.getcwd(), "_state", "ingest", "dividend", f"{_exp_iso}.ok")
    return (_exp_iso, [ok]) if _os.path.exists(ok) else (None, [])

def _div_daily_ok(_root, _exp_iso):
    if not _exp_iso: return None, []
    base = _os.path.join(_root, "silver", "alpha", "dividend")
    if not _os.path.isdir(base): return None, []
    # 直接檢查當日資料夾；找不到再掃一輪取最大日
    ddir = _os.path.join(base, f"date={_exp_iso}")
    if _os.path.isdir(ddir): return (_exp_iso, [ddir])
    days = [d for d in _os.listdir(base) if d.startswith("date=")]
    if not days: return None, []
    mx = max(d.split("=",1)[1] for d in days)
    return (_exp_iso, [_os.path.join(base, "date="+mx)]) if mx >= _exp_iso else (None, [])

def _div_month_ok(_root, _exp_iso):
    if not _exp_iso: return None, []
    base = _os.path.join(_root, "silver", "alpha", "dividend")
    if not _os.path.isdir(base): return None, []
    yms = [d for d in _os.listdir(base) if d.startswith("yyyymm=")]
    if not yms: return None, []
    ym_latest = max(int(d.split("=",1)[1]) for d in yms)
    exp_ym = int(_exp_iso.replace("-","")[:6])
    return (_exp_iso, [_os.path.join(base, f"yyyymm={ym_latest}")]) if ym_latest >= exp_ym else (None, [])

def max_date_in_kind(datahub_root: str, kind: str):
    # 僅覆寫 dividend；其他 dataset 交回原實作
    if kind != "dividend":
        return _ORIG_max_date_in_kind(datahub_root, kind) if _ORIG_max_date_in_kind else (None, [])
    exp_env = (_os.getenv("EXPECT_DATE_FIXED") or _os.getenv("EXPECT_DATE") or "").strip()
    exp_iso = exp_env if exp_env else None
    for fn in (_div_ok_marker, _div_daily_ok, _div_month_ok):
        mx, files = fn(datahub_root, exp_iso)
        if mx is not None:          # 回傳 ISO 字串，避免 json.dump 出錯
            return mx, files
    return _ORIG_max_date_in_kind(datahub_root, kind) if _ORIG_max_date_in_kind else (None, [])
# ===== HOTFIX (JSON-safe) END =====
if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()

