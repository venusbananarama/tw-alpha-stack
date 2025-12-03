# -*- coding: utf-8 -*-
# FinMind Backfill (API, Strict + Day-Index Gating + end< filter)
#
# 用途：
#   - 給 FullMarket.ps1 / HH 線呼叫，負責四個 dataset：
#       prices   → TaiwanStockPrice
#       chip     → TaiwanStockInstitutionalInvestorsBuySell
#       per      → TaiwanStockPER
#       dividend → TaiwanStockDividend
#   - 採用「日索引 + 半開區間」：
#       * --start/--end 是 [start, end) 半開區間
#       * 只檢查對應索引日的銀河是否已有資料，有的股票就不再打 API
#   - 單支股票一個 API call，以 FINMIND_QPS / FINMIND_QPS_* 控制速率。
#
# 介面（保持跟舊版完全一致）：
#   --datasets     一個或多個 dataset alias（可含逗號）例如：prices,chip
#   --symbols      選擇性，指定一批個股；不給就用 investable_universe.txt
#   --start        起始日（含）
#   --end          結束日（不含）
#   --datahub-root 預設 datahub
#   --force        跳過覆蓋判斷（不看 DayIndex，全部重打一遍）

import os
import sys
import json
import time
import math
import argparse
import glob
import datetime
from urllib import request, parse
from typing import Optional, List, Tuple, Iterable

import pandas as pd

BASE = os.environ.get("FINMIND_BASE_URL", "https://api.finmindtrade.com/api/v4/data")
TOKEN = (os.environ.get("FINMIND_TOKEN") or "").strip()
if not TOKEN:
    print("ERROR: FINMIND_TOKEN 未設定", file=sys.stderr)
    sys.exit(2)


# ---------------------------------------------------------------------------
# Dataset alias / kind helpers
# ---------------------------------------------------------------------------


def alias_to_dataset(name: str) -> str:
    m = {
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
    k = (name or "").strip().lower()
    return m.get(k, name)


def dataset_to_kind(ds: str) -> str:
    a = ds.lower()
    if "price" in a:
        return "prices"
    if "buysell" in a or "institutional" in a:
        return "chip"
    if ("per" in a and "taiwanstockper" in a) or a.endswith("per"):
        return "per"
    if "dividend" in a:
        return "dividend"
    return "prices"


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


# ---------------------------------------------------------------------------
# FinMind API helpers
# ---------------------------------------------------------------------------


def http_get(dataset: str, data_id: str, start: str, end: str) -> pd.DataFrame:
    qs = parse.urlencode(
        {
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start,
            "end_date": end,
            "token": TOKEN,
        }
    )
    req = request.Request(f"{BASE}?{qs}")
    with request.urlopen(req, timeout=30) as r:
        obj = json.loads(r.read().decode("utf-8"))

    data = obj.get("data") or []
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].astype(str)
    df["symbol"] = df.get("stock_id", "").astype(str).str.replace(".TW", "", regex=False)
    return df


def fetch_with_retry(
    dataset: str,
    data_id: str,
    start: str,
    end: str,
    retries: int = 5,
    backoff: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    對單一 (dataset, stock_id, [start,end)) 做帶 backoff 的重試。

    - 成功回傳 DataFrame（可能為空）
    - 超過 retries 仍失敗則把最後一次例外拋出
    """
    if backoff is None:
        backoff = [3, 5, 8, 13, 21, 34]

    attempt = 0
    while True:
        try:
            return http_get(dataset, data_id, start, end)
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > retries:
                raise
            delay = backoff[min(attempt - 1, len(backoff) - 1)]
            print(
                f"[retry] {dataset} {data_id} {start}→{end} "
                f"attempt {attempt}/{retries} error={exc!r}, sleep={delay}s",
                file=sys.stderr,
            )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# 銀河 DayIndex & 寫入 helpers
# ---------------------------------------------------------------------------


def build_day_index(root: str, kind: str, day: str) -> Tuple[set, int]:
    """
    掃描銀河 shard，找出某一天已有資料的 stock_id 集合。

    回傳：
      (已涵蓋的 symbol 集合, 掃描的檔案數)
    """
    base = os.path.join(root, "silver", "alpha", kind)
    d = parse_date(day)
    ym = f"{d.year:04d}{d.month:02d}"
    pats = [os.path.join(base, f"yyyymm={ym}", "**", "*.parquet")]

    # 若 anchor 是月初，順便掃前一個月（避免跨月殘留）
    if d.day == 1:
        prev = d.replace(day=1) - datetime.timedelta(days=1)
        pats.append(
            os.path.join(base, f"yyyymm={prev.year:04d}{prev.month:02d}", "**", "*.parquet")
        )

    files: List[str] = []
    for p in pats:
        files.extend(glob.glob(p, recursive=True))

    seen: set = set()
    for fpath in files:
        try:
            df = pd.read_parquet(fpath, columns=["date", "stock_id"])
        except Exception:
            continue
        if df.empty:
            continue
        s = df["date"].astype(str) == day
        if not s.any():
            continue
        sy = df.loc[s, "stock_id"].astype(str).str.replace(".TW", "", regex=False).tolist()
        seen.update(sy)

    return seen, len(files)


def write_silver(df: pd.DataFrame, root: str, kind: str) -> int:
    """
    依 yyyymm 分片寫入銀河 silver/alpha/<kind>/yyyymm=YYYYMM/*.parquet
    """
    if df is None or df.empty:
        return 0

    df = df.copy()
    df["yyyymm"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m")

    total = 0
    for ym, g in df.groupby("yyyymm"):
        outdir = os.path.join(root, "silver", "alpha", kind, f"yyyymm={ym}")
        os.makedirs(outdir, exist_ok=True)
        out = os.path.join(outdir, f"ing_{kind}_{ym}_{int(time.time() * 1000)}.parquet")
        g.drop(columns=["yyyymm"], errors="ignore").to_parquet(out, index=False)
        total += len(g)

    return total


# ---------------------------------------------------------------------------
# Trading calendar helpers
# ---------------------------------------------------------------------------


def safe_read_csv(path: str) -> Optional[pd.DataFrame]:
    """
    安全讀取 CSV，失敗時回傳 None 並印警告。
    """
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001
        print(f"[cal] failed to read trading_days.csv at {path}: {exc!r}", file=sys.stderr)
        return None
    if df.empty:
        print(f"[cal] trading_days.csv at {path} is empty", file=sys.stderr)
        return None
    return df


def load_trading_days(path: str) -> List[datetime.date]:
    """
    從指定 CSV 路徑載入交易日清單。

    期望有一欄為日期；優先使用 'date' 欄，找不到就用第一欄。
    """
    df = safe_read_csv(path)
    if df is None:
        return []

    col = "date"
    if col not in df.columns:
        col = df.columns[0]

    try:
        s = pd.to_datetime(df[col], errors="coerce").dt.date
    except Exception as exc:  # noqa: BLE001
        print(f"[cal] failed to parse dates from {path}: {exc!r}", file=sys.stderr)
        return []

    dates = [d for d in s.tolist() if isinstance(d, datetime.date)]
    dates = sorted(set(dates))
    return dates


def resolve_trading_calendar_path(repo_root: str, datahub_root: str) -> Optional[str]:
    """
    找出 trading_days.csv 的實際路徑。

    搜尋順序：
      1) <datahub_root>/ref/trading_days.csv
      2) <repo_root>/datahub/ref/trading_days.csv
      3) <repo_root>/cal/trading_days.csv

    找到第一個存在者就回傳其路徑，全部不存在回傳 None。
    """
    candidates = [
        os.path.join(datahub_root, "ref", "trading_days.csv"),
        os.path.join(repo_root, "datahub", "ref", "trading_days.csv"),
        os.path.join(repo_root, "cal", "trading_days.csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def last_trading_day_before(end: str, trading_days: List[datetime.date]) -> str:
    """
    給定 end（YYYY-MM-DD；不含），找出 < end 的最後一個交易日。

    若 trading_days 為空或無符合，退回 end-1。
    """
    end_d = parse_date(end)
    if not trading_days:
        return (end_d - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    candidates = [d for d in trading_days if d < end_d]
    if not candidates:
        return (end_d - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    return max(candidates).strftime("%Y-%m-%d")


def has_trading_day_between(
    start: str,
    end: str,
    trading_days: List[datetime.date],
) -> bool:
    """
    判斷在 [start, end) 區間內是否存在至少一個交易日。

    trading_days 預期為升冪排序的 date list。
    """
    if not trading_days:
        # 沒有日曆就視為可能有交易日，不做任何優化。
        return True

    s = parse_date(start)
    e = parse_date(end)
    for d in trading_days:
        if d < s:
            continue
        if d >= e:
            break
        return True
    return False


# ---------------------------------------------------------------------------
# Universe / pool helpers
# ---------------------------------------------------------------------------


def load_pool(root: str = ".") -> List[str]:
    """
    Load investable universe (stock_id list) with a clear SSOT priority:

    1) <root>/investable_universe.txt          ← 新版唯一 SSOT
    2) <root>/configs/investable_universe.txt ← 舊版相容
    3) <root>/universe.tw_all.txt             ← 備援（全市場）
    """
    candidates = [
        os.path.join(root, "investable_universe.txt"),
        os.path.join(root, "configs", "investable_universe.txt"),
        os.path.join(root, "universe.tw_all.txt"),
    ]
    for p in candidates:
        if os.path.exists(p):
            syms: List[str] = []
            with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    x = line.strip().replace(".TW", "")
                    if x and len(x) == 4 and x.isdigit():
                        syms.append(x)
            syms = sorted(set(syms))
            print(f"[pool] loaded {len(syms)} symbols from {p}", file=sys.stderr)
            return syms

    print(
        "[pool] no investable_universe.txt or universe.tw_all.txt found; using empty universe",
        file=sys.stderr,
    )
    return []


def chunked(seq: Iterable[str], n: int) -> Iterable[List[str]]:
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


# ---------------------------------------------------------------------------
# QPS helpers
# ---------------------------------------------------------------------------


def resolve_dataset_qps(kind: str) -> float:
    """
    依 dataset kind (prices/chip/per/dividend) 決定 QPS。

    環境變數優先順序：
      1) FINMIND_QPS_<KIND>（例如 FINMIND_QPS_PRICES）
      2) FINMIND_QPS
      3) 預設 1.5
    """
    base_raw = os.environ.get("FINMIND_QPS", "1.5")
    kind_key = {
        "prices": "PRICES",
        "chip": "CHIP",
        "per": "PER",
        "dividend": "DIVIDEND",
    }.get(kind, None)

    raw = None
    if kind_key is not None:
        raw = os.environ.get(f"FINMIND_QPS_{kind_key}")

    if not raw:
        raw = base_raw

    try:
        qps = float(raw)
    except Exception:
        qps = 1.5

    if qps <= 0:
        qps = 1.5

    return qps


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", required=True)
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)  # 不含
    ap.add_argument("--datahub-root", default="datahub")
    ap.add_argument(
        "--force",
        action="store_true",
        help="跳過覆蓋判斷，直接打 API",
    )
    args = ap.parse_args()

    # 處理 datasets（alias → FinMind dataset 名稱）
    ds_list: List[str] = []
    for token in args.datasets:
        for part in (token.split(",") if "," in token else [token]):
            ds_list.append(alias_to_dataset(part))
    ds_list = list(dict.fromkeys(ds_list))  # 去重保持順序

    # 處理 universe
    syms: List[str] = []
    if args.symbols:
        for s in args.symbols:
            for p in (s.split(",") if "," in s else [s]):
                q = p.strip().replace(".TW", "")
                if q and len(q) == 4 and q.isdigit():
                    syms.append(q)
        syms = sorted(set(syms))
    if not syms:
        # 使用目前工作目錄作為 root（例如由 FullMarket.ps1 設定為 repo root）
        syms = load_pool(root=".")

    print("=== FinMind Backfill (API, Strict + DayIndex + TradingCalendar) ===")
    print(f"Start={args.start} End={args.end} Universe={'TSE' if syms else 'N/A'}")
    print(f"Datasets={','.join([dataset_to_kind(d) for d in ds_list])}")
    mode = ("單股 指定 %d 檔" % len(syms)) if syms else "全市場（本地投資池）"
    print(f"Mode={mode}")

    # 基礎 QPS 提示
    base_qps = resolve_dataset_qps("prices")
    print(f"Base QPS (FINMIND_QPS or *_PRICES)≈{base_qps:.3f}")

    # Trading calendar：嘗試從 datahub/ref 或 cal/ 取得 trading_days.csv
    repo_root = os.getcwd()
    datahub_root_abs = os.path.abspath(args.datahub_root)
    cal_path = resolve_trading_calendar_path(repo_root, datahub_root_abs)
    if cal_path:
        trading_days = load_trading_days(cal_path)
        print(
            f"[cal] loaded {len(trading_days)} trading days from {cal_path}",
            file=sys.stderr,
        )
    else:
        trading_days = []
        print(
            "[cal] trading_days.csv not found; trading calendar optimization disabled",
            file=sys.stderr,
        )

    if trading_days:
        t0 = last_trading_day_before(args.end, trading_days)
    else:
        t0 = (parse_date(args.end) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    totals: List[dict] = []

    for ds in ds_list:
        kind = dataset_to_kind(ds)
        rows_written = 0
        files_out = 0
        sink_dir = os.path.join(args.datahub_root, "silver", "alpha", kind)

        ds_qps = resolve_dataset_qps(kind)
        sleep_sec = max(0.0, 1.0 / ds_qps)
        print(f"== Dataset {kind}: QPS={ds_qps:.3f}, sleep={sleep_sec:.3f}s ==")

        # 若交易日行事曆存在且此區間完全沒有交易日，整段直接視為 no-op
        if trading_days and not has_trading_day_between(args.start, args.end, trading_days):
            print(
                f"== Phase: {kind} trading_calendar === "
                f"no trading day in [{args.start},{args.end}), skip API"
            )
            totals.append(
                dict(
                    dataset=kind,
                    mode=mode,
                    estcalls=0,
                    rows_written=0,
                    files_out=0,
                    sink_dir=sink_dir,
                )
            )
            continue

        if args.force:
            todo = list(syms)
            print(f"== Phase: {kind}  FORCE mode todo={len(todo)}")
        else:
            day_syms, fcnt = build_day_index(args.datahub_root, kind, t0)
            todo = [s for s in syms if s not in day_syms]
            print(
                f"== Phase: {kind}  index(anchor={t0}) files={fcnt} "
                f"covered={len(day_syms)} todo={len(todo)}"
            )

        for s in todo:
            try:
                df = fetch_with_retry(ds, s, args.start, args.end)
                # SSOT：--end 為「不含」，過濾掉 >= end 的資料
                if df is not None and not df.empty:
                    try:
                        di = pd.to_datetime(df["date"], errors="coerce")
                        df = df[di < pd.to_datetime(args.end)]
                    except Exception:
                        pass
                if df is not None and not df.empty:
                    w = write_silver(df, args.datahub_root, kind)
                    rows_written += w
                    if w > 0:
                        files_out += 1
            except Exception as e:  # noqa: BLE001
                print(f"[WARN] {kind} {s}: {e}", file=sys.stderr)
            finally:
                if sleep_sec > 0:
                    time.sleep(sleep_sec)

        print(f"OK {kind}: rows_written={rows_written} files_out={files_out}")
        totals.append(
            dict(
                dataset=kind,
                mode=mode,
                estcalls=len(todo),
                rows_written=rows_written,
                files_out=files_out,
                sink_dir=sink_dir,
            )
        )

    os.makedirs("metrics", exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    outcsv = os.path.join("metrics", f"ingest_summary_{ts}_finmind.csv")
    pd.DataFrame(totals).to_csv(outcsv, index=False, encoding="utf-8")
    print(f"=== Backfill Done ===  metrics: {os.path.abspath(outcsv)}")


if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
