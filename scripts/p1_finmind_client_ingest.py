#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
p1_finmind_client_ingest.py

FinMind 抓資料引擎 v2（單一入口）

職責：
- 與 FinMind /api/v4/data 溝通（HTTP GET）
- 統一處理：
    - dataset alias → 正式 FinMind dataset 名稱
    - token / base_url / timeout / Qps / Rpm / max_retries
    - 節流（Qps / Rpm）
    - retry
    - JSON 解析與簡單 schema 檢查
    - 回傳 pandas.DataFrame
- 不負責：
    - 檔案 I/O（parquet）
    - .ok checkpoint
    - ingest_ledger
    - Gate / Preflight

供 HH / CodeD / FullMarket-DateID 等 orchestrator 呼叫。
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase1 import rate_control  # noqa: E402

# -----------------------------------------------------------------------------
# 常數與 dataset alias
# -----------------------------------------------------------------------------

DEFAULT_BASE_URL: str = "https://api.finmindtrade.com/api/v4/data"

#: tw-alpha-stack Phase-1 / DateID 會用到的 dataset alias
DATASET_ALIASES: Dict[str, str] = {
    # 四表（D 線）
    "prices": "TaiwanStockPrice",
    "price": "TaiwanStockPrice",
    "taiwanstockprice": "TaiwanStockPrice",
    "chip": "TaiwanStockInstitutionalInvestorsBuySell",
    "per": "TaiwanStockPER",
    "dividend": "TaiwanStockDividend",
    # dateID 六表
    "finstmt": "TaiwanStockFinancialStatements",
    "bs": "TaiwanStockBalanceSheet",
    "cfs": "TaiwanStockCashFlowsStatement",
    "shareholding": "TaiwanStockShareholding",
    "inst_total": "TaiwanStockTotalInstitutionalInvestors",
    "gov_bank": "TaiwanStockGovernmentBankBuySell",
}

# -----------------------------------------------------------------------------
# 例外型別
# -----------------------------------------------------------------------------

class FinMindError(Exception):
    """基底例外型別。"""


class FinMindAuthError(FinMindError):
    """token 或認證相關錯誤。"""


class FinMindNetworkError(FinMindError):
    """網路 / timeout / 連線錯誤。"""


class FinMindRateLimitError(FinMindError):
    """被 API rate-limit 或明顯 Qps/Rpm 超標。"""


class FinMindSchemaError(FinMindError):
    """回傳 JSON / 欄位與預期 schema 不符。"""


class FinMindConfigError(FinMindError):
    """設定錯誤（dataset alias、Qps/Rpm 等）。"""


# -----------------------------------------------------------------------------
# 設定與統計
# -----------------------------------------------------------------------------

@dataclass
class FinMindConfig:
    base_url: str
    token: str
    timeout_sec: float
    qps: float
    rpm: int
    max_concurrency: int
    max_retries: int


@dataclass
class IngestStats:
    dataset: str
    num_ids: int
    start_date: Optional[str]
    end_date: Optional[str]
    date_id_count: Optional[int]
    total_requests: int = 0
    total_retries: int = 0
    total_failures: int = 0
    elapsed_sec: float = 0.0


def load_config_from_env() -> FinMindConfig:
    """
    從環境變數載入 FinMind 設定。

    環境變數：
        FINMIND_TOKEN            (必填)
        FINMIND_BASE_URL         (選填, 預設 DEFAULT_BASE_URL)
        FINMIND_QPS              (選填, 預設 0.5)
        FINMIND_RPM              (選填, 預設 30)
        FINMIND_MAX_CONCURRENCY  (選填, 預設 1)
        FINMIND_TIMEOUT_SEC      (選填, 預設 30)
        FINMIND_MAX_RETRIES      (選填, 預設 3)
    """
    token = (os.environ.get("FINMIND_TOKEN") or "").strip()
    if not token:
        raise FinMindAuthError("FINMIND_TOKEN 未設定，無法呼叫 FinMind API。")

    base_url = (os.environ.get("FINMIND_BASE_URL") or DEFAULT_BASE_URL).strip()

    def _float_env(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    def _int_env(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    timeout_sec = _float_env("FINMIND_TIMEOUT_SEC", 30.0)
    qps = _float_env("FINMIND_QPS", 0.5)
    rpm = _int_env("FINMIND_RPM", 30)
    max_concurrency = _int_env("FINMIND_MAX_CONCURRENCY", 1)
    max_retries = _int_env("FINMIND_MAX_RETRIES", 3)

    if qps <= 0:
        raise FinMindConfigError(f"FINMIND_QPS 必須 > 0, 目前={qps}")
    if rpm <= 0:
        raise FinMindConfigError(f"FINMIND_RPM 必須 > 0, 目前={rpm}")
    if max_concurrency <= 0:
        raise FinMindConfigError(
            f"FINMIND_MAX_CONCURRENCY 必須 > 0, 目前={max_concurrency}"
        )

    if _shared_bucket_enabled():
        qps = max(qps, 2.0)
        rpm = max(rpm, 120)

    return FinMindConfig(
        base_url=base_url,
        token=token,
        timeout_sec=timeout_sec,
        qps=qps,
        rpm=rpm,
        max_concurrency=max_concurrency,
        max_retries=max_retries,
    )


def override_config(
    cfg: FinMindConfig,
    *,
    qps: Optional[float] = None,
    rpm: Optional[int] = None,
    max_concurrency: Optional[int] = None,
) -> FinMindConfig:
    """
    在既有 config 上覆寫少數欄位（qps / rpm / max_concurrency）。
    不會修改原物件，回傳新 config。
    """
    new_cfg = dataclasses.replace(cfg)
    if qps is not None:
        if qps <= 0:
            raise FinMindConfigError(f"qps 必須 > 0, 目前={qps}")
        new_cfg.qps = qps
    if rpm is not None:
        if rpm <= 0:
            raise FinMindConfigError(f"rpm 必須 > 0, 目前={rpm}")
        new_cfg.rpm = rpm
    if max_concurrency is not None:
        if max_concurrency <= 0:
            raise FinMindConfigError(
                f"max_concurrency 必須 > 0, 目前={max_concurrency}"
            )
        new_cfg.max_concurrency = max_concurrency
    return new_cfg


# -----------------------------------------------------------------------------
# RateLimiter（目前實作為單執行緒用，max_concurrency 保留未來擴充）
# -----------------------------------------------------------------------------

@dataclass
class RateLimiterState:
    last_request_ts: float
    window_start_ts: float
    requests_in_window: int


class RateLimiter:
    """
    以 Qps / Rpm 實作的簡單節流器。

    注意：目前實作以「單執行緒？」的迴圈為前提，max_concurrency
    尚未啟用多執行緒，只當作設定保留，避免超前複雜度。
    """

    def __init__(self, cfg: FinMindConfig) -> None:
        self.cfg = cfg
        now = time.monotonic()
        self.state = RateLimiterState(
            last_request_ts=0.0,
            window_start_ts=now,
            requests_in_window=0,
        )

    def acquire(self) -> None:
        """
        在每次送 request 前呼叫，根據 qps / rpm 決定是否 sleep。
        """
        now = time.monotonic()
        sleep_sec = 0.0

        # QPS 控制（最小間隔）
        if self.cfg.qps > 0 and self.state.last_request_ts > 0:
            min_interval = 1.0 / self.cfg.qps
            since_last = now - self.state.last_request_ts
            if since_last < min_interval:
                sleep_sec = max(sleep_sec, min_interval - since_last)

        # RPM 控制（60 秒窗口）
        window_len = 60.0
        if now - self.state.window_start_ts >= window_len:
            # 新視窗
            self.state.window_start_ts = now
            self.state.requests_in_window = 0
        else:
            if (
                self.cfg.rpm > 0
                and self.state.requests_in_window >= self.cfg.rpm
            ):
                # 本視窗已達上限，等待到下一視窗
                sleep_sec = max(
                    sleep_sec,
                    window_len - (now - self.state.window_start_ts),
                )

        if sleep_sec > 0:
            time.sleep(sleep_sec)

        # 更新狀態
        now2 = time.monotonic()
        self.state.last_request_ts = now2
        if now2 - self.state.window_start_ts >= window_len:
            # 視窗剛好滾動
            self.state.window_start_ts = now2
            self.state.requests_in_window = 0
        self.state.requests_in_window += 1


# -----------------------------------------------------------------------------
# 公共 helper：dataset name 正規化
# -----------------------------------------------------------------------------

def resolve_dataset_name(alias_or_name: str) -> str:
    """
    將簡寫（prices/chip/...）或原始 FinMind dataset 名稱，
    正規化成正式 FinMind dataset 名。
    """
    key = (alias_or_name or "").strip()
    if not key:
        raise FinMindConfigError("dataset 名稱不可為空。")

    lower = key.lower()
    if lower in DATASET_ALIASES:
        return DATASET_ALIASES[lower]

    # 若不是 alias，就當作原始 dataset 名；這裡不強制檢查有效性，
    # 由 FinMind API 回應決定。
    return key


# -----------------------------------------------------------------------------
# HTTP client + retry
# -----------------------------------------------------------------------------

_logger = logging.getLogger(__name__)
_SHARED_BUCKET: Optional[rate_control.SharedTokenBucket] = None
_SHARED_BUCKET_READY = False


def _shared_bucket_enabled() -> bool:
    raw = (os.environ.get("FINMIND_SHARED_BUCKET") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _get_shared_bucket() -> Optional[rate_control.SharedTokenBucket]:
    global _SHARED_BUCKET
    global _SHARED_BUCKET_READY
    if not _SHARED_BUCKET_READY:
        _SHARED_BUCKET = rate_control.load_bucket_from_env(_REPO_ROOT)
        _SHARED_BUCKET_READY = True
        if _SHARED_BUCKET is not None:
            _logger.info(
                "shared bucket enabled rpm=%s burst=%s lease=%s max_wait=%s state=%s",
                _SHARED_BUCKET.rpm,
                _SHARED_BUCKET.burst,
                _SHARED_BUCKET.lease_size,
                _SHARED_BUCKET.max_wait_sec,
                _SHARED_BUCKET.state_path,
            )
    return _SHARED_BUCKET


def _send_request_once(
    cfg: FinMindConfig,
    session: requests.Session,
    params: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    單次 HTTP 呼叫：不含 retry / 節流，僅負責：
      - GET
      - HTTP status 檢查
      - JSON 解析
      - FinMind status/msg 檢查
      - 回傳 data list
    """
    # 加入 token
    full_params = dict(params)
    full_params.setdefault("token", cfg.token)

    try:
        resp = session.get(
            cfg.base_url,
            params=full_params,
            timeout=cfg.timeout_sec,
        )
    except requests.RequestException as exc:  # 網路層錯誤
        raise FinMindNetworkError(f"FinMind 請求失敗：{exc!r}") from exc

    if resp.status_code == 429:
        raise FinMindRateLimitError("FinMind 回應 429 Too Many Requests")

    if resp.status_code >= 500:
        raise FinMindNetworkError(
            f"FinMind 伺服器錯誤, status={resp.status_code}"
        )

    if resp.status_code >= 400:
        raise FinMindError(
            f"FinMind 回應非 2xx, status={resp.status_code}"
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        raise FinMindSchemaError(f"FinMind 回應非 JSON：{exc!r}") from exc

    status = payload.get("status")
    msg = str(payload.get("msg", "") or "")

    # v4 通常 status=200 為成功
    if status is not None and status != 200:
        msg_lower = msg.lower()
        if "too many" in msg_lower or "rate limit" in msg_lower:
            raise FinMindRateLimitError(
                f"FinMind rate-limit：status={status}, msg={msg}"
            )
        raise FinMindError(
            f"FinMind 回應錯誤：status={status}, msg={msg}"
        )

    data = payload.get("data")
    if data is None:
        raise FinMindSchemaError("FinMind 回應缺少 'data' 欄位。")
    if not isinstance(data, list):
        raise FinMindSchemaError("'data' 不是 list。")

    return data  # type: ignore[return-value]


def _send_request_with_retry(
    cfg: FinMindConfig,
    limiter: RateLimiter,
    session: requests.Session,
    params: Dict[str, str],
    stats: Optional[IngestStats] = None,
) -> List[Dict[str, Any]]:
    """
    在單一查詢上套用節流 + retry。
    只重試 Network / RateLimit 類錯誤，其餘錯誤直接拋出。
    """
    last_err: Optional[Exception] = None

    for attempt in range(cfg.max_retries + 1):
        if stats is not None:
            stats.total_requests += 1

        bucket = _get_shared_bucket()
        if bucket is not None:
            bucket.acquire()

        # 節流
        limiter.acquire()

        try:
            return _send_request_once(cfg, session, params)
        except (FinMindNetworkError, FinMindRateLimitError) as exc:
            last_err = exc
            if attempt >= cfg.max_retries:
                if stats is not None:
                    stats.total_failures += 1
                _logger.warning(
                    "FinMind 查詢多次失敗，放棄：params=%s err=%r",
                    params,
                    exc,
                )
                raise
            # 尚可重試
            if stats is not None:
                stats.total_retries += 1
            backoff = min(2.0 ** attempt, 30.0)  # 指數回退，上限 30 秒
            _logger.info(
                "FinMind 查詢失敗，%s 秒後重試 (%d/%d)：%r",
                backoff,
                attempt + 1,
                cfg.max_retries,
                exc,
            )
            time.sleep(backoff)
        except Exception:
            # 其他錯誤（例如 schema）不重試，直接拋出
            raise

    # 理論上不會到這裡
    if last_err is not None:
        raise last_err
    raise FinMindError("未知錯誤：_send_request_with_retry 流程意外結束。")


# -----------------------------------------------------------------------------
# JSON → DataFrame 正規化
# -----------------------------------------------------------------------------

def _normalize_records_to_dataframe(
    dataset: str,
    records: Sequence[Dict[str, Any]],
) -> pd.DataFrame:
    """
    將 FinMind record list 轉成 DataFrame 並做基本處理：
      - 日期欄位轉 datetime64[ns]（若存在）
      - 確保關鍵欄位存在（如 stock_id / date）
      - 簡單排序
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)

    # 嘗試猜測日期欄位名稱
    date_cols = [c for c in df.columns if c.lower() in ("date", "trading_date")]
    if date_cols:
        col = date_cols[0]
        try:
            df[col] = pd.to_datetime(df[col])
        except Exception:
            # 若轉換失敗就保持原樣，不中斷
            _logger.warning("日期欄位 %s 轉 datetime 失敗，維持原字串。", col)

    # 檢查 stock_id（若適用）
    if "stock_id" in df.columns:
        sort_cols: List[str] = ["stock_id"]
        if date_cols:
            sort_cols.append(date_cols[0])
        df = df.sort_values(sort_cols).reset_index(drop=True)
    else:
        # 沒有 stock_id，就只按日期排序
        if date_cols:
            df = df.sort_values(date_cols[0]).reset_index(drop=True)

    return df


# -----------------------------------------------------------------------------
# 高階 API：日期區間版（四表）
# -----------------------------------------------------------------------------

def _build_params_for_date_range(
    dataset_name: str,
    stock_id: str,
    start_date: str,
    end_date: str,
) -> Dict[str, str]:
    """
    組 FinMind date-range 查詢參數（v4）。
    注意：FinMind 的 end_date 是「含 end_date 當天」的閉區間。
    半開區間 [Start, End) 的語意由 orchestrator 決定如何轉換。
    """
    return {
        "dataset": dataset_name,
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
    }


def fetch_by_date_range(
    dataset: str,
    stock_ids: List[str],
    start_date: str,
    end_date: str,
    *,
    cfg: Optional[FinMindConfig] = None,
    return_stats: bool = False,
) -> pd.DataFrame | Tuple[pd.DataFrame, IngestStats]:
    """
    高階 API：四表 / D 線用。

    Args:
        dataset: 'prices' | 'chip' | 'per' | 'dividend' 或原始 FinMind dataset 名。
        stock_ids: 股票代碼列表。
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        cfg: 若 None 則從環境載入。
        return_stats: True 時回傳 (df, stats)。

    Returns:
        DataFrame 或 (DataFrame, IngestStats)
    """
    if not stock_ids:
        raise FinMindConfigError("stock_ids 不可為空。")

    dataset_name = resolve_dataset_name(dataset)
    cfg = cfg or load_config_from_env()

    stats = IngestStats(
        dataset=dataset_name,
        num_ids=len(stock_ids),
        start_date=start_date,
        end_date=end_date,
        date_id_count=None,
    )

    t0 = time.monotonic()
    limiter = RateLimiter(cfg)
    session = requests.Session()
    all_records: List[Dict[str, Any]] = []

    try:
        for sid in stock_ids:
            params = _build_params_for_date_range(
                dataset_name=dataset_name,
                stock_id=sid,
                start_date=start_date,
                end_date=end_date,
            )
            recs = _send_request_with_retry(
                cfg=cfg,
                limiter=limiter,
                session=session,
                params=params,
                stats=stats,
            )
            all_records.extend(recs)
    finally:
        stats.elapsed_sec = time.monotonic() - t0
        session.close()

    df = _normalize_records_to_dataframe(dataset_name, all_records)
    if return_stats:
        return df, stats
    return df


# -----------------------------------------------------------------------------
# 高階 API：date-id 版（DateID 六表）
# -----------------------------------------------------------------------------

def _build_params_for_date_id(
    dataset_name: str,
    date_id: str,
    stock_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    組 FinMind date-id 查詢參數。
    不同 dataset 對 date_id / stock_id 的解讀由 FinMind 決定。
    """
    params: Dict[str, str] = {
        "dataset": dataset_name,
        "date": date_id,
    }
    if stock_id is not None:
        params["data_id"] = stock_id
    return params


def fetch_by_date_id(
    dataset: str,
    date_ids: List[str],
    *,
    stock_ids: Optional[List[str]] = None,
    cfg: Optional[FinMindConfig] = None,
    return_stats: bool = False,
) -> pd.DataFrame | Tuple[pd.DataFrame, IngestStats]:
    """
    高階 API：DateID 六表用。

    Args:
        dataset: 'finstmt' | 'bs' | 'cfs' | 'shareholding' | 'inst_total' | 'gov_bank'
                 或原始 FinMind dataset 名。
        date_ids: 期別或日期字串列表（FinMind 接受的格式）。
        stock_ids: None 表示「不指定個股」，由 FinMind 決定範圍；
                   否則僅限指定股票。
        cfg: 若 None 則從環境載入。
        return_stats: True 時回傳 (df, stats)。

    Returns:
        DataFrame 或 (DataFrame, IngestStats)
    """
    if not date_ids:
        raise FinMindConfigError("date_ids 不可為空。")

    dataset_name = resolve_dataset_name(dataset)
    cfg = cfg or load_config_from_env()

    stats = IngestStats(
        dataset=dataset_name,
        num_ids=len(stock_ids) if stock_ids is not None else 0,
        start_date=None,
        end_date=None,
        date_id_count=len(date_ids),
    )

    t0 = time.monotonic()
    limiter = RateLimiter(cfg)
    session = requests.Session()
    all_records: List[Dict[str, Any]] = []

    try:
        if stock_ids is None:
            # 只依 date_id 查（FinMind 若支援整體市場）
            for did in date_ids:
                params = _build_params_for_date_id(
                    dataset_name=dataset_name,
                    date_id=did,
                    stock_id=None,
                )
                recs = _send_request_with_retry(
                    cfg=cfg,
                    limiter=limiter,
                    session=session,
                    params=params,
                    stats=stats,
                )
                all_records.extend(recs)
        else:
            # date_id × stock_id 交叉
            for did in date_ids:
                for sid in stock_ids:
                    params = _build_params_for_date_id(
                        dataset_name=dataset_name,
                        date_id=did,
                        stock_id=sid,
                    )
                    recs = _send_request_with_retry(
                        cfg=cfg,
                        limiter=limiter,
                        session=session,
                        params=params,
                        stats=stats,
                    )
                    all_records.extend(recs)
    finally:
        stats.elapsed_sec = time.monotonic() - t0
        session.close()

    df = _normalize_records_to_dataframe(dataset_name, all_records)
    if return_stats:
        return df, stats
    return df


# -----------------------------------------------------------------------------
# CLI（debug / 單步測試用）
# -----------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FinMind ingest engine v2（單檔版）",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="dataset alias 或 FinMind dataset 名，例如 prices/chip/per/dividend/finstmt/...",
    )
    parser.add_argument(
        "--mode",
        choices=("date-range", "date-id"),
        default="date-range",
        help="date-range=四表; date-id=DateID 六表",
    )
    parser.add_argument(
        "--stock-id",
        dest="stock_ids",
        action="append",
        help="可多次指定的股票代碼（2330 之類）",
    )
    parser.add_argument(
        "--start",
        dest="start_date",
        help="起始日期 YYYY-MM-DD（date-range 模式）",
    )
    parser.add_argument(
        "--end",
        dest="end_date",
        help="結束日期 YYYY-MM-DD（date-range 模式）",
    )
    parser.add_argument(
        "--date-id",
        dest="date_ids",
        action="append",
        help="可多次指定的 date_id / date（date-id 模式）",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="同時輸出簡易 stats",
    )
    parser.add_argument(
        "--qps",
        type=float,
        default=None,
        help="覆寫 QPS（預設讀環境 FINMIND_QPS）",
    )
    parser.add_argument(
        "--rpm",
        type=int,
        default=None,
        help="覆寫 RPM（預設讀環境 FINMIND_RPM）",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="覆寫 max_concurrency（目前實作仍為單執行緒）",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args(argv)

    cfg = load_config_from_env()
    cfg = override_config(
        cfg,
        qps=args.qps,
        rpm=args.rpm,
        max_concurrency=args.max_concurrency,
    )

    if args.mode == "date-range":
        if not args.start_date or not args.end_date:
            raise SystemExit("--mode date-range 需要 --start 與 --end")
        if not args.stock_ids:
            raise SystemExit("--mode date-range 至少指定一個 --stock-id")

        df, stats = fetch_by_date_range(
            dataset=args.dataset,
            stock_ids=args.stock_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            cfg=cfg,
            return_stats=True,
        )
    else:
        if not args.date_ids:
            raise SystemExit("--mode date-id 需要至少一個 --date-id")
        df, stats = fetch_by_date_id(
            dataset=args.dataset,
            date_ids=args.date_ids,
            stock_ids=args.stock_ids,
            cfg=cfg,
            return_stats=True,
        )

    print("=== DataFrame 頭部 ===")
    with pd.option_context("display.max_rows", 10, "display.width", 120):
        print(df.head())

    print(f"\nrows = {len(df)}")
    if args.stats:
        print("\n=== IngestStats ===")
        for field in dataclasses.fields(stats):
            name = field.name
            value = getattr(stats, name)
            print(f"{name}: {value!r}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
