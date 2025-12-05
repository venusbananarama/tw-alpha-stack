# alpha_core/factor_impl/__init__.py
# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl

Dispatch layer for Phase-2 factor implementations.
Step 1-5: Real Implementation with Robust Routing & IO.
"""

from __future__ import annotations

import logging
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from alpha_core import io as factor_io

# 相對匯入各因子實作模組
# 使用 try-import 以容許部分實作尚未就緒
try:
    from .mom_impl import run_mom_factor
except Exception:
    run_mom_factor = None

try:
    from .value_impl import run_value_factor
except Exception:
    run_value_factor = None

try:
    from .quality_impl import run_quality_factor
except Exception:
    run_quality_factor = None

try:
    from .size_impl import run_size_factor
except Exception:
    run_size_factor = None

try:
    from .beta_impl import run_beta_factor
except Exception:
    run_beta_factor = None

try:
    from .liq_impl import run_liquidity_factor
except Exception:
    run_liquidity_factor = None

try:
    from .vol_impl import run_vol_factor
except Exception:
    run_vol_factor = None

# 型別別名
FactorImpl = Callable[..., pd.DataFrame]
Logger = logging.Logger


# ---------------------------
# 設定：因子 → 需要的 input dataset
# ---------------------------

#: 每顆因子需要哪些銀河 dataset（Phase-1 十表中的 key）
#: 這裡先針對「種子因子」列出，之後要擴充因子只要補這張表即可。
FACTOR_REQUIRED_INPUTS: Dict[str, List[str]] = {
    # 動量
    "mom_6m": ["prices"],
    "mom_12m": ["prices"],
    # 價值
    "value_pe": ["prices", "per"],
    # 品質
    "quality_roeq": ["finstmt"],  # 或 bs/cfs，看你實作，這裡先給 finstmt
    # 規模
    "size_log_mktcap": ["prices"],
    # beta / 波動
    "beta_252d": ["prices"],
    "vol_20d": ["prices"],
    # 流動性
    "liq_turnover_20d": ["prices", "shareholding"],  # 或 inst_total，看你之後怎麼接
}


@dataclass
class FactorIOConfig:
    """I/O 設定，讓 run_factor_task 可以保持簡潔。"""

    root: Path
    factor_id: str
    window: int
    end_date: date

    @property
    def factor_root(self) -> Path:
        return self.root / "datahub" / "silver" / "alpha" / "factor" / self.factor_id

    @property
    def start_date(self) -> date:
        # 以 window（月）+ 2 個月 buffer 往前推，確保 shift 計算時頭部資料足夠
        return self.end_date - relativedelta(months=self.window + 2)


# =====================================================================
# Public API
# =====================================================================


def run_factor_task(
    factor_id: str,
    window: int,
    end_date: date,
    root: Path,
    rules: Mapping[str, Any],
    dry_run: bool = False,
    logger: Optional[Logger] = None,
    params: Optional[Mapping[str, Any]] = None,
    # 預留參數，目前未用但保持介面彈性
    rules_path: Optional[Path] = None,
    ledger_path: Optional[Path] = None,
    run_id: str = "default_run",
) -> Dict[str, Any]:
    """
    單一因子任務入口，由 alpha_core.factor_engine 呼叫。

    Parameters
    ----------
    factor_id: 因子 ID
    window: Walk-forward 視窗（月）
    end_date: 計算截止日
    root: Repo root
    rules: 該因子在 rules_factors.yaml 裡的設定
    dry_run: 是否乾跑
    logger: Logger
    params: 額外參數
    rules_path: (Reserved)
    ledger_path: (Reserved) 若有值會寫入 ledger
    run_id: (Reserved) 執行 ID

    Returns
    -------
    Dict[str, Any]
        結果摘要 (factor_id, window, status, parquet_root, parquet_path, etc.)
    """
    log = logger or logging.getLogger(__name__)
    io_cfg = FactorIOConfig(root=root, factor_id=factor_id, window=window, end_date=end_date)
    full_params: Dict[str, Any] = {**(rules.get("params") or {}), **(params or {})}

    log.info(
        "Run factor task: factor_id=%s window=%s end_date=%s dry_run=%s",
        factor_id,
        window,
        end_date,
        dry_run,
    )

    # 基礎回傳結構，確保無論成功失敗都有這些欄位
    base_result = {
        "factor_id": factor_id,
        "window": window,
        "dry_run": dry_run,
        "parquet_root": str(io_cfg.factor_root),
        "parquet_path": None,  # 成功時會填入
        "yyyymm_written": [],
        "rows": 0,
        "universe_size": 0,
        "start": None,  # 實跑時會填入
        "end": end_date.isoformat(),
    }

    # 1. 乾跑模式
    if dry_run:
        # 乾跑只驗流程，不讀資料、不寫檔
        base_result.update({
            "status": "ok",
            # Dry-run 時 parquet_root 仍指向該資料夾，但 parquet_path 為 None 表示沒產檔
            "parquet_path": None,
        })
        return base_result

    # 2. 實戰模式：讀資料 → 算因子 → 寫 parquet
    try:
        # 更新實際計算的起始日
        base_result["start"] = io_cfg.start_date.isoformat()

        # 推斷並載入資料
        required_inputs = _infer_required_inputs(factor_id, rules)
        input_data = _load_input_data(
            cfg=io_cfg,
            required_inputs=required_inputs,
            root=root,
            logger=log,
        )

        # 檢查是否缺資料 (若某個 dataset 完全讀不到)
        for req in required_inputs:
            if req not in input_data or input_data[req].empty:
                log.warning("Missing input data for %s: %s", factor_id, req)
                base_result.update({
                    "status": "error",
                    "reason": f"Missing input data: {req}",
                })
                return base_result

        # 計算因子
        df_factor = _route_and_compute(
            factor_id=factor_id,
            window=window,
            end_date=end_date,
            inputs=input_data,
            params=full_params,
        )

        # 寫入 parquet
        io_result = _write_factor_parquet(
            cfg=io_cfg,
            df_factor=df_factor,
            run_id=run_id,
            logger=log,
        )

        # 準備寫入 ledger (如果 ledger_path 有提供)
        rows_written = io_result["rows"]
        written_files = io_result["written_files"]
        
        if ledger_path and rows_written > 0:
            record = {
                "run_id": run_id,
                "factor_id": factor_id,
                "window": window,
                "end_date": str(end_date),
                "rows": rows_written,
                "files": [str(p.relative_to(root) if p.is_relative_to(root) else p) for p in written_files],
                "timestamp": str(date.today()),
            }
            factor_io.append_jsonlines(ledger_path, [record])

        # 計算 universe size
        universe_size = 0
        if not df_factor.empty:
            if isinstance(df_factor.index, pd.MultiIndex) and "stock_id" in df_factor.index.names:
                universe_size = df_factor.index.get_level_values("stock_id").nunique()
            elif "stock_id" in df_factor.columns:
                universe_size = df_factor["stock_id"].nunique()
            else:
                universe_size = df_factor.shape[0]

        # 更新回傳結果
        # parquet_path 指向這次寫入的根目錄 (雖然是 partitioned，但 engine 通常只需要 root)
        out_path = str(io_cfg.factor_root)
        
        base_result.update({
            "status": "ok",
            "parquet_root": out_path,
            "parquet_path": out_path,
            "yyyymm_written": sorted(io_result["yyyymm_written"]),
            "rows": rows_written,
            "universe_size": universe_size,
        })
        return base_result

    except NotImplementedError as e:
        log.warning("Factor %s not implemented: %s", factor_id, e)
        base_result.update({
            "status": "skipped",
            "reason": "not_implemented",
        })
        return base_result

    except Exception as exc:  # noqa: BLE001
        log.exception("Factor task failed: factor_id=%s window=%s", factor_id, window)
        base_result.update({
            "status": "error",
            "reason": str(exc),
        })
        return base_result


# =====================================================================
# Internal helpers
# =====================================================================


def _infer_required_inputs(
    factor_id: str,
    rules: Mapping[str, Any],
) -> List[str]:
    """
    決定該因子需要哪些銀河 dataset。

    優先順序：
    1) rules["inputs"] 如果有明寫，直接採用
    2) 否則從 FACTOR_REQUIRED_INPUTS lookup
    3) 簡單 fallback 邏輯
    """
    explicit = rules.get("inputs")
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes)):
        return list(explicit)

    inputs = FACTOR_REQUIRED_INPUTS.get(factor_id)
    if inputs:
        return list(inputs)
    
    # Fallback inference
    fid = factor_id.lower()
    if fid.startswith("value_"):
        return ["prices", "per"]
    if fid.startswith("size_"):
        return ["prices"]
    
    # 預設只給 prices
    return ["prices"]


def _load_input_data(
    cfg: FactorIOConfig,
    required_inputs: Sequence[str],
    root: Path,
    logger: Logger,
) -> Dict[str, pd.DataFrame]:
    """
    從銀河 parquet 載入所需期間的 panel 資料。
    """
    start = cfg.start_date
    end = cfg.end_date
    logger.info(
        "Load input data for factor=%s, window=%s, range=%s→%s, inputs=%s",
        cfg.factor_id,
        cfg.window,
        start,
        end,
        list(required_inputs),
    )

    results: Dict[str, pd.DataFrame] = {}
    for ds in required_inputs:
        # 呼叫 io.load_silver_data (Flat DataFrame)
        # columns 暫時傳 None (讀全部)，未來可傳入需要的欄位優化 IO
        df = factor_io.load_silver_data(
            root=root,
            dataset=ds,
            start_date=start,
            end_date=end,
            columns=None,
        )
        if not df.empty:
            results[ds] = df
        else:
            logger.warning("Dataset %s is empty or missing for range %s~%s", ds, start, end)

    return results


def _route_and_compute(
    factor_id: str,
    window: int,
    end_date: date,
    inputs: Dict[str, pd.DataFrame],
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    將資料分派給具體的實作函式。
    重點：明確傳遞 window, end_date 給 impl。
    """
    fid = factor_id.lower()
    
    # 1. Momentum Family
    if fid.startswith("mom_") and run_mom_factor:
        return run_mom_factor(
            prices=inputs["prices"],
            window=window,
            end_date=end_date,
            **params
        )

    # 2. Value Family (PE/PB)
    elif fid.startswith("value_") and run_value_factor:
        return run_value_factor(
            per=inputs["per"],
            window=window,
            end_date=end_date,
            **params
        )

    # 3. Quality Family
    elif fid.startswith("quality_") and run_quality_factor:
        # 修正 DataFrame 的布林值判斷錯誤
        # 明確檢查 inputs 中的 key
        finstmt = inputs.get("finstmt")
        if finstmt is None:
            finstmt = inputs.get("bs")
        if finstmt is None:
            finstmt = pd.DataFrame()
            
        return run_quality_factor(
            finstmt=finstmt,
            window=window,
            end_date=end_date,
            **params
        )

    # 4. Liquidity Family
    elif fid.startswith("liq_") and run_liquidity_factor:
        return run_liquidity_factor(
            prices=inputs["prices"],
            window=window,
            end_date=end_date,
            **params
        )

    # 5. Beta Family
    elif fid.startswith("beta_") and run_beta_factor:
        return run_beta_factor(
            prices=inputs["prices"],
            window=window,
            end_date=end_date,
            **params
        )

    # 6. Volatility Family (vol_)
    elif fid.startswith("vol_") and run_vol_factor:
        return run_vol_factor(
            prices=inputs["prices"],
            window=window,
            end_date=end_date,
            **params
        )

    # 7. Size Family
    elif fid.startswith("size_") and run_size_factor:
        return run_size_factor(
            prices=inputs["prices"],
            window=window,
            end_date=end_date,
            **params
        )

    raise NotImplementedError(f"No implementation found or loaded for factor_id={factor_id}")


def _write_factor_parquet(
    cfg: FactorIOConfig,
    df_factor: pd.DataFrame,
    run_id: str,
    logger: Logger,
) -> Dict[str, Any]:
    """
    將因子結果依 yyyymm 分區寫入 parquet。
    回傳寫入統計。
    """
    rows_written, written_files = factor_io.write_factor_parquet(
        df=df_factor,
        factor_root=cfg.factor_root,
        factor_id=cfg.factor_id,
        run_id=run_id,
    )
    
    yyyymm_written = []
    if written_files:
        # 從路徑解析 yyyymm: .../yyyymm=202301/data.parquet
        # path.parent.name -> "yyyymm=202301"
        yyyymm_written = sorted(list({p.parent.name.split('=')[-1] for p in written_files}))

    return {
        "rows": rows_written,
        "written_files": written_files,
        "yyyymm_written": yyyymm_written,
    }