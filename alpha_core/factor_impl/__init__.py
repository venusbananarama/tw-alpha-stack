# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl

Dispatch layer for Phase-2 factor implementations.

目前角色：
- 提供單一入口 `run_factor_task` 給 alpha_core.factor_engine 呼叫。
- 先把介面跟 pipeline 打通，確保 factor_engine 可以乾跑 / 正常寫 summary。
- 之後如果要真的算因子，再在這裡接各個子模組（mom_impl / beta_impl / liq_impl / value_impl …）。

設計原則：
- 介面要跟 factor_engine._run_single_task 相容。
- `params` 為「可選參數」，缺省時使用空 dict，不再噴 TypeError。
- 在 dry_run=True 時，不做 heavy 計算，只回傳一個輕量結果，讓流程可以驗證。
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

# 預先載入子模組，之後要接真實實作時可以直接使用
try:
    from .mom_impl import run_mom_factor  # noqa: F401
except Exception:  # noqa: BLE001
    run_mom_factor = None  # type: ignore[assignment]

try:
    from .beta_impl import run_beta_factor  # noqa: F401
except Exception:  # noqa: BLE001
    run_beta_factor = None  # type: ignore[assignment]

try:
    from .liq_impl import run_liquidity_factor  # noqa: F401
except Exception:  # noqa: BLE001
    run_liquidity_factor = None  # type: ignore[assignment]

try:
    from .ai_impl_stub import run_ai_xgb_alpha  # noqa: F401
except Exception:  # noqa: BLE001
    run_ai_xgb_alpha = None  # type: ignore[assignment]


LOG = logging.getLogger(__name__)


def run_factor_task(
    *,
    root: Path,
    rules_path: Path,
    factor_root: Path,
    ledger_path: Path,
    factor_id: str,
    window: int,
    end_date: date,
    dry_run: bool,
    run_id: str,
    logger: Optional[logging.Logger] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    給 alpha_core.factor_engine 呼叫的單一入口。

    目前版本的目標：
    - 修正介面不相容（`params` 變成可選）。
    - 讓 dry-run 可以順利跑完，不再噴 TypeError。
    - 尚未接上實際因子運算，只是 pipeline-safe 的 stub。

    之後要真的算因子時，可以依 factor_id / engine：
    - 讀取銀河 parquet
    - 組出 pandas.DataFrame
    - 呼叫 run_mom_factor / run_beta_factor / run_liquidity_factor / run_value_factor ...
    - 把結果寫回 factor_root 底下的 parquet，並在回傳 dict 裡填入 parquet_path。
    """
    log = logger or LOG
    eff_params: Dict[str, Any] = dict(params or {})

    log.info(
        "run_factor_task stub: factor_id=%s window=%s end=%s dry_run=%s run_id=%s params=%s",
        factor_id,
        window,
        end_date,
        dry_run,
        run_id,
        eff_params,
    )

    # 現階段：在 dry-run 模式下，不做任何實際 I/O 或計算，
    # 只回傳一個輕量結果，讓 factor_engine 可以寫 summary。
    if dry_run:
        return {
            "factor_id": factor_id,
            "window": window,
            "parquet_path": None,
            "dry_run": True,
        }

    # 未來要跑正式因子計算時，再把下面這段換成真正的 dispatch：
    # if factor_id in ("mom_6m", "mom_12m") and run_mom_factor is not None:
    #     ...
    # elif factor_id == "beta_252d" and run_beta_factor is not None:
    #     ...
    # ...
    raise NotImplementedError(
        f"run_factor_task for factor_id={factor_id!r}, window={window} "
        f"尚未實作正式運算（目前只支援 dry_run）"
    )
