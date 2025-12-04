# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.ai_impl_stub
AI / ML Factor Engine Skeleton
"""
from __future__ import annotations
import logging
from typing import Any, Dict
import pandas as pd

logger = logging.getLogger(__name__)

def run_ai_xgb_alpha(
    *,
    features: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    AI alpha 引擎 Stub
    """
    _ = params 

    if features is None or features.empty:
        logger.info("run_ai_xgb_alpha: features empty or None")
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    logger.info(f"run_ai_xgb_alpha: stub called with shape={features.shape}. No inference performed.")
    
    # 回傳空表，保持 Pipeline 不中斷
    return pd.DataFrame(columns=["date", "stock_id", "factor_value"])