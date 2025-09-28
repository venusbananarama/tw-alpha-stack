from __future__ import annotations
import pandas as pd
from typing import Dict

def simple_backtest(df: pd.DataFrame, enter_th: float, exit_th: float, fees_bps: float = 2.5, tax_bps: float = 30.0) -> Dict[str, float]:
    # 極簡 backtest：僅示範框架，真實化需加入滑點、漲跌停、IOC 成交率等
    pos = 0
    entry_price = 0.0
    pnl = 0.0
    trades = 0
    for i in range(1, len(df)):
        score = df.iloc[i]["final_score"]
        price = df.iloc[i]["close"]
        if pos == 0 and score >= enter_th:
            pos = 1
            entry_price = price * (1 + fees_bps/10000.0)
            trades += 1
        elif pos == 1 and score <= exit_th:
            exit_price = price * (1 - (fees_bps + tax_bps)/10000.0)
            pnl += (exit_price - entry_price)
            pos = 0
    if pos == 1:
        pnl += (df.iloc[-1]["close"] - entry_price)
        pos = 0
    return {"trades": trades, "pnl": pnl, "avg_pnl": pnl / max(trades,1)}
