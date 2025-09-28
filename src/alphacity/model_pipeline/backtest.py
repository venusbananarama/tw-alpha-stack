\
import pandas as pd
import numpy as np

def rebalance_dates(dates: pd.Series, freq: str) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(dates.unique()).sort_values()
    if freq == "D":
        return idx
    elif freq.upper().startswith("W"):
        # weekly e.g., "W-FRI"
        return pd.date_range(idx.min(), idx.max(), freq=freq).intersection(idx)
    elif freq == "M":
        return pd.date_range(idx.min(), idx.max(), freq="M").intersection(idx)
    else:
        return idx

def daily_equal_weight_backtest(df: pd.DataFrame, score_col: str, top_n: int = 100, long_only=True,
                                rebalance_freq: str = "W-FRI", tc_bps: float = 5):
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values(["date","symbol"])

    # Prepare forward daily returns for PnL (1-day ahead)
    data["ret_1d_fwd"] = data.groupby("symbol")["close"].pct_change().shift(-1)

    # Rebalance schedule
    rebal_days = set(rebalance_dates(data["date"], rebalance_freq))

    # Weights per day
    w_list = []
    prev_w = {}
    dates = pd.DatetimeIndex(data["date"].unique()).sort_values()
    for d in dates:
        g = data.loc[data["date"] == d, ["symbol", score_col, "adv_20"]].dropna(subset=[score_col])
        if d in rebal_days:
            # liquidity filter: require adv_20 > 0
            g = g[g["adv_20"] > 0]
            g = g.sort_values(score_col, ascending=False)
            picks = g.head(top_n)["symbol"].tolist()
            w = {s: 1.0/len(picks) for s in picks} if picks else {}
        else:
            w = prev_w  # hold
        w_list.append((d, w))
        prev_w = w

    # Compute portfolio daily returns with transaction cost on weight change
    rets, costs = [], []
    for (d, w), (d_prev, w_prev) in zip(w_list, [w_list[0]] + w_list[:-1]):
        day = data.loc[data["date"] == d, ["symbol","ret_1d_fwd"]].dropna()
        prem = 0.0
        if d != d_prev:
            # turnover cost: sum |Δw| * tc_rate
            all_syms = set(w.keys()).union(w_prev.keys())
            turnover = sum(abs(w.get(s,0.0) - w_prev.get(s,0.0)) for s in all_syms)
            prem = turnover * (tc_bps / 10000.0)
        # portfolio ret
        pr = sum(w.get(s,0.0) * day.set_index("symbol").at[s,"ret_1d_fwd"] for s in w.keys() if s in set(day["symbol"]))
        rets.append(pr - prem)
        costs.append(prem)

    port = pd.DataFrame({"date": dates, "ret": rets})
    port["equity"] = (1 + port["ret"].fillna(0)).cumprod()
    port["cum_cost"] = np.cumsum(costs)
    return port, w_list

def summarize_performance(eq: pd.Series, rets: pd.Series):
    rets = rets.dropna()
    if len(rets) == 0:
        return {"CAGR": 0.0, "Sharpe": 0.0, "MaxDD": 0.0}
    # assume ~252 trading days/year
    ann_ret = (eq.iloc[-1] ** (252/len(eq)) - 1.0) if len(eq)>0 else 0.0
    ann_vol = rets.std(ddof=0) * np.sqrt(252)
    sharpe = ann_ret / (ann_vol + 1e-9)
    # max drawdown
    roll_max = eq.cummax()
    dd = eq / roll_max - 1.0
    maxdd = dd.min()
    return {"CAGR": float(ann_ret), "Sharpe": float(sharpe), "MaxDD": float(maxdd)}
