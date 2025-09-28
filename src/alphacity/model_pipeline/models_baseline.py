\
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score

FEATURE_COLS_DEFAULT = [
    "ret_1","ret_5","ret_20",
    "sma_5","sma_20","sma_60",
    "vol_5","vol_20","vol_60",
    "mom_20","mom_60",
    "rsi","macd","macd_signal","macd_hist",
    "adv_20"
]

def single_split_train_predict(df: pd.DataFrame, train_end: str, feature_cols=None):
    if feature_cols is None:
        feature_cols = FEATURE_COLS_DEFAULT

    df = df.copy()
    df = df.dropna(subset=feature_cols + ["target_fwd_ret"])
    df["date"] = pd.to_datetime(df["date"])

    train = df[df["date"] <= pd.to_datetime(train_end)]
    test  = df[df["date"]  > pd.to_datetime(train_end)]

    X_tr = train[feature_cols].values
    y_tr = train["target_fwd_ret"].values
    X_te = test[feature_cols].values

    # Simple RF
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=None,
        min_samples_leaf=3,
        n_jobs=-1,
        random_state=42
    )
    model.fit(X_tr, y_tr)

    test = test.copy()
    test["pred"] = model.predict(X_te)

    # Optional in-sample fit metric
    in_r2 = r2_score(y_tr, model.predict(X_tr))
    return test, {"in_sample_r2": float(in_r2), "n_train": int(len(train)), "n_test": int(len(test))}
