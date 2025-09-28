\
import argparse, os, json
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

from utils import ensure_sorted, add_dollar_volume
from features import build_features
from labels import make_labels
from models_baseline import single_split_train_predict, FEATURE_COLS_DEFAULT
from backtest import daily_equal_weight_backtest, summarize_performance
from reporting import write_excel_summary

def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def filter_universe(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    if cfg["universe"].get("start_date"):
        df = df[df["date"] >= pd.to_datetime(cfg["universe"]["start_date"])]
    if cfg["universe"].get("end_date"):
        df = df[df["date"] <= pd.to_datetime(cfg["universe"]["end_date"])]

    # min history per symbol
    g = df.groupby("symbol")["date"].count()
    keep = g[g >= cfg["universe"]["min_history_days"]].index
    df = df[df["symbol"].isin(keep)]

    # min avg dollar volume
    df["dollar_volume"] = df["close"] * df["volume"]
    liq = df.groupby("symbol")["dollar_volume"].mean()
    keep2 = liq[liq >= cfg["universe"]["min_avg_dollar_volume"]].index
    df = df[df["symbol"].isin(keep2)]

    return df

def main(cfg_path: str):
    cfg = load_cfg(cfg_path)

    merged = cfg["data"]["merged_parquet"]
    print(f"[INFO] Load {merged} ...")
    df = pd.read_parquet(merged)
    df["date"] = pd.to_datetime(df["date"])
    df = ensure_sorted(df)

    # Universe
    df = filter_universe(df, cfg)
    print(f"[INFO] Universe rows: {len(df):,}, symbols: {df['symbol'].nunique()}")

    # Build features & labels
    df_f = build_features(df, cfg["features"])
    df_fl = make_labels(df_f, cfg["labels"]["forward_return_days"], cfg["labels"]["cls_quantile"])

    # Train & predict (single split)
    pred_df, train_meta = single_split_train_predict(df_fl, cfg["split"]["train_end"])
    print(f"[MODEL] Train meta: {train_meta}")

    # Merge prediction back to feature frame on symbol/date
    key_cols = ["symbol","date"]
    merged_pred = df_f.merge(pred_df[key_cols + ["pred"]], on=key_cols, how="left")

    # Choose score for ranking
    score_col = "pred" if cfg["backtest"]["use_prediction"] else cfg["backtest"]["factor_column"]
    if cfg["backtest"]["use_prediction"] and "pred" not in merged_pred.columns:
        raise RuntimeError("Prediction column missing.")

    # Backtest
    port, wlist = daily_equal_weight_backtest(
        merged_pred.dropna(subset=[score_col, "close","adv_20"]),
        score_col=score_col,
        top_n=cfg["backtest"]["top_n"],
        long_only=cfg["backtest"]["long_only"],
        rebalance_freq=cfg["backtest"]["rebalance_freq"],
        tc_bps=cfg["backtest"]["transaction_cost_bps"],
    )
    perf = summarize_performance(port["equity"], port["ret"])
    print(f"[BT] Performance: {perf}")

    # QC summary (quick)
    qc = df.groupby("symbol").agg(first=("date","min"), last=("date","max"), rows=("date","size")).reset_index()
    feats_head = df_f.head(200).copy()

    # Feature importances (from RF) — approximate via permutation not included; fall back to report default list
    try:
        importances = getattr(__import__("models_baseline"), "FEATURE_COLS_DEFAULT")
        cols = FEATURE_COLS_DEFAULT
        top_factors = pd.DataFrame({"feature": cols})
    except Exception:
        top_factors = pd.DataFrame({"feature": FEATURE_COLS_DEFAULT})

    # Report
    xlsx_path = write_excel_summary(
        cfg["report"]["out_dir"],
        cfg["report"]["xlsx_name"],
        qc, feats_head, perf, port, top_factors
    )
    print(f"[DONE] Report: {xlsx_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    args = ap.parse_args()
    main(args.config)
