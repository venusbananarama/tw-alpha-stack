from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

from twalpha.features.ta_basic import ema, roc, atr, rsi, adx, mfi, vol_z
from twalpha.signals.ensemble import calibrate_columns, layered_scores, combine_layers, risk_flags
from twalpha.signals.regime import detect_regime
from twalpha.report.daily_md import write_markdown
from twalpha.data.adapter_fatai import merge_fatai_indicators  # optional

def _load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"])
    return df.sort_values("date").dropna(subset=["close"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="configs/universe.tw_all.txt")
    ap.add_argument("--data", default="data")
    ap.add_argument("--out", default="reports")
    ap.add_argument("--fatai", default=None, help="可選：C:\\\\AI\\fatai 根目錄，會自動合併舊指標")
    args = ap.parse_args()

    syms = [s.strip() for s in Path(args.universe).read_text(encoding="utf-8").splitlines() if s.strip()]
    rows = []
    for sym in syms:
        f = Path(args.data) / f"{sym.replace('.','_')}.csv"
        if not f.exists(): continue
        df = _load_csv(f)
        if len(df) < 120: continue

        # 合併 fatai 舊指標（可選）
        if args.fatai:
            df = merge_fatai_indicators(df, sym, args.fatai)

        # features
        df["ema_fast"] = ema(df["close"], 26)
        df["ema_slow"] = ema(df["close"], 121)
        df["roc10"] = roc(df["close"], 10)
        df["rsi14"] = rsi(df["close"], 14)
        df["mfi14"] = mfi(df, 14)
        df["adx14"] = adx(df, 14)
        df["atr14"] = atr(df, 14)
        macd = ema(df["close"], 12) - ema(df["close"], 26)
        df["macd"] = macd
        df["volz60"] = vol_z(df["volume"], 60)
        df["choch"] = np.sign(df["ema_fast"].diff())

        # calibration
        calib_cols = ["roc10","rsi14","mfi14","adx14","macd","volz60"]
        df = calibrate_columns(df, calib_cols, p=0.01)
        df["trend_bin"] = (df["ema_fast"] > df["ema_slow"]).astype(float)

        # layered schema & weights
        schema = {
            "trend":   ["trend_bin","adx14"],
            "momentum":["roc10","rsi14","macd"],
            "volume":  ["volz60","mfi14"],
            "smc":     ["choch"],
            "special": []
        }
        df = layered_scores(df, schema)
        weights = {"trend":0.30, "momentum":0.35, "volume":0.20, "smc":0.10, "special":0.05}
        df["final_score"] = combine_layers(df, weights)
        df["regime"] = detect_regime(df)

        last = df.iloc[-1]
        close = float(last["close"])
        atrv = float(last["atr14"]) if np.isfinite(last["atr14"]) else 0.0
        sl = max(0.0, close - 3*atrv) if atrv > 0 else ""
        tp = (close + 3*(close - sl)) if sl != "" else ""

        rows.append({
            "symbol": sym,
            "date": last["date"].strftime("%Y-%m-%d"),
            "close": f"{close:.2f}",
            "final_score": round(float(last["final_score"]), 4),
            "regime": last["regime"],
            "entry": f"{close:.2f}",
            "sl": f"{sl:.2f}" if sl != "" else "",
            "tp": f"{tp:.2f}" if tp != "" else "",
            "risk_flags": risk_flags(df).iloc[-1],
            "reason_codes": ""
        })

        out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)

    if not rows:
        print("[warn] No symbols produced (likely no data files). Did the downloader succeed?")
        return

    result = pd.DataFrame(rows).sort_values(["final_score","symbol"], ascending=[False, True])
    if not result.empty:
        (out_dir / "signals_today.csv").write_text(result.to_csv(index=False), encoding="utf-8")
        try:
            result.to_excel(out_dir / "signals_clean.xlsx", index=False)
        except Exception:
            pass
        write_markdown(result, out_dir / f"daily_{pd.Timestamp.today().date()}.md")
        print(f"Saved to: {out_dir}")
    else:
        print("[warn] Result is empty after scoring. Check data/universe and indicator merge.")


if __name__ == "__main__":
    main()

