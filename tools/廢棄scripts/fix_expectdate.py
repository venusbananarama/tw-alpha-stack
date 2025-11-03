from __future__ import annotations
import json, sys
from pathlib import Path
import pandas as pd

def main(root: Path | str = ".") -> int:
    root = Path(root).resolve()
    rp = root / "reports" / "preflight_report.json"
    cal = root / "cal" / "trading_days.csv"

    if not rp.exists():
        sys.stderr.write("[fix_expectdate] 缺少 reports/preflight_report.json\n")
        return 1
    if not cal.exists():
        sys.stderr.write("[fix_expectdate] 缺少 cal/trading_days.csv\n")
        return 2

    df = pd.read_csv(cal)
    if "date" not in df.columns:
        sys.stderr.write('[fix_expectdate] trading_days.csv 需要 "date" 欄\n')
        return 3

    # 轉日期並清理
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["date"])

    # 補 is_open 欄位並轉 int
    if "is_open" not in df.columns:
        df["is_open"] = 1
    df["is_open"] = df["is_open"].fillna(1).astype(int)

    # 統一設為 Asia/Taipei（避免 tz-naive/aware 比較錯誤）
    try:
        df["date"] = df["date"].dt.tz_localize("Asia/Taipei")
    except TypeError:
        # 已是 tz-aware，轉到台北時區
        df["date"] = df["date"].dt.tz_convert("Asia/Taipei")

    today = pd.Timestamp.now(tz="Asia/Taipei").normalize()
    mask = (df["is_open"] == 1) & (df["date"] <= today)
    if not mask.any():
        sys.stderr.write("[fix_expectdate] 沒有 <= 今天 的交易日（請檢查日曆 is_open 欄）\n")
        return 4

    exp = df.loc[mask, "date"].max().tz_convert(None).date().isoformat()

    obj = json.loads(rp.read_text(encoding="utf-8"))
    obj.setdefault("meta", {})["expect_date"] = exp
    rp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[fix_expectdate] expect_date -> {exp}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
