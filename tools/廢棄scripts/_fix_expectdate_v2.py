import json, sys
from pathlib import Path
import pandas as pd

root = Path(".").resolve()
cal  = pd.read_csv(root/"cal"/"trading_days.csv")
# 轉成 tz-aware（台北），並正規成 00:00（normalize）
cal["date"] = pd.to_datetime(cal["date"], format="%Y-%m-%d", errors="coerce").dt.tz_localize("Asia/Taipei")
cal = cal.dropna(subset=["date"])
cal["is_open"] = cal.get("is_open", 1).fillna(1).astype(int)

today = pd.Timestamp.now(tz="Asia/Taipei").normalize()
mask  = (cal["date"] <= today) & (cal["is_open"]==1)
if not mask.any():
    sys.exit(0)
exp = cal.loc[mask, "date"].max().date().isoformat()

rp = root/"reports"/"preflight_report.json"
obj = json.loads(rp.read_text(encoding="utf-8"))
obj.setdefault("meta", {})["expect_date"] = exp
rp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[PATCH] expect_date -> {exp}")
