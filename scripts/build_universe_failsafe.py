from pathlib import Path
import sys, re

root = Path(__file__).resolve().parents[1]
all_txt = root/"configs"/"groups"/"ALL.txt"
out_txt = root/"configs"/"investable_universe.txt"

prices = root/"datahub"/"silver"/"alpha"/"prices"
if not prices.exists():
    print(f"[FAIL] {prices} 不存在", file=sys.stderr); sys.exit(2)

# 找最新分區：同時容許 yyyymm=* 與 date=*，遞迴取最大日期/月份
cands = []
for p in prices.rglob("*"):
    n = p.name
    if p.is_dir() and (n.startswith("yyyymm=") or n.startswith("date=")):
        cands.append(p)
if not cands:
    print("[FAIL] prices 下沒有 yyyymm=/date= 分區，請先回填價格。", file=sys.stderr); sys.exit(2)

def _key(p: Path):
    n = p.name
    if n.startswith("date="):
        return ("D", n.split("=",1)[1])
    if n.startswith("yyyymm="):
        return ("M", n.split("=",1)[1])
    return ("Z", n)

latest = max(cands, key=_key)

# 來源一：ALL.txt（若有）
syms = []
if all_txt.exists():
    for s in all_txt.read_text(encoding="utf-8").splitlines():
        s = s.strip()
        if re.fullmatch(r"\d{4,6}", s): syms.append(s)

# 來源二：掃描最新分區下的 symbol 目錄（symbol=2330 或 2330 兩種）
if not syms:
    for dp in latest.rglob("*"):
        n = dp.name
        if n.startswith("symbol="):
            s = n.split("=",1)[1]
            if re.fullmatch(r"\d{4,6}", s): syms.append(s)
        elif re.fullmatch(r"\d{4,6}", n):
            syms.append(n)

syms = sorted(set(syms))
if not syms:
    print("[FAIL] 仍沒有任何可用代號（請先做價格回填）。", file=sys.stderr); sys.exit(3)

out_txt.write_text("\n".join(syms) + "\n", encoding="utf-8")
print(f"[OK] universe={len(syms)} -> {out_txt}")
