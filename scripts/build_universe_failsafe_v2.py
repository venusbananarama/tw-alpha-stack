from pathlib import Path
import json, sys, re

root = Path(__file__).resolve().parents[1]
pf = json.loads((root/"reports"/"preflight_report.json").read_text(encoding="utf-8"))
expect = pf.get("meta",{}).get("expect_date")
fresh = pf.get("freshness",{}).get("prices",{}).get("max_date")
target = expect or fresh
if not target:
    print("[FAIL] preflight_report.json 缺少 meta.expect_date / freshness.prices.max_date", file=sys.stderr)
    sys.exit(2)

yyyymm = target[:7].replace("-","")
prices = root/"datahub"/"silver"/"alpha"/"prices"

# 優先找 yyyymm 分區，找不到就回退到現存的最大 yyyymm
part = prices/f"yyyymm={yyyymm}"
if not part.exists():
    parts = [p for p in prices.glob("yyyymm=*") if p.is_dir()]
    if not parts:
        print("[FAIL] prices 下沒有任何 yyyymm= 分區，請先回填價格資料。", file=sys.stderr)
        sys.exit(3)
    part = max(parts, key=lambda p: p.name)

# 從 ALL.txt 取 baseline；若不存在，改以分區掃描
all_txt = root/"configs"/"groups"/"ALL.txt"
syms = []
if all_txt.exists():
    for s in all_txt.read_text(encoding="utf-8").splitlines():
        s = s.strip()
        if s.isdigit():
            syms.append(s)

# 掃描分區可以接受多種命名（symbol=2330 / stock_id=2330 / code=2330 / 2330）
avail = set()
pat = re.compile(r"^(?:.+?=)?(\d{4,6})$")
for p in part.rglob("*"):
    name = p.name
    m = pat.match(name)
    if m: avail.add(m.group(1))
    # 也從檔名中抓（*.parquet 之類）
    if p.is_file():
        m2 = pat.search(p.stem)
        if m2: avail.add(m2.group(1))

if not syms:
    syms = sorted(avail)

# 僅保留在本分區「確定有資料痕跡」的代號
valid = [s for s in syms if s in avail]
if not valid:
    # 最後兜底：若還是空，直接用 avail（以確保 pipeline 可繼續）
    valid = sorted(avail)

out_txt = root/"configs"/"investable_universe.txt"
out_txt.write_text("\n".join(valid) + "\n", encoding="utf-8")
print(f"[OK] universe={len(valid)}  yyyymm={part.name}  -> {out_txt}")
