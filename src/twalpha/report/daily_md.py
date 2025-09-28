from __future__ import annotations
from pathlib import Path
import pandas as pd

def write_markdown(df: pd.DataFrame, out_path: str | Path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = [c for c in ["symbol","date","close","final_score","regime","entry","sl","tp","risk_flags","reason_codes"] if c in df.columns]
    df2 = df[cols].sort_values(["final_score","symbol"], ascending=[False, True])
    md = []
    md.append("# Daily Signals\n")
    md.append("| " + " | ".join(df2.columns) + " |")
    md.append("|" + "|".join(["---"]*len(df2.columns)) + "|")
    for _, r in df2.iterrows():
        md.append("| " + " | ".join(str(r[c]) for c in df2.columns) + " |")
    out_path.write_text("\n".join(md), encoding="utf-8")
