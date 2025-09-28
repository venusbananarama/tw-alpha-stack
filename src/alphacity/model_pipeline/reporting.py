\
import os
from pathlib import Path
import pandas as pd

def write_excel_summary(out_dir: str, xlsx_name: str,
                        qc_summary: pd.DataFrame,
                        feats_head: pd.DataFrame,
                        perf_summary: dict,
                        equity: pd.DataFrame,
                        top_factors: pd.DataFrame):
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = out_dir / xlsx_name

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        qc_summary.to_excel(writer, sheet_name="QC_Summary", index=False)
        feats_head.to_excel(writer, sheet_name="Features_Sample", index=False)
        pd.DataFrame([perf_summary]).to_excel(writer, sheet_name="Performance", index=False)
        equity.to_excel(writer, sheet_name="Equity_Curve", index=False)
        top_factors.to_excel(writer, sheet_name="Feature_Importances", index=False)
    print(f"[REPORT] Wrote {xlsx_path}")
    return str(xlsx_path)
