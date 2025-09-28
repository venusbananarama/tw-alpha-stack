import argparse
import pandas as pd
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ohlcv-dir", required=True)
    parser.add_argument("--merged-path", required=True)
    parser.add_argument("--board-csv", required=True)
    parser.add_argument("--report-xlsx", required=True)
    parser.add_argument("--detail-sample", default="2330.TW,2317.TW,1101.TW")
    parser.add_argument("--topn", type=int, default=100)
    parser.add_argument("--with-charts", action="store_true")
    parser.add_argument("--clean-reports", action="store_true")
    args = parser.parse_args()

    print("[INFO] 合併數據...")
    df = pd.read_parquet(args.merged_path)
    print(f"[INFO] 股票數: {df['symbol'].nunique()}, 總筆數: {len(df)}")

    if args.clean_reports:
        outdir = Path(args.report_xlsx).parent
        for f in outdir.glob("*.*"):
            f.unlink()
        print(f"[INFO] 清理報告資料夾: {outdir}")

    # 這裡可以加入更多分析模組
    # 為簡化先輸出 Excel
    print(f"[INFO] 輸出 Excel 報告 → {args.report_xlsx}")
    df.head(500).to_excel(args.report_xlsx, index=False)

if __name__ == "__main__":
    main()
