import os, glob, shutil, argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

CAND_DATE_COLS = ["date", "trade_date", "Date"]

def find_date_col(cols):
    for c in CAND_DATE_COLS:
        if c in cols:
            return c
    return None

def coerce_date_series(s: pd.Series) -> pd.Series:
    # 盡量容錯：支援 YYYY-MM-DD / 時間戳 / 20250919 這種整數格式
    s2 = pd.to_datetime(s, errors="coerce", format=None)
    if s2.isna().mean() > 0.8:
        s2 = pd.to_datetime(s, errors="coerce", format="%Y%m%d")
    return s2.dt.date

def fix_file(path):
    try:
        t = pq.read_table(path)
        cols = t.column_names
        dcol = find_date_col(cols)
        if not dcol:
            return ("SKIP_NO_DATE_COL", path)

        df = t.to_pandas(types_mapper=None)
        before_dtype = str(df[dcol].dtype)
        df[dcol] = coerce_date_series(df[dcol])

        if pd.isna(df[dcol]).all():
            return ("FAIL_ALL_NAT", path)

        # 轉回 Arrow
        table = pa.Table.from_pandas(df, preserve_index=False)
        # 將 date 欄位強制 cast 成 date32
        if dcol in table.column_names:
            i = table.column_names.index(dcol)
            col = table[dcol]
            if not pa.types.is_date32(col.type):
                col = pc.cast(col, pa.date32())
            table = table.set_column(i, dcol, col)

        # 備份並覆蓋
        bak = path + ".bak"
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(table, tmp, compression="snappy")
        os.replace(tmp, path)
        return ("FIXED", f"{path} | {before_dtype} -> date32")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--datasets", nargs="+", default=["prices","chip"])
    args = ap.parse_args()

    patterns = []
    for ds in args.datasets:
        patterns.append(os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet"))

    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))

    if not files:
        print("[REPAIR] No parquet files found. Check --datahub-root")
        return

    cnt = {"FIXED":0,"SKIP_NO_DATE_COL":0,"FAIL_ALL_NAT":0,"ERROR":0}
    for p in files:
        tag, msg = fix_file(p)
        cnt[tag] = cnt.get(tag,0)+1
        print(f"[{tag}] {msg}")

    print("[SUMMARY]", cnt)

if __name__ == "__main__":
    main()
