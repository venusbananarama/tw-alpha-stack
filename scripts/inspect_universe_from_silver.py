from __future__ import annotations

from pathlib import Path
from typing import Dict, Set

import pandas as pd


def collect_stock_ids(dataset_root: Path) -> Set[str]:
    """Scan all parquet files under dataset_root and collect distinct stock_id."""
    all_ids: Set[str] = set()
    if not dataset_root.exists():
        print(f"[WARN] dataset root not found: {dataset_root}")
        return all_ids

    files = list(dataset_root.rglob("*.parquet"))
    if not files:
        print(f"[WARN] no parquet files under {dataset_root}")
        return all_ids

    print(f"[INFO] scanning {len(files)} files under {dataset_root} ...")
    for i, p in enumerate(files, start=1):
        df = pd.read_parquet(p, columns=["stock_id"])
        ids = df["stock_id"].dropna().unique().tolist()
        all_ids.update(ids)
        if i % 50 == 0:
            print(f"  ... {i} files, current distinct={len(all_ids)}")

    return all_ids


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    silver_root = repo_root / "datahub" / "silver" / "alpha"

    datasets = {
        "prices": silver_root / "prices",
        "chip": silver_root / "chip",
        "per": silver_root / "per",
        "dividend": silver_root / "dividend",
    }

    results: Dict[str, Set[str]] = {}
    for name, path in datasets.items():
        print(f"\n==== {name} ====")
        ids = collect_stock_ids(path)
        print(f"[RESULT] {name}: distinct stock_id = {len(ids)}")
        results[name] = ids

    # 交集 / 聯集也可以看一下
    if results:
        all_union: Set[str] = set().union(*results.values())
        print(f"\n[SUMMARY] union of all 4 datasets: {len(all_union)} distinct stock_id")

        # 只要 prices 的 universe
        print(f"[SUMMARY] prices only (same as above for prices): {len(results['prices'])}")


if __name__ == "__main__":
    main()
