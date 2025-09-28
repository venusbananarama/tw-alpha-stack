from __future__ import annotations
import yaml
from pathlib import Path
from twalpha.data.loader_twse import load_universe, read_local_csv
from twalpha.features.indicators_core import core_feature_block
from twalpha.features.indicators_smc import smc_block
from twalpha.features.indicators_custom import custom_block
from twalpha.signals.ensemble import ensemble_score
from twalpha.backtest.engine import simple_backtest

def main(cfg_file: str = "configs/strategy.default.yaml"):
    cfg = yaml.safe_load(Path(cfg_file).read_text(encoding="utf-8"))
    uni = load_universe(cfg["universe_file"])
    sym = uni[0]
    df = read_local_csv(f"data/{sym.replace('.','_')}.csv")
    df = core_feature_block(df, **cfg["indicators"]["core"])
    df = smc_block(df, **cfg["indicators"]["smc"])
    df = custom_block(df, **cfg["indicators"]["custom"])
    df["final_score"] = ensemble_score(df, cfg["signals"]["weights"])
    stats = simple_backtest(df, cfg["signals"]["threshold_enter"], cfg["signals"]["threshold_exit"])
    print("Backtest:", stats)

if __name__ == "__main__":
    main()
