# -*- coding: utf-8 -*-
"""
scripts.factor_engine

Phase-2 因子引擎的 CLI entry（命令列入口）。

設計原則：
- 不實作任何商業邏輯。
- 所有核心行為（參數解析、FactorEngineConfig 建立、實際執行）
  一律委託給 `alpha_core.phase2.corelib.factor_engine`。
- 這個檔案只負責：
  1) 把 repo root 加進 sys.path（方便用 `python scripts/p2/factor_engine.py` 呼叫）
  2) 把命令列參數轉交給 `alpha_core.phase2.corelib.factor_engine.main()`

實際用法（維持不變）：

    python .\scripts\p2\factor_engine.py `
      --root . `
      --rules .\rules_factors.yaml `
      --factors mom_6m,mom_12m,value_pe `
      --end 2025-11-10 `
      --windows 6,12 `
      --dry-run

所有參數的定義與預設值，以 `alpha_core.phase2.corelib.factor_engine` 為準。
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Sequence

# ---------------------------------------------------------------------------
# bootstrap：把 repo root 加進 sys.path，讓 alpha_core 可以被 import
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 這裡才 import alpha_core.phase2.corelib.factor_engine
from alpha_core.phase2.corelib import factor_engine as _factor_engine  # noqa: E402


# ---------------------------------------------------------------------------
# main：單純把 argv 轉交給 alpha_core.phase2.corelib.factor_engine.main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Thin wrapper around alpha_core.phase2.corelib.factor_engine.main.

    這裡不做任何額外處理，只是把 argv 傳過去：
    - 若 argv 為 None，則由 alpha_core.phase2.corelib.factor_engine.main 自己使用 sys.argv[1:].
    - 回傳值同樣是 int（0=成功，非 0=失敗）。
    """
    return _factor_engine.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())

