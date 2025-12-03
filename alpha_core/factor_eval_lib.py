from __future__ import annotations

"""
alpha_core.factor_eval_lib

Shared helpers for Phase-2 factor evaluation artefacts.

角色：
- 當作 factor_eval JSON 的「技術視圖」工具箱：
  - 定位 factor_eval 目錄與檔名
  - 載入單一或多個 *_summary.json
  - 提供 evaluate_and_load() 幫你「算 skeleton → 讀回來」

設計原則：
- deterministic / idempotent：同一組輸入重跑不會產生矛盾狀態。
- 不決定 gate 規則，也不直接修改 wf_summary.json。
- 真正「產生 / 更新 eval JSON」仍委託 alpha_core.factor_eval.evaluate_factors。
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

import json

# 從本尊因子評估模組重用 evaluate_factors / evaluate_single_factor
try:
    from .factor_eval import (
        evaluate_single_factor as _evaluate_single_factor,
        evaluate_factors as _evaluate_factors,
    )
except Exception:  # pragma: no cover
    _evaluate_single_factor = None  # type: ignore[assignment]
    _evaluate_factors = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FactorEvalFile:
    """
    單一因子 eval JSON 的檔案視圖。

    Attributes
    ----------
    factor_id : str
        因子 ID（例如 "mom_6m"）。
    path : Path
        JSON 檔案實體位置。
    data : Mapping[str, Any]
        反序列化後的 JSON 內容（top-level object）。
    """

    factor_id: str
    path: Path
    data: Mapping[str, Any]


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def get_factor_eval_dir(root: Path | str) -> Path:
    """
    取得 factor_eval 目錄（預設 <root>/reports/factor_eval）。

    不會自動建立目錄，只負責 path 計算。
    """
    root_path = Path(root).resolve()
    return root_path / "reports" / "factor_eval"


def get_factor_eval_path(root: Path | str, factor_id: str) -> Path:
    """
    取得某個因子 eval JSON 的預設路徑。

    例如：
        get_factor_eval_path(root, "mom_6m")
        -> <root>/reports/factor_eval/mom_6m_summary.json
    """
    factor_id = str(factor_id).strip()
    if not factor_id:
        raise ValueError("factor_id must be non-empty")

    return get_factor_eval_dir(root) / f"{factor_id}_summary.json"


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------


def _load_json_object(path: Path) -> Mapping[str, Any]:
    """
    讀取 JSON 檔並確保 top-level 是 object。

    Raises:
        FileNotFoundError, json.JSONDecodeError, ValueError
    """
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError(f"factor_eval file is empty: {path}")
    obj = json.loads(text)
    if not isinstance(obj, MutableMapping):
        raise ValueError(f"factor_eval JSON must be an object at top level: {path}")
    return obj


def load_factor_eval(root: Path | str, factor_id: str) -> FactorEvalFile:
    """
    載入單一因子的 factor_eval summary。

    Raises:
        FileNotFoundError, json.JSONDecodeError, ValueError
    """
    path = get_factor_eval_path(root, factor_id)
    data = _load_json_object(path)
    return FactorEvalFile(factor_id=str(factor_id), path=path, data=data)


def load_all_factor_eval(
    root: Path | str,
    factor_ids: Optional[Iterable[str]] = None,
) -> Dict[str, FactorEvalFile]:
    """
    載入 factor_eval 目錄下的一批 *_summary.json。

    參數：
        root      : repo root
        factor_ids: 若為 None → 掃描整個目錄；
                    若提供 → 只載入指定清單（找不到檔會直接 raise）。

    回傳：
        { factor_id: FactorEvalFile(...) }
    """
    eval_dir = get_factor_eval_dir(root)
    if not eval_dir.exists():
        raise FileNotFoundError(f"factor_eval dir not found: {eval_dir}")

    result: Dict[str, FactorEvalFile] = {}

    if factor_ids is None:
        # 掃描整個目錄
        for path in sorted(eval_dir.glob("*_summary.json")):
            name = path.stem
            fid = name[:-8] if name.endswith("_summary") else name
            if not fid:
                continue
            try:
                data = _load_json_object(path)
            except Exception:
                # 對於全域掃描採「best-effort」策略：壞檔案先略過，
                # 真要查再用單一 load_factor_eval() 把錯丟出來。
                continue
            result[fid] = FactorEvalFile(factor_id=fid, path=path, data=data)
        return result

    # 僅載入指定清單；找不到檔會直接 raise
    for fid in factor_ids:
        fid = str(fid).strip()
        if not fid:
            continue
        path = get_factor_eval_path(root, fid)
        data = _load_json_object(path)
        result[fid] = FactorEvalFile(factor_id=fid, path=path, data=data)

    return result


# ---------------------------------------------------------------------------
# Evaluate helpers（呼叫本尊 alpha_core.factor_eval）
# ---------------------------------------------------------------------------


def evaluate_single_factor(
    root: Path | str,
    factor_id: str,
    wf_windows: Sequence[int],
    as_of: Optional[str] = None,
) -> FactorEvalFile:
    """
    對單一因子執行 skeleton eval，並載入結果。

    等價於：
      1) alpha_core.factor_eval.evaluate_single_factor(...)
      2) 讀回 <root>/reports/factor_eval/<factor_id>_summary.json
    """
    if _evaluate_single_factor is None:
        raise RuntimeError("alpha_core.factor_eval.evaluate_single_factor is not available")

    root_path = Path(root).resolve()
    _evaluate_single_factor(
        root=root_path,
        factor_id=factor_id,
        wf_windows=wf_windows,
        as_of=as_of,
    )
    return load_factor_eval(root_path, factor_id)


def evaluate_factors(
    root: Path | str,
    factor_ids: Iterable[str],
    wf_windows: Sequence[int],
    as_of: Optional[str] = None,
) -> Dict[str, FactorEvalFile]:
    """
    對多個因子執行 skeleton eval，並載入結果。

    回傳：
        { factor_id: FactorEvalFile(...) }
    """
    if _evaluate_factors is None:
        raise RuntimeError("alpha_core.factor_eval.evaluate_factors is not available")

    root_path = Path(root).resolve()
    # 先執行產檔（與原本 evaluate_factors 行為一致）
    _evaluate_factors(
        root=root_path,
        factor_ids=list(factor_ids),
        wf_windows=list(wf_windows),
        as_of=as_of,
    )

    # 再把檔案讀回來
    return load_all_factor_eval(root_path, factor_ids=list(factor_ids))


# ---------------------------------------------------------------------------
# Simple metrics helpers
# ---------------------------------------------------------------------------


def get_window_metric(
    eval_obj: Mapping[str, Any],
    window: int,
    metric: str,
) -> Optional[float]:
    """
    從 factor_eval JSON 取得單一 window 的某個 metric。

    只讀：
        eval["windows"][str(window)][metric]

    取不到就回傳 None，不丟例外。
    """
    windows = eval_obj.get("windows")
    if not isinstance(windows, Mapping):
        return None
    win = windows.get(str(window))
    if not isinstance(win, Mapping):
        return None
    val = win.get(metric)
    if isinstance(val, (int, float)):
        return float(val)
    return None


def get_aggregated_metric(
    eval_obj: Mapping[str, Any],
    metric: str,
    windows: Sequence[int],
    mode: str = "min",
) -> Optional[float]:
    """
    在多個 window 上聚合某個 metric，方便快速查看。

    攻略順序與 compose_factors_to_wf 裡的邏輯一致：

      1) 優先看 eval["windows"][str(w)][metric]
      2) 若全都沒有 → 看 eval["overall"][metric]
      3) 若還是沒有 → 看 eval[metric]

    mode:
      - "min"：取最小值
      - "max"：取最大值
    """
    values: list[float] = []

    windows_block = eval_obj.get("windows")
    if isinstance(windows_block, Mapping):
        for w in windows:
            node = windows_block.get(str(w))
            if not isinstance(node, Mapping):
                continue
            v = node.get(metric)
            if isinstance(v, (int, float)):
                values.append(float(v))

    if not values:
        overall = eval_obj.get("overall")
        if isinstance(overall, Mapping):
            v = overall.get(metric)
            if isinstance(v, (int, float)):
                values.append(float(v))

    if not values:
        v = eval_obj.get(metric)
        if isinstance(v, (int, float)):
            values.append(float(v))

    if not values:
        return None

    if mode == "min":
        return min(values)
    if mode == "max":
        return max(values)
    raise ValueError(f"unsupported mode: {mode!r}")
