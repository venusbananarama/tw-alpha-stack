# C:\AI\tw-alpha-stack\alpha_core\combo_lib.py
# -*- coding: utf-8 -*-
"""
combo_lib

Phase-2 Step3：因子組合（combo）共用函式。

角色：
- 從 factor_eval summary JSON 建立「因子 × 視窗」評分表。
- 依照品質指標（例如 Rank-IC / Sharpe / PSR 等）計算 score。
- 可選：根據因子相關係數矩陣做「去相關」挑選（max |corr| 門檻）。
- 輸出一份 combo_plan 給 CLI（scripts/factor_combo.py）或其他流程使用。

設計重點：
- 不假設特定 factor_eval schema，用「盡可能偵測常見欄位」的策略。
- 缺欄位時，用保守 fallback，不讓流程直接爆炸。
- deterministic / idempotent：
  同樣輸入（root/as_of/因子列表/視窗），重跑結果一致。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


# ---- 資料結構 -----------------------------------------------------------------


@dataclass
class FactorWindowMetrics:
    """單一因子在單一視窗上的核心指標與分數。"""

    factor_id: str
    window: int
    score: float
    rank_ic: Optional[float] = None
    ic: Optional[float] = None
    sharpe: Optional[float] = None
    psr: Optional[float] = None
    t_stat: Optional[float] = None
    turnover: Optional[float] = None
    coverage: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FactorComboPlan:
    """
    因子組合計畫的最終輸出結構。

    windows_selected: 每個視窗選到哪些因子（已考慮 max_corr / topK）。
    score_table: 因子×視窗分數表（僅保留必要欄位）。
    meta: 版本、參數等描述。
    """

    as_of: str
    windows_selected: Dict[int, List[str]]
    score_table: List[Dict[str, Any]]
    meta: Dict[str, Any]

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "as_of": self.as_of,
            "windows_selected": {
                str(k): v for k, v in self.windows_selected.items()
            },
            "score_table": self.score_table,
            "meta": self.meta,
        }


# ---- 讀取 factor_eval summary --------------------------------------------------


def _discover_factor_eval_files(
    root: Path, factor_ids: Optional[Iterable[str]] = None
) -> Dict[str, Path]:
    """
    掃描 reports/factor_eval 資料夾，找出 *_summary.json。

    若有傳入 factor_ids，只回傳其中存在的那幾顆。
    """
    eval_dir = root / "reports" / "factor_eval"
    if not eval_dir.exists():
        raise FileNotFoundError(f"找不到 factor_eval 目錄：{eval_dir}")

    result: Dict[str, Path] = {}
    for path in eval_dir.glob("*_summary.json"):
        stem = path.stem  # e.g. mom_6m_summary
        if stem.endswith("_summary"):
            factor_id = stem[: -len("_summary")]
        else:
            factor_id = stem

        if factor_ids is not None and factor_id not in factor_ids:
            continue

        result[factor_id] = path

    if factor_ids is not None:
        missing = [fid for fid in factor_ids if fid not in result]
        if missing:
            # 不丟錯，讓上層決定，只打 warning。
            print(
                f"[combo_lib] WARNING: 找不到下列因子的 eval summary 檔案：{', '.join(missing)}"
            )

    return result


def _load_json(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---- score 計算邏輯 -----------------------------------------------------------


_SCORE_METRIC_CANDIDATES = [
    # (候選欄位名稱, 權重, 越大越好)
    ("rank_ic", 1.0),
    ("rank_ic_mean", 1.0),
    ("ic", 0.8),
    ("ic_mean", 0.8),
    ("sharpe", 0.7),
    ("sharpe_after_costs", 0.9),
    ("psr", 0.6),
    ("t_stat", 0.6),
]


def _extract_numeric(m: Mapping[str, Any], key: str) -> Optional[float]:
    if key not in m:
        return None
    value = m[key]
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _choose_window_block(
    summary: Mapping[str, Any], window: int
) -> Mapping[str, Any]:
    """
    從 factor_eval summary 中抓出指定 window 的統計區塊。

    典型結構假設（但不強制）：
    {
        "factor_id": "...",
        "windows": {
            "6": {...},
            "12": {...}
        },
        "overall": {...}
    }
    """
    windows = summary.get("windows") or {}

    # 嘗試用字串 key
    block = windows.get(str(window))
    if isinstance(block, Mapping):
        return block

    # 有些實作可能用 int key
    block = windows.get(window)
    if isinstance(block, Mapping):
        return block

    # 如果 windows 裡找不到，退回整個 summary 嘗試
    return summary


def _compute_score_for_window_block(block: Mapping[str, Any]) -> FactorWindowMetrics:
    """
    從單一視窗的統計區塊推導 metrics + score。

    此函式只負責計算「一組 metrics」，factor_id / window 由外層填。
    """
    # 先把可能用到的指標全部抓出來
    rank_ic = (
        _extract_numeric(block, "rank_ic")
        or _extract_numeric(block, "rank_ic_mean")
        or _extract_numeric(block, "weekly_rank_ic")
    )
    ic = _extract_numeric(block, "ic") or _extract_numeric(block, "ic_mean")
    sharpe = (
        _extract_numeric(block, "sharpe_after_costs")
        or _extract_numeric(block, "sharpe")
    )
    psr = _extract_numeric(block, "psr")
    t_stat = _extract_numeric(block, "t_stat") or _extract_numeric(block, "t_value")
    turnover = _extract_numeric(block, "turnover")
    coverage = _extract_numeric(block, "coverage")

    # 用權重組合成 score，缺值略過
    score = 0.0
    weight_sum = 0.0

    def add_score(value: Optional[float], weight: float) -> None:
        nonlocal score, weight_sum
        if value is None:
            return
        score += weight * float(value)
        weight_sum += abs(weight)

    # 按照候選列表加分
    for key, weight in _SCORE_METRIC_CANDIDATES:
        if "rank_ic" in key and rank_ic is not None:
            add_score(rank_ic, weight)
        elif key.startswith("ic") and ic is not None:
            add_score(ic, weight)
        elif key.startswith("sharpe") and sharpe is not None:
            add_score(sharpe, weight)
        elif key == "psr" and psr is not None:
            add_score(psr, weight)
        elif key == "t_stat" and t_stat is not None:
            add_score(t_stat, weight)

    # 若完全沒有任何指標，就讓 score=0
    if weight_sum > 0:
        score /= weight_sum

    metrics = FactorWindowMetrics(
        factor_id="",  # 由外層填
        window=0,  # 由外層填
        score=score,
        rank_ic=rank_ic,
        ic=ic,
        sharpe=sharpe,
        psr=psr,
        t_stat=t_stat,
        turnover=turnover,
        coverage=coverage,
    )
    return metrics


def build_score_table(
    root: Path,
    as_of: str,
    windows: Sequence[int],
    factor_ids: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    建立「因子 × 視窗」的分數表（DataFrame）。

    DataFrame 欄位：
        factor_id, window, score,
        rank_ic, ic, sharpe, psr, t_stat, turnover, coverage
    """
    root = root.resolve()
    files = _discover_factor_eval_files(root, factor_ids=factor_ids)

    records: List[Dict[str, Any]] = []

    for factor_id, path in sorted(files.items()):
        try:
            summary = _load_json(path)
        except Exception as exc:  # noqa: BLE001
            print(f"[combo_lib] WARNING: 讀取 {path} 發生錯誤：{exc}")
            continue

        for w in windows:
            block = _choose_window_block(summary, w)
            metrics = _compute_score_for_window_block(block)
            metrics.factor_id = factor_id
            metrics.window = int(w)
            records.append(metrics.to_dict())

    if not records:
        raise RuntimeError(
            f"[combo_lib] 無法建立 score 表（沒有任何有效 factor_eval），"
            f"root={root}, as_of={as_of}, windows={windows}"
        )

    df = pd.DataFrame.from_records(records)
    df.sort_values(["window", "score"], ascending=[True, False], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ---- 去相關的 factor 選擇邏輯 -------------------------------------------------


def _load_corr_matrix(corr_path: Optional[Path]) -> Optional[pd.DataFrame]:
    """
    從 CSV 或 Parquet 讀取因子相關係數矩陣。

    要求：
    - index 與 columns 都是 factor_id。
    - 對稱矩陣（程式不強制，但假設如此）。
    """
    if corr_path is None:
        return None
    if not corr_path.exists():
        print(f"[combo_lib] WARNING: 找不到 corr 檔案：{corr_path}，將不使用去相關邏輯")
        return None

    if corr_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(corr_path)
    else:
        df = pd.read_csv(corr_path, index_col=0)

    return df


def select_factors_for_window(
    df_scores: pd.DataFrame,
    window: int,
    max_factors: int,
    corr_matrix: Optional[pd.DataFrame] = None,
    max_corr: float = 0.7,
) -> List[str]:
    """
    針對單一視窗，依 score 排序並套用 max_corr 去相關條件。

    參數：
        df_scores : build_score_table 回傳的 DataFrame。
        window    : 目標視窗（月）。
        max_factors : 最多選幾顆因子。
        corr_matrix : 若提供，會用來做 |corr| <= max_corr 的篩選。
        max_corr    : 去相關門檻，預設 0.7。

    回傳：
        選到的 factor_id 清單（按分數高到低排序）。
    """
    sub = df_scores[df_scores["window"] == int(window)].copy()
    if sub.empty:
        return []

    selected: List[str] = []

    for _, row in sub.iterrows():
        fid = str(row["factor_id"])
        if fid in selected:
            continue

        if corr_matrix is not None:
            if fid not in corr_matrix.index:
                # 若相關矩陣裡沒有這顆，視為無法檢查，保守接受
                pass
            else:
                ok = True
                for s in selected:
                    if s not in corr_matrix.columns:
                        continue
                    corr_val = corr_matrix.loc[fid, s]
                    try:
                        if abs(float(corr_val)) > max_corr:
                            ok = False
                            break
                    except (TypeError, ValueError):
                        # 非數字就忽略
                        continue
                if not ok:
                    continue

        selected.append(fid)
        if len(selected) >= max_factors:
            break

    return selected


# ---- 高階 API：產生 combo 計畫 -------------------------------------------------


def build_combo_plan(
    root: Path,
    as_of: str,
    windows: Sequence[int],
    max_factors_per_window: int,
    factor_ids: Optional[Iterable[str]] = None,
    corr_path: Optional[Path] = None,
    max_corr: float = 0.7,
    spec_version: str = "factor_combo.v1",
) -> FactorComboPlan:
    """
    產生完整的因子組合計畫。

    root   : repo 根目錄（例如 C:\\AI\\tw-alpha-stack）
    as_of  : W-FRI 字串（YYYY-MM-DD）
    windows: 要考慮的 WF 視窗列表，例如 [6, 12, 24]
    max_factors_per_window: 每個視窗最多挑幾顆因子。
    factor_ids: 若給定，僅限於這一批因子；否則自動從 factor_eval 資料夾掃描。
    corr_path: 若提供，會讀取相關矩陣做去相關挑選。
    max_corr : 去相關門檻。

    回傳：
        FactorComboPlan 物件，可呼叫 to_json_dict() 寫成 JSON。
    """
    root = root.resolve()
    windows = [int(w) for w in windows]

    df_scores = build_score_table(root=root, as_of=as_of, windows=windows, factor_ids=factor_ids)
    corr_matrix = _load_corr_matrix(corr_path)

    windows_selected: Dict[int, List[str]] = {}
    for w in windows:
        selected = select_factors_for_window(
            df_scores=df_scores,
            window=w,
            max_factors=max_factors_per_window,
            corr_matrix=corr_matrix,
            max_corr=max_corr,
        )
        windows_selected[w] = selected

    # score_table 只序列化必要欄位，避免 JSON 過胖
    score_records = df_scores.to_dict(orient="records")

    meta = {
        "spec_version": spec_version,
        "root": str(root),
        "as_of": as_of,
        "windows": list(windows),
        "max_factors_per_window": max_factors_per_window,
        "corr_path": str(corr_path) if corr_path else None,
        "max_corr": max_corr,
    }

    plan = FactorComboPlan(
        as_of=as_of,
        windows_selected=windows_selected,
        score_table=score_records,
        meta=meta,
    )
    return plan


def save_combo_plan(plan: FactorComboPlan, output_path: Path) -> None:
    """
    將 combo 計畫寫成 JSON 檔。

    output_path 通常會是：
        C:\\AI\\tw-alpha-stack\\reports\\factor_combo.<as_of>.json
    """
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = plan.to_json_dict()
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[combo_lib] Combo plan written to {output_path}")
