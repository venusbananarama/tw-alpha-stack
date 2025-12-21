# C:\AI\tw-alpha-stack\scripts\compose_factors_to_wf.py
#!/usr/bin/env python
"""
Compose factor_eval summaries into wf_summary.json.factors / factor_candidates，
並在同一階段計算因子層 SLO，寫入 wf_summary["factor_slo"]。

設計重點：
- 以 rules_factors.yaml 為 SSOT：透過 factor_registry 取得 gate_rules。
- 支援 global gate_rules 以及 per-window gate_rules：
    gate_rules:
      min_rank_ic: 0.03
      per_window:
        "6":
          min_rank_ic: 0.02
          max_turnover: 1.0
        "12":
          min_rank_ic: 0.03
- 通過門檻 → wf_summary["factors"][factor_id]
  未通過或無門檻 → wf_summary["factor_candidates"][factor_id]（或依 mode 決定是否寫入）。
- 完成 factors 組裝後，透過 alpha_core.factor_slo_lib：
  - 讀 rules_factors.yaml.gate_ready（含 engine/profile override）
  - 對 wf_summary["factors"] 做 SLO 評估
  - 結果寫入 wf_summary["factor_slo"]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SLO library（純函式，負責 gate_ready → SLO 結果）
#   1) 首選 alpha_core.factor_slo_lib（當專案以 package 執行時）
#   2) 若找不到 alpha_core，將 repo root 加入 sys.path 後再嘗試一次
#   3) 若仍失敗，退回為「無 SLO 功能」（load_factor_slo_config / evaluate_factor_slo = None）
# ---------------------------------------------------------------------------

try:
    # Case 1: 例如 python -m scripts.compose_factors_to_wf，root 已在 sys.path
    from alpha_core.factor_slo_lib import (  # type: ignore[import]
        load_factor_slo_config,
        evaluate_factor_slo,
    )
except Exception:  # pragma: no cover
    try:
        # Case 2: 例如 python scripts/compose_factors_to_wf.py 從 repo root 執行
        _ROOT = Path(__file__).resolve().parents[1]
        if str(_ROOT) not in sys.path:
            sys.path.insert(0, str(_ROOT))
        from alpha_core.factor_slo_lib import (  # type: ignore[import]
            load_factor_slo_config,
            evaluate_factor_slo,
        )
    except Exception:  # pragma: no cover
        # 在 unit test 或特殊環境下可能沒有 alpha_core.factor_slo_lib
        load_factor_slo_config = None  # type: ignore[assignment]
        evaluate_factor_slo = None  # type: ignore[assignment]


__all__ = [
    "GateRulesView",
    "FactorConfigView",
    "load_registry",
    "factor_passes_basic_gate",
    "compose_factors_to_wf",
]


# ---------------------------------------------------------------------------
# Data model stub（type hint 用；實際類別由 factor_registry 提供）
# ---------------------------------------------------------------------------


@dataclass
class GateRulesView:
    min_rank_ic: Optional[float]
    max_turnover: Optional[float]
    max_corr: Optional[float]
    min_coverage: Optional[float]
    extras: Mapping[str, Any]


@dataclass
class FactorConfigView:
    factor_id: str
    enabled: bool
    gate_rules: GateRulesView


# ---------------------------------------------------------------------------
# Helpers to load factor registry via tools/factors/factor_registry.py
# ---------------------------------------------------------------------------


def load_registry(root: Path, rules_file: Path) -> Tuple[Any, Dict[str, FactorConfigView]]:
    """
    Load factor registry and build a simple factor_id -> FactorConfigView map.

    root      : repo root
    rules_file: resolved path to rules_factors.yaml
    """
    tools_dir = root / "tools" / "factors"
    if not tools_dir.exists():
        raise FileNotFoundError(f"tools/factors directory not found at {tools_dir}")

    # 放在 sys.path 開頭，確保載到的是專案內的 factor_registry
    sys.path.insert(0, str(tools_dir))

    try:
        from factor_registry import load_factor_registry  # type: ignore[import]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"failed to import factor_registry from {tools_dir}: {exc}") from exc

    registry = load_factor_registry(root=root, rules_path=rules_file)

    by_id: Dict[str, FactorConfigView] = {}
    for cfg in registry.factors:
        gr = cfg.gate_rules
        gr_view = GateRulesView(
            min_rank_ic=getattr(gr, "min_rank_ic", None),
            max_turnover=getattr(gr, "max_turnover", None),
            max_corr=getattr(gr, "max_corr", None),
            min_coverage=getattr(gr, "min_coverage", None),
            extras=getattr(gr, "extras", {}) or {},
        )
        by_id[cfg.factor_id] = FactorConfigView(
            factor_id=cfg.factor_id,
            enabled=bool(getattr(cfg, "enabled", True)),
            gate_rules=gr_view,
        )

    logger.info(
        "Loaded registry: %d factors (%d enabled)",
        len(by_id),
        sum(1 for v in by_id.values() if v.enabled),
    )
    if getattr(registry, "errors", None):
        logger.warning("Registry contains %d validation errors", len(registry.errors))

    return registry, by_id


# ---------------------------------------------------------------------------
# Metric extraction / gating
# ---------------------------------------------------------------------------


def _to_float(val: Any) -> Optional[float]:
    """Best-effort 轉成 float；失敗回傳 None。"""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_metric_over_windows(
    eval_obj: Mapping[str, Any],
    metric: str,
    wf_windows: Sequence[int],
    mode: str,
) -> Optional[float]:
    """
    從 factor_eval JSON 裡抽出某個 metric 在多個 WF 視窗上的聚合值。

    優先順序：
    1) eval["windows"][window]["metric"]
    2) eval["overall"]["metric"]
    3) eval["metric"]

    mode='min' → 取所有 window 中的最小值
    mode='max' → 取所有 window 中的最大值
    """
    values: List[float] = []

    windows_block = eval_obj.get("windows")
    if isinstance(windows_block, Mapping):
        for w in wf_windows:
            key = str(w)
            win = windows_block.get(key)
            if isinstance(win, Mapping):
                v = win.get(metric)
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
    raise ValueError(f"unsupported mode {mode!r}")


def _extract_metric_for_window(
    eval_obj: Mapping[str, Any],
    metric: str,
    window: int,
) -> Optional[float]:
    """
    只取單一 window 的 metric。

    - 僅讀 eval["windows"][str(window)][metric]
    - 若沒有該 window 或該 metric → 回傳 None（視為 gating 失敗）。
    """
    windows_block = eval_obj.get("windows")
    if not isinstance(windows_block, Mapping):
        return None

    win = windows_block.get(str(window))
    if not isinstance(win, Mapping):
        return None

    v = win.get(metric)
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _has_any_gate_rule(gr: GateRulesView) -> bool:
    """檢查是否有設定任何基本門檻（全 None 且 extras 空代表沒門檻）。"""
    if gr.min_rank_ic is not None:
        return True
    if gr.max_turnover is not None:
        return True
    if gr.max_corr is not None:
        return True
    if gr.min_coverage is not None:
        return True
    if gr.extras:
        return True
    return False


def _normalize_coverage_block(block: Dict[str, Any]) -> None:
    """
    Normalize coverage-related metrics into a stable contract:
      - coverage / coverage_ratio: ratio in [0, 1] or None
      - coverage_count           : optional integer count (if provided)
      - sample_days              : integer or None

    If coverage is clearly a count (>1) and coverage_count not provided, we keep it
    in coverage_count and clear coverage_ratio to avoid mixing semantics.
    """
    if not isinstance(block, Mapping):
        return

    def _clamp_ratio(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            v = float(val)
        except (TypeError, ValueError):
            return None
        return max(0.0, min(v, 1.0))

    coverage_ratio: Optional[float] = None
    coverage_count: Optional[int] = None

    cov_raw = block.get("coverage")
    cov_ratio_raw = block.get("coverage_ratio")
    cov_count_raw = block.get("coverage_count")

    if isinstance(cov_ratio_raw, (int, float)):
        coverage_ratio = _clamp_ratio(cov_ratio_raw)

    if isinstance(cov_count_raw, (int, float)):
        try:
            coverage_count = int(cov_count_raw)
        except (TypeError, ValueError):
            coverage_count = None

    if isinstance(cov_raw, (int, float)):
        if cov_raw > 1 and coverage_count is None:
            # Treat as count, leave ratio unset
            coverage_count = int(cov_raw)
        elif cov_raw <= 1 and coverage_ratio is None:
            coverage_ratio = _clamp_ratio(cov_raw)

    sample_days_raw = block.get("sample_days")
    sample_days = 0
    if isinstance(sample_days_raw, (int, float)):
        try:
            sample_days = int(sample_days_raw)
        except (TypeError, ValueError):
            sample_days = 0
    sample_days = max(sample_days, 0)

    # If sample_days is zero/None, wipe all metrics to avoid contradictions
    if sample_days == 0:
        coverage_ratio = None
        coverage_count = None
        for key in (
            "ic",
            "rank_ic",
            "ic_mean",
            "ic_std",
            "rank_ic_mean",
            "rank_ic_std",
            "psr",
            "dsr",
            "turnover",
            "max_corr",
        ):
            if key in block:
                block[key] = None

    block["coverage_ratio"] = coverage_ratio
    block["coverage_count"] = coverage_count
    block["coverage"] = coverage_ratio
    block["sample_days"] = sample_days


def _normalize_eval_metrics(eval_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize coverage/sample_days metrics across overall and per-window blocks.
    """
    if not isinstance(eval_obj, dict):
        return eval_obj

    overall = eval_obj.get("overall")
    if isinstance(overall, dict):
        _normalize_coverage_block(overall)

    windows_block = eval_obj.get("windows")
    if isinstance(windows_block, Mapping):
        for win_val in windows_block.values():
            if isinstance(win_val, dict):
                _normalize_coverage_block(win_val)

    return eval_obj


def factor_passes_basic_gate(
    cfg: FactorConfigView,
    eval_obj: Mapping[str, Any],
    wf_windows: Sequence[int],
) -> Optional[bool]:
    """
    根據 gate_rules + factor_eval 判斷是否通過「基本門檻」。

    回傳：
      True  : 通過所有定義的門檻 → 放進 wf_summary.factors
      False : 未通過（含指標缺失 / 任何一項不達標）→ 放進 wf_summary.factor_candidates
      None  : 該因子沒定義任何 gate_rules → 視作 candidate（讓 Gate 做後續審查）

    global 規則：
    - min_rank_ic：取所有 window 的最小 rank_ic，必須 ≥ 門檻。
    - max_turnover：取所有 window 的最大 turnover，必須 ≤ 門檻。
    - max_corr：取所有 window 的最大 max_corr，必須 ≤ 門檻。
    - min_coverage：取所有 window 的最小 coverage，必須 ≥ 門檻。

    per-window 規則（可選）：
    gate_rules.extras.per_window["6"].min_rank_ic 等：
    - 如果有定義，單一 window 不達標也視為整體 Fail。
    - 若 eval 缺該 window 的 metric → Fail（視為資料不足）。
    """
    gr = cfg.gate_rules
    if not _has_any_gate_rule(gr):
        # 沒有設定任何 gate_rules：交給後續 Gate 再審 → candidate
        return None

    # ----- Global 門檻 -----
    if gr.min_rank_ic is not None:
        v = _extract_metric_over_windows(eval_obj, "rank_ic", wf_windows, mode="min")
        if v is None or v < gr.min_rank_ic:
            logger.debug(
                "factor=%s failed global min_rank_ic: value=%s, threshold=%s",
                cfg.factor_id,
                v,
                gr.min_rank_ic,
            )
            return False

    if gr.max_turnover is not None:
        v = _extract_metric_over_windows(eval_obj, "turnover", wf_windows, mode="max")
        if v is None or v > gr.max_turnover:
            logger.debug(
                "factor=%s failed global max_turnover: value=%s, threshold=%s",
                cfg.factor_id,
                v,
                gr.max_turnover,
            )
            return False

    if gr.max_corr is not None:
        v = _extract_metric_over_windows(eval_obj, "max_corr", wf_windows, mode="max")
        if v is None or v > gr.max_corr:
            logger.debug(
                "factor=%s failed global max_corr: value=%s, threshold=%s",
                cfg.factor_id,
                v,
                gr.max_corr,
            )
            return False

    if gr.min_coverage is not None:
        v = _extract_metric_over_windows(eval_obj, "coverage", wf_windows, mode="min")
        if v is None or v < gr.min_coverage:
            logger.debug(
                "factor=%s failed global min_coverage: value=%s, threshold=%s",
                cfg.factor_id,
                v,
                gr.min_coverage,
            )
            return False

    # ----- per-window 門檻（extras.per_window）-----
    per_window_cfg = gr.extras.get("per_window") if isinstance(gr.extras, Mapping) else None
    if isinstance(per_window_cfg, Mapping):
        for w in wf_windows:
            win_cfg = per_window_cfg.get(str(w))
            if not isinstance(win_cfg, Mapping):
                # 沒有針對這個 window 設特別門檻 → 跳過
                continue

            # min_rank_ic（per-window）
            thr_min_rank_ic = _to_float(win_cfg.get("min_rank_ic"))
            if thr_min_rank_ic is not None:
                v = _extract_metric_for_window(eval_obj, "rank_ic", w)
                if v is None or v < thr_min_rank_ic:
                    logger.debug(
                        "factor=%s failed per_window[%s].min_rank_ic: value=%s, threshold=%s",
                        cfg.factor_id,
                        w,
                        v,
                        thr_min_rank_ic,
                    )
                    return False

            # max_turnover（per-window）
            thr_max_turnover = _to_float(win_cfg.get("max_turnover"))
            if thr_max_turnover is not None:
                v = _extract_metric_for_window(eval_obj, "turnover", w)
                if v is None or v > thr_max_turnover:
                    logger.debug(
                        "factor=%s failed per_window[%s].max_turnover: value=%s, threshold=%s",
                        cfg.factor_id,
                        w,
                        v,
                        thr_max_turnover,
                    )
                    return False

            # max_corr（per-window）
            thr_max_corr = _to_float(win_cfg.get("max_corr"))
            if thr_max_corr is not None:
                v = _extract_metric_for_window(eval_obj, "max_corr", w)
                if v is None or v > thr_max_corr:
                    logger.debug(
                        "factor=%s failed per_window[%s].max_corr: value=%s, threshold=%s",
                        cfg.factor_id,
                        w,
                        v,
                        thr_max_corr,
                    )
                    return False

            # min_coverage（per-window）
            thr_min_coverage = _to_float(win_cfg.get("min_coverage"))
            if thr_min_coverage is not None:
                v = _extract_metric_for_window(eval_obj, "coverage", w)
                if v is None or v < thr_min_coverage:
                    logger.debug(
                        "factor=%s failed per_window[%s].min_coverage: value=%s, threshold=%s",
                        cfg.factor_id,
                        w,
                        v,
                        thr_min_coverage,
                    )
                    return False

    # 所有已定義的門檻皆通過
    return True


# ---------------------------------------------------------------------------
# Core compose logic
# ---------------------------------------------------------------------------


def compose_factors_to_wf(
    root: Path,
    rules_file: Path,
    wf_summary_path: Path,
    factor_eval_dir: Path,
    wf_windows: Sequence[int],
    mode: str = "all",
    slo_profile: Optional[str] = None,
    slo_engine: str = "classic",
) -> None:
    """
    讀取 registry + factor_eval + wf_summary，產生 factors / factor_candidates 區塊，
    並依照 rules_factors.yaml.gate_ready 計算 SLO，寫入 wf_summary["factor_slo"]。

    mode:
      - "all"          : 重寫 factors / factor_candidates 兩個區塊。
      - "factors_only" : 只重寫 factors，並移除 factor_candidates。

    slo_profile / slo_engine:
      - 對應 rules_factors.yaml.gate_ready.profiles / engines 的 key。
      - 若為 None / "classic" 以外的 key，則由 load_factor_slo_config 做 fallback。
    """
    mode = mode.lower()
    if mode not in ("all", "factors_only"):
        raise ValueError(f"unsupported mode: {mode!r}")

    if not wf_summary_path.exists():
        raise FileNotFoundError(f"wf_summary.json not found at {wf_summary_path}")

    if not factor_eval_dir.exists():
        raise FileNotFoundError(f"factor_eval dir not found at {factor_eval_dir}")

    # 1) 讀取 registry（含 gate_rules）
    registry, cfg_by_id = load_registry(root=root, rules_file=rules_file)

    # 2) 讀取 wf_summary.json
    with wf_summary_path.open("r", encoding="utf-8") as f:
        wf_summary = json.load(f)

    if not isinstance(wf_summary, dict):
        raise ValueError("wf_summary.json root must be an object")

    # 3) 掃描 factor_eval/*_summary.json
    eval_files = sorted(factor_eval_dir.glob("*_summary.json"))
    logger.info("Found %d factor_eval summary files in %s", len(eval_files), factor_eval_dir)

    # === 沒有任何 eval 檔時，不要動現有 wf_summary ===
    if not eval_files:
        logger.warning(
            "No factor_eval summary files found in %s; "
            "wf_summary.factors / factor_candidates / factor_slo will be left unchanged.",
            factor_eval_dir,
        )
        return

    new_factors: Dict[str, Any] = {}
    new_candidates: Dict[str, Any] = {}

    updated_pass = 0
    updated_candidate = 0
    skipped = 0

    for path in eval_files:
        name = path.stem  # e.g. mom_12m_summary
        factor_id = name[:-8] if name.endswith("_summary") else name

        cfg = cfg_by_id.get(factor_id)
        if cfg is None:
            logger.debug("Skipping eval for unknown factor_id=%s (%s)", factor_id, path.name)
            skipped += 1
            continue
        if not cfg.enabled:
            logger.debug("Skipping eval for disabled factor_id=%s", factor_id)
            skipped += 1
            continue

        try:
            with path.open("r", encoding="utf-8") as f:
                eval_obj = json.load(f)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load eval JSON for factor=%s: %s", factor_id, exc)
            skipped += 1
            continue

        if not isinstance(eval_obj, Mapping):
            logger.warning("Eval JSON for factor=%s is not an object, skipping", factor_id)
            skipped += 1
            continue

        eval_obj = _normalize_eval_metrics(eval_obj)

        verdict = factor_passes_basic_gate(cfg, eval_obj, wf_windows=wf_windows)

        if verdict is True:
            new_factors[factor_id] = eval_obj
            updated_pass += 1
        else:
            # verdict False 或 None（無 gate_rules）都視為 candidate
            if mode == "all":
                new_candidates[factor_id] = eval_obj
                updated_candidate += 1
            else:
                # factors_only 模式下，candidate 只計數不寫出
                updated_candidate += 1

    logger.info(
        "Composed into wf_summary.json: %d passed, %d candidates, %d skipped (mode=%s)",
        updated_pass,
        updated_candidate,
        skipped,
        mode,
    )

    # 4) canonical factors_by_status
    canonical = {
        "passed": dict(new_factors),
        "candidates": dict(new_candidates) if mode == "all" else {},
        "skipped": {},
    }
    wf_summary["factors_by_status"] = canonical

    # 5) legacy mirrors (must stay identical)
    wf_summary["factors"] = dict(canonical)
    wf_summary["factor_candidates"] = dict(canonical["candidates"])

    # 6) fail-fast consistency checks
    if wf_summary.get("factors_by_status") != canonical:
        raise ValueError("wf_summary.factors_by_status is not identical to canonical layout")
    if wf_summary.get("factors") != canonical:
        raise ValueError("wf_summary.factors is not identical to canonical factors_by_status")
    if wf_summary.get("factor_candidates", {}) != canonical["candidates"]:
        raise ValueError("wf_summary.factor_candidates is not identical to canonical candidates")

    # 5) 依 gate_ready 設定計算 SLO，寫入 wf_summary["factor_slo"]
    if load_factor_slo_config is not None and evaluate_factor_slo is not None:
        slo_cfg = load_factor_slo_config(
            rules_path=rules_file,
            profile=slo_profile,
            engine=slo_engine,
        )
        slo_result = evaluate_factor_slo(
            wf_summary=wf_summary,
            slo=slo_cfg,
            windows=list(wf_windows),
            wf_summary_path=str(wf_summary_path),
        )
        wf_summary["factor_slo"] = asdict(slo_result)

        logger.info(
            "factor_slo: satisfied=%s total_factors=%d per_window=%s missing_required=%s",
            slo_result.satisfied,
            slo_result.total_factors,
            slo_result.per_window_counts,
            slo_result.missing_required_factors,
        )
    else:
        logger.warning(
            "factor_slo_lib is not available; wf_summary.factor_slo will not be populated."
        )

    # 6) 寫回 wf_summary.json
    with wf_summary_path.open("w", encoding="utf-8") as f:
        json.dump(wf_summary, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compose factor_eval summaries into wf_summary.json using gate_rules and factor SLO.",
    )
    parser.add_argument(
        "--root",
        "-R",
        type=str,
        default=".",
        help="Project root (default: current directory).",
    )
    parser.add_argument(
        "--rules-file",
        type=str,
        default=None,
        help="Path to rules_factors.yaml (default: <root>/rules_factors.yaml).",
    )
    parser.add_argument(
        "--wf-summary",
        type=str,
        default=None,
        help="Path to wf_summary.json (default: <root>/reports/wf_summary.json).",
    )
    parser.add_argument(
        "--factor-eval-dir",
        type=str,
        default=None,
        help=(
            "Directory containing factor_eval *_summary.json "
            "(default: <root>/reports/factor_eval)."
        ),
    )
    parser.add_argument(
        "--wf-windows",
        type=int,
        nargs="+",
        default=[6, 12, 24],
        help="WF windows (months) to consider when aggregating metrics; default: 6 12 24.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["all", "factors_only"],
        default="all",
        help="Write mode: 'all' (factors + factor_candidates) or 'factors_only' (only factors).",
    )
    parser.add_argument(
        "--slo-profile",
        type=str,
        default=None,
        help="Profile name for factor SLO (e.g. dev/test/live); default: None (no profile override).",
    )
    parser.add_argument(
        "--slo-engine",
        type=str,
        default="classic",
        help="Engine key for factor SLO (classic/ai); default: classic.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR); default=INFO.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    root = Path(args.root).resolve()
    if not root.exists():
        raise SystemExit(f"root path does not exist: {root}")

    rules_file = Path(args.rules_file).resolve() if args.rules_file else (root / "rules_factors.yaml")
    wf_summary_path = (
        Path(args.wf_summary).resolve()
        if args.wf_summary
        else (root / "reports" / "wf_summary.json")
    )
    factor_eval_dir = (
        Path(args.factor_eval_dir).resolve()
        if args.factor_eval_dir
        else (root / "reports" / "factor_eval")
    )

    logger.info("root           = %s", root)
    logger.info("rules_file     = %s", rules_file)
    logger.info("wf_summary     = %s", wf_summary_path)
    logger.info("factor_eval_dir= %s", factor_eval_dir)
    logger.info("wf_windows     = %s", args.wf_windows)
    logger.info("mode           = %s", args.mode)
    logger.info("slo_profile    = %s", args.slo_profile)
    logger.info("slo_engine     = %s", args.slo_engine)

    try:
        compose_factors_to_wf(
            root=root,
            rules_file=rules_file,
            wf_summary_path=wf_summary_path,
            factor_eval_dir=factor_eval_dir,
            wf_windows=args.wf_windows,
            mode=args.mode,
            slo_profile=args.slo_profile,
            slo_engine=args.slo_engine,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("compose_factors_to_wf failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
