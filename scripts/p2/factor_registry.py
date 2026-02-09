#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/p2/factor_registry.py

Phase-2 因子 registry SSOT（library + CLI 一檔到位）。

- 唯一實作：FactorConfig / GateRules / FactorRegistry + load_factor_registry。
- CLI 提供 JSON 視圖給 Run-Phase2-OneClick.ps1 使用：
  {
    "factors": [
      {
        "factor_id": "...",
        "category": "...",
        "engine": "...",
        "enabled": true,
        "gate_rules": {
          "min_rank_ic": float | null,
          "max_turnover": float | null,
          "max_corr": float | null,
          "min_coverage": float | null,
          "extras": { ... }
        }
      },
      ...
    ],
    "errors": [ "..." , ... ]
  }
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import yaml

# ---------------------------------------------------------------------------
# Logging / constants
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

WF_ALLOWED_WINDOWS: Tuple[int, ...] = (6, 12, 24)
ALLOWED_DATA_FREQ: Tuple[str, ...] = ("daily", "weekly", "monthly")


# ---------------------------------------------------------------------------
# Data model（schema-first）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateRules:
    """Gate rule thresholds for a single factor."""
    min_rank_ic: Optional[float] = None
    max_turnover: Optional[float] = None
    max_corr: Optional[float] = None
    min_coverage: Optional[float] = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any]) -> "GateRules":
        known_keys = {"min_rank_ic", "max_turnover", "max_corr", "min_coverage"}
        extras = {k: v for k, v in raw.items() if k not in known_keys}
        return cls(
            min_rank_ic=_maybe_float(raw.get("min_rank_ic")),
            max_turnover=_maybe_float(raw.get("max_turnover")),
            max_corr=_maybe_float(raw.get("max_corr")),
            min_coverage=_maybe_float(raw.get("min_coverage")),
            extras=extras,
        )


@dataclass(frozen=True)
class FactorConfig:
    """
    Normalised factor configuration used by downstream tools.

    這是 rules_factors.yaml 單一 factor 的「檢查過」視圖。
    原始 YAML 會另外存在 FactorRegistry.raw_specs 給 engine 用。
    """

    factor_id: str
    category: str
    description: str

    universe: str
    data_freq: str
    engine: str
    enabled: bool

    start_date: Optional[str]
    end_date: Optional[str]

    wf_windows: Tuple[int, ...]
    gate_rules: GateRules

    # Governance / risk metadata
    owner: Optional[str] = None
    reviewed_by: Optional[str] = None
    last_reviewed: Optional[str] = None
    risk_family: Optional[str] = None
    neutralization: Optional[Mapping[str, Any]] = None

    # Tagging / documentation
    tags: Tuple[str, ...] = field(default_factory=tuple)
    impl_notes: Optional[str] = None

    # Any other fields we don't want to model explicitly
    extras: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(
        cls,
        raw: Mapping[str, Any],
        meta: Mapping[str, Any],
    ) -> "FactorConfig":
        """
        從 YAML 單一 factor mapping 建立 FactorConfig。

        - 套用 meta.default_* fallback（universe / data_freq / engine / owner...）
        - 驗證 wf_windows ∈ {6,12,24} 且不可為空。
        - 未建模欄位塞進 extras，保留 forward compatibility。
        """
        factor_id = str(raw.get("factor_id", "")).strip()
        if not factor_id:
            raise ValueError("factor_id is required for every factor entry")

        category = str(raw.get("category", "")).strip() or "classic"
        description = str(raw.get("description", "")).strip()
        if not description:
            raise ValueError(f"{factor_id}: description is required")

        universe = _resolve_required_str(
            raw,
            meta,
            key="universe",
            meta_key="default_universe",
            ctx=factor_id,
        )
        data_freq = _resolve_required_str(
            raw,
            meta,
            key="data_freq",
            meta_key="default_data_freq",
            ctx=factor_id,
        )
        if data_freq not in ALLOWED_DATA_FREQ:
            raise ValueError(
                f"{factor_id}: data_freq must be one of {ALLOWED_DATA_FREQ}, "
                f"got {data_freq!r}"
            )

        engine = _resolve_required_str(
            raw,
            meta,
            key="engine",
            meta_key="default_engine",
            ctx=factor_id,
        )

        enabled_val = raw.get("enabled", True)
        enabled = bool(enabled_val)

        start_date = _normalize_date(raw.get("start_date"))
        end_date = _normalize_date(raw.get("end_date"))

        wf_raw = raw.get("wf_windows")
        if wf_raw is None:
            wf_seq: Sequence[int] = WF_ALLOWED_WINDOWS
        elif isinstance(wf_raw, Sequence) and not isinstance(wf_raw, (str, bytes)):
            wf_seq = [int(x) for x in wf_raw]
        else:
            raise ValueError(f"{factor_id}: wf_windows must be a list of integers or null")

        wf_windows = tuple(sorted(set(wf_seq)))
        invalid = [w for w in wf_windows if w not in WF_ALLOWED_WINDOWS]
        if invalid:
            raise ValueError(
                f"{factor_id}: wf_windows {invalid!r} not allowed; "
                f"allowed set = {WF_ALLOWED_WINDOWS}"
            )
        if not wf_windows:
            raise ValueError(f"{factor_id}: wf_windows cannot be empty")

        # Gate rules
        gr_raw = raw.get("gate_rules") or {}
        if not isinstance(gr_raw, Mapping):
            raise ValueError(f"{factor_id}: gate_rules must be a mapping if provided")
        gate_rules = GateRules.from_raw(gr_raw)

        # Governance / risk metadata（fallback 到 meta.default_*）
        owner = _resolve_optional_str(
            raw, meta, key="owner", meta_key="default_owner"
        )
        reviewed_by = _resolve_optional_str(
            raw, meta, key="reviewed_by", meta_key="default_reviewed_by"
        )
        last_reviewed = _resolve_optional_str(
            raw, meta, key="last_reviewed", meta_key="default_last_reviewed"
        )

        risk_family = _maybe_str(raw.get("risk_family"))
        neutralization = (
            raw.get("neutralization")
            if isinstance(raw.get("neutralization"), Mapping)
            else None
        )

        # Tags: normalise to tuple[str, ...]
        tags_raw = raw.get("tags")
        if tags_raw is None:
            tags: Tuple[str, ...] = tuple()
        elif isinstance(tags_raw, Sequence) and not isinstance(tags_raw, (str, bytes)):
            tags = tuple(
                s for s in (str(t).strip() for t in tags_raw) if s
            )
        else:
            single = str(tags_raw).strip()
            tags = (single,) if single else tuple()

        impl_notes = _maybe_str(raw.get("impl_notes"))

        # Extras = everything not explicitly modelled here
        known_top_keys = {
            "factor_id",
            "category",
            "description",
            "universe",
            "data_freq",
            "engine",
            "enabled",
            "start_date",
            "end_date",
            "wf_windows",
            "gate_rules",
            "owner",
            "reviewed_by",
            "last_reviewed",
            "risk_family",
            "neutralization",
            "tags",
            "impl_notes",
        }
        extras = {k: v for k, v in raw.items() if k not in known_top_keys}

        return cls(
            factor_id=factor_id,
            category=category,
            description=description,
            universe=universe,
            data_freq=data_freq,
            engine=engine,
            enabled=enabled,
            start_date=start_date,
            end_date=end_date,
            wf_windows=wf_windows,
            gate_rules=gate_rules,
            owner=owner,
            reviewed_by=reviewed_by,
            last_reviewed=last_reviewed,
            risk_family=risk_family,
            neutralization=neutralization,
            tags=tags,
            impl_notes=impl_notes,
            extras=extras,
        )


@dataclass(frozen=True)
class FactorRegistry:
    """
    In-memory registry of factor definitions loaded from rules_factors.yaml.

    這個物件刻意保持簡單且 deterministic，方便 cache / 序列化。
    """

    root: Path
    rules_path: Path
    meta: Mapping[str, Any]
    factors: Tuple[FactorConfig, ...]
    errors: Tuple[str, ...]
    # Raw YAML specs for each factor_id, used by factor_engine as `spec` arg.
    raw_specs: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    # Fast lookup map: factor_id -> FactorConfig
    by_id: Mapping[str, FactorConfig] = field(default_factory=dict)

    # API expected by factor_engine / factor_status
    def list_factor_ids(self, enabled_only: bool = True) -> List[str]:
        """Return factor_ids in deterministic order."""
        if enabled_only:
            return [f.factor_id for f in self.factors if f.enabled]
        return [f.factor_id for f in self.factors]

    def get_factor_config(self, factor_id: str) -> Optional[FactorConfig]:
        """Return FactorConfig for a given factor_id, or None if not present."""
        return self.by_id.get(factor_id)

    def get_factor_spec(self, factor_id: str) -> Optional[Mapping[str, Any]]:
        """
        Return raw spec mapping for a given factor_id.

        This is what factor_engine passes to compute_factor() as `spec`.
        """
        return self.raw_specs.get(factor_id)

    def list_by_category(
        self,
        category: str,
        enabled_only: bool = True,
    ) -> List[FactorConfig]:
        """Return factors matching a given category (case-insensitive)."""
        cat = category.lower().strip()
        result: List[FactorConfig] = []
        for f in self.factors:
            if enabled_only and not f.enabled:
                continue
            if f.category.lower().strip() == cat:
                result.append(f)
        return result

    def list_by_risk_family(
        self,
        risk_family: str,
        enabled_only: bool = True,
    ) -> List[FactorConfig]:
        """Return factors matching a given risk_family (case-insensitive)."""
        fam = risk_family.lower().strip()
        result: List[FactorConfig] = []
        for f in self.factors:
            if enabled_only and not f.enabled:
                continue
            if f.risk_family and f.risk_family.lower().strip() == fam:
                result.append(f)
        return result

    def list_by_tag(
        self,
        tag: str,
        enabled_only: bool = True,
    ) -> List[FactorConfig]:
        """Return factors that contain the given tag (case-insensitive)."""
        t = tag.lower().strip()
        result: List[FactorConfig] = []
        for f in self.factors:
            if enabled_only and not f.enabled:
                continue
            for ft in f.tags:
                if ft.lower().strip() == t:
                    result.append(f)
                    break
        return result

    def list_by_owner(
        self,
        owner: str,
        enabled_only: bool = True,
    ) -> List[FactorConfig]:
        """Return factors owned by the given owner (case-insensitive)."""
        o = owner.lower().strip()
        result: List[FactorConfig] = []
        for f in self.factors:
            if enabled_only and not f.enabled:
                continue
            if f.owner and f.owner.lower().strip() == o:
                result.append(f)
        return result


# ---------------------------------------------------------------------------
# YAML loading / registry 建立
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"rules_factors.yaml not found at {path}; "
            "ensure Phase-2 factor registry file exists."
        )

    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    if not isinstance(data, MutableMapping):
        raise ValueError("rules_factors.yaml root must be a mapping object")
    return data


def load_factor_registry(root: Path | str, rules_path: Path | str | None = None) -> FactorRegistry:
    """
    單一入口：從 rules_factors.yaml 載入並驗證 registry。

    其他模組一律呼叫這裡。
    """
    root_path = Path(root).resolve()
    if rules_path is None:
        rules_path = root_path / "rules_factors.yaml"
    rules_path = Path(rules_path).resolve()

    logger.debug("Loading factor registry from %s (root=%s)", rules_path, root_path)
    raw = _load_yaml(rules_path)

    meta = raw.get("meta") or {}
    if not isinstance(meta, Mapping):
        raise ValueError("meta section in rules_factors.yaml must be a mapping if present")

    factors_raw = raw.get("factors")
    if not isinstance(factors_raw, Sequence):
        raise ValueError("rules_factors.yaml must contain a 'factors' list")

    factors: List[FactorConfig] = []
    errors: List[str] = []
    raw_specs: Dict[str, Mapping[str, Any]] = {}
    by_id: Dict[str, FactorConfig] = {}
    seen_ids: set[str] = set()

    for idx, item in enumerate(factors_raw):
        if not isinstance(item, Mapping):
            errors.append(f"factors[{idx}] is not a mapping; got {type(item).__name__}")
            continue

        try:
            cfg = FactorConfig.from_raw(item, meta=meta)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"factor #{idx}: {exc}")
            continue

        if cfg.factor_id in seen_ids:
            errors.append(f"duplicate factor_id: {cfg.factor_id}")
            continue

        seen_ids.add(cfg.factor_id)
        factors.append(cfg)
        raw_specs[cfg.factor_id] = dict(item)
        by_id[cfg.factor_id] = cfg

    logger.info(
        "Loaded factor registry: %d factors (%d enabled), %d errors",
        len(factors),
        sum(1 for f in factors if f.enabled),
        len(errors),
    )

    return FactorRegistry(
        root=root_path,
        rules_path=rules_path,
        meta=meta,
        factors=tuple(factors),
        errors=tuple(errors),
        raw_specs=raw_specs,
        by_id=by_id,
    )


# Convenience helpers for other modules / debugging
def enabled_factors(registry: FactorRegistry) -> Iterable[FactorConfig]:
    """Yield enabled factor configs in deterministic order."""
    for f in registry.factors:
        if f.enabled:
            yield f


def registry_to_dict(registry: FactorRegistry) -> Mapping[str, Any]:
    """完整 dump，用在手動除錯／檢查用，不是給 PS1 用的 JSON 格式。"""
    return {
        "root": str(registry.root),
        "rules_path": str(registry.rules_path),
        "meta": dict(registry.meta),
        "errors": list(registry.errors),
        "factors": [
            {
                "factor_id": f.factor_id,
                "category": f.category,
                "description": f.description,
                "universe": f.universe,
                "data_freq": f.data_freq,
                "engine": f.engine,
                "enabled": f.enabled,
                "start_date": f.start_date,
                "end_date": f.end_date,
                "wf_windows": list(f.wf_windows),
                "owner": f.owner,
                "reviewed_by": f.reviewed_by,
                "last_reviewed": f.last_reviewed,
                "risk_family": f.risk_family,
                "neutralization": f.neutralization,
                "tags": list(f.tags),
                "impl_notes": f.impl_notes,
                "gate_rules": {
                    "min_rank_ic": f.gate_rules.min_rank_ic,
                    "max_turnover": f.gate_rules.max_turnover,
                    "max_corr": f.gate_rules.max_corr,
                    "min_coverage": f.gate_rules.min_coverage,
                    "extras": dict(f.gate_rules.extras),
                },
                "extras": dict(f.extras),
            }
            for f in registry.factors
        ],
    }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _maybe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _maybe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_date(val: Any) -> Optional[str]:
    """
    Normalise date fields to 'YYYY-MM-DD' string or None.

    我們刻意只保留字串，日期運算交給其他層處理。
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Minimal sanity check: YYYY-MM-DD length and dashes
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"invalid date format (expected YYYY-MM-DD): {s!r}")
    return s


def _resolve_required_str(
    raw: Mapping[str, Any],
    meta: Mapping[str, Any],
    key: str,
    meta_key: str,
    ctx: str,
) -> str:
    """
    Resolve a required string setting with optional meta default.

    如果 raw[key] 空白，就 fallback 到 meta[meta_key]；兩個都沒值就丟錯。
    """
    val = raw.get(key)
    s = str(val).strip() if val is not None else ""
    if not s:
        fallback = meta.get(meta_key)
        s = str(fallback).strip() if fallback is not None else ""
    if not s:
        raise ValueError(f"{ctx}: required field {key!r} missing and no meta.{meta_key} provided")
    return s


def _resolve_optional_str(
    raw: Mapping[str, Any],
    meta: Mapping[str, Any],
    key: str,
    meta_key: str,
) -> Optional[str]:
    """
    Resolve an optional string setting with optional meta default.

    raw[key] 空就用 meta[meta_key]；兩邊都沒值就回 None。
    """
    val = raw.get(key)
    s = str(val).strip() if val is not None else ""
    if s:
        return s
    fallback = meta.get(meta_key)
    if fallback is None:
        return None
    s = str(fallback).strip()
    return s or None


# ---------------------------------------------------------------------------
# Registry → JSON 視圖（給 PS1 用）
# ---------------------------------------------------------------------------


def _gate_rules_to_dict(gr: Any) -> Dict[str, Any]:
    """
    gate_rules 通用轉 dict：

    - dataclass → asdict()
    - dict → 補齊缺欄位
    - 其他 → 以屬性存取
    """
    if gr is None:
        return {
            "min_rank_ic": None,
            "max_turnover": None,
            "max_corr": None,
            "min_coverage": None,
            "extras": {},
        }

    if is_dataclass(gr):
        d = asdict(gr)
        d.setdefault("extras", {})
        return d

    if isinstance(gr, Mapping):
        d = dict(gr)
        d.setdefault("min_rank_ic", None)
        d.setdefault("max_turnover", None)
        d.setdefault("max_corr", None)
        d.setdefault("min_coverage", None)
        d.setdefault("extras", {})
        return d

    # fallback：當作一般物件讀屬性
    d = {
        "min_rank_ic": getattr(gr, "min_rank_ic", None),
        "max_turnover": getattr(gr, "max_turnover", None),
        "max_corr": getattr(gr, "max_corr", None),
        "min_coverage": getattr(gr, "min_coverage", None),
        "extras": getattr(gr, "extras", {}) or {},
    }
    return d


def _factor_config_to_dict(cfg: Any) -> Dict[str, Any]:
    """
    將單一 factor config 轉成 JSON safe dict（給 PS1 用的精簡版）。
    """
    factor_id = (
        getattr(cfg, "factor_id", None)
        or getattr(cfg, "id", None)
        or getattr(cfg, "name", None)
    )
    if not factor_id:
        raise ValueError("factor config missing factor_id / id / name")

    category = getattr(cfg, "category", None)
    engine = getattr(cfg, "engine", "classic")
    enabled = bool(getattr(cfg, "enabled", True))

    gate_rules_obj = getattr(cfg, "gate_rules", None)
    gate_rules = _gate_rules_to_dict(gate_rules_obj)

    return {
        "factor_id": str(factor_id),
        "category": category,
        "engine": str(engine) if engine is not None else "classic",
        "enabled": enabled,
        "gate_rules": gate_rules,
    }


def registry_to_jsonable(
    registry: Any,
    engine_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    將 registry 轉成 JSON safe dict，並依 engine_filter 篩選。

    engine_filter:
      - None        → 不過濾
      - "classic"   → 只留 engine=="classic"
      - "ai"        → 只留 engine=="ai"
      - "all"       → 不過濾（等同 None）
    """
    factors_obj = getattr(registry, "factors", [])
    errors_obj = getattr(registry, "errors", [])

    json_factors: List[Dict[str, Any]] = []
    for cfg in factors_obj:
        try:
            item = _factor_config_to_dict(cfg)
        except Exception:
            # 出現異常時，不讓整體失敗；把錯誤記進 errors
            msg = f"failed to serialize factor config: {cfg!r}"
            if isinstance(errors_obj, list):
                errors_obj.append(msg)
            continue

        if engine_filter and engine_filter not in ("all", "ALL"):
            if item.get("engine") != engine_filter:
                continue

        json_factors.append(item)

    # 確保 errors 是 list[str]；空 tuple / list 會變成 []
    if isinstance(errors_obj, (list, tuple)):
        errors = [str(e) for e in errors_obj]
    else:
        errors = [str(errors_obj)]

    return {
        "factors": json_factors,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor registry CLI (JSON view for PS1 tools)."
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root directory (default: current directory).",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        help="Path to rules_factors.yaml (default: <root>/rules_factors.yaml).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print registry as JSON to stdout (for PowerShell / automation).",
    )
    parser.add_argument(
        "--engine",
        type=str,
        default=None,
        choices=["classic", "ai", "all"],
        help="Optional engine filter for JSON output (classic/ai/all). Default: no filter.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


def configure_logging(level_name: str) -> logging.Logger:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(__name__)


def main(argv: Optional[Iterable[str]] = None) -> int:
    ns = parse_args(argv)
    cli_logger = configure_logging(ns.log_level)

    root = Path(ns.root).resolve()
    if not root.exists():
        cli_logger.error("root directory does not exist: %s", root)
        return 1

    if ns.rules:
        rules_path = Path(ns.rules).resolve()
    else:
        rules_path = root / "rules_factors.yaml"

    if not rules_path.exists():
        cli_logger.error("rules_factors.yaml not found: %s", rules_path)
        return 1

    try:
        registry = load_factor_registry(root=root, rules_path=rules_path)
    except Exception as exc:  # noqa: BLE001
        cli_logger.error(
            "load_factor_registry(root=%s, rules_path=%s) failed: %s",
            root,
            rules_path,
            exc,
        )
        return 1

    # 沒有要求 JSON：只印 log summary，不動 stdout，方便其他工具 pipe。
    if not ns.json:
        factor_count = len(getattr(registry, "factors", []))
        err_count = len(getattr(registry, "errors", []) or [])
        cli_logger.info(
            "Loaded factor registry from %s: %d factors, %d errors",
            rules_path,
            factor_count,
            err_count,
        )
        return 0

    # JSON 模式：stdout 只印一行 JSON，其餘訊息走 stderr(logging)
    engine_filter = ns.engine
    if engine_filter == "all":
        engine_filter = None

    payload = registry_to_jsonable(registry, engine_filter=engine_filter)
    text = json.dumps(payload, ensure_ascii=False)
    sys.stdout.write(text)
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

