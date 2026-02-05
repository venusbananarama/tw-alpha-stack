from __future__ import annotations

"""
alpha_core.config

SSOT config helpers for the tw-alpha-stack project.

Responsibilities:
- Load and validate:
  - rules.yaml（dataset-level 規則）
  - investable_universe.txt
- 建立「因子技術視圖」FactorDefinition：
  - **不直接讀 rules_factors.yaml**
  - 透過 scripts.factor_registry.load_factor_registry() 取得 raw_specs
    → rules_factors.yaml 的唯一 parser 還是 scripts/factor_registry.py

設計目標：
- deterministic
- idempotent
- schema-first 但對小幅度變動具容錯
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml  # rules.yaml 是核心 SSOT，這裡要求一定要有 PyYAML

# 注意：只在「因子視圖」這邊 import 本尊，不反向 import alpha_core，以避免循環引用
try:
    from scripts.factor_registry import load_factor_registry  # type: ignore[import]
except Exception:  # noqa: BLE001
    # 允許在某些工具 / 測試環境下沒 scripts 模組時載不進來，
    # 真正用到 load_factor_definitions 時再丟錯。
    load_factor_registry = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConfigError(RuntimeError):
    """Raised when configuration files are missing or inconsistent."""


# ---------------------------------------------------------------------------
# Dataset-level configs (rules.yaml)
# ---------------------------------------------------------------------------


@dataclass
class DatasetConfig:
    """
    Dataset configuration, derived from rules.yaml.

    Attributes:
        name: logical dataset name, e.g. "prices", "chip"
        path_pattern: glob pattern or directory template for parquet files
        partition_type: "date" or "yyyymm"
        extra: free-form dictionary for additional fields (freshness rules, schema, etc.)
    """

    name: str
    path_pattern: str
    partition_type: str
    extra: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Factor-level technical view（engine 用）
# ---------------------------------------------------------------------------


@dataclass
class GateRule:
    """
    Per-factor gate rules, derived from rules_factors.yaml.gate_rules.

    這是「技術視圖」版，重點放在 engine / Gate 會用到的數值欄位；
    治理層（owner, tags...）交給 factor_registry 那邊處理。
    """

    min_rank_ic: Optional[float] = None
    min_psr: Optional[float] = None
    max_turnover: Optional[float] = None
    max_corr: Optional[float] = None
    min_coverage: Optional[float] = None
    max_maxdd: Optional[float] = None
    min_t_value: Optional[float] = None
    min_dsr: Optional[float] = None
    max_replay_mae_bps: Optional[float] = None
    min_replay_match: Optional[float] = None

    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FactorDefinition:
    """
    Factor definition: 技術 / engine 視圖。

    注意：
    - 治理欄位（owner / tags / universe 等）仍由 scripts.factor_registry.FactorConfig 負責。
    - 這裡只放「計算 & Gate」需要的資訊。
    """

    factor_id: str
    category: str
    engine: str
    inputs: List[str]
    label: Optional[str]
    horizon: Optional[str]
    frequency: Optional[str]
    start_date: Optional[date]
    wf_windows: List[int]
    gate_rules: GateRule
    params: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


# ---------------------------------------------------------------------------
# 共用小工具
# ---------------------------------------------------------------------------


def _ensure_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    return p


def _parse_date_maybe(value: Any) -> Optional[date]:
    """
    Try to parse a date from a value.
    - If already datetime.date, return as-is.
    - If string 'YYYY-MM-DD', parse.
    - Otherwise, return None.
    """
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ConfigError(f"Invalid date string in config: {value!r}") from exc
    return None


def _to_float_maybe(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Expected a float-like value, got {value!r}") from exc


def _to_int_list(value: Any, field_name: str) -> List[int]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError(f"{field_name} must be a list of ints, got {type(value).__name__}")
    result: List[int] = []
    for v in value:
        try:
            result.append(int(v))
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"{field_name} must contain integer-like values, got {v!r}"
            ) from exc
    return result


def _to_bool_maybe(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "f", "no", "n", "off"}:
            return False
    raise ConfigError(f"{field_name} must be a boolean-like value, got {value!r}")


def _load_yaml(path: str | Path) -> Any:
    """
    Generic YAML loader，現在只用在 rules.yaml，
    不再用來直接解析 rules_factors.yaml（因子 YAML 的唯一入口是 factor_registry）。
    """
    p = _ensure_path(path)
    try:
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as exc:  # type: ignore[attr-defined]
        raise ConfigError(f"Failed to parse YAML file: {p}") from exc


# ---------------------------------------------------------------------------
# Public API: rules.yaml（資料集層）
# ---------------------------------------------------------------------------


def load_rules(path: str | Path) -> Mapping[str, Any]:
    """
    Load rules.yaml and return the raw mapping.

    這邊只做最小驗證；結構化存取請用其他 helper。
    """
    data = _load_yaml(path)
    if not isinstance(data, Mapping):
        raise ConfigError(f"rules.yaml must be a mapping at top-level, got {type(data).__name__}")
    return data


def load_dataset_configs(rules: Mapping[str, Any]) -> Dict[str, DatasetConfig]:
    """
    Extract dataset configurations from rules.yaml.

    支援兩種結構：

    1) datasets:
         prices:
           path_pattern: "datahub/silver/alpha/prices/yyyymm=YYYYMM/data.parquet"
           partition_type: "yyyymm"
           freshness: {...}
         chip:
           ...

    2) datasets:
         - name: "prices"
           path_pattern: "..."
           partition_type: "yyyymm"
         - name: "chip"
           ...

    若 partition_type 缺省則預設 "date"。
    """
    datasets_node = rules.get("datasets")
    if datasets_node is None:
        raise ConfigError("rules.yaml is missing 'datasets' section")

    result: Dict[str, DatasetConfig] = {}

    # Pattern 1: mapping from name -> config
    if isinstance(datasets_node, Mapping):
        for name, cfg in datasets_node.items():
            if isinstance(cfg, str):
                # simplest form: value is path_pattern
                dc = DatasetConfig(
                    name=name,
                    path_pattern=cfg,
                    partition_type="date",
                    extra={},
                )
            elif isinstance(cfg, Mapping):
                path_pattern = str(cfg.get("path_pattern") or cfg.get("path") or "")
                if not path_pattern:
                    raise ConfigError(
                        f"Dataset {name!r} is missing 'path_pattern' in rules.yaml"
                    )
                partition_type = str(cfg.get("partition_type") or "date")
                extra = {
                    k: v
                    for k, v in cfg.items()
                    if k not in {"path_pattern", "path", "partition_type"}
                }
                dc = DatasetConfig(
                    name=name,
                    path_pattern=path_pattern,
                    partition_type=partition_type,
                    extra=extra,
                )
            else:
                raise ConfigError(
                    f"Unexpected dataset config type for {name!r}: {type(cfg).__name__}"
                )

            result[name] = dc

    # Pattern 2: list of dataset entries `{name: ..., path_pattern: ..., ...}`
    elif isinstance(datasets_node, list):
        for item in datasets_node:
            if not isinstance(item, Mapping):
                raise ConfigError(
                    f"Dataset entry must be a mapping, got {type(item).__name__}"
                )
            name = str(item.get("name") or "").strip()
            if not name:
                raise ConfigError("Dataset entry is missing 'name'")
            path_pattern = str(
                item.get("path_pattern") or item.get("path") or ""
            ).strip()
            if not path_pattern:
                raise ConfigError(
                    f"Dataset {name!r} is missing 'path_pattern' in rules.yaml"
                )
            partition_type = str(item.get("partition_type") or "date")
            extra = {
                k: v
                for k, v in item.items()
                if k not in {"name", "path_pattern", "path", "partition_type"}
            }
            result[name] = DatasetConfig(
                name=name,
                path_pattern=path_pattern,
                partition_type=partition_type,
                extra=extra,
            )
    else:
        raise ConfigError(
            f"'datasets' must be a mapping or list, got {type(datasets_node).__name__}"
        )

    return result


# ---------------------------------------------------------------------------
# Public API: investable_universe.txt
# ---------------------------------------------------------------------------


def load_investable_universe(path: str | Path) -> List[str]:
    """
    Load investable_universe.txt and return a list of stock_id strings.

    Rules:
    - Each non-empty line is trimmed and treated as one stock_id.
    - Lines starting with '#' are treated as comments and ignored.
    """
    p = _ensure_path(path)
    universe: List[str] = []
    with p.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            universe.append(line)
    return universe


# ---------------------------------------------------------------------------
# Public API: FactorDefinition（透過 factor_registry raw_specs）
# ---------------------------------------------------------------------------


def _parse_gate_rules(node: Mapping[str, Any] | None) -> GateRule:
    if node is None:
        return GateRule()

    if not isinstance(node, Mapping):
        raise ConfigError(f"gate_rules must be a mapping, got {type(node).__name__}")

    known_keys = {
        "min_rank_ic",
        "min_psr",
        "max_turnover",
        "max_corr",
        "min_coverage",
        "max_maxdd",
        "min_t_value",
        "min_dsr",
        "max_replay_mae_bps",
        "min_replay_match",
    }

    kwargs: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for k, v in node.items():
        if k in known_keys:
            kwargs[k] = _to_float_maybe(v)
        else:
            extra[k] = v

    kwargs["extra"] = extra
    return GateRule(**kwargs)


def _parse_factor_definition(raw: Mapping[str, Any]) -> FactorDefinition:
    """
    將 rules_factors.yaml 單一 factor 的 raw spec 轉成技術視圖 FactorDefinition。

    注意：這裡的 raw 來源是 factor_registry.FactorRegistry.raw_specs[factor_id]，
    alpha_core 不直接讀 YAML。
    """
    if not isinstance(raw, Mapping):
        raise ConfigError(
            f"Factor definition must be a mapping, got {type(raw).__name__}"
        )

    fid = str(raw.get("factor_id") or raw.get("id") or "").strip()
    if not fid:
        raise ConfigError("Factor definition is missing 'factor_id'")

    category = str(raw.get("category") or "").strip() or "unknown"
    engine = str(raw.get("engine") or "classic").strip()

    inputs_raw = raw.get("inputs") or raw.get("features")
    if inputs_raw is None:
        inputs: List[str] = []
    elif isinstance(inputs_raw, list):
        inputs = [str(x).strip() for x in inputs_raw if str(x).strip()]
    else:
        raise ConfigError(
            f"Factor {fid!r}: 'inputs' or 'features' must be a list, got {type(inputs_raw).__name__}"
        )

    label = raw.get("label")
    if label is not None:
        label = str(label).strip() or None

    horizon = raw.get("horizon")
    if horizon is not None:
        horizon = str(horizon).strip() or None

    frequency = raw.get("frequency")
    if frequency is not None:
        frequency = str(frequency).strip() or None

    start_date_val = raw.get("start_date")
    start_date = _parse_date_maybe(start_date_val)

    wf_windows_val = raw.get("wf_windows")
    wf_windows = _to_int_list(wf_windows_val, "wf_windows")

    gate_rules_node = raw.get("gate_rules")
    gate_rules = _parse_gate_rules(gate_rules_node)

    params = raw.get("params") or {}
    if not isinstance(params, Mapping):
        raise ConfigError(
            f"Factor {fid!r}: 'params' must be a mapping, got {type(params).__name__}"
        )
    params_dict: Dict[str, Any] = dict(params)

    meta = raw.get("meta") or {}
    if not isinstance(meta, Mapping):
        raise ConfigError(
            f"Factor {fid!r}: 'meta' must be a mapping, got {type(meta).__name__}"
        )
    meta_dict: Dict[str, Any] = dict(meta)

    if "enabled" in raw:
        enabled = _to_bool_maybe(raw.get("enabled"), f"Factor {fid!r}: enabled")
    elif "enabled" in meta_dict:
        enabled = _to_bool_maybe(meta_dict.get("enabled"), f"Factor {fid!r}: meta.enabled")
    else:
        enabled = True

    return FactorDefinition(
        factor_id=fid,
        category=category,
        engine=engine,
        inputs=inputs,
        label=label,
        horizon=horizon,
        frequency=frequency,
        start_date=start_date,
        wf_windows=wf_windows,
        gate_rules=gate_rules,
        params=params_dict,
        meta=meta_dict,
        enabled=enabled,
    )


def validate_factor_definition(fd: FactorDefinition) -> None:
    """
    Basic consistency checks on a FactorDefinition.
    """
    if not fd.factor_id:
        raise ConfigError("FactorDefinition.factor_id must not be empty")

    if not fd.engine:
        raise ConfigError(f"Factor {fd.factor_id!r}: engine must not be empty")

    if fd.wf_windows:
        for m in fd.wf_windows:
            if m <= 0:
                raise ConfigError(
                    f"Factor {fd.factor_id!r}: wf_windows must be positive integers, got {fd.wf_windows!r}"
                )


def load_factor_definitions(
    root: str | Path,
    rules_path: str | Path | None = None,
) -> Dict[str, FactorDefinition]:
    """
    從「本尊」 factor_registry 取得 raw_specs，轉成 engine 用的 FactorDefinition。

    重要：這是 **唯一建議使用的因子技術視圖入口**，
    alpha_core 不會再自己直接讀 rules_factors.yaml。
    """
    if load_factor_registry is None:
        raise ConfigError(
            "scripts.factor_registry.load_factor_registry is not available; "
            "make sure 'scripts' is importable when calling load_factor_definitions()."
        )

    root_path = Path(root).resolve()
    rules: Path | str | None = rules_path
    if rules is not None:
        rules = Path(rules).resolve()

    registry = load_factor_registry(root=root_path, rules_path=rules)  # type: ignore[arg-type]

    result: Dict[str, FactorDefinition] = {}
    raw_specs = getattr(registry, "raw_specs", {}) or {}
    if not isinstance(raw_specs, Mapping):
        raise ConfigError("FactorRegistry.raw_specs must be a mapping")

    for fid, spec in raw_specs.items():
        fd = _parse_factor_definition(spec)
        validate_factor_definition(fd)
        result[fid] = fd

    return result
