# alpha_core/__init__.py
"""
alpha_core v0

Common utilities for the tw-alpha-stack project:
- Date utilities (W-FRI, half-open intervals).
- Config loaders for SSOT (rules.yaml, rules_factors.yaml).
- IO helpers for parquet / checkpoints / ledgers.
- Factor engine + evaluation + WF composer (Phase-2 and Gate).

This package is designed to be:
- deterministic
- idempotent
- side-effect minimal (no global state, no environment mutation)
"""

__version__ = "0.1.0"

__all__ = [
    "get_version",
    # 子模組名稱先列在這裡，等你逐步實作：
    "dates",
    "config",
    "io",
    "factor_engine",
    "eval_engine",
    "wf_compose",
]


def get_version() -> str:
    """
    Return the alpha_core package version string.
    """
    return __version__
