from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, runtime_checkable, Union

import pandas as pd


@runtime_checkable
class FubonProvider(Protocol):
    def connect(self, **kwargs) -> Any: ...
    def fetch_positions(self, client: Any, **kwargs) -> Union[pd.DataFrame, list[dict]]: ...
    def fetch_account(self, client: Any, **kwargs) -> Dict[str, Any]: ...
    def close(self, client: Any) -> None: ...


@dataclass
class FubonAdapterConfig:
    provider_module: Optional[str] = None
    profile: Optional[str] = None
    account_id: Optional[str] = None
    use_keyring: bool = True
    timeout_s: int = 30


class FubonBrokerAdapter:
    """
    Read-only broker adapter.
    - No order placement.
    - Only positions/account snapshot.
    """

    def __init__(self, cfg: Optional[FubonAdapterConfig] = None) -> None:
        self.cfg = cfg or FubonAdapterConfig()
        self._provider: Optional[FubonProvider] = None
        self._client: Any = None
        self._connected: bool = False

    @staticmethod
    def _load_provider(module_name: str) -> FubonProvider:
        mod = importlib.import_module(module_name)

        # Validate required callables exist
        required = ["connect", "fetch_positions", "fetch_account", "close"]
        missing = [fn for fn in required if not hasattr(mod, fn)]
        if missing:
            raise RuntimeError(
                f"FUBON_PROVIDER_INVALID: module='{module_name}' missing callables={missing}"
            )
        return mod  # type: ignore[return-value]

    def connect(self) -> None:
        if self._connected:
            return

        provider_module = (
            self.cfg.provider_module
            or os.environ.get("FUBON_PROVIDER_MODULE")
            or None
        )

        # Auto candidates (best-effort). You can add more without changing schema/runner.
        candidates = [m for m in [
            provider_module,
            "tools.fubon.provider",
            "tools.fubon.fubon_provider",
            "tools.fubon",
        ] if m]

        last_err: Optional[Exception] = None
        for m in candidates:
            try:
                self._provider = self._load_provider(m)
                break
            except Exception as e:
                last_err = e
                self._provider = None

        if not self._provider:
            raise RuntimeError(
                "FUBON_PROVIDER_NOT_FOUND: "
                f"tried={candidates}. last_error={repr(last_err)}"
            )

        # Connect via provider
        try:
            self._client = self._provider.connect(
                profile=self.cfg.profile,
                account_id=self.cfg.account_id,
                use_keyring=self.cfg.use_keyring,
                timeout_s=self.cfg.timeout_s,
            )
        except Exception as e:
            raise RuntimeError(f"FUBON_CONNECT_FAILED: {e!r}") from e

        self._connected = True

    def close(self) -> None:
        # Idempotent close
        if not self._provider or not self._connected:
            self._connected = False
            self._client = None
            return

        try:
            self._provider.close(self._client)
        except Exception:
            # close must be best-effort; never explode shutdown path
            pass
        finally:
            self._connected = False
            self._client = None

    def fetch_positions(self, **kwargs) -> pd.DataFrame:
        if not self._connected or not self._provider:
            raise RuntimeError("FUBON_NOT_CONNECTED")

        out = self._provider.fetch_positions(self._client, **kwargs)
        if isinstance(out, pd.DataFrame):
            return out.copy()

        if isinstance(out, list):
            return pd.DataFrame(out)

        raise RuntimeError(f"FUBON_POSITIONS_INVALID_TYPE: {type(out)}")

    def fetch_account(self, **kwargs) -> Dict[str, Any]:
        if not self._connected or not self._provider:
            raise RuntimeError("FUBON_NOT_CONNECTED")

        out = self._provider.fetch_account(self._client, **kwargs)
        if not isinstance(out, dict):
            raise RuntimeError(f"FUBON_ACCOUNT_INVALID_TYPE: {type(out)}")
        return dict(out)
