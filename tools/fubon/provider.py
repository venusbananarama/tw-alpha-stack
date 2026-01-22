from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


# ---------- helpers ----------

KEYRING_SERVICE = "alphacity_fubon"
_LAST_LOGIN_FAIL_TS: Optional[float] = None


def _require_env(name: str, fallback: Optional[str]) -> str:
    v = (fallback or "").strip() or (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"FUBON_CONFIG_MISSING: {name} is required")
    return v


def _bool_env(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _sanitize_text(s: str) -> str:
    lower = s.lower()
    if "token" in lower or "password" in lower or "secret" in lower:
        return "[REDACTED]"
    return s


def _summary_text(v: Any, *, limit: int = 160) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v)
    except Exception:
        return None
    s = s.replace("\r", " ").replace("\n", " ")
    s = _sanitize_text(s)
    if len(s) > limit:
        s = s[:limit] + "..."
    return s


def _get_login_cooldown_secs() -> float:
    raw = (os.getenv("FUBON_LOGIN_COOLDOWN_SECS") or "").strip()
    if not raw:
        return 30.0
    try:
        v = float(raw)
    except Exception:
        return 30.0
    return max(0.0, v)


def _cooldown_remaining() -> float:
    if _LAST_LOGIN_FAIL_TS is None:
        return 0.0
    cooldown = _get_login_cooldown_secs()
    if cooldown <= 0:
        return 0.0
    elapsed = time.monotonic() - _LAST_LOGIN_FAIL_TS
    remaining = cooldown - elapsed
    return remaining if remaining > 0 else 0.0


def _mark_login_fail() -> None:
    global _LAST_LOGIN_FAIL_TS
    _LAST_LOGIN_FAIL_TS = time.monotonic()


def _get_keyring_password(service: str, username: str) -> Optional[str]:
    try:
        import keyring  # type: ignore
    except Exception:
        raise RuntimeError("FUBON_KEYRING_NOT_AVAILABLE: pip install keyring")

    try:
        return keyring.get_password(service, username)
    except Exception as e:
        msg = _summary_text(e)
        detail = f"{type(e).__name__}: {msg}" if msg else type(e).__name__
        raise RuntimeError(f"FUBON_KEYRING_READ_FAIL: {detail}")


def _resolve_secret(
    name: str,
    fallback: Optional[str],
    *,
    use_keyring: bool,
    keyring_user: str,
) -> str:
    v = (fallback or "").strip() or (os.getenv(name) or "").strip()
    if v:
        return v
    if use_keyring:
        v = _get_keyring_password(KEYRING_SERVICE, keyring_user)
        if v:
            return v
        raise RuntimeError(
            f"FUBON_CONFIG_MISSING: {name} is required (env missing; keyring empty). "
            "Run scripts/exec/fubon_keyring_set.py or set env."
        )
    raise RuntimeError(f"FUBON_CONFIG_MISSING: {name} is required")


def _to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    # common SDK response objects
    if hasattr(x, "to_dict") and callable(getattr(x, "to_dict")):
        try:
            return x.to_dict()
        except Exception:
            pass
    if hasattr(x, "__dict__"):
        return dict(x.__dict__)
    return {"value": x}


def _get_attr_or_key(obj: Any, keys: List[str]) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
        return None
    for k in keys:
        if hasattr(obj, k):
            v = getattr(obj, k)
            if v is not None:
                return v
    return None


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return None


@dataclass
class FubonContext:
    sdk: Any
    account: Any
    closed: bool = False


def _login_response_hint(accounts_obj: Any) -> Dict[str, Any]:
    """Extract non-sensitive hints from login response for debugging."""
    hint: Dict[str, Any] = {}
    try:
        hint["type"] = type(accounts_obj).__name__
        # common fields in SDK responses
        hint["is_success"] = _get_attr_or_key(accounts_obj, ["is_success", "success", "ok", "status"])
        hint["code"] = _summary_text(_get_attr_or_key(accounts_obj, ["code", "error_code", "status_code"]))
        hint["message"] = _summary_text(_get_attr_or_key(accounts_obj, ["message", "msg", "error", "err", "error_message"]))
        data = _get_attr_or_key(accounts_obj, ["data", "Data", "accounts", "Accounts"])
        if isinstance(data, list):
            hint["data_len"] = len(data)
        else:
            hint["data_len"] = 0 if data is None else 1
    except Exception as e:
        hint["hint_error"] = repr(e)
    return hint


def _pick_account(
    accounts_obj: Any,
    *,
    account_no: Optional[str] = None,
    account_index: Optional[int] = None,
) -> Any:
    """
    Pick an account from login response.
    Priority:
      1) match account_no
      2) account_index
      3) first
    """
    data = _get_attr_or_key(accounts_obj, ["data", "Data", "accounts", "Accounts"])
    if data is None and isinstance(accounts_obj, list):
        data = accounts_obj

    if not isinstance(data, list) or not data:
        raise RuntimeError(f"FUBON_LOGIN_NO_ACCOUNT: accounts is empty. hint={_login_response_hint(accounts_obj)}")

    # 1) account_no match
    if account_no:
        target = str(account_no).strip()
        for a in data:
            d = _to_dict(a)
            cand = _get_attr_or_key(d, ["account", "account_no", "accountNo", "account_id", "id"])
            if cand is not None and str(cand).strip() == target:
                return a
        raise RuntimeError(
            f"FUBON_ACCOUNT_NOT_FOUND: account_no={target}. available={len(data)}. hint={_login_response_hint(accounts_obj)}"
        )

    # 2) index
    if account_index is not None:
        idx = int(account_index)
        if idx < 0 or idx >= len(data):
            raise RuntimeError(
                f"FUBON_ACCOUNT_INDEX_OOR: index={idx} size={len(data)}. hint={_login_response_hint(accounts_obj)}"
            )
        return data[idx]

    # 3) default first
    return data[0]


# ---------- Provider contract ----------

def connect(
    *,
    user_id: Optional[str] = None,
    password: Optional[str] = None,
    pfx_path: Optional[str] = None,
    pfx_password: Optional[str] = None,
    account_no: Optional[str] = None,
    account_index: Optional[int] = None,
    use_keyring: Optional[bool] = None,
    **_: Any,
) -> FubonContext:
    """
    Connect to Fubon Neo SDK (read-only use).
    Env defaults:
      FUBON_USER_ID, FUBON_PASSWORD, FUBON_PFX_PATH, FUBON_PFX_PASSWORD
      FUBON_ACCOUNT_NO (optional), FUBON_ACCOUNT_INDEX (optional)
    Keyring (optional):
      FUBON_USE_KEYRING=1, service=alphacity_fubon, usernames=FUBON_PASSWORD/FUBON_PFX_PASSWORD
    """
    try:
        from fubon_neo.sdk import FubonSDK  # type: ignore
    except Exception as e:
        raise RuntimeError(f"FUBON_SDK_MISSING: cannot import fubon_neo.sdk ({e})")

    remaining = _cooldown_remaining()
    if remaining > 0:
        wait = int(math.ceil(remaining))
        print(f"FUBON_LOGIN_COOLDOWN: wait={wait}")
        raise RuntimeError("FUBON_LOGIN_COOLDOWN")

    uid = _require_env("FUBON_USER_ID", user_id)
    pfx = _require_env("FUBON_PFX_PATH", pfx_path)
    allow_keyring = (use_keyring is not False) and _bool_env("FUBON_USE_KEYRING")
    pwd = _resolve_secret("FUBON_PASSWORD", password, use_keyring=allow_keyring, keyring_user="FUBON_PASSWORD")
    pfx_pwd = _resolve_secret("FUBON_PFX_PASSWORD", pfx_password, use_keyring=allow_keyring, keyring_user="FUBON_PFX_PASSWORD")

    # account selection from env
    env_no = (os.getenv("FUBON_ACCOUNT_NO") or "").strip()
    env_idx = (os.getenv("FUBON_ACCOUNT_INDEX") or "").strip()
    if not account_no and env_no:
        account_no = env_no
    if account_index is None and env_idx:
        try:
            account_index = int(env_idx)
        except Exception:
            raise RuntimeError(f"FUBON_ACCOUNT_INDEX_INVALID: {env_idx!r}")

    try:
        sdk = FubonSDK()
        accounts = sdk.login(uid, pwd, pfx, pfx_pwd)
    except Exception as e:
        _mark_login_fail()
        msg = _summary_text(e)
        detail = f"type={type(e).__name__} message={msg}" if msg else f"type={type(e).__name__}"
        raise RuntimeError(f"FUBON_CONNECT_FAIL: {detail}") from e

    hint = _login_response_hint(accounts)
    if hint.get("is_success") is False or (hint.get("data_len") == 0 and hint.get("is_success") is not True):
        _mark_login_fail()
        raise RuntimeError(f"FUBON_LOGIN_FAILED: hint={hint}")

    account = _pick_account(accounts, account_no=account_no, account_index=account_index)

    return FubonContext(sdk=sdk, account=account, closed=False)


def fetch_positions(
    ctx: FubonContext,
    *,
    mark_price: Optional[float] = None,
    **_: Any,
) -> List[Dict[str, Any]]:
    """
    Returns normalized positions list:
      [{symbol, qty, avg_cost, market_price, market_value, raw}, ...]
    """
    if ctx is None or ctx.closed:
        raise RuntimeError("FUBON_NOT_CONNECTED")

    sdk = ctx.sdk
    acct = ctx.account

    try:
        resp = sdk.accounting.inventories(acct)  # fubon_neo API
    except Exception as e:
        raise RuntimeError(f"FUBON_FETCH_POSITIONS_FAIL: {e!r}")

    items = _get_attr_or_key(resp, ["data", "Data", "items", "Items"]) or []
    if not isinstance(items, list):
        items = []

    out: List[Dict[str, Any]] = []
    for it in items:
        d = _to_dict(it)

        symbol = _get_attr_or_key(d, ["symbol", "stockNo", "stock_no", "stock", "code", "stk_no"])
        qty = _get_attr_or_key(d, ["qty", "quantity", "shares", "share", "qty_share", "inventoryQty", "inventory_qty"])
        avg_cost = _get_attr_or_key(d, ["avg_price", "avgPrice", "cost", "cost_price", "costPrice", "averageCost"])

        mkt_price = _get_attr_or_key(d, ["market_price", "marketPrice", "last_price", "lastPrice", "price"])
        q = _safe_int(qty) or 0

        ap = _safe_float(avg_cost)
        mp = _safe_float(mkt_price)
        if mp is None:
            mp = _safe_float(mark_price)

        mv = None
        if mp is not None:
            mv = float(mp) * float(q)

        out.append(
            {
                "symbol": str(symbol) if symbol is not None else "",
                "qty": q,
                "avg_cost": ap,
                "market_price": mp,
                "market_value": mv,
                "raw": d,
            }
        )
    return out


def fetch_account(
    ctx: FubonContext,
    **_: Any,
) -> Dict[str, Any]:
    """
    Returns normalized account dict:
      {cash, buying_power, raw}
    """
    if ctx is None or ctx.closed:
        raise RuntimeError("FUBON_NOT_CONNECTED")

    sdk = ctx.sdk
    acct = ctx.account

    try:
        resp = sdk.accounting.bank_remain(acct)  # fubon_neo API
    except Exception as e:
        raise RuntimeError(f"FUBON_FETCH_ACCOUNT_FAIL: {e!r}")

    d = _to_dict(resp)
    data = _get_attr_or_key(resp, ["data", "Data"]) or _get_attr_or_key(d, ["data", "Data"]) or d

    # best-effort mapping for cash/buying power
    cash = _safe_float(_get_attr_or_key(data, ["cash", "available_balance", "availableBalance", "balance", "remain", "bank_remain"]))
    if cash is None:
        cash = 0.0

    buying_power = _safe_float(_get_attr_or_key(data, ["buying_power", "buyingPower", "available_buying_power", "availableBuyingPower"]))
    if buying_power is None:
        buying_power = cash

    return {"cash": float(cash), "buying_power": float(buying_power), "raw": _to_dict(data)}


def close(ctx: Optional[FubonContext], **_: Any) -> None:
    """
    Idempotent close.
    """
    if ctx is None:
        return
    if getattr(ctx, "closed", False):
        return
    try:
        # some SDKs have logout/close; best-effort
        sdk = ctx.sdk
        if hasattr(sdk, "logout") and callable(getattr(sdk, "logout")):
            try:
                sdk.logout()
            except Exception:
                pass
        if hasattr(sdk, "close") and callable(getattr(sdk, "close")):
            try:
                sdk.close()
            except Exception:
                pass
    finally:
        ctx.closed = True
