import argparse
import json
import os
import sys
import traceback
import getpass
from datetime import datetime, timezone

EXIT_OK = 0
EXIT_LOGIN_FAILED = 10
EXIT_NOT_ENABLED = 20
EXIT_MISSING_ARGS = 30
EXIT_IMPORT_ERROR = 40
EXIT_EXCEPTION = 50

PFX_ROOT = r"C:\CAFubon"

# 用來判斷「尚未開通/未完成簽署」的常見訊息片段（依你實測訊息）
NOT_ENABLED_HINTS = [
    "無簽署完成API使用風險暨聲明書",
    "使用權限將應於次日開通",
    "請與營業員聯絡",
]

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def is_not_enabled_message(msg: str) -> bool:
    if not msg:
        return False
    return any(h in msg for h in NOT_ENABLED_HINTS)

def coerce_login_fields(res):
    """
    SDK 回傳形狀可能不同：盡量抽出 is_success/message。
    - 常見：res.is_success, res.message
    - 或 dict：res["is_success"], res["message"]
    """
    is_success = getattr(res, "is_success", None)
    message = getattr(res, "message", None)

    if isinstance(res, dict):
        if is_success is None:
            is_success = res.get("is_success")
        if message is None:
            message = res.get("message")

    return is_success, message

def print_result(result: dict, as_json: bool):
    print("login.is_success =", result["login"]["is_success"])
    print("login.message    =", result["login"]["message"])
    print("realtime.ok      =", result["realtime_init"]["ok"])
    print("reason_code      =", result["reason_code"])
    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))

def _find_latest_pfx(root_dir: str, personal_id: str | None) -> str | None:
    if not os.path.isdir(root_dir):
        return None
    latest_path = None
    latest_mtime = -1.0
    for dirpath, _, filenames in os.walk(root_dir):
        if personal_id and os.path.basename(dirpath) != personal_id:
            continue
        for name in filenames:
            if not name.lower().endswith(".pfx"):
                continue
            path = os.path.join(dirpath, name)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if mtime > latest_mtime:
                latest_mtime = mtime
                latest_path = path
    return latest_path

def _resolve_keyring_identity(personal_id: str, cert_path: str):
    personal_id = (personal_id or "").strip()
    cert_path = (cert_path or "").strip()

    if cert_path and not personal_id:
        personal_id = os.path.basename(os.path.dirname(cert_path))

    if not cert_path:
        cert_path = _find_latest_pfx(PFX_ROOT, personal_id)
        if not cert_path:
            return None, None, f"No .pfx found under {PFX_ROOT}."
        if not personal_id:
            personal_id = os.path.basename(os.path.dirname(cert_path))

    if not personal_id or not cert_path:
        return None, None, "Missing personal_id or cert_path."

    return personal_id, os.path.abspath(cert_path), None

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fubon Neo API login check (no secrets persisted).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--personal-id", default=os.environ.get("FUBON_ID", "").strip(),
                        help="Login personal id (prefer env:FUBON_ID).")
    parser.add_argument("--cert-path", default=os.environ.get("FUBON_CERT_PATH", "").strip(),
                        help="Path to .pfx (prefer env:FUBON_CERT_PATH).")
    parser.add_argument("--mode", choices=["Speed", "Normal"], default="Speed",
                        help="Realtime mode for init_realtime(). Only attempted when login success.")
    parser.add_argument("--echo", action="store_true",
                        help="DANGEROUS: echo passwords to terminal input (local debug only).")
    parser.add_argument("--use-keyring", action="store_true",
                        help="Load secrets from keyring (no prompts).")
    parser.add_argument("--json", action="store_true",
                        help="Also output machine-readable JSON.")
    args = parser.parse_args()

    personal_id = args.personal_id
    cert_path = args.cert_path
    if args.use_keyring:
        personal_id, cert_path, err = _resolve_keyring_identity(personal_id, cert_path)
        if err:
            print(err)
            return EXIT_MISSING_ARGS
    else:
        if not personal_id or not cert_path:
            print("Missing required args: --personal-id and/or --cert-path (or env:FUBON_ID/env:FUBON_CERT_PATH).")
            return EXIT_MISSING_ARGS

    try:
        from fubon_neo.sdk import FubonSDK, Mode
    except Exception as e:
        print("ImportError: cannot import fubon_neo.sdk. Ensure you are using .venv_trade and fubon-neo is installed.")
        print(f"error={repr(e)}")
        return EXIT_IMPORT_ERROR

    # 重要：getpass 不回顯是正常的（看起來「黑的」），直接輸入/貼上後按 Enter 即可
    if args.use_keyring:
        try:
            import keyring
        except Exception as e:
            print(f"Keyring import failed: {type(e).__name__}: {e}")
            return EXIT_EXCEPTION
        password = keyring.get_password("fubon-neo", personal_id)
        cert_pwd = keyring.get_password("fubon-neo-cert", cert_path)
        if not password or not cert_pwd:
            missing = []
            if not password:
                missing.append("fubon-neo")
            if not cert_pwd:
                missing.append("fubon-neo-cert")
            print(f"Missing keyring entries: {', '.join(missing)}")
            return EXIT_MISSING_ARGS
    elif args.echo:
        password = input("Fubon password (ECHO ON): ")
        cert_pwd = input("Cert password (ECHO ON): ")
    else:
        password = getpass.getpass("Fubon password (hidden): ")
        cert_pwd = getpass.getpass("Cert password (hidden): ")

    result = {
        "ts_utc": now_iso_utc(),
        "personal_id": personal_id,
        "cert_path": cert_path,
        "login": {"is_success": None, "message": None},
        "realtime_init": {"attempted": False, "ok": False, "error": None, "mode": args.mode},
        "reason_code": None,
    }

    try:
        sdk = FubonSDK()
        # 官方 Quick Start：accounts = sdk.login(personal_id, password, cert_path, cert_pwd)
        res = sdk.login(personal_id, password, cert_path, cert_pwd)

        is_success, message = coerce_login_fields(res)
        result["login"]["is_success"] = is_success
        result["login"]["message"] = message

        if is_success is False:
            if is_not_enabled_message(message or ""):
                result["reason_code"] = "NOT_ENABLED"
                print_result(result, args.json)
                return EXIT_NOT_ENABLED
            result["reason_code"] = "LOGIN_FAILED"
            print_result(result, args.json)
            return EXIT_LOGIN_FAILED

        # 只有在 login 明確成功時才嘗試 init_realtime（避免你之前遇到「尚未登入」的 ValueError）
        if is_success is True:
            result["realtime_init"]["attempted"] = True
            m = Mode.Speed if args.mode == "Speed" else Mode.Normal
            try:
                sdk.init_realtime(m)
                result["realtime_init"]["ok"] = True
                result["reason_code"] = "OK"
                print_result(result, args.json)
                return EXIT_OK
            except Exception as e:
                result["realtime_init"]["error"] = f"{type(e).__name__}: {e}"
                result["reason_code"] = "REALTIME_INIT_FAILED"
                print_result(result, args.json)
                return EXIT_EXCEPTION

        # is_success 抽不到（None）時：保守回報，不硬闖 init_realtime
        result["reason_code"] = "UNKNOWN_LOGIN_SHAPE"
        print_result(result, args.json)
        return EXIT_EXCEPTION

    except SystemExit:
        raise
    except Exception as e:
        result["reason_code"] = "EXCEPTION"
        result["realtime_init"]["error"] = f"{type(e).__name__}: {e}"
        print_result(result, args.json)
        print("\n--- traceback (for diagnosis) ---")
        traceback.print_exc()
        return EXIT_EXCEPTION

if __name__ == "__main__":
    sys.exit(main())
