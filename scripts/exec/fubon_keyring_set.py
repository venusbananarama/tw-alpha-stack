from __future__ import annotations

import argparse
import getpass
import sys
from typing import List

SERVICE = "alphacity_fubon"
USER_PASSWORD = "FUBON_PASSWORD"
USER_PFX_PASSWORD = "FUBON_PFX_PASSWORD"

EXIT_OK = 0
EXIT_KEYRING_MISSING = 62
EXIT_EXCEPTION = 63


def _load_keyring():
    try:
        import keyring  # type: ignore
    except Exception:
        return None
    return keyring


def _targets(only: str | None) -> List[str]:
    if only == "password":
        return [USER_PASSWORD]
    if only == "pfx":
        return [USER_PFX_PASSWORD]
    return [USER_PASSWORD, USER_PFX_PASSWORD]


def parse_args():
    ap = argparse.ArgumentParser(
        description="Set/check Fubon secrets in Windows Credential Manager (keyring).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--clear", action="store_true", help="Delete both keyring entries.")
    group.add_argument("--check", action="store_true", help="Check whether entries exist (prints True/False).")
    ap.add_argument("--only", choices=["password", "pfx"], default=None, help="Only handle one secret.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    keyring = _load_keyring()
    if keyring is None:
        print("KEYRING_NOT_AVAILABLE: pip install keyring")
        return EXIT_KEYRING_MISSING

    try:
        if args.check:
            names = _targets(args.only)
            ok = True
            for name in names:
                if not keyring.get_password(SERVICE, name):
                    ok = False
                    break
            print("True" if ok else "False")
            return EXIT_OK

        if args.clear:
            names = _targets(None)
            for name in names:
                try:
                    keyring.delete_password(SERVICE, name)
                except Exception:
                    pass
            print("CLEARED")
            return EXIT_OK

        names = _targets(args.only)
        for name in names:
            prompt = f"{name}: "
            value = getpass.getpass(prompt)
            keyring.set_password(SERVICE, name, value)
        print("OK")
        return EXIT_OK
    except Exception:
        return EXIT_EXCEPTION


if __name__ == "__main__":
    sys.exit(main())
