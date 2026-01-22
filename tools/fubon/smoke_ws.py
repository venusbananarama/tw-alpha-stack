import os
import time
import getpass
from fubon_neo.sdk import FubonSDK, Mode

def main():
    personal_id = os.environ.get("FUBON_ID") or input("Fubon personal_id (login id): ").strip()
    cert_path   = os.environ.get("FUBON_CERT_PATH") or input("Cert .pfx path: ").strip()
    symbol      = os.environ.get("FUBON_SYMBOL", "2330").strip()

    # 輸入不回顯是正常的：直接打字/貼上，按 Enter
    password = getpass.getpass("Fubon password (hidden): ")
    cert_pwd = getpass.getpass("Cert password (hidden): ")

    sdk = FubonSDK()
    res = sdk.login(personal_id, password, cert_path, cert_pwd)

    print("login.is_success =", getattr(res, "is_success", None))
    print("login.message    =", getattr(res, "message", None))

    if not getattr(res, "is_success", False):
        raise SystemExit("Login failed. Stop before init_realtime().")

    sdk.init_realtime(Mode.Speed)

    def handle_message(message): print(message)
    def handle_connect(): print("market data connected")
    def handle_disconnect(code, message): print(f"market data disconnect: {code}, {message}")
    def handle_error(error): print(f"market data error: {error}")

    stock = sdk.marketdata.websocket_client.stock
    stock.on("message", handle_message)
    stock.on("connect", handle_connect)
    stock.on("disconnect", handle_disconnect)
    stock.on("error", handle_error)

    stock.connect()
    stock.subscribe({"channel": "trades", "symbol": symbol})

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
