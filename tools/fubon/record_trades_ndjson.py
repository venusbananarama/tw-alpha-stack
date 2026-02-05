import argparse
import csv
import getpass
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from alpha_core.common.lockfile import FileLock, LockActiveError

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def _resolve_tz():
    if ZoneInfo is None:
        raise SystemExit("ERROR: zoneinfo not available in this Python.")
    try:
        return ZoneInfo("Asia/Taipei")
    except Exception as exc:
        raise SystemExit(f"ERROR: invalid tz 'Asia/Taipei': {exc}")


TZ = _resolve_tz()


SOURCE = "fubon_neo"


def _now_iso():
    return datetime.now(tz=TZ).isoformat(timespec="milliseconds")


def _read_text(prompt, env_key, arg_value):
    if arg_value:
        return arg_value.strip()
    env_value = os.environ.get(env_key)
    if env_value:
        return env_value.strip()
    return input(prompt).strip()


def _read_secret(prompt, echo, warned):
    if echo:
        if not warned[0]:
            print("WARNING: --echo reads secrets in plain text.", file=sys.stderr)
            warned[0] = True
        return input(prompt)
    return getpass.getpass(prompt)


def _safe_json(value):
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _normalize_key_part(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value).strip()


def _is_trade_item(item):
    return isinstance(item, dict) and "symbol" in item and "time" in item and "serial" in item


def _extract_trade_items(message):
    msg = message
    if isinstance(msg, (bytes, bytearray)):
        msg = msg.decode("utf-8", "replace")
    if isinstance(msg, str):
        try:
            msg = json.loads(msg)
        except json.JSONDecodeError:
            return []
    if isinstance(msg, dict):
        payload = msg.get("data", msg)
        if isinstance(payload, list):
            return [item for item in payload if _is_trade_item(item)]
        if _is_trade_item(payload):
            return [payload]
        return []
    return []


def _setup_logging(log_dir, symbol, level_name):
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now(tz=TZ).strftime("%Y-%m-%d")
    log_path = os.path.join(log_dir, f"record_{date_str}_{symbol}.log")
    try:
        with open(log_path, "a", encoding="utf-8") as handle:
            stamp = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S")
            handle.write(f"{stamp} INFO log_start path={log_path}\n")
    except OSError:
        pass
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(level)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    if sys.stdout is not None and hasattr(sys.stdout, "write"):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)
    logging.info("log_file=%s", log_path)
    try:
        file_handler.flush()
    except Exception:
        pass
    return log_path


def _mask_argv(argv):
    sensitive = {
        "--personal-id",
        "--cert-path",
        "--password",
        "--pwd",
        "--secret",
        "--token",
    }
    masked = []
    i = 0
    while i < len(argv):
        arg = str(argv[i])
        if arg in sensitive and i + 1 < len(argv):
            masked.extend([arg, "***"])
            i += 2
            continue
        matched = False
        for flag in sensitive:
            prefix = flag + "="
            if arg.startswith(prefix):
                masked.append(prefix + "***")
                matched = True
                break
        if matched:
            i += 1
            continue
        masked.append(arg)
        i += 1
    return masked


def _env_presence(keys):
    items = []
    for key in keys:
        present = bool(os.environ.get(key))
        items.append(f"{key}={int(present)}")
    return " ".join(items)


def _log_startup_header(args, symbol_hint):
    logging.info("startup sys.executable=%s", sys.executable)
    logging.info("startup cwd=%s", os.getcwd())
    logging.info("startup file=%s", os.path.abspath(__file__))
    logging.info("startup argv=%s", " ".join(_mask_argv(sys.argv)))
    logging.info("startup symbol_hint=%s", symbol_hint)
    logging.info("startup out_dir=%s out_dir_abs=%s", args.out_dir, os.path.abspath(args.out_dir))
    logging.info("startup log_dir=%s log_dir_abs=%s", args.log_dir, os.path.abspath(args.log_dir))
    logging.info(
        "startup trading_days_csv=%s trading_days_csv_abs=%s",
        args.trading_days_csv,
        os.path.abspath(args.trading_days_csv),
    )
    logging.info("startup use_keyring=%s", bool(args.use_keyring))
    logging.info(
        "startup env_present %s",
        _env_presence(
            [
                "FUBON_ID",
                "FUBON_CERT_PATH",
                "PYTHONPATH",
                "VIRTUAL_ENV",
                "CONDA_PREFIX",
                "PYTHONHOME",
            ]
        ),
    )


def _repo_root():
    return _REPO_ROOT


def _check_executable_drift():
    repo_root = _repo_root()
    expected_dir = os.path.join(repo_root, ".venv_trade", "Scripts")
    expected_norm = os.path.normcase(os.path.abspath(expected_dir))
    actual_norm = os.path.normcase(os.path.abspath(sys.executable))
    expected_prefix = expected_norm + os.sep
    if actual_norm.startswith(expected_prefix):
        logging.info(
            "executable_check=OK expected_prefix=%s actual=%s",
            expected_dir,
            sys.executable,
        )
        return
    logging.error(
        "executable_check=DRIFT expected_prefix=%s actual=%s reason_code=EXECUTABLE_DRIFT",
        expected_dir,
        sys.executable,
    )
    logging.error(
        "executable_fix=Check Task Scheduler Execute path for .venv_trade\\Scripts\\python.exe"
    )
    raise SystemExit(42)


def _resolve_value(arg_value, env_key):
    if arg_value:
        return arg_value.strip()
    env_value = os.environ.get(env_key)
    if env_value:
        return env_value.strip()
    return ""


def _find_latest_pfx(root_dir, personal_id):
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


def _normalize_date(date_str):
    if not date_str:
        return None
    text = str(date_str).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _load_trading_days(csv_path):
    if not os.path.isfile(csv_path):
        return None, f"trading_days_csv not found: {csv_path}"
    days = set()
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                head = row[0].strip()
                if not head or head.startswith("#"):
                    continue
                date_text = _normalize_date(head)
                if date_text:
                    days.add(date_text)
    except Exception as exc:
        return None, f"failed to read trading_days_csv: {exc}"
    return days, None


def _is_process_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _read_lock_info(lock_path):
    try:
        with open(lock_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _acquire_lock(lock_path, symbol):
    if os.path.exists(lock_path):
        info = _read_lock_info(lock_path)
        pid = info.get("pid") if isinstance(info, dict) else None
        if _is_process_alive(pid):
            logging.info("already_running symbol=%s pid=%s lock=%s", symbol, pid, lock_path)
            return False
        try:
            os.remove(lock_path)
            logging.info("removed stale lock: %s", lock_path)
        except OSError as exc:
            logging.error("failed to remove stale lock: %s error=%s", lock_path, exc)
            raise SystemExit(2)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        logging.info("already_running symbol=%s lock=%s", symbol, lock_path)
        return False
    except OSError as exc:
        logging.error("lock create failed: %s", exc)
        raise SystemExit(2)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            payload = {
                "pid": os.getpid(),
                "symbol": symbol,
                "started_at": _now_iso(),
                "argv": _mask_argv(sys.argv),
            }
            json.dump(payload, handle, ensure_ascii=True)
    except Exception as exc:
        logging.error("lock write failed: %s", exc)
        try:
            os.remove(lock_path)
        except OSError:
            pass
        raise SystemExit(2)
    logging.info("lock_acquired symbol=%s lock=%s", symbol, lock_path)
    return True


def _release_lock(lock_path):
    try:
        if lock_path and os.path.exists(lock_path):
            os.remove(lock_path)
            logging.info("lock_released lock=%s", lock_path)
    except OSError as exc:
        logging.warning("lock_release_failed lock=%s error=%s", lock_path, exc)


def _default_lock_path(symbol):
    return os.path.join("reports", "phase1", "locks", f"recorder_{symbol}.lock")


def _default_stop_token_path(symbol):
    return os.path.join("reports", "phase1", "signals", f"recorder_{symbol}.stop")


def _resolve_lock_path(arg_value, symbol):
    if arg_value:
        return str(arg_value).strip()
    return _default_lock_path(symbol)


def _resolve_stop_token_path(arg_value, symbol):
    if arg_value:
        return str(arg_value).strip()
    return _default_stop_token_path(symbol)


def _stop_token_exists(path):
    if not path:
        return False
    try:
        return os.path.exists(path)
    except Exception:
        return False


def _resolve_keyring_identity(personal_id, cert_path):
    personal_id = (personal_id or "").strip()
    cert_path = (cert_path or "").strip()

    if cert_path and not personal_id:
        personal_id = os.path.basename(os.path.dirname(cert_path))

    if not cert_path:
        cert_path = _find_latest_pfx(r"C:\CAFubon", personal_id)
        if not cert_path:
            return None, None, "No .pfx found under C:\\CAFubon."
        if not personal_id:
            personal_id = os.path.basename(os.path.dirname(cert_path))

    if not personal_id or not cert_path:
        return None, None, "Missing personal_id or cert_path."

    return personal_id, os.path.abspath(cert_path), None


def _load_secrets(personal_id, cert_path, use_keyring, echo):
    if use_keyring:
        try:
            import keyring
        except Exception as exc:
            logging.error("keyring import failed: %s", exc)
            raise SystemExit(3)
        password = keyring.get_password("fubon-neo", personal_id)
        cert_pwd = keyring.get_password("fubon-neo-cert", cert_path)
        if not password or not cert_pwd:
            missing = []
            if not password:
                missing.append("fubon-neo")
            if not cert_pwd:
                missing.append("fubon-neo-cert")
            logging.error("missing keyring entries: %s", ", ".join(missing))
            raise SystemExit(2)
        return password, cert_pwd
    warned = [False]
    password = _read_secret("Fubon password: ", echo, warned)
    cert_pwd = _read_secret("Cert password: ", echo, warned)
    return password, cert_pwd


class RotatingJSONLWriter:
    def __init__(self, out_dir, symbol, rotate_mode, max_mb):
        self._out_dir = out_dir
        self._symbol = symbol
        self._rotate_mode = rotate_mode
        self._max_bytes = int(max(0, max_mb) * 1024 * 1024)
        self._lock = threading.Lock()
        self._seq = 0
        self._file = None
        self._current_path = None
        self._bytes_written = 0
        self._date = None

    def _current_date(self):
        return datetime.now(tz=TZ).strftime("%Y-%m-%d")

    def _ensure_dir(self, date_str):
        path = os.path.join(self._out_dir, f"dt={date_str}")
        os.makedirs(path, exist_ok=True)
        return path

    def _open_new_file(self):
        if self._file:
            self._file.close()
        date_str = self._current_date()
        if self._date != date_str:
            self._date = date_str
            self._seq = 0
        self._seq += 1
        directory = self._ensure_dir(self._date)
        name = f"{self._symbol}.trades.{self._seq:04d}.jsonl"
        path = os.path.join(directory, name)
        self._file = open(path, "a", encoding="utf-8", newline="\n")
        self._current_path = path
        self._bytes_written = 0
        logging.info("jsonl output: %s", path)

    def _needs_rollover(self):
        if self._file is None:
            return True
        if self._date != self._current_date():
            return True
        if self._rotate_mode == "size" and self._max_bytes > 0:
            return self._bytes_written >= self._max_bytes
        return False

    def write_record(self, record):
        try:
            line = json.dumps(record, ensure_ascii=True)
        except TypeError:
            record["data"] = _safe_json(record.get("data"))
            line = json.dumps(record, ensure_ascii=True)
        with self._lock:
            if self._needs_rollover():
                self._open_new_file()
            self._file.write(line + "\n")
            self._file.flush()
            self._bytes_written += len(line) + 1

    def close(self):
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None

    def get_status(self):
        with self._lock:
            path = self._current_path
        size = 0
        if path and os.path.exists(path):
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
        return path, size


class TradeRecorder:
    def __init__(self, sdk, symbol, writer, status_interval, stop_token_path=None):
        self._sdk = sdk
        self._symbol = symbol
        self._writer = writer
        self._stock = sdk.marketdata.websocket_client.stock
        self._connected = False
        self._need_reconnect = True
        self._last_message_ts = time.monotonic()
        self._received_count = 0
        self._last_event_ts = None
        self._status_interval = max(1, int(status_interval))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._stop_token_path = stop_token_path

        self._stock.on("message", self._on_message)
        self._stock.on("connect", self._on_connect)
        self._stock.on("disconnect", self._on_disconnect)
        self._stock.on("error", self._on_error)

    def _on_message(self, message):
        now = time.monotonic()
        with self._lock:
            self._last_message_ts = now
        items = _extract_trade_items(message)
        if not items:
            return
        for item in items:
            symbol = _normalize_key_part(item.get("symbol"))
            time_key = _normalize_key_part(item.get("time"))
            serial_key = _normalize_key_part(item.get("serial"))
            if not symbol or not time_key or not serial_key:
                logging.warning("skip trade with missing key fields")
                continue
            ingest_ts = _now_iso()
            record = {
                "ingest_ts": ingest_ts,
                "source": SOURCE,
                "event": "trade",
                "symbol": symbol,
                "dedup_key": f"{symbol}|{time_key}|{serial_key}",
                "data": item,
            }
            self._writer.write_record(record)
            with self._lock:
                self._received_count += 1
                self._last_event_ts = item.get("time") or ingest_ts

    def _on_connect(self):
        logging.info("market data connected")
        with self._lock:
            self._connected = True
            self._need_reconnect = False
        try:
            self._stock.subscribe({"channel": "trades", "symbol": self._symbol})
        except Exception as exc:
            logging.error("subscribe failed for %s: %s", self._symbol, exc)

    def _on_disconnect(self, code, message):
        logging.warning("market data disconnect: %s, %s", code, message)
        with self._lock:
            self._connected = False
            self._need_reconnect = True

    def _on_error(self, error):
        logging.error("market data error: %s", error)
        with self._lock:
            self._need_reconnect = True

    def stop(self):
        self._stop.set()

    def run(self):
        backoff = 1
        max_backoff = 30
        idle_timeout = 120
        start_ts = time.monotonic()
        last_status_ts = start_ts - self._status_interval
        while not self._stop.is_set():
            if _stop_token_exists(self._stop_token_path):
                logging.info("stop_token_detected path=%s", self._stop_token_path)
                self._stop.set()
                try:
                    self._stock.disconnect()
                except Exception:
                    pass
                break
            now = time.monotonic()
            with self._lock:
                connected = self._connected
                need_reconnect = self._need_reconnect
                last_message_ts = self._last_message_ts

            if need_reconnect or not connected:
                try:
                    self._stock.disconnect()
                except Exception:
                    pass
                try:
                    self._stock.connect()
                except Exception as exc:
                    logging.error("connect failed: %s", exc)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
                time.sleep(1)
                backoff = 1
                if now - last_status_ts >= self._status_interval:
                    status_now = time.monotonic()
                    last_status_ts = self._log_status(status_now, start_ts)
                continue

            if (now - last_message_ts) >= idle_timeout:
                logging.warning("idle timeout reached, reconnecting")
                with self._lock:
                    self._need_reconnect = True

            if now - last_status_ts >= self._status_interval:
                status_now = time.monotonic()
                last_status_ts = self._log_status(status_now, start_ts)

            time.sleep(1)

    def _log_status(self, now, start_ts):
        with self._lock:
            connected = self._connected
            received_count = self._received_count
            last_event_ts = self._last_event_ts
        output_path, output_size = self._writer.get_status()
        status = "waiting_for_first_event" if received_count == 0 else "running"
        output_label = output_path if output_path else "pending"
        uptime_s = int(now - start_ts)
        logging.info(
            "status=%s connected=%s received_count=%d last_event_ts=%s output_file=%s "
            "output_size_bytes=%d uptime_s=%d",
            status,
            connected,
            received_count,
            last_event_ts,
            output_label,
            output_size,
            uptime_s,
        )
        return now


def _parse_args():
    parser = argparse.ArgumentParser(description="Record Fubon trades to NDJSON.")
    parser.add_argument("--personal-id", dest="personal_id", help="Fubon personal id.")
    parser.add_argument("--cert-path", dest="cert_path", help="Path to .pfx certificate.")
    parser.add_argument("--symbol", help="Symbol to subscribe.")
    parser.add_argument(
        "--out",
        dest="out_dir",
        default="datahub/bronze/fubon/trades",
        help="Output directory.",
    )
    parser.add_argument("--rotate", choices=["daily", "size"], default="daily", help="Rotate by daily or size.")
    parser.add_argument("--max-mb", type=float, default=256.0, help="Max file size (MB) when rotate=size.")
    parser.add_argument("--mode", choices=["Speed", "Normal"], default="Speed", help="Realtime mode.")
    parser.add_argument("--use-keyring", action="store_true", help="Load secrets from keyring (no prompts).")
    parser.add_argument("--status-interval", type=int, default=30, help="Status log interval (sec).")
    parser.add_argument("--log-dir", default="reports/fubon_recorder", help="Log directory.")
    parser.add_argument("--lock-path", default=None, help="Recorder lock path.")
    parser.add_argument("--stop-token-path", default=None, help="Stop token path.")
    parser.add_argument("--ttl-minutes", type=int, default=180, help="Lock TTL in minutes.")
    parser.add_argument("--only-trading-day", action="store_true", help="Exit 0 when not a trading day.")
    parser.add_argument(
        "--trading-days-csv",
        default="datahub/ref/trading_days.csv",
        help="CSV file containing trading days (YYYY-MM-DD in first column).",
    )
    parser.add_argument("--date", help="Override date (YYYY-MM-DD) for trading-day check.")
    parser.add_argument("--dry-run", action="store_true", help="Only evaluate trading-day check and exit.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    parser.add_argument(
        "--echo",
        action="store_true",
        help="Read secrets with echo (unsafe, local debug only).",
    )
    return parser.parse_args()


def _resolve_symbol(args):
    if args.symbol:
        return args.symbol.strip()
    env_symbol = os.environ.get("FUBON_SYMBOL")
    if env_symbol:
        return env_symbol.strip()
    return input("Symbol: ").strip()


def main():
    args = _parse_args()
    symbol_hint = _resolve_value(args.symbol, "FUBON_SYMBOL") or "unknown"
    _setup_logging(args.log_dir, symbol_hint, args.log_level)
    _log_startup_header(args, symbol_hint)
    _check_executable_drift()
    symbol = _resolve_symbol(args)
    if symbol and symbol != symbol_hint:
        logging.info("startup symbol_resolved=%s", symbol)

    check_date = _normalize_date(args.date) if args.date else None
    if args.date and not check_date:
        logging.error("invalid --date format, expected YYYY-MM-DD: %s", args.date)
        raise SystemExit(2)
    if args.only_trading_day or args.dry_run:
        days, err = _load_trading_days(args.trading_days_csv)
        if err:
            logging.error("%s", err)
            raise SystemExit(2)
        target_date = check_date or datetime.now(tz=TZ).strftime("%Y-%m-%d")
        is_trading_day = target_date in days
        logging.info("trading_days_loaded count=%d", len(days))
        logging.info("trading_day_check date=%s is_trading_day=%s", target_date, is_trading_day)
        if args.dry_run:
            logging.info("dry_run exit 0")
            raise SystemExit(0)
        if args.only_trading_day and not is_trading_day:
            logging.info("not_trading_day exit 0")
            raise SystemExit(0)

    if args.use_keyring:
        personal_id = _resolve_value(args.personal_id, "FUBON_ID")
        cert_path = _resolve_value(args.cert_path, "FUBON_CERT_PATH")
        personal_id, cert_path, err = _resolve_keyring_identity(personal_id, cert_path)
        if err:
            logging.error("identity resolution failed: %s", err)
            raise SystemExit(2)
    else:
        personal_id = _read_text("Fubon personal_id (login id): ", "FUBON_ID", args.personal_id)
        cert_path = _read_text("Cert .pfx path: ", "FUBON_CERT_PATH", args.cert_path)
        if not personal_id or not cert_path:
            logging.error("missing personal_id or cert_path")
            raise SystemExit(2)
        if not os.path.isfile(cert_path):
            logging.error("cert_path not found: %s", cert_path)
            raise SystemExit(2)

    if not symbol:
        logging.error("missing symbol")
        raise SystemExit(2)
    if "." in symbol:
        logging.warning("symbol contains '.', ensure it matches investable_universe.txt")

    lock_path = _resolve_lock_path(args.lock_path, symbol)
    stop_token_path = _resolve_stop_token_path(args.stop_token_path, symbol)
    logging.info("startup lock_path=%s lock_path_abs=%s", lock_path, os.path.abspath(lock_path))
    logging.info(
        "startup stop_token_path=%s stop_token_path_abs=%s",
        stop_token_path,
        os.path.abspath(stop_token_path),
    )
    logging.info("startup ttl_minutes=%s", int(args.ttl_minutes))

    lock = FileLock(lock_path, ttl_minutes=args.ttl_minutes, auto_break_stale=True)
    lock_acquired = False
    try:
        lock.acquire()
        lock_acquired = True
        logging.info("lock_acquired symbol=%s lock=%s", symbol, lock_path)
    except LockActiveError:
        logging.info("already_running symbol=%s lock=%s", symbol, lock_path)
        raise SystemExit(0)

    password, cert_pwd = _load_secrets(personal_id, cert_path, args.use_keyring, args.echo)

    try:
        from fubon_neo.sdk import FubonSDK, Mode
    except Exception as exc:
        logging.error("FubonSDK import failed: %s", exc)
        raise SystemExit(3)

    try:
        sdk = FubonSDK()
        res = sdk.login(personal_id, password, cert_path, cert_pwd)
        if not getattr(res, "is_success", False):
            logging.error("login failed: %s", getattr(res, "message", ""))
            raise SystemExit(4)

        mode = Mode.Speed if args.mode == "Speed" else Mode.Normal
        sdk.init_realtime(mode)

        writer = RotatingJSONLWriter(args.out_dir, symbol, args.rotate, args.max_mb)
        recorder = TradeRecorder(sdk, symbol, writer, args.status_interval, stop_token_path)

        try:
            recorder.run()
        except KeyboardInterrupt:
            logging.info("shutdown requested")
        finally:
            recorder.stop()
            writer.close()
    finally:
        if lock_acquired:
            lock.release()


if __name__ == "__main__":
    main()
