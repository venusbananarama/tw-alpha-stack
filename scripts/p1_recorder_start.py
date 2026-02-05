from __future__ import annotations

import sys


def main() -> int:
    print(
        "p1_recorder_start.py is deprecated. Use tools/fubon/record_trades_ndjson.py directly.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
