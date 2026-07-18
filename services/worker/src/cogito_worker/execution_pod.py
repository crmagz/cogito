from __future__ import annotations

import os
import signal
import time
from pathlib import Path


def main() -> None:
    """Initialize this execution pod's private workspace and await the harness."""

    workspace_root = Path(os.environ["COGITO_EXECUTION_WORKSPACE_ROOT"])
    idle_seconds = int(os.environ["COGITO_EXECUTION_IDLE_SECONDS"])
    workspace_root.mkdir(parents=True, exist_ok=True)

    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    deadline = time.monotonic() + idle_seconds
    while not stopping and time.monotonic() < deadline:
        time.sleep(1)


if __name__ == "__main__":
    main()
