from __future__ import annotations

import os
import re
import signal
import sys
import time
from pathlib import Path


_AUDIT_LOG_FILE = re.compile(r"^(?P<invocation_id>[a-f0-9]{64})\.(?P<stream>stdout|stderr)\.[A-Za-z0-9]+$")
_SENSITIVE_VALUE = re.compile(
    r"(?i)(?:authorization[\"']?\s*[:=]\s*[\"']?(?:[a-z][a-z0-9_-]*\s+)?|bearer\s+|(?:api[_ -]?key|access[_ -]?key|secret(?:[_ -]?key)?|token|password)[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+"
)


def _emit_audit_output(audit_dir: Path, offsets: dict[Path, int]) -> None:
    """Copy appended invocation output to pod stdout/stderr with redaction."""

    for path in sorted(audit_dir.glob("*.*")):
        match = _AUDIT_LOG_FILE.fullmatch(path.name)
        if match is None:
            continue
        offset = offsets.get(path, 0)
        try:
            with path.open("rb") as handle:
                handle.seek(offset)
                data = handle.read()
        except FileNotFoundError:
            continue
        if not data:
            continue
        offsets[path] = offset + len(data)
        stream = sys.stderr if match.group("stream") == "stderr" else sys.stdout
        invocation_id = match.group("invocation_id")
        for line in data.decode("utf-8", errors="replace").splitlines(keepends=True):
            stream.write(f"{invocation_id} {_SENSITIVE_VALUE.sub('[REDACTED]', line)}")
        stream.flush()


def main() -> None:
    """Initialize this execution pod's private workspace and await the harness."""

    workspace_root = Path(os.environ["COGITO_EXECUTION_WORKSPACE_ROOT"])
    idle_seconds = int(os.environ["COGITO_EXECUTION_IDLE_SECONDS"])
    workspace_root.mkdir(parents=True, exist_ok=True)
    audit_dir = workspace_root / ".cogito" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_offsets: dict[Path, int] = {}

    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    deadline = time.monotonic() + idle_seconds
    while not stopping and time.monotonic() < deadline:
        _emit_audit_output(audit_dir, audit_offsets)
        time.sleep(1)
    _emit_audit_output(audit_dir, audit_offsets)


if __name__ == "__main__":
    main()
