"""Run one execution command while retaining bounded streams for audit collection."""

from __future__ import annotations

import io
import subprocess
import sys
import threading
from pathlib import Path
from typing import BinaryIO

_AUDIT_OUTPUT_LIMIT_BYTES = 64 * 1024
_TRUNCATION_MARKER = b"\n[audit output truncated]\n"


def _binary_stream(stream: object) -> BinaryIO:
    return getattr(stream, "buffer", stream)


def _capture_stream(source: BinaryIO, destination: object, audit_path: Path) -> None:
    """Forward every byte to the exec client while storing a bounded audit copy."""

    existing = audit_path.read_bytes() if audit_path.exists() else b""
    retained = min(len(existing), _AUDIT_OUTPUT_LIMIT_BYTES)
    truncated = _TRUNCATION_MARKER in existing
    with audit_path.open("ab") as audit:
        while chunk := source.read(8192):
            output = _binary_stream(destination)
            output.write(chunk)
            output.flush()
            remaining = max(_AUDIT_OUTPUT_LIMIT_BYTES - retained, 0)
            if remaining:
                kept = chunk[:remaining]
                audit.write(kept)
                retained += len(kept)
            if len(chunk) > remaining and not truncated:
                audit.write(_TRUNCATION_MARKER)
                truncated = True


def main(arguments: list[str] | None = None) -> int:
    """Execute the command and let the pod entrypoint redact its retained output."""

    values = sys.argv[1:] if arguments is None else arguments
    if len(values) < 4 or values[2] != "--":
        raise ValueError("audit command requires an invocation, workspace, and command")
    invocation_id, workspace_root = values[:2]
    command = values[3:]
    audit_dir = Path(workspace_root) / ".cogito" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None and process.stderr is not None
    captures = [
        threading.Thread(target=_capture_stream, args=(process.stdout, sys.stdout, audit_dir / f"{invocation_id}.stdout.capture")),
        threading.Thread(target=_capture_stream, args=(process.stderr, sys.stderr, audit_dir / f"{invocation_id}.stderr.capture")),
    ]
    for capture in captures:
        capture.start()
    for capture in captures:
        capture.join()
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
