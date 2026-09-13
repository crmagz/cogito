"""Run one execution command while retaining bounded streams for audit collection."""

from __future__ import annotations

import io
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import BinaryIO

_AUDIT_OUTPUT_LIMIT_BYTES = 64 * 1024
_TRUNCATION_MARKER = b"\n[audit output truncated]\n"
_AUDIT_INVOCATION_ID = re.compile(r"^[a-f0-9]{64}$")
_SENSITIVE_VALUE = re.compile(
    r"(?i)(?:authorization[\"']?\s*[:=]\s*[\"']?(?:[a-z][a-z0-9_-]*\s+)?|bearer\s+|(?:api[_ -]?key|access[_ -]?key|secret(?:[_ -]?key)?|token|password)[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+"
)


def _binary_stream(stream: object) -> BinaryIO:
    return getattr(stream, "buffer", stream)


def _capture_stream(source: BinaryIO, destination: object, audit_path: Path) -> None:
    """Forward every byte to the exec client while storing a bounded audit copy."""

    existing = audit_path.read_bytes() if audit_path.exists() else b""
    retained = min(len(existing), _AUDIT_OUTPUT_LIMIT_BYTES)
    truncated = _TRUNCATION_MARKER in existing
    wrote_audit_output = False
    retained_ends_with_newline = existing.endswith(b"\n")
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
                wrote_audit_output = True
                retained_ends_with_newline = kept.endswith(b"\n")
            if len(chunk) > remaining and not truncated:
                audit.write(_TRUNCATION_MARKER)
                truncated = True
        if wrote_audit_output and not truncated and not retained_ends_with_newline:
            # Finalize a stream record so the polling collector never has to
            # emit an unterminated credential fragment.
            audit.write(b"\n")


def _emit_to_pod_log(invocation_id: str, audit_paths: list[Path]) -> None:
    """Write the completed bounded stream directly to PID 1's log FD.

    Kubernetes exec output is not container stdout.  Relying only on the
    execution pod's one-second file poll races workspace deletion, which makes
    short agent invocations invisible to Loki.  PID 1 remains the canonical
    container logger and receives a complete, redacted record before exec
    returns to the worker.
    """

    try:
        destination = Path("/proc/1/fd/1").open("w", encoding="utf-8", errors="replace")
    except OSError:
        return
    with destination:
        for audit_path in audit_paths:
            try:
                content = audit_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for line in content.splitlines():
                destination.write(f"{invocation_id} {_SENSITIVE_VALUE.sub('[REDACTED]', line)}\n")
        destination.flush()


def main(arguments: list[str] | None = None) -> int:
    """Execute the command and let the pod entrypoint redact its retained output."""

    values = sys.argv[1:] if arguments is None else arguments
    if len(values) < 4 or values[2] != "--":
        raise ValueError("audit command requires an invocation, workspace, and command")
    invocation_id, workspace_root = values[:2]
    if not _AUDIT_INVOCATION_ID.fullmatch(invocation_id):
        raise ValueError("audit command invocation identifier is invalid")
    if not workspace_root or not Path(workspace_root).is_absolute():
        raise ValueError("audit command workspace root must be an absolute path")
    command = values[3:]
    audit_dir = Path(workspace_root) / ".cogito" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None and process.stderr is not None
    audit_paths = [
        audit_dir / f"{invocation_id}.stdout.capture",
        audit_dir / f"{invocation_id}.stderr.capture",
    ]
    captures = [
        threading.Thread(target=_capture_stream, args=(process.stdout, sys.stdout, audit_paths[0])),
        threading.Thread(target=_capture_stream, args=(process.stderr, sys.stderr, audit_paths[1])),
    ]
    for capture in captures:
        capture.start()
    for capture in captures:
        capture.join()
    _emit_to_pod_log(invocation_id, audit_paths)
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
