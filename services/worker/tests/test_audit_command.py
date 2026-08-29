from __future__ import annotations

import io
from pathlib import Path

import pytest

from cogito_worker.audit_command import _AUDIT_OUTPUT_LIMIT_BYTES, _TRUNCATION_MARKER, _capture_stream, main


def test_capture_stream_retains_a_bounded_audit_copy_without_truncating_exec_output(tmp_path: Path) -> None:
    source_bytes = b"x" * (_AUDIT_OUTPUT_LIMIT_BYTES + 100)
    destination = io.BytesIO()
    audit_path = tmp_path / "audit-output"

    _capture_stream(io.BytesIO(source_bytes), destination, audit_path)

    assert destination.getvalue() == source_bytes
    assert audit_path.read_bytes().startswith(b"x" * _AUDIT_OUTPUT_LIMIT_BYTES)
    assert audit_path.read_bytes().endswith(_TRUNCATION_MARKER)
    assert audit_path.stat().st_size <= _AUDIT_OUTPUT_LIMIT_BYTES + len(_TRUNCATION_MARKER)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["../invalid", "/workspace", "--", "echo"], "invocation identifier"),
        (["a" * 64, "workspace", "--", "echo"], "workspace root"),
    ],
)
def test_audit_command_rejects_untrusted_output_paths(arguments: list[str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        main(arguments)
