from __future__ import annotations

from pathlib import Path

from cogito_worker.execution_pod import _emit_audit_output


def test_audit_output_redacts_all_authorization_schemes(tmp_path: Path, capsys) -> None:
    invocation_id = "a" * 64
    audit_dir = tmp_path / ".cogito" / "audit"
    audit_dir.mkdir(parents=True)
    (audit_dir / f"{invocation_id}.stdout.capture").write_text(
        "Authorization: Basic dXNlcjpwYXNz\nAuthorization=Bearer gateway-token\n"
        '{"authorization":"Bearer json-token"}\nBearer standalone-token\n',
        encoding="utf-8",
    )

    _emit_audit_output(audit_dir, {})

    output = capsys.readouterr().out
    assert "dXNlcjpwYXNz" not in output
    assert "gateway-token" not in output
    assert "json-token" not in output
    assert "standalone-token" not in output
    assert "[REDACTED]" in output
