"""Unit coverage for the non-secret Kind LiteLLM provider preflight."""

from __future__ import annotations

import io
import json
import tarfile

import pytest

from tests.integration.test_kind_e2e_phase13 import (
    KindConfig,
    KindControlPlane,
    provider_secret_key_names,
    require_e2e_confirmation,
)


@pytest.mark.parametrize(
    ("provider", "expected"),
    [
        ("anthropic", ("ANTHROPIC_API_KEY",)),
        ("OPENAI", ("OPENAI_API_KEY",)),
        (
            "bedrock",
            ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION"),
        ),
    ],
)
def test_provider_secret_key_names_never_returns_secret_values(provider: str, expected: tuple[str, ...]) -> None:
    assert provider_secret_key_names(provider) == expected


@pytest.mark.parametrize("provider", ("", "anthropic/key", "anthropic key", "../../secret"))
def test_provider_secret_key_names_rejects_unsafe_provider_names(provider: str) -> None:
    with pytest.raises(ValueError, match="uppercase letters"):
        provider_secret_key_names(provider)


def test_e2e_confirmation_guard_skips_before_cluster_access(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COGITO_E2E_CONFIRM", raising=False)

    with pytest.raises(pytest.skip.Exception, match="COGITO_E2E_CONFIRM=1"):
        require_e2e_confirmation()


def test_kind_config_rejects_a_mutable_fixture_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_E2E_SPEC_REF", "kind-e2e@latest")

    with pytest.raises(pytest.fail.Exception, match="immutable"):
        KindConfig.load()


def test_kind_config_rejects_an_existing_run_without_a_resume_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COGITO_E2E_EXISTING_RUN_ID", "run-from-another-workflow")
    monkeypatch.delenv("COGITO_E2E_RESUME_EXISTING_RUN", raising=False)

    with pytest.raises(pytest.fail.Exception, match="refusing to approve an existing run"):
        KindConfig.load()


def test_kind_config_allows_an_existing_run_with_a_resume_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COGITO_E2E_EXISTING_RUN_ID", "run-from-another-workflow")
    monkeypatch.setenv("COGITO_E2E_RESUME_EXISTING_RUN", "1")

    assert KindConfig.load().existing_run_id == "run-from-another-workflow"


def test_provider_preflight_names_the_configured_secret_when_provider_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = KindControlPlane(
        KindConfig(
            context="kind-test",
            namespace="test",
            execution_namespace="test-executions",
            release="cogito",
            spec_ref=None,
            target_repo="https://github.com/example/fixture.git#" + "a" * 40,
            timeout=60,
            existing_run_id=None,
            values_file=None,
        )
    )
    values = {
        "litellm": {
            "enabled": False,
            "existingSecret": "cogito-litellm-credentials",
            "rolePolicies": {"planner": {"model": "balanced"}},
            "tiers": {"balanced": {}},
        }
    }
    commands: list[tuple[str, ...]] = []

    def command(*args: str) -> str:
        commands.append(args)
        return json.dumps(values)

    monkeypatch.setattr(control, "command", command)

    with pytest.raises(pytest.fail.Exception, match="cogito-litellm-credentials"):
        control.litellm_overrides()

    assert commands == [
        (
            "helm",
            "get",
            "values",
            "cogito",
            "--kube-context",
            "kind-test",
            "--namespace",
            "test",
            "--all",
            "--output",
            "json",
        )
    ]


def test_secret_key_presence_checks_describe_metadata_without_reading_data(monkeypatch: pytest.MonkeyPatch) -> None:
    control = KindControlPlane(
        KindConfig(
            context="kind-test",
            namespace="test",
            execution_namespace="test-executions",
            release="cogito",
            spec_ref="kind-e2e@v1#sha256=" + "a" * 64,
            target_repo="https://github.com/example/fixture.git#" + "a" * 40,
            timeout=60,
            existing_run_id=None,
            values_file=None,
        )
    )
    calls: list[tuple[str, ...]] = []

    def describe_secret(*args: str, **_: object) -> str:
        calls.append(args)
        return """Name:         cogito-litellm-secret
Data
====
ANTHROPIC_API_KEY: 32 bytes
"""

    monkeypatch.setattr(control, "kubectl", describe_secret)

    assert control.secret_key_present("cogito-litellm-secret", "ANTHROPIC_API_KEY")
    assert not control.secret_key_present("cogito-litellm-secret", "MISSING_API_KEY")
    assert all("describe" in call for call in calls)


def test_python_fixture_uploads_an_immutable_reference_without_provider_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = KindControlPlane(
        KindConfig(
            context="kind-test",
            namespace="test",
            execution_namespace="test-executions",
            release="cogito",
            spec_ref=None,
            target_repo="https://github.com/example/fixture.git#" + "a" * 40,
            timeout=60,
            existing_run_id=None,
            values_file=None,
        )
    )
    uploaded_archives: list[bytes] = []

    def kubectl(*args: str, input_text: str | bytes | None = None, **_: object) -> str:
        if args[2:4] == ("get", "pod"):
            return "cogito-minio-0"
        if args[2:5] == ("exec", "-i", "cogito-minio-0"):
            assert isinstance(input_text, bytes)
            uploaded_archives.append(input_text)
            return ""
        raise AssertionError(f"unexpected kubectl invocation: {args}")

    monkeypatch.setattr(control, "kubectl", kubectl)

    reference = control.ensure_immutable_spec_fixture()

    assert reference.startswith("kind-e2e@v1-")
    assert "#sha256=" in reference
    assert len(uploaded_archives) == 1
    with tarfile.open(fileobj=io.BytesIO(uploaded_archives[0]), mode="r:gz") as archive:
        assert sorted(archive.getnames()) == ["manifest.yaml", "rules", "rules/e2e.md"]
