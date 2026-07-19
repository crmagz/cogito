from __future__ import annotations

import pytest

from cogito_api.config import load_settings


def test_production_rejects_static_operator_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("COGITO_AUTH_MODE", "static")

    with pytest.raises(ValueError, match="require COGITO_AUTH_MODE=oidc"):
        load_settings()


def test_oidc_requires_complete_verifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("COGITO_AUTH_MODE", "oidc")
    monkeypatch.delenv("COGITO_AUTH_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("COGITO_AUTH_OIDC_AUDIENCE", raising=False)
    monkeypatch.delenv("COGITO_AUTH_OIDC_JWKS_URL", raising=False)

    with pytest.raises(ValueError, match="requires issuer, audience, and JWKS URL"):
        load_settings()


def test_development_static_auth_remains_available_for_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("COGITO_AUTH_MODE", "static")

    assert load_settings().auth_mode == "static"
