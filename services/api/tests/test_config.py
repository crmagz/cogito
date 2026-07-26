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


def test_enabled_notifications_require_a_signing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_NOTIFICATION_ENABLED", "true")
    monkeypatch.setenv("COGITO_NOTIFICATION_WEBHOOK_URL", "https://receiver.example.test/events")
    monkeypatch.delenv("COGITO_NOTIFICATION_WEBHOOK_HMAC_SECRET", raising=False)

    with pytest.raises(ValueError, match="HMAC_SECRET is required"):
        load_settings()


def test_production_notifications_require_https(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("COGITO_AUTH_MODE", "oidc")
    monkeypatch.setenv("COGITO_AUTH_OIDC_ISSUER", "https://issuer.example.test")
    monkeypatch.setenv("COGITO_AUTH_OIDC_AUDIENCE", "cogito")
    monkeypatch.setenv("COGITO_AUTH_OIDC_JWKS_URL", "https://issuer.example.test/jwks")
    monkeypatch.setenv("COGITO_NOTIFICATION_ENABLED", "true")
    monkeypatch.setenv("COGITO_NOTIFICATION_WEBHOOK_URL", "http://receiver.example.test/events")
    monkeypatch.setenv("COGITO_NOTIFICATION_WEBHOOK_HMAC_SECRET", "test-secret")

    with pytest.raises(ValueError, match="must use HTTPS"):
        load_settings()
