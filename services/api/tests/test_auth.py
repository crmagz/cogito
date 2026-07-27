"""Authentication-boundary validation coverage."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from cogito_api.auth import ApprovalAuthenticator, Principal

from .conftest import make_settings


@pytest.mark.asyncio
async def test_oidc_rejects_non_string_roles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Malformed role claims must fail closed rather than raise a server error."""

    authenticator = ApprovalAuthenticator(
        make_settings(
            auth_mode="oidc",
            auth_oidc_issuer="https://issuer.example.test",
            auth_oidc_audience="cogito",
            auth_oidc_jwks_url="https://issuer.example.test/jwks",
        )
    )
    monkeypatch.setattr(
        authenticator,
        "_decode_oidc_token",
        lambda _: {
            "sub": "operator-1",
            "roles": ["cogito-viewer", {"name": "cogito-approver"}],
            "cogito_projects": ["default"],
        },
    )

    with pytest.raises(HTTPException) as error:
        await authenticator.authenticate("Bearer valid-token")

    assert error.value.status_code == 403
    assert error.value.detail == "operator is not authorized for a project scope"


def test_approver_authorization_error_is_operation_neutral() -> None:
    """Privilege failures must not imply the denied operation is plan approval."""

    authenticator = ApprovalAuthenticator(make_settings())

    with pytest.raises(HTTPException) as error:
        authenticator.require_approver(
            Principal(subject="viewer-1", projects=frozenset({"default"}), roles=frozenset({"cogito-viewer"}))
        )

    assert error.value.status_code == 403
    assert error.value.detail == "operator is not authorized to perform this operation"
