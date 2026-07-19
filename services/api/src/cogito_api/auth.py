"""Authentication boundary for human approval decisions."""

from __future__ import annotations

import asyncio
import hmac
from dataclasses import dataclass

import jwt
from fastapi import HTTPException

from .config import Settings


@dataclass(frozen=True)
class Principal:
    """Authenticated operator identity used in immutable approval records."""

    subject: str


class ApprovalAuthenticator:
    """Validates a development static token or production OIDC bearer token."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._jwks = jwt.PyJWKClient(settings.auth_oidc_jwks_url) if settings.auth_oidc_jwks_url else None

    async def authenticate(self, authorization: str | None) -> Principal:
        """Return an approval principal or fail closed."""

        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Bearer authentication is required")
        token = authorization.removeprefix("Bearer ")
        if self._settings.auth_mode == "static":
            if not self._settings.auth_static_token or not hmac.compare_digest(token, self._settings.auth_static_token):
                raise HTTPException(status_code=401, detail="invalid development operator token")
            return Principal(subject=self._settings.auth_static_subject)
        if self._settings.auth_mode != "oidc" or self._jwks is None:
            raise HTTPException(status_code=503, detail="approval authentication is not configured")
        try:
            claims = await asyncio.to_thread(self._decode_oidc_token, token)
        except jwt.PyJWTError as error:
            raise HTTPException(status_code=401, detail="invalid OIDC bearer token") from error
        subject = claims.get("sub")
        roles = claims.get(self._settings.auth_oidc_role_claim, [])
        if isinstance(roles, str):
            roles = [roles]
        if not isinstance(subject, str) or not subject or self._settings.auth_oidc_approval_role not in roles:
            raise HTTPException(status_code=403, detail="operator is not authorized to approve plans")
        return Principal(subject=subject)

    def _decode_oidc_token(self, token: str) -> dict:
        assert self._jwks is not None
        signing_key = self._jwks.get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=[signing_key.algorithm_name],
            audience=self._settings.auth_oidc_audience,
            issuer=self._settings.auth_oidc_issuer,
        )
