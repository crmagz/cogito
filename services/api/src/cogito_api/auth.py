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
    """Authenticated identity and server-validated Workbench scope."""

    subject: str
    projects: frozenset[str]
    roles: frozenset[str]


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
            return Principal(
                subject=self._settings.auth_static_subject,
                projects=frozenset(self._settings.auth_static_projects),
                roles=frozenset(self._settings.auth_static_roles),
            )
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
        projects = claims.get(self._settings.auth_oidc_project_claim, [])
        if isinstance(projects, str):
            projects = [projects]
        if (
            not isinstance(subject, str)
            or not subject
            or not isinstance(roles, list)
            or not isinstance(projects, list)
            or not projects
            or not all(isinstance(role, str) and role.strip() for role in roles)
            or not all(isinstance(project, str) and project.strip() for project in projects)
        ):
            raise HTTPException(status_code=403, detail="operator is not authorized for a project scope")
        return Principal(subject=subject, projects=frozenset(projects), roles=frozenset(roles))

    def require_viewer(self, principal: Principal) -> None:
        if not (
            {
                self._settings.auth_oidc_viewer_role,
                self._settings.auth_oidc_approval_role,
                self._settings.auth_oidc_admin_role,
            }
            & principal.roles
        ):
            raise HTTPException(status_code=403, detail="operator is not authorized to view Workbench runs")

    def require_approver(self, principal: Principal) -> None:
        if not (
            {self._settings.auth_oidc_approval_role, self._settings.auth_oidc_admin_role} & principal.roles
        ):
            raise HTTPException(status_code=403, detail="operator is not authorized to perform this operation")

    def require_product_manager(self, principal: Principal) -> None:
        if not (
            {
                self._settings.auth_oidc_product_manager_role,
                self._settings.auth_oidc_approval_role,
                self._settings.auth_oidc_admin_role,
            }
            & principal.roles
        ):
            raise HTTPException(status_code=403, detail="operator is not authorized to submit a product specification")

    def require_workflow_approver(self, principal: Principal) -> None:
        if not (
            {
                self._settings.auth_oidc_workflow_approver_role,
                self._settings.auth_oidc_approval_role,
                self._settings.auth_oidc_admin_role,
            }
            & principal.roles
        ):
            raise HTTPException(status_code=403, detail="operator is not authorized to approve workflow gates")

    def require_policy_editor(self, principal: Principal) -> None:
        if not ({self._settings.auth_oidc_policy_editor_role, self._settings.auth_oidc_admin_role} & principal.roles):
            raise HTTPException(status_code=403, detail="operator is not authorized to edit workflow configuration")

    def require_policy_publisher(self, principal: Principal) -> None:
        if not ({self._settings.auth_oidc_policy_publisher_role, self._settings.auth_oidc_admin_role} & principal.roles):
            raise HTTPException(status_code=403, detail="operator is not authorized to publish workflow configuration")

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
