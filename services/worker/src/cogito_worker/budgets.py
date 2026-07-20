"""Run-scoped LiteLLM key and Kubernetes Secret lifecycle management."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Protocol
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class RunBudget:
    """Immutable limits used to mint one execution-only gateway key."""

    run_id: str
    max_cost_usd: float
    model: str
    expires_in_seconds: int


class RunKeyManager(Protocol):
    """Creates and removes the opaque Secret mounted by one execution Job."""

    async def provision(self, budget: RunBudget) -> str: ...

    async def cleanup(self, run_id: str, secret_name: str) -> None: ...


class RunGitCredentialManager(Protocol):
    """Creates and removes the repository credential Secret for one execution."""

    async def provision(self, run_id: str) -> str: ...

    async def cleanup(self, run_id: str, secret_name: str) -> None: ...


class KubernetesLiteLLMRunKeyManager:
    """Provision one model-limited, budget-limited key and its private Secret."""

    def __init__(self, namespace: str, endpoint: str, management_key: str) -> None:
        if not management_key:
            raise ValueError("LiteLLM run-key management credential is not configured")
        try:
            from kubernetes import client, config
            from kubernetes.client.exceptions import ApiException
        except ImportError as error:
            raise RuntimeError("run-key provisioning requires the kubernetes dependency") from error
        config.load_incluster_config()
        self._namespace = namespace
        self._endpoint = endpoint.rstrip("/")
        self._management_key = management_key
        self._core_api = client.CoreV1Api()
        self._client = client
        self._api_exception: type[Exception] = ApiException

    async def provision(self, budget: RunBudget) -> str:
        """Create or reuse the deterministic run Secret without returning its token."""

        _validate_budget(budget)
        secret_name = run_key_secret_name(budget.run_id)
        existing = await self._read_secret(secret_name)
        if existing is not None:
            if _secret_token(existing):
                return secret_name
            await self._delete_secret(secret_name)

        token = f"sk-cogito-{secrets.token_urlsafe(32)}"
        try:
            await asyncio.to_thread(
                self._post_json,
                "/key/generate",
                {
                    "key": token,
                    "key_alias": f"cogito-{_run_hash(budget.run_id)}",
                    "models": [budget.model],
                    "max_budget": budget.max_cost_usd,
                    "budget_duration": f"{budget.expires_in_seconds}s",
                    "key_type": "llm_api",
                    "metadata": {"cogito_run_hash": _run_hash(budget.run_id)},
                },
            )
            body = self._client.V1Secret(
                metadata=self._client.V1ObjectMeta(
                    name=secret_name,
                    labels={"cogito.dev/run-hash": _run_hash(budget.run_id)},
                ),
                type="Opaque",
                data={"api-key": base64.b64encode(token.encode()).decode()},
            )
            await asyncio.to_thread(self._core_api.create_namespaced_secret, self._namespace, body)
        except Exception:
            # A token never enters logs, workflow inputs, or status metadata.
            await self._delete_gateway_key(token)
            raise
        return secret_name

    async def cleanup(self, run_id: str, secret_name: str) -> None:
        """Revoke the gateway key before deleting the labelled run Secret."""

        if secret_name != run_key_secret_name(run_id):
            raise ValueError("run key Secret does not match the execution run")
        secret = await self._read_secret(secret_name)
        if secret is None:
            return
        token = _secret_token(secret)
        if token:
            await self._delete_gateway_key(token)
        await self._delete_secret(secret_name)

    async def _read_secret(self, name: str):
        try:
            return await asyncio.to_thread(self._core_api.read_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status == 404:
                return None
            raise

    async def _delete_secret(self, name: str) -> None:
        try:
            await asyncio.to_thread(self._core_api.delete_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status != 404:
                raise

    async def _delete_gateway_key(self, token: str) -> None:
        await asyncio.to_thread(self._post_json, "/key/delete", {"keys": [token]})

    def _post_json(self, path: str, payload: dict[str, object]) -> None:
        request = Request(
            f"{self._endpoint}{path}",
            data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {self._management_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=30) as response:  # nosec B310: endpoint is operator controlled
            if response.status < 200 or response.status >= 300:
                raise RuntimeError("LiteLLM run-key management request was rejected")


class KubernetesRunGitCredentialManager:
    """Copies a worker-mounted repository credential into one run-private Secret."""

    def __init__(self, namespace: str, token: str) -> None:
        if not token:
            raise ValueError("execution Git credential is not configured")
        try:
            from kubernetes import client, config
            from kubernetes.client.exceptions import ApiException
        except ImportError as error:
            raise RuntimeError("run Git credential provisioning requires the kubernetes dependency") from error
        config.load_incluster_config()
        self._namespace = namespace
        self._token = token
        self._core_api = client.CoreV1Api()
        self._client = client
        self._api_exception: type[Exception] = ApiException

    async def provision(self, run_id: str) -> str:
        secret_name = run_git_secret_name(run_id)
        existing = await self._read_secret(secret_name)
        if existing is not None:
            if _secret_token(existing, key="token"):
                return secret_name
            await self._delete_secret(secret_name)
        body = self._client.V1Secret(
            metadata=self._client.V1ObjectMeta(
                name=secret_name,
                labels={"cogito.dev/run-hash": _run_hash(run_id)},
            ),
            type="Opaque",
            data={"token": base64.b64encode(self._token.encode()).decode()},
        )
        await asyncio.to_thread(self._core_api.create_namespaced_secret, self._namespace, body)
        return secret_name

    async def cleanup(self, run_id: str, secret_name: str) -> None:
        if secret_name != run_git_secret_name(run_id):
            raise ValueError("run Git Secret does not match the execution run")
        await self._delete_secret(secret_name)

    async def _read_secret(self, name: str):
        try:
            return await asyncio.to_thread(self._core_api.read_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status == 404:
                return None
            raise

    async def _delete_secret(self, name: str) -> None:
        try:
            await asyncio.to_thread(self._core_api.delete_namespaced_secret, name, self._namespace)
        except self._api_exception as error:
            if error.status != 404:
                raise


def run_key_secret_name(run_id: str) -> str:
    """Return a deterministic name that reveals no raw run identifier."""

    return f"cogito-run-key-{_run_hash(run_id)}"


def run_git_secret_name(run_id: str) -> str:
    """Return a deterministic name for the run-private Git credential Secret."""

    return f"cogito-run-git-{_run_hash(run_id)}"


def _run_hash(run_id: str) -> str:
    return hashlib.sha256(run_id.encode()).hexdigest()[:20]


def _secret_token(secret: object, key: str = "api-key") -> str | None:
    data = getattr(secret, "data", None) or {}
    encoded = data.get(key)
    if not isinstance(encoded, str):
        return None
    try:
        return base64.b64decode(encoded, validate=True).decode()
    except (ValueError, UnicodeDecodeError):
        return None


def _validate_budget(budget: RunBudget) -> None:
    if budget.max_cost_usd <= 0 or budget.expires_in_seconds < 1 or not budget.model:
        raise ValueError("run budget must have a positive cost, expiry, and model")
