"""Validation and immutable identity helpers for registry component releases."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from .models import RegistrationManifest, RegistrationReference


class ComponentCatalog(BaseModel):
    """Versioned monorepo definitions that may be registered by the Supervisor."""

    components: list[RegistrationManifest] = Field(
        min_length=1,
        max_length=128,
        description="Component releases declared by the monorepo catalog",
    )

    @model_validator(mode="after")
    def validate_unique_releases(self) -> "ComponentCatalog":
        """Require one immutable manifest for each registration ID and version."""

        releases = [(item.registration_id, item.version) for item in self.components]
        if len(set(releases)) != len(releases):
            raise ValueError("component catalog contains duplicate registration releases")
        component_releases = [(item.component_id, item.component_version) for item in self.components]
        if len(set(component_releases)) != len(component_releases):
            raise ValueError("component catalog contains duplicate component releases")
        known_tools = {
            (item.registration_id, item.version)
            for item in self.components
            if item.kind.value == "tool"
        }
        for item in self.components:
            for grant in item.grants:
                if (grant.tool_id, grant.tool_version) not in known_tools:
                    raise ValueError(
                        f"registration '{item.registration_id}' grants unknown tool "
                        f"'{grant.tool_id}' version '{grant.tool_version}'"
                    )
        return self


def canonical_manifest_bytes(manifest: RegistrationManifest) -> bytes:
    """Return stable non-secret bytes used for registration identity and audit."""

    return json.dumps(
        manifest.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def manifest_sha256(manifest: RegistrationManifest) -> str:
    """Calculate the SHA-256 identity for one canonical registration manifest."""

    return hashlib.sha256(canonical_manifest_bytes(manifest)).hexdigest()


def registration_reference(role: str, manifest: RegistrationManifest) -> RegistrationReference:
    """Create audit-safe run evidence from a role alias and selected release."""

    return RegistrationReference(
        role=role,
        registration_id=manifest.registration_id,
        version=manifest.version,
        manifest_sha256=manifest_sha256(manifest),
        component_id=manifest.component_id,
        component_version=manifest.component_version,
        grants=manifest.grants,
    )


def load_component_catalog(catalog_root: Path) -> ComponentCatalog:
    """Load all checked-in component definitions without allowing partial catalogs."""

    definitions = sorted(catalog_root.glob("**/component.json"))
    if not definitions:
        raise ValueError("component catalog does not contain any component definitions")
    components: list[RegistrationManifest] = []
    for definition in definitions:
        try:
            value = json.loads(definition.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"component definition '{definition}' is not valid JSON") from error
        components.append(RegistrationManifest.model_validate(value))
    return ComponentCatalog(components=components)
