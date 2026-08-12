from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tomllib

import jsonschema
import pytest
import yaml


ROOT = Path(__file__).parents[4]


def _json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def test_github_domain_metadata_declares_the_connector_boundary() -> None:
    schema = _json(ROOT / "packages" / "domain.schema.json")
    metadata = yaml.safe_load((ROOT / "packages" / "github" / "domain.yaml").read_text(encoding="utf-8"))

    jsonschema.validate(metadata, schema)

    assert metadata["services"] == [
        {
            "id": "readonly-mcp",
            "package": "packages/github/readonly-mcp",
            "version": "0.1.0",
            "registration": {"id": "github_readonly_mcp", "version": "1.0.0"},
        }
    ]
    assert metadata["credentials"]["secretKeys"] == ["app-id", "installation-id", "private-key"]
    assert metadata["network"] == {
        "ingress": "LiteLLM gateway only",
        "egress": "Environment-owned HTTPS egress proxy only",
    }
    project = tomllib.loads((ROOT / "packages" / "github" / "readonly-mcp" / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["version"] == metadata["services"][0]["version"]


def test_domain_metadata_schema_rejects_missing_operational_boundaries() -> None:
    schema = _json(ROOT / "packages" / "domain.schema.json")
    metadata = yaml.safe_load((ROOT / "packages" / "github" / "domain.yaml").read_text(encoding="utf-8"))

    del metadata["credentials"]

    with pytest.raises(jsonschema.ValidationError, match="credentials"):
        jsonschema.validate(metadata, schema)


def test_release_bundle_schema_requires_immutable_package_and_image_pins() -> None:
    bundle = {
        "schemaVersion": "1",
        "environment": "disposable-kind",
        "services": [
            {
                "package": "packages/github/readonly-mcp",
                "packageVersion": "0.1.0",
                "registration": "github_readonly_mcp@1.0.0",
                "manifestSha256": "33aab86822040fa5880ce0f9d89eebd000d24fc3c9986e020bd7b89ac0e2a298",
                "image": {
                    "repository": "registry.example/cogito-github-readonly-mcp",
                    "digest": "sha256:" + "a" * 64,
                },
            }
        ],
    }

    jsonschema.validate(bundle, _json(ROOT / "components" / "release-bundles" / "schema.json"))
    assert bundle["services"][0]["manifestSha256"] == "33aab86822040fa5880ce0f9d89eebd000d24fc3c9986e020bd7b89ac0e2a298"

    bundle["services"][0]["image"]["digest"] = "latest"
    with pytest.raises(jsonschema.ValidationError, match="digest"):
        jsonschema.validate(bundle, _json(ROOT / "components" / "release-bundles" / "schema.json"))


def test_release_bundle_references_match_the_github_package_contract() -> None:
    domain = yaml.safe_load((ROOT / "packages" / "github" / "domain.yaml").read_text(encoding="utf-8"))
    project = tomllib.loads((ROOT / "packages" / "github" / "readonly-mcp" / "pyproject.toml").read_text(encoding="utf-8"))
    manifest = _json(ROOT / "components" / "tools" / "github_readonly_mcp" / "component.json")
    bundle = _json(ROOT / "components" / "release-bundles" / "github-readonly-mcp.example.json")
    loaded = bundle["services"][0]
    jsonschema.validate(bundle, _json(ROOT / "components" / "release-bundles" / "schema.json"))
    assert loaded["package"] == domain["services"][0]["package"]
    assert loaded["packageVersion"] == domain["services"][0]["version"]
    assert loaded["registration"] == "{}@{}".format(
        manifest["registration_id"], manifest["version"]
    )


def test_release_metadata_verifier_accepts_the_checked_in_contract() -> None:
    subprocess.run(
        [sys.executable, "scripts/verify_release_metadata.py", "0.1.0"],
        cwd=ROOT / "packages" / "github" / "readonly-mcp",
        check=True,
    )
