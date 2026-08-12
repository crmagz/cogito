"""Validate the checked-in GitHub MCP package release contract."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).parents[4]
PACKAGE = "packages/github/readonly-mcp"
REGISTRATION = "github_readonly_mcp@1.0.0"
MANIFEST_SHA256 = "33aab86822040fa5880ce0f9d89eebd000d24fc3c9986e020bd7b89ac0e2a298"


def main() -> None:
    """Fail when package, domain, or release-bundle pins disagree."""

    expected_version = sys.argv[1] if len(sys.argv) == 2 else None
    version = tomllib.loads((ROOT / PACKAGE / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
    domain = yaml.safe_load((ROOT / "packages/github/domain.yaml").read_text(encoding="utf-8"))
    domain_service = next(item for item in domain["services"] if item["package"] == PACKAGE)
    bundle = json.loads((ROOT / "components/release-bundles/github-readonly-mcp.example.json").read_text(encoding="utf-8"))
    jsonschema.validate(bundle, json.loads((ROOT / "components/release-bundles/schema.json").read_text(encoding="utf-8")))
    bundle_service = next(item for item in bundle["services"] if item["package"] == PACKAGE)
    if domain_service["version"] != version or bundle_service["packageVersion"] != version:
        raise ValueError("package, domain metadata, and release-bundle versions must match")
    if domain_service["registration"] != {"id": "github_readonly_mcp", "version": "1.0.0"}:
        raise ValueError("domain metadata registration differs from the immutable connector release")
    if bundle_service["registration"] != REGISTRATION or bundle_service["manifestSha256"] != MANIFEST_SHA256:
        raise ValueError("release bundle differs from the immutable connector registration")
    if expected_version is not None and version != expected_version:
        raise ValueError(f"package metadata version {version} does not match computed release {expected_version}")


if __name__ == "__main__":
    main()
