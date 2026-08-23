"""Single-file, human-readable repository domain-context contracts.

The source of truth is one Markdown document (normally ``docs/domain.md``).
Its YAML front matter is schema-validated for agents, while the prose remains
human-maintained.  Cogito generates only the explicitly delimited Mermaid
region and never treats the diagram itself as authoritative data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


_FRONT_MATTER = re.compile(r"\A---\n(?P<metadata>.*?)\n---\n(?P<body>.*)\Z", re.DOTALL)
_GRAPH = re.compile(
    r"<!-- cogito:generated:domain-graph:start -->\n```mermaid\n(?P<graph>.*?)\n```\n<!-- cogito:generated:domain-graph:end -->",
    re.DOTALL,
)
_NODE_IDENTIFIER = re.compile(r"[^a-zA-Z0-9_]")


class DomainRelationship(BaseModel):
    """One repository relationship inferred from document or code evidence."""

    model_config = ConfigDict(extra="forbid")

    repository_id: str = Field(min_length=1, max_length=256)
    kind: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    direction: str = Field(pattern=r"^(inbound|outbound|bidirectional)$")


class DomainContextFrontMatter(BaseModel):
    """Machine-readable metadata embedded in one human-readable Markdown file."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1, le=1)
    domain_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    repository_id: str = Field(min_length=1, max_length=256)
    role: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    owners: list[str] = Field(min_length=1, max_length=32)
    relationships: list[DomainRelationship] = Field(default_factory=list, max_length=128)
    last_assessed_commit: str = Field(pattern=r"^[a-f0-9]{40,64}$")

    @model_validator(mode="after")
    def validate_relationships(self) -> "DomainContextFrontMatter":
        if any(not owner.strip() for owner in self.owners) or len(set(self.owners)) != len(self.owners):
            raise ValueError("domain context owners must be unique non-blank values")
        relationship_keys = [(item.repository_id, item.kind, item.direction) for item in self.relationships]
        if len(set(relationship_keys)) != len(relationship_keys):
            raise ValueError("domain context relationships must be unique")
        if any(item.repository_id == self.repository_id for item in self.relationships):
            raise ValueError("domain context cannot declare a relationship to itself")
        return self


@dataclass(frozen=True)
class DomainContextDocument:
    """Validated document plus the human-authored Markdown outside its graph block."""

    metadata: DomainContextFrontMatter
    markdown: str


def render_mermaid(metadata: DomainContextFrontMatter) -> str:
    """Render the deterministic review graph from structured front matter."""

    source = _node_id(metadata.repository_id)
    lines = ["flowchart LR", f"  {source}[{_label(metadata.repository_id)}]"]
    for relationship in sorted(metadata.relationships, key=lambda item: (item.repository_id, item.kind, item.direction)):
        target = _node_id(relationship.repository_id)
        lines.append(f"  {target}[{_label(relationship.repository_id)}]")
        if relationship.direction == "inbound":
            lines.append(f"  {target} -->|{relationship.kind}| {source}")
        elif relationship.direction == "outbound":
            lines.append(f"  {source} -->|{relationship.kind}| {target}")
        else:
            lines.append(f"  {source} <-->|{relationship.kind}| {target}")
    return "\n".join(lines)


def parse_domain_context(markdown: str) -> DomainContextDocument:
    """Parse and validate one Markdown document without accepting graph drift."""

    match = _FRONT_MATTER.fullmatch(markdown)
    if match is None:
        raise ValueError("domain context must start with YAML front matter delimited by ---")
    try:
        raw_metadata = yaml.safe_load(match.group("metadata"))
    except yaml.YAMLError as error:
        raise ValueError("domain context front matter is not valid YAML") from error
    if not isinstance(raw_metadata, dict):
        raise ValueError("domain context front matter must be a YAML mapping")
    metadata = DomainContextFrontMatter.model_validate(raw_metadata)
    graph_matches = list(_GRAPH.finditer(match.group("body")))
    if len(graph_matches) != 1:
        raise ValueError("domain context must contain exactly one generated Mermaid graph region")
    if graph_matches[0].group("graph").strip() != render_mermaid(metadata):
        raise ValueError("domain context Mermaid graph does not match its front matter")
    return DomainContextDocument(metadata=metadata, markdown=markdown)


def render_domain_context(metadata: DomainContextFrontMatter, narrative: str) -> str:
    """Create the single source file while preserving caller-owned narrative."""

    if "cogito:generated:domain-graph" in narrative:
        raise ValueError("narrative must not contain a generated domain graph marker")
    front_matter = yaml.safe_dump(
        metadata.model_dump(mode="json"), sort_keys=False, allow_unicode=True, default_flow_style=False
    ).strip()
    graph = render_mermaid(metadata)
    prose = narrative.strip()
    return (
        f"---\n{front_matter}\n---\n\n"
        f"# {metadata.repository_id} domain context\n\n"
        f"## Domain graph\n\n<!-- cogito:generated:domain-graph:start -->\n```mermaid\n{graph}\n```\n"
        f"<!-- cogito:generated:domain-graph:end -->\n"
        f"{f'\n\n{prose}' if prose else ''}\n"
    )


def _node_id(repository_id: str) -> str:
    normalized = _NODE_IDENTIFIER.sub("_", repository_id).strip("_")
    return f"repo_{normalized or 'unknown'}"


def _label(repository_id: str) -> str:
    return repository_id.replace("[", "(").replace("]", ")")
