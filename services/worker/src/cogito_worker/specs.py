from __future__ import annotations

import tarfile
from collections.abc import Mapping
from io import BytesIO
from pathlib import PurePosixPath
from typing import Protocol

import yaml

from .models import ResolvedSpecFile, ResolvedSpecSet
from .storage import SpecStore, validate_spec_ref

_GENERIC_SCOPE = "always"
_MANIFEST_PATH = "manifest.yaml"


class SpecResolutionError(ValueError):
    """Raised when an immutable spec archive is malformed or unsafe."""


class GenericSpecResolver(Protocol):
    """Resolves the generic portion of a versioned spec set."""

    def resolve_generic(self, ref: str) -> ResolvedSpecSet:
        """Return only the always-on rules and constraints for ``ref``."""


class SpecSetResolver:
    """Validates immutable archives and resolves generic manifest entries."""

    def __init__(self, store: SpecStore, max_extracted_bytes: int):
        self._store = store
        self._max_extracted_bytes = max_extracted_bytes

    def resolve_generic(self, ref: str) -> ResolvedSpecSet:
        """Resolve the always-on rules and constraints for an exact reference."""

        spec_ref = validate_spec_ref(ref)
        files = self._read_archive(self._store.get_archive(ref))
        manifest = self._read_manifest(files)
        self._validate_identity(manifest, spec_ref.name_version)
        generic_files = self._resolve_entries(manifest, files)
        return ResolvedSpecSet(ref=ref, files=generic_files)

    def _read_archive(self, archive_bytes: bytes) -> dict[str, bytes]:
        try:
            archive = tarfile.open(fileobj=BytesIO(archive_bytes), mode="r:gz")
        except tarfile.TarError as error:
            raise SpecResolutionError("spec archive is not a valid gzip tar archive") from error

        files: dict[str, bytes] = {}
        extracted_bytes = 0
        with archive:
            for member in archive.getmembers():
                path = self._validate_member(member)
                if member.isdir():
                    continue
                if path in files:
                    raise SpecResolutionError(f"spec archive contains a duplicate path: {path}")
                extracted_bytes += member.size
                if extracted_bytes > self._max_extracted_bytes:
                    raise SpecResolutionError("spec archive exceeds the configured extracted size limit")
                source = archive.extractfile(member)
                if source is None:
                    raise SpecResolutionError(f"could not read spec archive member: {path}")
                with source:
                    files[path] = source.read()
        return files

    @staticmethod
    def _validate_member(member: tarfile.TarInfo) -> str:
        path = PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts or member.name.startswith("\\"):
            raise SpecResolutionError(f"unsafe spec archive path: {member.name}")
        if not member.isfile() and not member.isdir():
            raise SpecResolutionError(f"unsupported spec archive member type: {member.name}")
        normalized = path.as_posix()
        if normalized in ("", "."):
            raise SpecResolutionError("spec archive contains an empty path")
        return normalized

    @staticmethod
    def _read_manifest(files: dict[str, bytes]) -> Mapping[str, object]:
        try:
            raw_manifest = files[_MANIFEST_PATH].decode("utf-8")
        except KeyError as error:
            raise SpecResolutionError("spec archive does not contain manifest.yaml") from error
        except UnicodeDecodeError as error:
            raise SpecResolutionError("manifest.yaml must be UTF-8") from error

        try:
            manifest = yaml.safe_load(raw_manifest)
        except yaml.YAMLError as error:
            raise SpecResolutionError("manifest.yaml is invalid YAML") from error
        if not isinstance(manifest, Mapping):
            raise SpecResolutionError("manifest.yaml must contain a mapping")
        return manifest

    @staticmethod
    def _validate_identity(manifest: Mapping[str, object], ref: str) -> None:
        name = manifest.get("name")
        version = manifest.get("version")
        if not isinstance(name, str) or not isinstance(version, (str, int, float)):
            raise SpecResolutionError("manifest.yaml must declare name and version")
        if f"{name}@{version}" != ref:
            raise SpecResolutionError("manifest name and version do not match the requested spec reference")

    @staticmethod
    def _resolve_entries(manifest: Mapping[str, object], files: dict[str, bytes]) -> list[ResolvedSpecFile]:
        resolved: list[ResolvedSpecFile] = []
        resolved_paths: set[str] = set()
        for section in ("rules", "constraints"):
            entries = manifest.get(section, [])
            if not isinstance(entries, list):
                raise SpecResolutionError(f"manifest {section} must be a list")
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise SpecResolutionError(f"manifest {section} entries must be mappings")
                if entry.get("scope") != _GENERIC_SCOPE:
                    continue
                path = entry.get("path")
                priority = entry.get("priority", "")
                if not isinstance(path, str) or not isinstance(priority, str):
                    raise SpecResolutionError(f"generic manifest {section} entries need string path and priority")
                safe_path = SpecSetResolver._validate_referenced_path(path)
                if safe_path in resolved_paths:
                    raise SpecResolutionError(f"manifest references a generic spec more than once: {safe_path}")
                try:
                    content = files[safe_path].decode("utf-8")
                except KeyError as error:
                    raise SpecResolutionError(f"manifest references a missing spec file: {safe_path}") from error
                except UnicodeDecodeError as error:
                    raise SpecResolutionError(f"spec file must be UTF-8: {safe_path}") from error
                resolved_paths.add(safe_path)
                resolved.append(ResolvedSpecFile(path=safe_path, content=content, priority=priority))
        return resolved

    @staticmethod
    def _validate_referenced_path(path: str) -> str:
        normalized = PurePosixPath(path)
        if normalized.is_absolute() or ".." in normalized.parts or path.startswith("\\"):
            raise SpecResolutionError(f"unsafe manifest path: {path}")
        rendered = normalized.as_posix()
        if rendered in ("", "."):
            raise SpecResolutionError("manifest contains an empty path")
        return rendered
