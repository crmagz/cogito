from __future__ import annotations

import tarfile
from hashlib import sha256
from io import BytesIO

import pytest

from cogito_worker.specs import SpecResolutionError, SpecSetResolver
from cogito_worker.storage import MinioSpecStore


class InMemorySpecStore:
    def __init__(self, archives: dict[str, bytes]):
        self._archives = archives

    def get_archive(self, ref: str) -> bytes:
        return self._archives[ref]


class FakeResponse:
    def __init__(self, data: bytes):
        self._data = data
        self.headers = {"content-length": str(len(data))}
        self.closed = False
        self.released = False

    def read(self, amount: int) -> bytes:
        return self._data[:amount]

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class FakeMinioClient:
    def __init__(self, response: FakeResponse):
        self._response = response
        self.requests: list[tuple[str, str]] = []

    def get_object(self, bucket: str, object_name: str) -> FakeResponse:
        self.requests.append((bucket, object_name))
        return self._response


def make_archive(members: dict[str, bytes], link_name: str | None = None) -> bytes:
    """Build an in-memory gzip tar archive for spec resolver tests."""

    output = BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for path, content in members.items():
            info = tarfile.TarInfo(path)
            info.size = len(content)
            archive.addfile(info, BytesIO(content))
        if link_name is not None:
            link = tarfile.TarInfo(link_name)
            link.type = tarfile.SYMTYPE
            link.linkname = "manifest.yaml"
            archive.addfile(link)
    return output.getvalue()


def generic_spec_archive() -> bytes:
    manifest = b"""
name: typescript-backend
version: v2.1
rules:
  - path: rules/naming.md
    scope: always
    priority: high
  - path: rules/testing.md
    scope: when-testing
    priority: medium
patterns:
  - path: patterns/service.md
    trigger: creating-service
constraints:
  - path: constraints/security.md
    scope: always
    priority: critical
examples:
  - path: examples/service
    for-pattern: service
"""
    return make_archive(
        {
            "manifest.yaml": manifest,
            "rules/naming.md": b"Use clear names.\n",
            "rules/testing.md": b"Write tests.\n",
            "patterns/service.md": b"Service pattern.\n",
            "constraints/security.md": b"Never log secrets.\n",
        }
    )


def immutable_ref(archive: bytes) -> str:
    return f"typescript-backend@v2.1#sha256={sha256(archive).hexdigest()}"


def test_minio_spec_store_loads_an_exact_versioned_archive() -> None:
    archive = generic_spec_archive()
    response = FakeResponse(archive)
    client = FakeMinioClient(response)
    store = MinioSpecStore(client, "specs", "standards/current", max_archive_bytes=1024 * 1024)

    result = store.get_archive(immutable_ref(archive))

    assert result == archive
    assert client.requests == [("specs", "standards/current/typescript-backend@v2.1/spec-set.tar.gz")]
    assert response.closed is True
    assert response.released is True


def test_minio_spec_store_rejects_an_archive_with_a_different_digest() -> None:
    archive = generic_spec_archive()
    response = FakeResponse(archive)
    client = FakeMinioClient(response)
    store = MinioSpecStore(client, "specs", "standards/current", max_archive_bytes=1024 * 1024)

    with pytest.raises(ValueError, match="digest does not match"):
        store.get_archive("typescript-backend@v2.1#sha256=" + "0" * 64)

    assert response.closed is True
    assert response.released is True


def test_resolve_generic_specs_returns_only_always_scope() -> None:
    resolver = SpecSetResolver(
        InMemorySpecStore({immutable_ref(generic_spec_archive()): generic_spec_archive()}),
        max_extracted_bytes=1024 * 1024,
    )

    result = resolver.resolve_generic(immutable_ref(generic_spec_archive()))

    assert [spec.path for spec in result.files] == ["rules/naming.md", "constraints/security.md"]
    assert [spec.priority for spec in result.files] == ["high", "critical"]
    assert all("testing" not in spec.path for spec in result.files)


@pytest.mark.parametrize(
    ("archive", "message"),
    [
        (make_archive({"../escape.md": b"unsafe"}), "unsafe spec archive path"),
        (make_archive({"rules/naming.md": b"missing manifest"}), "does not contain manifest"),
        (make_archive({"manifest.yaml": b"name: bad\nversion: v1"}, link_name="rules/link.md"), "unsupported"),
    ],
)
def test_spec_archive_rejects_unsafe_members_or_missing_manifest(archive: bytes, message: str) -> None:
    resolver = SpecSetResolver(
        InMemorySpecStore({immutable_ref(archive): archive}),
        max_extracted_bytes=1024 * 1024,
    )

    with pytest.raises(SpecResolutionError, match=message):
        resolver.resolve_generic(immutable_ref(archive))


def test_spec_store_rejects_path_like_references_before_object_lookup() -> None:
    response = FakeResponse(generic_spec_archive())
    client = FakeMinioClient(response)
    store = MinioSpecStore(client, "specs", "specs", max_archive_bytes=1024 * 1024)

    with pytest.raises(ValueError, match="name@version"):
        store.get_archive("../../plans@v1")

    assert client.requests == []
