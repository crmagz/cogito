"""Safety fixtures for opt-in tests that mutate the shared Kind release."""

from __future__ import annotations

import fcntl
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def exclusive_kind_cluster() -> Iterator[None]:
    """Fail fast instead of allowing concurrent tests to race one Helm release."""
    if os.environ.get("COGITO_E2E_ENABLED") != "1":
        yield
        return
    path = Path(tempfile.gettempdir()) / "cogito-kind-e2e.lock"
    with path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pytest.fail("another Cogito Kind E2E test is already using the shared cluster")
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
