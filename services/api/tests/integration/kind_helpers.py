"""Shared subprocess helpers for opt-in Kind integration tests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]


class KindHarness:
    def __init__(self, *, context: str, namespace: str, execution_namespace: str, release: str, timeout: int):
        self.context = context
        self.namespace = namespace
        self.execution_namespace = execution_namespace
        self.release = release
        self.timeout = timeout

    @classmethod
    def from_environment(cls, *, default_context: str) -> KindHarness:
        if os.environ.get("COGITO_E2E_ENABLED") != "1":
            pytest.skip("set COGITO_E2E_ENABLED=1 to run Kind integration coverage")
        if shutil.which("kubectl") is None:
            pytest.skip("kubectl is unavailable")
        return cls(
            context=os.environ.get("COGITO_E2E_CONTEXT", default_context),
            namespace=os.environ.get("COGITO_E2E_NAMESPACE", "cogito"),
            execution_namespace=os.environ.get("COGITO_E2E_EXECUTION_NAMESPACE", "cogito-executions"),
            release=os.environ.get("COGITO_E2E_RELEASE", "cogito"),
            timeout=int(os.environ.get("COGITO_E2E_TIMEOUT_SECONDS", "900")),
        )

    def command(self, *args: str, input_text: str | None = None, check: bool = True) -> str:
        result = subprocess.run(args, cwd=REPO_ROOT, input=input_text, text=True, capture_output=True, check=False)
        if check and result.returncode:
            pytest.fail(f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr[-4000:]}")
        return result.stdout

    def kubectl(self, *args: str, input_text: str | None = None, check: bool = True) -> str:
        return self.command("kubectl", "--context", self.context, *args, input_text=input_text, check=check)

    def exec_python(self, resource: str, source: str) -> str:
        """Run test-only Python inside one trusted workload without exposing Secrets."""

        return self.kubectl(
            "-n",
            self.namespace,
            "exec",
            "-i",
            resource,
            "--",
            "python",
            "-c",
            source,
        )

    def assert_context(self) -> None:
        if self.context not in self.command("kubectl", "config", "get-contexts", "-o", "name").splitlines():
            pytest.skip(f"Kind context unavailable: {self.context}")

    def api(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None = None,
        *,
        authenticated: bool = True,
        idempotency_key: str | None = None,
    ) -> tuple[int, dict[str, object]]:
        code = """
import json,os,sys,urllib.error,urllib.request
payload=json.loads(sys.stdin.read()); headers={"Content-Type":"application/json"}
if sys.argv[3] == "true": headers.update({"Authorization":"Bearer "+os.environ["COGITO_AUTH_STATIC_TOKEN"],"Idempotency-Key":sys.argv[4]})
request=urllib.request.Request("http://127.0.0.1:8000"+sys.argv[2],data=(json.dumps(payload).encode() if payload is not None else None),headers=headers,method=sys.argv[1])
try:
 response=urllib.request.urlopen(request,timeout=90); print(json.dumps({"status":response.status,"body":json.loads(response.read())}))
except urllib.error.HTTPError as error: print(json.dumps({"status":error.code,"body":json.loads(error.read() or b"{}")}))
"""
        output = self.kubectl(
            "-n", self.namespace, "exec", "-i", f"deployment/{self.release}-api", "--", "python", "-c", code,
            method, path, str(authenticated).lower(), idempotency_key or f"kind-pytest-{path}", input_text=json.dumps(body),
        )
        result = json.loads(output)
        return int(result["status"]), dict(result["body"])

    def wait_for(self, path: str, expected: str, field: str = "status") -> dict[str, object]:
        deadline = time.monotonic() + self.timeout
        latest: dict[str, object] = {}
        while time.monotonic() < deadline:
            status, latest = self.api("GET", path, authenticated=True)
            assert status == 200, latest
            if latest.get(field) == expected:
                return latest
            time.sleep(5)
        pytest.fail(f"timed out waiting for {expected}: {latest}")

    def snapshot(self, reference: str) -> dict[str, object]:
        code = """import json,os,sys
from minio import Minio
from urllib.parse import urlparse
p=urlparse(sys.argv[1]); client=Minio(os.environ['MINIO_ENDPOINT'],access_key=os.environ['MINIO_ACCESS_KEY'],secret_key=os.environ['MINIO_SECRET_KEY'],secure=os.environ.get('MINIO_SECURE','false').lower()=='true'); response=client.get_object(p.netloc,p.path.lstrip('/'))
try: print(response.read().decode())
finally: response.close(); response.release_conn()
"""
        return json.loads(self.kubectl("-n", self.namespace, "exec", f"deployment/{self.release}-api", "--", "python", "-c", code, reference))

    def assert_no_execution_resources(self, run_id: str) -> None:
        run_hash = __import__("hashlib").sha256(run_id.encode()).hexdigest()[:20]
        leftovers = self.kubectl("-n", self.execution_namespace, "get", "jobs,pods,secrets", "-l", f"cogito.dev/run-hash={run_hash}", "-o", "name", check=False)
        assert not leftovers.strip(), leftovers

    def registry_roles(self, run_id: str) -> list[tuple[str, str, str, str]]:
        """Read only non-secret durable registration identities for one run."""
        sql = "SELECT role, registration_id, registration_version, manifest_sha256 FROM run_registration_resolutions WHERE run_id = '" + run_id + "' ORDER BY role"
        code = "PGPASSWORD=\"$(cat \"$POSTGRES_PASSWORD_FILE\")\" psql -U postgres -d cogito -At -F '|' -c \"$1\""
        output = self.kubectl("-n", self.namespace, "exec", f"statefulset/{self.release}-postgresql", "--", "sh", "-ec", code, "--", sql)
        return [tuple(line.split("|")) for line in output.splitlines() if line]
