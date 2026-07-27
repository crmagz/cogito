"""Kind integration coverage for the Phase 13 coordination boundary.

This is deliberately opt-in: it changes a disposable Kind release and needs
Docker, Helm, Kubernetes, and valid execution-provider credentials. Run it
directly with pytest and the ``kind_e2e`` marker.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
pytestmark = pytest.mark.kind_e2e


def helm_image_overrides(component: str, image: str) -> tuple[str, ...]:
    """Render Helm overrides that preserve a running workload image exactly."""
    if "@" in image:
        repository, digest = image.split("@", maxsplit=1)
        if not repository or not digest.startswith("sha256:"):
            pytest.fail(f"unsupported {component} image reference: {image}")
        return (
            "--set-string", f"{component}.image.repository={repository}",
            "--set-string", f"{component}.image.digest={digest}",
            "--set-string", f"{component}.image.tag=",
        )

    last_slash = image.rfind("/")
    last_colon = image.rfind(":")
    if last_colon > last_slash:
        repository, tag = image[:last_colon], image[last_colon + 1 :]
    else:
        repository, tag = image, "latest"
    if not repository or not tag:
        pytest.fail(f"unsupported {component} image reference: {image}")
    return (
        "--set-string", f"{component}.image.repository={repository}",
        "--set-string", f"{component}.image.tag={tag}",
        "--set-string", f"{component}.image.digest=",
    )


@dataclass(frozen=True)
class KindConfig:
    context: str
    namespace: str
    execution_namespace: str
    release: str
    spec_ref: str
    target_repo: str
    timeout: int
    existing_run_id: str | None
    values_file: Path | None

    @classmethod
    def load(cls) -> KindConfig:
        spec_ref = os.environ.get("COGITO_E2E_SPEC_REF")
        if not spec_ref:
            pytest.fail("COGITO_E2E_SPEC_REF is required and must be immutable")
        values_file = os.environ.get("COGITO_E2E_VALUES_FILE")
        resolved_values_file = Path(values_file).resolve() if values_file else None
        if resolved_values_file and not resolved_values_file.is_file():
            pytest.fail(f"COGITO_E2E_VALUES_FILE does not exist: {resolved_values_file}")
        return cls(
            context=os.environ.get("COGITO_E2E_CONTEXT", "kind-cogito-observability"),
            namespace=os.environ.get("COGITO_E2E_NAMESPACE", "cogito"),
            execution_namespace=os.environ.get("COGITO_E2E_EXECUTION_NAMESPACE", "cogito-executions"),
            release=os.environ.get("COGITO_E2E_RELEASE", "cogito"),
            spec_ref=spec_ref,
            target_repo=os.environ.get(
                "COGITO_E2E_TARGET_REPO",
                "https://github.com/crmagz/cogito-kind-e2e-fixture.git#7d1ddc14c1cbaf666641c7235c89fa937bb1bd50",
            ),
            timeout=int(os.environ.get("COGITO_E2E_TIMEOUT_SECONDS", "900")),
            existing_run_id=os.environ.get("COGITO_E2E_EXISTING_RUN_ID") or None,
            values_file=resolved_values_file,
        )


class KindControlPlane:
    """Test-only adapter for the in-cluster API and temporary signed receiver."""

    def __init__(self, config: KindConfig):
        self.config = config
        self.receiver = f"{config.release}-phase13-receiver"
        self.receiver_secret = f"{config.release}-phase13-notification-hmac"
        self.hmac_secret = secrets.token_urlsafe(32)
        self._litellm_was_enabled: bool | None = None
        self.run_id: str | None = None

    def command(self, *args: str, input_text: str | None = None, check: bool = True) -> str:
        result = subprocess.run(args, cwd=REPO_ROOT, input=input_text, text=True, capture_output=True, check=False)
        if check and result.returncode:
            pytest.fail(f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr[-4000:]}")
        return result.stdout

    def kubectl(self, *args: str, input_text: str | None = None, check: bool = True) -> str:
        return self.command("kubectl", "--context", self.config.context, *args, input_text=input_text, check=check)

    def running_image(self, component: str) -> str:
        image = self.kubectl(
            "-n", self.config.namespace, "get", f"deployment/{self.config.release}-{component}",
            "-o", f"jsonpath={{.spec.template.spec.containers[?(@.name==\"{component}\")].image}}",
        ).strip()
        if not image or " " in image:
            pytest.fail(f"unable to resolve exactly one running {component} image: {image!r}")
        return image

    def workload_image_overrides(self) -> tuple[str, ...]:
        return (*helm_image_overrides("api", self.running_image("api")), *helm_image_overrides("worker", self.running_image("worker")))

    def values_file_arguments(self) -> tuple[str, ...]:
        return ("--values", str(self.config.values_file)) if self.config.values_file else ()

    def github_repository(self) -> str:
        source = self.config.target_repo.partition("#")[0]
        parsed = urlparse(source.removesuffix(".git"))
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.scheme != "https" or parsed.hostname != "github.com" or len(parts) != 2:
            pytest.fail(f"E2E target repository is not a GitHub HTTPS repository: {self.config.target_repo}")
        return "/".join(parts)

    def assert_github_pr_access(self) -> None:
        code = """
import os, sys, urllib.error, urllib.request
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": "Bearer " + os.environ["COGITO_GITHUB_PULL_REQUEST_TOKEN"].strip(),
    "X-GitHub-Api-Version": "2022-11-28",
}
request = urllib.request.Request(
    os.environ["COGITO_GITHUB_API_URL"].rstrip("/") + "/repos/" + sys.argv[1] + "/pulls?state=all&per_page=1",
    headers=headers,
)
try:
    print(urllib.request.urlopen(request, timeout=30).status)
except urllib.error.HTTPError as error:
    print(error.code)
"""
        status = self.kubectl(
            "-n", self.config.namespace, "exec", f"deployment/{self.config.release}-worker", "--",
            "python", "-c", code, self.github_repository(),
        ).strip()
        if status != "200":
            pytest.fail(
                "GitHub pull-request credential cannot list the E2E fixture repository "
                f"(HTTP {status or 'no response'}); refresh cogito-github-pull-request/token"
            )

    def restart_worker(self) -> None:
        selector = f"app.kubernetes.io/instance={self.config.release},app.kubernetes.io/name=worker"
        previous_pods = self.kubectl(
            "-n", self.config.namespace, "get", "pods", "-l", selector, "-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}"
        ).splitlines()
        self.kubectl("-n", self.config.namespace, "rollout", "restart", f"deployment/{self.config.release}-worker")
        self.kubectl("-n", self.config.namespace, "rollout", "status", f"deployment/{self.config.release}-worker", "--timeout=180s")
        deadline = time.monotonic() + 180
        remaining = set(previous_pods)
        while remaining and time.monotonic() < deadline:
            remaining = {
                pod for pod in remaining
                if self.kubectl("-n", self.config.namespace, "get", "pod", pod, "-o", "name", check=False).strip()
            }
            if remaining:
                time.sleep(1)
        if remaining:
            pytest.fail(f"worker restart did not terminate old pods: {', '.join(sorted(remaining))}")

    def litellm_overrides(self, *, restore: bool = False) -> tuple[str, ...]:
        if restore:
            return ("--set", "litellm.enabled=false") if self._litellm_was_enabled is False else ()
        values = json.loads(
            self.command("helm", "get", "values", self.config.release, "--namespace", self.config.namespace, "--all", "--output", "json")
        )
        litellm = dict(values.get("litellm", {}))
        planner_policy = dict(dict(litellm.get("rolePolicies", {})).get("planner", {}))
        planner_model = str(planner_policy.get("model", ""))
        provider = os.environ.get(
            "COGITO_E2E_LITELLM_PROVIDER",
            str(dict(dict(litellm.get("tiers", {})).get(planner_model, {})).get("provider", "")),
        ).upper()
        provider_secret = str(litellm.get("existingSecret", ""))
        required_keys = (
            ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION")
            if provider == "BEDROCK"
            else (f"{provider}_API_KEY",)
        )
        missing_keys = [
            key for key in required_keys
            if not provider_secret or not self.kubectl(
                "-n", self.config.namespace, "get", "secret", provider_secret, "-o", f"jsonpath={{.data.{key}}}"
            ).strip()
        ]
        if not provider or missing_keys:
            pytest.fail(
                "full Kind E2E requires non-empty "
                f"{', '.join(missing_keys) or '<PROVIDER>_API_KEY'} in secret {provider_secret or '<litellm secret>'}"
            )
        self._litellm_was_enabled = bool(dict(values.get("litellm", {})).get("enabled", False))
        return () if self._litellm_was_enabled else ("--set", "litellm.enabled=true")

    def setup(self) -> None:
        contexts = self.command("kubectl", "config", "get-contexts", "-o", "name").splitlines()
        if self.config.context not in contexts:
            pytest.skip(f"Kind context unavailable: {self.config.context}")
        secret = self.kubectl(
            "-n", self.config.namespace, "create", "secret", "generic", self.receiver_secret,
            f"--from-literal=hmac-secret={self.hmac_secret}", "--dry-run=client", "-o", "yaml",
        )
        self.kubectl("apply", "-f", "-", input_text=secret)
        self.kubectl("-n", self.config.namespace, "label", "secret", self.receiver_secret, "cogito.dev/phase13-e2e=true", "--overwrite")
        self.kubectl("-n", self.config.namespace, "apply", "-f", "-", input_text=self.receiver_manifest())
        self.kubectl("-n", self.config.namespace, "rollout", "status", f"deployment/{self.receiver}", "--timeout=180s")
        image_overrides = self.workload_image_overrides()
        litellm_overrides = self.litellm_overrides()
        self.command(
            "helm", "upgrade", self.config.release, str(REPO_ROOT / "charts"), "--kube-context", self.config.context,
            "--namespace", self.config.namespace, "--reuse-values", "--wait", "--timeout", "10m",
            *self.values_file_arguments(),
            *image_overrides,
            *litellm_overrides,
            "--set", "api.notifications.enabled=true",
            "--set", f"api.notifications.webhookUrl=http://{self.receiver}.{self.config.namespace}.svc:8080/events",
            "--set", f"api.notifications.existingSecret={self.receiver_secret}",
            "--set", "api.notifications.hmacSecretKey=hmac-secret",
        )
        self.restart_worker()
        self.assert_github_pr_access()

    def cleanup(self) -> None:
        if self._litellm_was_enabled is not None:
            image_overrides = self.workload_image_overrides()
            self.command(
                "helm", "upgrade", self.config.release, str(REPO_ROOT / "charts"), "--kube-context", self.config.context,
                "--namespace", self.config.namespace, "--reuse-values", "--wait", "--timeout", "10m",
                *self.values_file_arguments(),
                *image_overrides,
                *self.litellm_overrides(restore=True),
                "--set", "api.notifications.enabled=false", "--set", "api.notifications.webhookUrl=",
                "--set", "api.notifications.existingSecret=", check=False,
            )
        self.kubectl("-n", self.config.namespace, "delete", "deployment,service,secret", "-l", "cogito.dev/phase13-e2e=true", "--ignore-not-found", check=False)
        if self.run_id:
            run_hash = hashlib.sha256(self.run_id.encode()).hexdigest()[:20]
            self.kubectl(
                "-n", self.config.execution_namespace, "delete", "jobs,pods,secrets",
                "-l", f"cogito.dev/run-hash={run_hash}", "--ignore-not-found", check=False,
            )

    def api(self, method: str, path: str, body: dict[str, object] | None = None, *, authenticated: bool = True) -> tuple[int, dict[str, object]]:
        code = """
import json, os, sys, urllib.error, urllib.request
payload = json.loads(sys.stdin.read())
headers = {"Content-Type": "application/json"}
if sys.argv[3] == "true":
    headers["Authorization"] = "Bearer " + os.environ["COGITO_AUTH_STATIC_TOKEN"]
    if sys.argv[1] == "POST": headers["Idempotency-Key"] = "phase13-pytest-" + sys.argv[2]
request = urllib.request.Request("http://127.0.0.1:8000" + sys.argv[2], data=(json.dumps(payload).encode() if payload is not None else None), headers=headers, method=sys.argv[1])
try:
    response = urllib.request.urlopen(request, timeout=90)
    print(json.dumps({"status": response.status, "body": json.loads(response.read())}))
except urllib.error.HTTPError as error:
    print(json.dumps({"status": error.code, "body": json.loads(error.read() or b"{}")}))
"""
        response = self.kubectl(
            "-n", self.config.namespace, "exec", "-i", f"deployment/{self.config.release}-api", "--",
            "python", "-c", code, method, path, str(authenticated).lower(),
            input_text=json.dumps(body),
        )
        payload = json.loads(response)
        return int(payload["status"]), dict(payload["body"])

    def wait_for_status(self, run_id: str, expected: str) -> dict[str, object]:
        deadline = time.monotonic() + self.config.timeout
        latest: dict[str, object] = {}
        while time.monotonic() < deadline:
            status, latest = self.api("GET", f"/api/v1/planning-runs/{run_id}")
            assert status == 200, latest
            if latest["status"] == expected:
                return latest
            if latest["status"] in {"completed", "planning_failed", "rejected", "revision_requested"}:
                pytest.fail(f"run reached terminal status {latest['status']} while waiting for {expected}: {latest}")
            time.sleep(5)
        pytest.fail(f"timed out waiting for {expected}: {latest}")

    def wait_for_event(self, event_type: str) -> None:
        deadline = time.monotonic() + self.config.timeout
        while time.monotonic() < deadline:
            output = self.kubectl("-n", self.config.namespace, "logs", f"deployment/{self.receiver}", check=False)
            for line in output.splitlines():
                if not line.startswith("{"):
                    continue
                event = json.loads(line)
                if event["event_type"] == event_type and event["valid"] is True and event["attempt"] >= 2:
                    return
            time.sleep(2)
        pytest.fail(f"timed out waiting for signed retry: {event_type}")

    def receiver_manifest(self) -> str:
        return f"""apiVersion: v1
kind: Service
metadata:
  name: {self.receiver}
  labels: {{cogito.dev/phase13-e2e: \"true\"}}
spec:
  selector: {{app: {self.receiver}}}
  ports: [{{port: 8080, targetPort: http}}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {self.receiver}
  labels: {{cogito.dev/phase13-e2e: \"true\"}}
spec:
  replicas: 1
  selector: {{matchLabels: {{app: {self.receiver}}}}}
  template:
    metadata: {{labels: {{app: {self.receiver}, cogito.dev/phase13-e2e: \"true\"}}}}
    spec:
      automountServiceAccountToken: false
      containers:
      - name: receiver
        image: python:3.14-alpine
        ports: [{{name: http, containerPort: 8080}}]
        env: [{{name: RECEIVER_HMAC_SECRET, valueFrom: {{secretKeyRef: {{name: {self.receiver_secret}, key: hmac-secret}}}}}}]
        command: [python, -c]
        args:
        - |
          import hashlib,hmac,json,os
          from http.server import BaseHTTPRequestHandler,HTTPServer
          seen={{}}
          class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
              body=self.rfile.read(int(self.headers.get('Content-Length','0'))); event=json.loads(body); event_id=event['event_id']; seen[event_id]=seen.get(event_id,0)+1
              expected='sha256='+hmac.new(os.environ['RECEIVER_HMAC_SECRET'].encode(),body,hashlib.sha256).hexdigest()
              valid=hmac.compare_digest(expected,self.headers.get('X-Cogito-Signature',''))
              print(json.dumps({{'event_type':event['event_type'],'valid':valid,'attempt':seen[event_id]}}),flush=True)
              self.send_response(500 if seen[event_id] == 1 else (204 if valid else 401)); self.end_headers()
          HTTPServer(('',8080),Handler).serve_forever()
"""


@pytest.fixture
def control_plane() -> Iterator[KindControlPlane]:
    if os.environ.get("COGITO_E2E_ENABLED") != "1":
        pytest.skip("set COGITO_E2E_ENABLED=1 to run Kind integration coverage")
    missing = [name for name in ("kubectl", "helm") if shutil.which(name) is None]
    if missing:
        pytest.skip(f"missing Kind integration prerequisites: {', '.join(missing)}")
    control = KindControlPlane(KindConfig.load())
    try:
        control.setup()
        yield control
    finally:
        control.cleanup()


def test_provider_neutral_coordination_end_to_end(control_plane: KindControlPlane) -> None:
    """Assert retry, restart, authenticated replay, both gates, and cleanup."""
    config = control_plane.config
    if config.existing_run_id:
        run_id = config.existing_run_id
        planning = control_plane.wait_for_status(run_id, "awaiting_plan_approval")
    else:
        marker = f".cogito-phase13-{secrets.token_hex(6)}"
        status, submission = control_plane.api("POST", "/api/v1/planning-runs", {
            "initial_specification": f"Create exactly one phase that creates {marker} containing phase-13. Verify only with test -f {marker}.",
            "target_repos": [config.target_repo], "spec_set": config.spec_ref,
            "constraints": {"max_wall_clock_minutes": 8, "max_cost_usd": 3.0, "max_review_rounds": 1, "max_turns_per_phase": 50, "backup_reserve_turns": 20},
            "priority": "normal",
        })
        assert status == 202, submission
        run_id = str(submission["run_id"])
        status, generated = control_plane.api("POST", f"/api/v1/planning-runs/{run_id}/generate-plan", {})
        assert status == 200, generated
        planning = control_plane.wait_for_status(run_id, "awaiting_plan_approval")
        control_plane.wait_for_event("plan_approval_requested")

    run_id = str(planning["run_id"])
    control_plane.run_id = run_id
    plan_sha256 = str(dict(planning["plan_artifact"])["sha256"])
    control_plane.restart_worker()
    status, coordination = control_plane.api("GET", f"/api/v1/planning-runs/{run_id}/coordination")
    assert status == 200 and coordination["active_gate"] == "plan"
    assert sum(event["event_type"] == "plan_approval_requested" for event in coordination["events"]) == 1
    status, _ = control_plane.api("POST", f"/api/v1/coordination/runs/{run_id}/actions/plan", {"decision": "approve", "artifact_sha256": plan_sha256}, authenticated=False)
    assert status == 401
    action = {"decision": "approve", "artifact_sha256": plan_sha256}
    first_status, first = control_plane.api("POST", f"/api/v1/coordination/runs/{run_id}/actions/plan", action)
    second_status, second = control_plane.api("POST", f"/api/v1/coordination/runs/{run_id}/actions/plan", action)
    assert first_status == second_status == 202 and first["decision_id"] == second["decision_id"]
    control_plane.wait_for_event("plan_approval_recorded")
    implementation = control_plane.wait_for_status(run_id, "awaiting_implementation_approval")
    implementation_sha256 = str(dict(implementation["implementation_artifact"])["sha256"])
    control_plane.wait_for_event("implementation_approval_requested")
    status, approval = control_plane.api("POST", f"/api/v1/coordination/runs/{run_id}/actions/implementation", {"decision": "approve", "artifact_sha256": implementation_sha256})
    assert status == 202, approval
    assert control_plane.wait_for_status(run_id, "completed")["status"] == "completed"
    run_hash = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    leftovers = control_plane.kubectl("-n", config.execution_namespace, "get", "jobs,pods,secrets", "-l", f"cogito.dev/run-hash={run_hash}", "-o", "name", check=False)
    assert not leftovers.strip(), leftovers
