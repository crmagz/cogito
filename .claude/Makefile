SHELL := /bin/bash
.DEFAULT_GOAL := help
# Invoke as `make -C .claude <target>` from the repo root — paths below are relative to this file.

CLUSTER_NAME := cogito
CHART_DIR    := ../charts
KIND_CONFIG  := kind-config.yaml
RELEASE_NAME := cogito
NAMESPACE    := cogito
LITELLM_SECRET := cogito-litellm-secret
EXECUTION_NAMESPACE := cogito-executions
SPEC_READER_SECRET := cogito-execution-spec-reader
SPEC_READER_ACCESS_KEY := cogito-spec-reader
SPEC_PREFIX ?= specs
PLAN_SNAPSHOTS_BUCKET := plan-snapshots
MC_IMAGE := quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
GITHUB_META_URL := https://api.github.com/meta

API_IMAGE    := cogito-api:local
WORKER_IMAGE := cogito-worker:local
BUILD_STAMP_DIR := .cache/images
API_IMAGE_STAMP := $(BUILD_STAMP_DIR)/api.local
WORKER_IMAGE_STAMP := $(BUILD_STAMP_DIR)/worker.local

# These are the files copied into each image. Changes to tests and other local
# development artifacts do not trigger an image rebuild.
API_BUILD_SOURCES := ../services/api/Dockerfile ../services/api/pyproject.toml $(shell find ../services/api/src -type f)
WORKER_BUILD_SOURCES := ../services/worker/Dockerfile ../services/worker/pyproject.toml $(shell find ../services/worker/src -type f)

# Bedrock inference-profile IDs. Override these when the assumed role has access to
# different approved models or inference profiles.
BEDROCK_DISCOVERY_MODEL ?= us.anthropic.claude-3-haiku-20240307-v1:0
BEDROCK_BALANCED_MODEL  ?= us.anthropic.claude-sonnet-5
BEDROCK_COMPLEX_MODEL   ?= us.anthropic.claude-opus-4-8
AWS_ROLE_SESSION_NAME ?= cogito-local-kind
LITELLM_BEDROCK_VALUES := \
	--set-string litellm.tiers.discovery.provider=bedrock \
	--set-string litellm.tiers.discovery.model=$(BEDROCK_DISCOVERY_MODEL) \
	--set-string litellm.tiers.balanced.provider=bedrock \
	--set-string litellm.tiers.balanced.model=$(BEDROCK_BALANCED_MODEL) \
	--set-string litellm.tiers.complex.provider=bedrock \
	--set-string litellm.tiers.complex.model=$(BEDROCK_COMPLEX_MODEL)

BASE_IMAGES := \
	bitnami/postgresql:latest \
	temporalio/server:1.31.1 \
	temporalio/admin-tools:1.31.1 \
	temporalio/ui:2.51.1 \
	quay.io/minio/minio:RELEASE.2024-12-18T13-15-44Z \
	quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z \
	ghcr.io/berriai/litellm-database:v1.90.2

LOCAL_IMAGES := $(API_IMAGE) $(WORKER_IMAGE)
IMAGES := $(BASE_IMAGES) $(LOCAL_IMAGES)

.PHONY: cluster pull build rebuild load deps secrets spec-reader install rollout up down destroy reset status port-forward help

cluster: ## Create kind cluster
	@if kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "==> Kind cluster '$(CLUSTER_NAME)' already exists"; \
	else \
		echo "==> Creating kind cluster '$(CLUSTER_NAME)'"; \
		kind create cluster --config $(KIND_CONFIG); \
	fi
	@kubectl config use-context "kind-$(CLUSTER_NAME)"

pull: ## Pull container images to local Docker
	@echo "==> Pulling container images"
	@for img in $(BASE_IMAGES); do \
		if docker image inspect "$$img" >/dev/null 2>&1; then \
			echo "  cached: $$img"; \
		else \
			echo "  pulling: $$img"; \
			docker pull "$$img"; \
		fi; \
		done

build: $(API_IMAGE_STAMP) $(WORKER_IMAGE_STAMP) ## Build local application images when their source changes

rebuild: ## Force a rebuild of both local application images
	@rm -f $(API_IMAGE_STAMP) $(WORKER_IMAGE_STAMP)
	@$(MAKE) build

$(API_IMAGE_STAMP): $(API_BUILD_SOURCES)
	@echo "==> Building $(API_IMAGE)"
	@mkdir -p $(@D)
	@docker build --tag $(API_IMAGE) ../services/api
	@touch $@

$(WORKER_IMAGE_STAMP): $(WORKER_BUILD_SOURCES)
	@echo "==> Building $(WORKER_IMAGE)"
	@mkdir -p $(@D)
	@docker build --tag $(WORKER_IMAGE) ../services/worker
	@touch $@

load: pull build ## Pull, build, and load only missing or changed images into kind nodes
	@echo "==> Loading missing or changed images into kind nodes"
	@nodes=$$(kind get nodes --name "$(CLUSTER_NAME)" 2>/dev/null); \
	if [ -z "$$nodes" ]; then \
		echo "ERR: No kind nodes found for cluster '$(CLUSTER_NAME)'" >&2; exit 1; \
	fi; \
	mkdir -p $(BUILD_STAMP_DIR); \
	set -o pipefail; \
	for img in $(IMAGES); do \
		case "$$img" in \
			*/*) registry="$${img%%/*}"; \
				if [[ "$$registry" == *.* || "$$registry" == *:* || "$$registry" == "localhost" ]]; then node_image="$$img"; else node_image="docker.io/$$img"; fi ;; \
			*) node_image="docker.io/library/$$img" ;; \
		esac; \
		image_key="$${img//[\/:@]/_}"; \
		load_stamp="$(BUILD_STAMP_DIR)/$$image_key.loaded"; \
		image_id="$$(docker image inspect "$$img" --format '{{.Id}}')"; \
		needs_load=false; \
		case "$$img" in cogito-api:local|cogito-worker:local) needs_load=true ;; esac; \
		if [ "$$(cat "$$load_stamp" 2>/dev/null)" != "$$image_id" ]; then needs_load=true; fi; \
		while IFS= read -r node; do \
			if ! docker exec "$$node" ctr --namespace=k8s.io images ls -q | grep -Fxq "$$node_image"; then needs_load=true; fi; \
		done <<< "$$nodes"; \
		if [ "$$needs_load" = false ]; then \
			echo "  cached in nodes: $$img"; \
			continue; \
		fi; \
		echo "  loading: $$img"; \
		while IFS= read -r node; do \
			echo "    node: $$node"; \
			docker save "$$img" | docker exec --privileged -i "$$node" \
				ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -; \
		done <<< "$$nodes"; \
		printf '%s' "$$image_id" > "$$load_stamp"; \
	done

deps: ## Build helm chart dependencies
	@echo "==> Building chart dependencies"
	@cd $(CHART_DIR) && helm dependency build

secrets: ## Create/update local LiteLLM credentials from an assumed AWS role
	@set -e; \
	if [ -z "$${AWS_ACCESS_KEY_ID:-}" ] || [ -z "$${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "$${AWS_SESSION_TOKEN:-}" ]; then \
		if [ -n "$${AWS_ROLE_ARN:-}" ]; then \
			read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN <<< "$$(aws sts assume-role --role-arn "$${AWS_ROLE_ARN}" --role-session-name "$(AWS_ROLE_SESSION_NAME)" --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)"; \
		else \
			aws_credentials="$$(aws configure export-credentials --format env-no-export 2>/dev/null)" || { \
				echo "ERR: No assumed-role credentials were found. Set AWS_ROLE_ARN, run your SSO/role-assumption command, or set AWS_PROFILE to its configured profile." >&2; exit 1; \
			}; \
			eval "$$aws_credentials"; \
		fi; \
	fi; \
	aws_region="$${AWS_REGION:-$${AWS_DEFAULT_REGION:-$$(aws configure get region 2>/dev/null)}}"; \
	if [ -z "$${AWS_ACCESS_KEY_ID:-}" ] || [ -z "$${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "$${AWS_SESSION_TOKEN:-}" ]; then \
		echo "ERR: The resolved AWS credentials must be an assumed-role session with AWS_SESSION_TOKEN." >&2; exit 1; \
	fi; \
	if [ -z "$$aws_region" ]; then \
		echo "ERR: Set AWS_REGION/AWS_DEFAULT_REGION or configure a profile region for Amazon Bedrock." >&2; exit 1; \
	fi; \
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f - >/dev/null; \
	master_key="$$(kubectl get secret $(LITELLM_SECRET) --namespace $(NAMESPACE) -o jsonpath='{.data.LITELLM_MASTER_KEY}' 2>/dev/null | base64 --decode 2>/dev/null)"; \
	if [ -z "$$master_key" ]; then master_key="sk-$$(openssl rand -hex 32)"; fi; \
	kubectl create secret generic $(LITELLM_SECRET) --namespace $(NAMESPACE) \
		--from-literal=LITELLM_MASTER_KEY="$$master_key" \
		--from-literal=AWS_ACCESS_KEY_ID="$${AWS_ACCESS_KEY_ID}" \
		--from-literal=AWS_SECRET_ACCESS_KEY="$${AWS_SECRET_ACCESS_KEY}" \
		--from-literal=AWS_SESSION_TOKEN="$${AWS_SESSION_TOKEN}" \
		--from-literal=AWS_REGION_NAME="$$aws_region" \
		--dry-run=client -o yaml | kubectl apply -f - >/dev/null
	@echo "==> LiteLLM configured for Amazon Bedrock using assumed-role credentials"

spec-reader: ## Create local MinIO credentials limited to reads under specs/<prefix>/
	@set -e; \
	root_user="$$(kubectl -n $(NAMESPACE) get secret $(RELEASE_NAME)-minio -o jsonpath='{.data.rootUser}' | base64 --decode)"; \
	root_password="$$(kubectl -n $(NAMESPACE) get secret $(RELEASE_NAME)-minio -o jsonpath='{.data.rootPassword}' | base64 --decode)"; \
	spec_secret="$$(kubectl -n $(EXECUTION_NAMESPACE) get secret $(SPEC_READER_SECRET) -o jsonpath='{.data.secret-key}' 2>/dev/null | base64 --decode || true)"; \
	if [ -z "$$spec_secret" ]; then spec_secret="$$(openssl rand -hex 32)"; fi; \
		policy_b64="$$(printf '%s' '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetBucketLocation"],"Resource":["arn:aws:s3:::specs"]},{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::specs/$(SPEC_PREFIX)/*"]}]}' | base64)"; \
	kubectl -n $(NAMESPACE) run cogito-spec-reader-bootstrap --rm -i --restart=Never --image=$(MC_IMAGE) \
		--env="ROOT_USER=$$root_user" --env="ROOT_PASSWORD=$$root_password" \
		--env="SPEC_ACCESS_KEY=$(SPEC_READER_ACCESS_KEY)" --env="SPEC_SECRET_KEY=$$spec_secret" --env="POLICY_B64=$$policy_b64" \
		--command -- sh -ceu '\
			mc alias set local http://$(RELEASE_NAME)-minio:9000 "$$ROOT_USER" "$$ROOT_PASSWORD"; \
			mc mb --with-lock local/$(PLAN_SNAPSHOTS_BUCKET) >/dev/null 2>&1 || true; \
			mc retention set --default compliance 30d local/$(PLAN_SNAPSHOTS_BUCKET) >/dev/null; \
			printf "%s" "$$POLICY_B64" | base64 -d >/tmp/spec-reader-policy.json; \
			mc admin policy remove local cogito-spec-reader >/dev/null 2>&1 || true; \
			mc admin policy create local cogito-spec-reader /tmp/spec-reader-policy.json; \
			mc admin user add local "$$SPEC_ACCESS_KEY" "$$SPEC_SECRET_KEY"; \
			mc admin policy attach local cogito-spec-reader --user "$$SPEC_ACCESS_KEY"; \
		'; \
	kubectl -n $(EXECUTION_NAMESPACE) create secret generic $(SPEC_READER_SECRET) \
		--from-literal=access-key="$(SPEC_READER_ACCESS_KEY)" --from-literal=secret-key="$$spec_secret" \
		--dry-run=client -o yaml | kubectl apply -f - >/dev/null
	@echo "==> Execution spec reader configured with read-only access under specs/$(SPEC_PREFIX)/"

install: secrets ## Install cogito helm chart
	@set -e; \
	github_git_egress="$$(curl --fail --silent --show-error --retry 3 $(GITHUB_META_URL) | jq -ce '[.git[] | {ipBlock: {cidr: .}}]')"; \
	if [ "$$github_git_egress" = "[]" ]; then echo "ERR: GitHub meta endpoint returned no Git CIDRs" >&2; exit 1; fi; \
	echo "==> Installing cogito with explicit GitHub Git egress CIDRs"; \
	helm upgrade --install $(RELEASE_NAME) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set temporal.server.config.namespaces.create=true \
		--set-json worker.execution.networkPolicy.gitEgress="$$github_git_egress" \
		$(LITELLM_BEDROCK_VALUES) \
		--timeout 10m \
		--wait
	@$(MAKE) spec-reader
	@$(MAKE) rollout

rollout: ## Restart local application pods so they consume newly loaded :local images
	@echo "==> Restarting local application deployments"
	@kubectl rollout restart deployment/$(RELEASE_NAME)-api deployment/$(RELEASE_NAME)-worker -n $(NAMESPACE)
	@kubectl rollout status deployment/$(RELEASE_NAME)-api -n $(NAMESPACE) --timeout=5m
	@kubectl rollout status deployment/$(RELEASE_NAME)-worker -n $(NAMESPACE) --timeout=5m

up: cluster load deps install status ## Full setup: cluster, images, chart install
	@echo ""
	@echo "==> Cogito is running. Use 'make port-forward' to access services."

down: ## Uninstall helm release (keeps cluster)
	@echo "==> Uninstalling cogito"
	@helm uninstall $(RELEASE_NAME) -n $(NAMESPACE) 2>/dev/null || true
	@kubectl delete pvc --all -n $(NAMESPACE) 2>/dev/null || true

destroy: ## Delete the kind cluster entirely
	@echo "==> Destroying kind cluster '$(CLUSTER_NAME)'"
	@kind delete cluster --name "$(CLUSTER_NAME)"

reset: down install ## Reinstall chart (keeps cluster and images)

status: ## Show cluster and pod status
	@echo ""
	@echo "--- Nodes ---"
	@kubectl get nodes 2>/dev/null || echo "  cluster not running"
	@echo ""
	@echo "--- Pods ($(NAMESPACE)) ---"
	@kubectl get pods -n $(NAMESPACE) 2>/dev/null || echo "  namespace not found"
	@echo ""
	@echo "--- Helm Releases ---"
	@helm list -n $(NAMESPACE) 2>/dev/null || true

port-forward: ## Port-forward all services for local access
	@echo "Starting port-forwards (Ctrl+C to stop)..."
	@kubectl port-forward svc/$(RELEASE_NAME)-temporal-web -n $(NAMESPACE) 8080:8080 &
	@kubectl port-forward svc/$(RELEASE_NAME)-minio-console -n $(NAMESPACE) 9001:9001 &
	@kubectl port-forward svc/$(RELEASE_NAME)-temporal-frontend -n $(NAMESPACE) 7233:7233 &
	@kubectl port-forward svc/$(RELEASE_NAME)-minio -n $(NAMESPACE) 9000:9000 &
	@kubectl port-forward svc/$(RELEASE_NAME)-litellm -n $(NAMESPACE) 4000:4000 &
	@echo ""
	@echo "  Temporal UI:   http://localhost:8080"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Temporal gRPC: localhost:7233"
	@echo "  MinIO API:     localhost:9000"
	@echo "  LiteLLM:       http://localhost:4000"
	@wait

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
