# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# MS23 Kind-based Deployment
# ==========================
#
# Usage:
#   make up      - Create cluster and deploy all services
#   make down    - Delete cluster (preserves images)
#   make clean   - Delete cluster and clean up all resources
#   make status  - Show cluster and service status
#   make logs    - Follow operator logs
#
.PHONY: up down clean status logs help check-deps cluster metallb minio kafscale wait-ready

# Configuration
CLUSTER_NAME    := ks-ms23
NAMESPACE       := ks-ms23-01
REGISTRY        := 192.168.0.131:5100
TAG             := dev
HELM_CHART      := ../helm/kafscale
METALLB_VERSION := v0.14.5

# Colors for output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m # No Color

help: ## Show this help
	@echo "MS23 Kind Deployment"
	@echo "===================="
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}'

check-deps: ## Check required dependencies
	@echo "$(YELLOW)Checking dependencies...$(NC)"
	@command -v kind >/dev/null 2>&1 || { echo "$(RED)kind not found. Install: brew install kind$(NC)"; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "$(RED)kubectl not found$(NC)"; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "$(RED)helm not found. Install: brew install helm$(NC)"; exit 1; }
	@echo "$(GREEN)All dependencies found$(NC)"

cluster: check-deps ## Create Kind cluster
	@echo "$(YELLOW)Creating Kind cluster '$(CLUSTER_NAME)'...$(NC)"
	@if kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "$(GREEN)Cluster '$(CLUSTER_NAME)' already exists$(NC)"; \
	else \
		kind create cluster --config kind-config.yaml; \
		echo "$(GREEN)Cluster created$(NC)"; \
	fi
	@kubectl cluster-info --context kind-$(CLUSTER_NAME)

metallb: cluster ## Install MetalLB load balancer
	@echo "$(YELLOW)Installing MetalLB...$(NC)"
	@kubectl apply -f ../kind/metallb-native.yaml
	@echo "Waiting for MetalLB controller..."
	@kubectl wait --namespace metallb-system \
		--for=condition=ready pod \
		--selector=component=controller \
		--timeout=120s || true
	@sleep 5
	@kubectl apply -f metallb-config.yaml
	@echo "$(GREEN)MetalLB configured$(NC)"

namespace: cluster ## Create namespace
	@echo "$(YELLOW)Creating namespace '$(NAMESPACE)'...$(NC)"
	@kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@echo "$(GREEN)Namespace ready$(NC)"

minio: namespace ## Deploy MinIO
	@echo "$(YELLOW)Deploying MinIO...$(NC)"
	@kubectl apply -f minio.yaml
	@echo "Waiting for MinIO..."
	@kubectl wait --namespace $(NAMESPACE) \
		--for=condition=available deployment/minio \
		--timeout=180s || true
	@echo "Waiting for bucket initialization..."
	@kubectl wait --namespace $(NAMESPACE) \
		--for=condition=complete job/minio-init \
		--timeout=120s || true
	@echo "$(GREEN)MinIO ready$(NC)"

kafscale: minio metallb ## Deploy KafScale via Helm
	@echo "$(YELLOW)Deploying KafScale...$(NC)"
	@helm upgrade --install kafscale $(HELM_CHART) \
		--namespace $(NAMESPACE) \
		--values values.yaml \
		--wait --timeout 10m
	@echo "$(GREEN)KafScale deployed$(NC)"

wait-ready: ## Wait for all pods to be ready
	@echo "$(YELLOW)Waiting for all pods...$(NC)"
	@kubectl wait --namespace $(NAMESPACE) \
		--for=condition=ready pod \
		--all \
		--timeout=300s || true
	@echo "$(GREEN)All pods ready$(NC)"

up: kafscale wait-ready ## Full deployment (cluster + all services)
	@echo ""
	@echo "$(GREEN)=== MS23 Deployment Complete ===$(NC)"
	@echo ""
	@echo "Access Points:"
	@echo "  LFS Proxy HTTP:    http://localhost:8080"
	@echo "  KafScale Console:  http://localhost:3080"
	@echo "  E72 Browser Demo:  http://localhost:3072"
	@echo "  MinIO Console:     http://localhost:9001"
	@echo "  Kafka Broker:      localhost:9092"
	@echo ""
	@echo "Credentials:"
	@echo "  MinIO:  minioadmin / minioadmin"
	@echo ""
	@echo "Commands:"
	@echo "  make status  - Show service status"
	@echo "  make logs    - Follow operator logs"
	@echo "  make down    - Delete cluster"
	@echo ""

down: ## Delete Kind cluster
	@echo "$(YELLOW)Deleting Kind cluster '$(CLUSTER_NAME)'...$(NC)"
	@kind delete cluster --name $(CLUSTER_NAME) || true
	@echo "$(GREEN)Cluster deleted$(NC)"

clean: down ## Delete cluster and clean up
	@echo "$(YELLOW)Cleaning up...$(NC)"
	@docker volume prune -f || true
	@echo "$(GREEN)Cleanup complete$(NC)"

status: ## Show cluster and service status
	@echo "$(YELLOW)=== Cluster Status ===$(NC)"
	@kubectl cluster-info --context kind-$(CLUSTER_NAME) 2>/dev/null || echo "$(RED)Cluster not running$(NC)"
	@echo ""
	@echo "$(YELLOW)=== Pods in $(NAMESPACE) ===$(NC)"
	@kubectl get pods -n $(NAMESPACE) 2>/dev/null || echo "$(RED)Namespace not found$(NC)"
	@echo ""
	@echo "$(YELLOW)=== Services ===$(NC)"
	@kubectl get svc -n $(NAMESPACE) 2>/dev/null || true

logs: ## Follow operator logs
	@kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=kafscale-operator -f --tail=100

logs-lfs: ## Follow LFS proxy logs
	@kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=kafscale-lfs-proxy -f --tail=100

logs-broker: ## Follow broker logs
	@kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=kafscale-broker -f --tail=100

test-upload: ## Test LFS upload
	@echo "$(YELLOW)Testing LFS upload...$(NC)"
	@dd if=/dev/urandom bs=1024 count=10 2>/dev/null | \
		curl -s -X POST http://localhost:8080/lfs/produce \
		-H "X-Kafka-Topic: test-uploads" \
		-H "Content-Type: application/octet-stream" \
		--data-binary @- | jq .

shell: ## Open shell in operator pod
	@kubectl exec -it -n $(NAMESPACE) \
		$$(kubectl get pod -n $(NAMESPACE) -l app.kubernetes.io/name=kafscale-operator -o jsonpath='{.items[0].metadata.name}') \
		-- /bin/sh
