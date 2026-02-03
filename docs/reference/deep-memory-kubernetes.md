---
summary: "Deploy deep-memory-server (Qdrant + Neo4j) to Kubernetes"
read_when:
  - You are deploying deep memory in Kubernetes
  - You want recommended probes, secrets, and network policies
title: "Deep memory on Kubernetes"
---

# Deep memory on Kubernetes

This repo includes a Kustomize base that deploys:

- Qdrant (StatefulSet + PVC)
- Neo4j (StatefulSet + PVC)
- deep-memory-server (Deployment + PVC for queue/audit data)

Manifests live under:

- `deploy/k8s/deep-memory`

## Apply (Kustomize)

```bash
kubectl apply -k deploy/k8s/deep-memory
```

The base uses namespace `openclaw`.

## Important: set secrets before production use

Edit these resources before applying in production:

- `deploy/k8s/deep-memory/neo4j.yaml` (`neo4j-auth` Secret password)
- `deploy/k8s/deep-memory/deep-memory-server.yaml` (`deep-memory-server-secrets`):
  - `API_KEYS_JSON` (required)
  - `QDRANT_API_KEY` (optional, if you secure Qdrant)

## Probes

deep-memory-server uses:

- readiness: `GET /readyz`
- liveness: `GET /health`

These endpoints are designed for orchestration.

## Metrics scraping

`GET /metrics` is:

- admin-only when API keys are required
- intentionally disabled by default when running without API keys, unless `ALLOW_UNAUTHENTICATED_METRICS=true`

In Kubernetes, the recommended pattern is:

1. Keep `ALLOW_UNAUTHENTICATED_METRICS=false`
2. Allow Prometheus to reach deep-memory-server via NetworkPolicy
3. Use an in-cluster scrape config that injects the required auth (if your Prometheus supports it)

If your Prometheus cannot inject `x-api-key`, you can temporarily set:

- `ALLOW_UNAUTHENTICATED_METRICS=true`

and rely on **NetworkPolicy** + cluster RBAC boundaries to restrict access to `/metrics`.

## NetworkPolicy

The base includes a minimal NetworkPolicy:

- `deploy/k8s/deep-memory/networkpolicy.yaml`

It currently allows in-namespace ingress. Tighten it to only:

- OpenClaw gateway pods
- Prometheus pods (if scraping metrics)
