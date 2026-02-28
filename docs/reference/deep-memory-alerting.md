---
summary: "Prometheus alerting for deep-memory-server (recommended rules and dashboards)"
read_when:
  - You are monitoring deep-memory-server in production
  - You want recommended alerts for errors, latency, and queue backlog
title: "Deep memory alerting"
---

# Deep memory alerting

deep-memory-server exposes Prometheus metrics at `GET /metrics`.

In production, `/metrics` should be protected (admin-only when API keys are required). If you run without API keys, metrics are intentionally disabled by default unless you set `ALLOW_UNAUTHENTICATED_METRICS=true`.

## Metrics to scrape

Scrape deep-memory-server:

- `deep_memory_http_requests_total`
- `deep_memory_http_request_duration_seconds` (histogram)
- `deep_memory_queue_pending`
- `deep_memory_queue_active`
- `deep_memory_queue_inflight_keys`

## Recommended alert rules

This repo includes a starter Prometheus rules file:

- `deploy/prometheus/deep-memory.rules.yml`

Import it into your Prometheus (or convert to a `PrometheusRule` CRD for kube-prometheus-stack).

### Suggested thresholds

You should tune thresholds to your expected load, but these defaults are usually a good starting point:

- **5xx error rate**: page at > 2% for 10m
- **p95 latency**: page at > 1s for 10m
- **queue backlog**: page at pending > 1000 for 15m

## Readiness probing

Prometheus itself does not inspect HTTP status codes from scrapes. For readiness:

- Use a blackbox exporter to probe `GET /readyz` and alert on failures (`probe_success == 0`).
- Alternatively, have your orchestrator (Compose/Kubernetes) gate traffic on readiness probes.
