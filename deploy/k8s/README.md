# Kubernetes deployment (stretch)

The design doc marks Kubernetes as a stretch goal. The intended shape (so the
story is complete even before the manifests land):

- **One Helm chart or Kustomize overlay** for the four application services
  (`gateway`, `ingestor`, `maintenance`, `platform-api`) with resource requests
  and limits, readiness/liveness probes, and a ConfigMap + Secret for settings.
- **Ingestor** runs as a `Deployment` scaled 1→4 replicas; the README demo shows a
  consumer-group rebalance as replicas are added (partition count is 12).
- **Maintenance** runs as a `CronJob` (compaction every 15 min on hot partitions,
  snapshot expiry daily, orphan cleanup weekly).
- **Broker, MinIO, Postgres** use community Helm charts — not rewritten by hand.
- Target platform: `kind` locally, managed Kubernetes in production.

Secrets come from Kubernetes Secrets, never the repo. Containers run non-root with
pinned image versions (see `deploy/compose/Dockerfile`).

> Not yet implemented. `docker compose` (see `deploy/compose/`) is the supported
> path today.
